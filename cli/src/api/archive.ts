// ABOUTME: Dependency-free AiryFS tree archive codec for the CLI (push encode, pull extract).
// ABOUTME: Mirrors the Worker wire format; duplicated deliberately to avoid cross-package imports.

import { createReadStream, createWriteStream } from 'node:fs';
import { lstat, mkdir, readdir, readlink, symlink } from 'node:fs/promises';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

const MAGIC_TEXT = 'AIRYFS';
const VERSION = 1;
const MAGIC = Uint8Array.from([...MAGIC_TEXT].map((c) => c.charCodeAt(0)).concat(VERSION));
const MAGIC_LENGTH = MAGIC.length;

const MAX_HEADER_BYTES = 64 * 1024;
const MAX_FILE_SIZE = 2 ** 50;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface TreeSummary {
  files: number;
  directories: number;
  symlinks: number;
  bytes: number;
}

export class ArchiveError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ArchiveError';
  }
}

type EntryType = 'd' | 'f' | 'l';
interface EntryHeader {
  t: EntryType;
  p: string;
  s?: number;
  l?: string;
}

// ---------------------------------------------------------------------------
// Path validation
// ---------------------------------------------------------------------------

function validateRelativePath(p: unknown): string {
  if (typeof p !== 'string') throw new ArchiveError('Entry path must be a string');
  if (p === '') return '';
  if (p.startsWith('/')) throw new ArchiveError(`Absolute path not allowed: ${p}`);
  if (p.includes('\0')) throw new ArchiveError('Path contains a null byte');
  for (const segment of p.split('/')) {
    if (segment === '' || segment === '.' || segment === '..') {
      throw new ArchiveError(`Unsafe path segment in: ${p}`);
    }
  }
  return p;
}

class PathRegistry {
  private readonly seen = new Map<string, EntryType>();

  add(p: string, type: EntryType): void {
    if (this.seen.has(p)) throw new ArchiveError(`Duplicate path: ${p}`);
    const parts = p.split('/');
    for (let i = 1; i < parts.length; i++) {
      const ancestorType = this.seen.get(parts.slice(0, i).join('/'));
      if (ancestorType !== undefined && ancestorType !== 'd') {
        throw new ArchiveError(`Path conflicts with a non-directory ancestor: ${p}`);
      }
    }
    this.seen.set(p, type);
  }
}

// ---------------------------------------------------------------------------
// Encoding a local directory
// ---------------------------------------------------------------------------

function encodeHeader(header: EntryHeader): Uint8Array {
  const json = encoder.encode(JSON.stringify(header));
  if (json.byteLength > MAX_HEADER_BYTES) {
    throw new ArchiveError('Entry header exceeds the maximum header length');
  }
  const frame = new Uint8Array(4 + json.byteLength);
  new DataView(frame.buffer).setUint32(0, json.byteLength, false);
  frame.set(json, 4);
  return frame;
}

async function* encodeLocalTree(root: string): AsyncGenerator<Uint8Array> {
  yield MAGIC;
  yield encodeHeader({ t: 'd', p: '' });
  yield* walkLocal(root, '');
  yield new Uint8Array(4); // end marker
}

async function* walkLocal(dirAbs: string, prefix: string): AsyncGenerator<Uint8Array> {
  const entries = (await readdir(dirAbs)).slice().sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  for (const name of entries) {
    const rel = prefix ? `${prefix}/${name}` : name;
    const abs = path.join(dirAbs, name);
    const stats = await lstat(abs);
    if (stats.isDirectory()) {
      yield encodeHeader({ t: 'd', p: rel });
      yield* walkLocal(abs, rel);
    } else if (stats.isSymbolicLink()) {
      const target = await readlink(abs);
      yield encodeHeader({ t: 'l', p: rel, l: target });
    } else if (stats.isFile()) {
      yield encodeHeader({ t: 'f', p: rel, s: stats.size });
      for await (const chunk of createReadStream(abs)) {
        yield chunk as Uint8Array;
      }
    }
    // Other node types are skipped.
  }
}

/** Build a Node Readable that emits the AiryFS archive for `root`, suitable as a fetch body. */
export function createLocalTreeStream(root: string): Readable {
  return Readable.from(encodeLocalTree(root));
}

// ---------------------------------------------------------------------------
// Streaming decode into a local directory
// ---------------------------------------------------------------------------

class ByteStreamReader {
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
  private buffer: Uint8Array = new Uint8Array(0);
  private done = false;

  constructor(stream: ReadableStream<Uint8Array>) {
    this.reader = stream.getReader();
  }

  private async fill(): Promise<boolean> {
    if (this.done) return false;
    const { done, value } = await this.reader.read();
    if (done) {
      this.done = true;
      return false;
    }
    if (value && value.byteLength > 0) {
      if (this.buffer.byteLength === 0) {
        this.buffer = value;
      } else {
        const merged = new Uint8Array(this.buffer.byteLength + value.byteLength);
        merged.set(this.buffer, 0);
        merged.set(value, this.buffer.byteLength);
        this.buffer = merged;
      }
    }
    return true;
  }

  async atEnd(): Promise<boolean> {
    while (this.buffer.byteLength === 0 && !this.done) await this.fill();
    return this.buffer.byteLength === 0 && this.done;
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.byteLength < n) {
      if (!(await this.fill())) throw new ArchiveError('Truncated archive: unexpected end of stream');
    }
    const out = this.buffer.subarray(0, n);
    this.buffer = this.buffer.subarray(n);
    return out;
  }

  async readSome(max: number): Promise<Uint8Array> {
    while (this.buffer.byteLength === 0) {
      if (!(await this.fill())) throw new ArchiveError('Truncated archive: unexpected end of stream');
    }
    const take = Math.min(max, this.buffer.byteLength);
    const out = this.buffer.subarray(0, take);
    this.buffer = this.buffer.subarray(take);
    return out;
  }

  async cancel(reason?: unknown): Promise<void> {
    await this.reader.cancel(reason).catch(() => undefined);
  }
}

async function readHeader(reader: ByteStreamReader): Promise<EntryHeader | null> {
  const lengthBytes = await reader.readExact(4);
  const length = new DataView(lengthBytes.buffer, lengthBytes.byteOffset, 4).getUint32(0, false);
  if (length === 0) return null;
  if (length > MAX_HEADER_BYTES) throw new ArchiveError('Entry header exceeds the maximum header length');
  const bytes = await reader.readExact(length);
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(bytes));
  } catch {
    throw new ArchiveError('Malformed entry header JSON');
  }
  if (typeof parsed !== 'object' || parsed === null) throw new ArchiveError('Malformed entry header');
  const record = parsed as Record<string, unknown>;
  if (record.t !== 'd' && record.t !== 'f' && record.t !== 'l') {
    throw new ArchiveError(`Unknown entry type: ${String(record.t)}`);
  }
  return record as unknown as EntryHeader;
}

function validateSize(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > MAX_FILE_SIZE) {
    throw new ArchiveError(`Unsafe or invalid file size: ${String(value)}`);
  }
  return value;
}

/** Resolve a validated relative path under `targetDir`, refusing any escape. */
function safeJoin(targetDir: string, rel: string): string {
  const resolved = path.resolve(targetDir, rel);
  const relative = path.relative(targetDir, resolved);
  if (relative !== '' && (relative.startsWith('..') || path.isAbsolute(relative))) {
    throw new ArchiveError(`Archive path escapes the extraction directory: ${rel}`);
  }
  return resolved;
}

/**
 * Extract an archive `stream` into an existing directory `targetDir`. Streams
 * file bodies in chunks, preserves symlink targets verbatim, and refuses any
 * entry whose path would escape `targetDir`.
 */
export async function extractLocalTree(
  stream: ReadableStream<Uint8Array>,
  targetDir: string,
): Promise<TreeSummary> {
  const reader = new ByteStreamReader(stream);
  const summary: TreeSummary = { files: 0, directories: 0, symlinks: 0, bytes: 0 };
  const registry = new PathRegistry();
  let firstEntry = true;
  try {
    const magic = await reader.readExact(MAGIC_LENGTH);
    for (let i = 0; i < MAGIC_LENGTH; i++) {
      if (magic[i] !== MAGIC[i]) throw new ArchiveError('Not a AiryFS archive (bad magic or version)');
    }

    while (true) {
      const header = await readHeader(reader);
      if (header === null) break;
      const rel = validateRelativePath(header.p);
      if (firstEntry && (rel !== '' || header.t !== 'd')) {
        throw new ArchiveError('The first archive entry must be the root directory');
      }
      firstEntry = false;
      registry.add(rel, header.t);
      const abs = safeJoin(targetDir, rel);

      if (header.t === 'd') {
        await mkdir(abs, { recursive: true });
        if (rel !== '') summary.directories++;
      } else if (header.t === 'l') {
        if (typeof header.l !== 'string') throw new ArchiveError('Symlink entry missing target');
        await symlink(header.l, abs);
        summary.symlinks++;
      } else {
        const size = validateSize(header.s);
        await writeLocalFile(abs, reader, size);
        summary.files++;
        summary.bytes += size;
      }
    }

    if (!(await reader.atEnd())) throw new ArchiveError('Trailing bytes after archive end marker');
    return summary;
  } catch (error) {
    await reader.cancel(error);
    throw error;
  }
}

async function writeLocalFile(abs: string, reader: ByteStreamReader, size: number): Promise<void> {
  let remaining = size;
  async function* body(): AsyncGenerator<Buffer> {
    while (remaining > 0) {
      const chunk = await reader.readSome(remaining);
      remaining -= chunk.byteLength;
      yield Buffer.from(chunk);
    }
  }
  // 'wx' refuses to clobber an existing file; the caller extracts into a fresh
  // temporary directory, so a collision here signals a malformed archive.
  await pipeline(body(), createWriteStream(abs, { flags: 'wx' }));
}
