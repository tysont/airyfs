// ABOUTME: Dependency-free AiryFS tree archive codec for transactional directory push/pull.
// ABOUTME: Streams length-prefixed JSON entry headers plus raw file bytes; never buffers a whole tree.

import { Buffer } from 'buffer';
import type { FileSystem } from 'agentfs-sdk/cloudflare';

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------
//
//   MAGIC        7 bytes  ASCII "AIRYFS" + one version byte (0x01)
//   then repeated entries, each:
//     headerLen  uint32 big-endian
//                headerLen == 0  -> end of archive (stop reading)
//                headerLen  > MAX_HEADER_BYTES -> reject (unreasonable header)
//     header     headerLen bytes of UTF-8 JSON:
//                  { t: 'd' | 'f' | 'l', p: string, s?: number, l?: string }
//                  t=d directory, t=f regular file (s=size), t=l symlink (l=target)
//     body       for t=f only: exactly `s` raw bytes
//
// Paths are relative POSIX paths. The root directory is a single t=d entry with
// p === "". All other paths reject absolute prefixes, ".", "..", and empty
// segments. Directories are always emitted before their children.

const MAGIC_TEXT = 'AIRYFS';
const VERSION = 1;
export const MAGIC = Uint8Array.from([...MAGIC_TEXT].map((c) => c.charCodeAt(0)).concat(VERSION));
export const MAGIC_LENGTH = MAGIC.length;

export const MAX_HEADER_BYTES = 64 * 1024;
export const MAX_FILE_SIZE = 2 ** 50; // 1 PiB sanity cap; rejects unsafe sizes
const READ_CHUNK_SIZE = 256 * 1024;

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

/** Validate a relative POSIX archive path. The empty string denotes the root directory. */
export function validateRelativePath(path: unknown): string {
  if (typeof path !== 'string') throw new ArchiveError('Entry path must be a string');
  if (path === '') return '';
  if (path.startsWith('/')) throw new ArchiveError(`Absolute path not allowed: ${path}`);
  if (path.includes('\0')) throw new ArchiveError('Path contains a null byte');
  const segments = path.split('/');
  for (const segment of segments) {
    if (segment === '' || segment === '.' || segment === '..') {
      throw new ArchiveError(`Unsafe path segment in: ${path}`);
    }
  }
  return segments.join('/');
}

/** Tracks entry types to reject duplicate and directory/file-conflicting paths. */
export class PathRegistry {
  private readonly seen = new Map<string, EntryType>();

  add(path: string, type: EntryType): void {
    if (this.seen.has(path)) throw new ArchiveError(`Duplicate path: ${path}`);
    const parts = path.split('/');
    for (let i = 1; i < parts.length; i++) {
      const ancestor = parts.slice(0, i).join('/');
      const ancestorType = this.seen.get(ancestor);
      if (ancestorType !== undefined && ancestorType !== 'd') {
        throw new ArchiveError(`Path conflicts with a non-directory ancestor: ${path}`);
      }
    }
    this.seen.set(path, type);
  }
}

// ---------------------------------------------------------------------------
// Encoding
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

function endMarker(): Uint8Array {
  return new Uint8Array(4); // uint32 zero
}

// ---------------------------------------------------------------------------
// Reusable bounded frame helpers
// ---------------------------------------------------------------------------
//
// These expose the wire-format primitives so an alternate producer (for
// example the snapshot exporter, which reads raw SQL rows rather than a live
// FileSystem) can emit a byte-identical AIRYFS stream without duplicating the
// framing, bounds checks, or path validation. Every path is validated with
// {@link validateRelativePath}, and headers are bounded by MAX_HEADER_BYTES.

/** Leading magic + version bytes that open every archive stream. */
export function archiveMagic(): Uint8Array {
  return MAGIC;
}

/** Encode the root directory record. The root is always path "". */
export function encodeRootEntry(): Uint8Array {
  return encodeHeader({ t: 'd', p: '' });
}

/** Encode a directory entry frame for a validated relative path. */
export function encodeDirectoryEntry(path: string): Uint8Array {
  return encodeHeader({ t: 'd', p: validateRelativePath(path) });
}

/** Encode a regular-file entry frame; exactly `size` body bytes must follow. */
export function encodeFileEntry(path: string, size: number): Uint8Array {
  const rel = validateRelativePath(path);
  if (rel === '') throw new ArchiveError('The root cannot be a file entry');
  if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 0 || size > MAX_FILE_SIZE) {
    throw new ArchiveError(`Unsafe or invalid file size: ${String(size)}`);
  }
  return encodeHeader({ t: 'f', p: rel, s: size });
}

/** Encode a symlink entry frame carrying its target. */
export function encodeSymlinkEntry(path: string, target: string): Uint8Array {
  const rel = validateRelativePath(path);
  if (rel === '') throw new ArchiveError('The root cannot be a symlink entry');
  if (typeof target !== 'string') throw new ArchiveError('Symlink target must be a string');
  return encodeHeader({ t: 'l', p: rel, l: target });
}

/** Trailing end-of-archive marker (a uint32 zero length). */
export function archiveEndMarker(): Uint8Array {
  return endMarker();
}

function joinAbsolute(dir: string, name: string): string {
  return dir === '/' ? `/${name}` : `${dir}/${name}`;
}

/**
 * Produce the archive byte stream for a directory subtree. Callers must hold a
 * read lock covering `root` for the full lifetime of the returned iterator so
 * the stream is a point-in-time-consistent snapshot.
 */
export async function* encodeTree(fs: FileSystem, root: string): AsyncGenerator<Uint8Array> {
  const stats = await fs.stat(root);
  if (!stats.isDirectory()) {
    throw new ArchiveError(`Export target is not a directory: ${root}`);
  }
  yield MAGIC;
  yield encodeHeader({ t: 'd', p: '' }); // root directory record
  yield* walk(fs, root, '');
  yield endMarker();
}

async function* walk(fs: FileSystem, dirAbs: string, prefix: string): AsyncGenerator<Uint8Array> {
  const entries = (await fs.readdirPlus(dirAbs)).slice().sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  for (const entry of entries) {
    const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
    const abs = joinAbsolute(dirAbs, entry.name);
    if (entry.stats.isDirectory()) {
      yield encodeHeader({ t: 'd', p: rel });
      yield* walk(fs, abs, rel);
    } else if (entry.stats.isSymbolicLink()) {
      const target = await fs.readlink(abs);
      yield encodeHeader({ t: 'l', p: rel, l: target });
    } else if (entry.stats.isFile()) {
      yield encodeHeader({ t: 'f', p: rel, s: entry.stats.size });
      yield* fileBytes(fs, abs, entry.stats.size);
    }
    // Other node types (devices, fifos) are skipped intentionally.
  }
}

async function* fileBytes(fs: FileSystem, path: string, size: number): AsyncGenerator<Uint8Array> {
  if (size === 0) return;
  const handle = await fs.open(path);
  let offset = 0;
  while (offset < size) {
    const chunk = await handle.pread(offset, Math.min(READ_CHUNK_SIZE, size - offset));
    if (chunk.byteLength === 0) {
      throw new ArchiveError(`File shrank while it was being archived: ${path}`);
    }
    offset += chunk.byteLength;
    yield new Uint8Array(chunk);
  }
}

/** Wrap {@link encodeTree} as a ReadableStream, releasing `release` on completion or cancel. */
export function encodeTreeStream(
  fs: FileSystem,
  root: string,
  release: () => void,
): ReadableStream<Uint8Array> {
  const iterator = encodeTree(fs, root)[Symbol.asyncIterator]();
  let released = false;
  const releaseOnce = (): void => {
    if (released) return;
    released = true;
    release();
  };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await iterator.next();
        if (done) {
          controller.close();
          releaseOnce();
          return;
        }
        controller.enqueue(value);
      } catch (error) {
        controller.error(error);
        releaseOnce();
      }
    },
    async cancel() {
      await iterator.return?.(undefined).catch(() => undefined);
      releaseOnce();
    },
  });
}

// ---------------------------------------------------------------------------
// Streaming decode
// ---------------------------------------------------------------------------

/** Buffered reader over a byte stream that supports exact and partial reads. */
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

  /** True once the stream is fully consumed with no buffered bytes remaining. */
  async atEnd(): Promise<boolean> {
    while (this.buffer.byteLength === 0 && !this.done) {
      await this.fill();
    }
    return this.buffer.byteLength === 0 && this.done;
  }

  /** Read exactly `n` bytes or throw if the stream is truncated. */
  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.byteLength < n) {
      const more = await this.fill();
      if (!more) throw new ArchiveError('Truncated archive: unexpected end of stream');
    }
    const out = this.buffer.subarray(0, n);
    this.buffer = this.buffer.subarray(n);
    return out;
  }

  /** Read up to `max` bytes, pulling from the stream when the buffer is empty. */
  async readSome(max: number): Promise<Uint8Array> {
    while (this.buffer.byteLength === 0) {
      const more = await this.fill();
      if (!more) throw new ArchiveError('Truncated archive: unexpected end of stream');
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
  if (length > MAX_HEADER_BYTES) {
    throw new ArchiveError('Entry header exceeds the maximum header length');
  }
  const bytes = await reader.readExact(length);
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(bytes));
  } catch {
    throw new ArchiveError('Malformed entry header JSON');
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new ArchiveError('Malformed entry header');
  }
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

/**
 * Extract an archive stream into an existing directory `targetDir`. Creates
 * directories, files, and symlinks; streams file bodies in chunks. Returns a
 * summary of what was written. Throws {@link ArchiveError} on malformed input.
 */
export async function extractTree(
  fs: FileSystem,
  targetDir: string,
  stream: ReadableStream<Uint8Array>,
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
      const abs = rel === '' ? targetDir : `${targetDir}/${rel}`;

      if (header.t === 'd') {
        if (rel !== '') {
          await fs.mkdir(abs);
          summary.directories++;
        }
      } else if (header.t === 'l') {
        if (typeof header.l !== 'string') throw new ArchiveError('Symlink entry missing target');
        await fs.symlink(header.l, abs);
        summary.symlinks++;
      } else {
        const size = validateSize(header.s);
        await writeFileBody(fs, abs, reader, size);
        summary.files++;
        summary.bytes += size;
      }
    }

    if (!(await reader.atEnd())) {
      throw new ArchiveError('Trailing bytes after archive end marker');
    }
    return summary;
  } catch (error) {
    await reader.cancel(error);
    throw error;
  }
}

async function writeFileBody(
  fs: FileSystem,
  path: string,
  reader: ByteStreamReader,
  size: number,
): Promise<void> {
  await fs.writeFile(path, Buffer.alloc(0));
  if (size === 0) return;
  const handle = await fs.open(path);
  let remaining = size;
  let offset = 0;
  while (remaining > 0) {
    const chunk = await reader.readSome(remaining);
    await handle.pwrite(offset, Buffer.from(chunk));
    offset += chunk.byteLength;
    remaining -= chunk.byteLength;
  }
  await handle.fsync();
}
