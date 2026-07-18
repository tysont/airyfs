// ABOUTME: Unit tests for the CLI AiryFS archive codec (local encode + extract).
// ABOUTME: Covers binary files, nested dirs, symlinks, chunk boundaries, and traversal defenses.

import { mkdir, mkdtemp, readFile, readlink, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ArchiveError, createLocalTreeStream, extractLocalTree } from '../src/api/archive.js';

const temporaryPaths: string[] = [];

afterEach(async () => {
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

async function scratch(prefix: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), prefix));
  temporaryPaths.push(dir);
  return dir;
}

async function collect(stream: NodeJS.ReadableStream): Promise<Uint8Array> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return new Uint8Array(Buffer.concat(chunks));
}

const encoder = new TextEncoder();

function frame(obj: unknown): Uint8Array {
  const json = encoder.encode(JSON.stringify(obj));
  const out = new Uint8Array(4 + json.byteLength);
  new DataView(out.buffer).setUint32(0, json.byteLength, false);
  out.set(json, 4);
  return out;
}

const MAGIC = Uint8Array.from([...'AIRYFS'].map((c) => c.charCodeAt(0)).concat(1));
const END = new Uint8Array(4);

function concat(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function streamFromBytes(bytes: Uint8Array, chunkSize: number): ReadableStream<Uint8Array> {
  let offset = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset >= bytes.byteLength) {
        controller.close();
        return;
      }
      const end = Math.min(offset + chunkSize, bytes.byteLength);
      controller.enqueue(bytes.subarray(offset, end));
      offset = end;
    },
  });
}

describe('CLI archive codec', () => {
  async function seed(dir: string): Promise<void> {
    await mkdir(join(dir, 'nested'), { recursive: true });
    await mkdir(join(dir, 'empty'), { recursive: true });
    await writeFile(join(dir, 'a.txt'), 'hello world');
    await writeFile(join(dir, 'nested', 'bin.dat'), Buffer.from([0, 255, 1, 128, 42, 7]));
    await symlink('../a.txt', join(dir, 'nested', 'link'));
  }

  it('round-trips nested directories, binary files, and symlinks', async () => {
    const source = await scratch('airyfs-arc-src-');
    await seed(source);
    const bytes = await collect(createLocalTreeStream(source));

    const target = await scratch('airyfs-arc-dst-');
    const summary = await extractLocalTree(streamFromBytes(bytes, 64 * 1024), target);

    expect(summary).toEqual({ files: 2, directories: 2, symlinks: 1, bytes: 11 + 6 });
    expect(await readFile(join(target, 'a.txt'), 'utf8')).toBe('hello world');
    expect(new Uint8Array(await readFile(join(target, 'nested', 'bin.dat')))).toEqual(
      Uint8Array.from([0, 255, 1, 128, 42, 7]),
    );
    expect(await readlink(join(target, 'nested', 'link'))).toBe('../a.txt');
  });

  it('reassembles correctly across arbitrary stream chunk boundaries', async () => {
    const source = await scratch('airyfs-arc-src-');
    await seed(source);
    const bytes = await collect(createLocalTreeStream(source));

    for (const chunkSize of [1, 2, 3, 7]) {
      const target = await scratch('airyfs-arc-chunk-');
      const summary = await extractLocalTree(streamFromBytes(bytes, chunkSize), target);
      expect(summary.files).toBe(2);
      expect(new Uint8Array(await readFile(join(target, 'nested', 'bin.dat')))).toEqual(
        Uint8Array.from([0, 255, 1, 128, 42, 7]),
      );
    }
  });

  it('rejects absolute paths in an archive', async () => {
    const target = await scratch('airyfs-arc-abs-');
    const bytes = concat(MAGIC, frame({ t: 'f', p: '/etc/passwd', s: 0 }), END);
    await expect(extractLocalTree(streamFromBytes(bytes, 8), target)).rejects.toThrow(ArchiveError);
  });

  it('rejects parent-directory traversal', async () => {
    const target = await scratch('airyfs-arc-trav-');
    const bytes = concat(MAGIC, frame({ t: 'f', p: '../escape.txt', s: 0 }), END);
    await expect(extractLocalTree(streamFromBytes(bytes, 8), target)).rejects.toThrow(/Unsafe path/);
  });

  it('rejects a truncated archive', async () => {
    const target = await scratch('airyfs-arc-trunc-');
    const bytes = concat(
      MAGIC,
      frame({ t: 'd', p: '' }),
      frame({ t: 'f', p: 'a.txt', s: 10 }),
      Uint8Array.from([1, 2, 3]),
    );
    await expect(extractLocalTree(streamFromBytes(bytes, 4), target)).rejects.toThrow(/Truncated/);
  });

  it('rejects duplicate root records', async () => {
    const target = await scratch('airyfs-arc-root-');
    const root = frame({ t: 'd', p: '' });
    const bytes = concat(MAGIC, root, root, END);
    await expect(extractLocalTree(streamFromBytes(bytes, 4), target)).rejects.toThrow(/Duplicate/);
  });

  it('rejects a bad magic header', async () => {
    const target = await scratch('airyfs-arc-magic-');
    const bytes = concat(Uint8Array.from([9, 9, 9, 9, 9, 9, 9]), END);
    await expect(extractLocalTree(streamFromBytes(bytes, 4), target)).rejects.toThrow(/magic/i);
  });
});
