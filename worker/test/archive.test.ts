// ABOUTME: Tests for the dependency-free AiryFS tree archive codec.
// ABOUTME: Covers round-trips, binary/symlink fidelity, chunk boundaries, and malformed input.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import {
  ArchiveError,
  encodeTree,
  encodeTreeStream,
  extractTree,
  MAX_HEADER_BYTES,
  MAX_FILE_SIZE,
} from '../src/archive';
import { createTestStorage } from './support/storage';

const textEncoder = new TextEncoder();

function frame(obj: unknown): Uint8Array {
  const json = textEncoder.encode(JSON.stringify(obj));
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

async function collect(iterator: AsyncGenerator<Uint8Array> | ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  if (iterator instanceof ReadableStream) {
    const reader = iterator.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  } else {
    for await (const chunk of iterator) chunks.push(chunk);
  }
  return concat(...chunks);
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

describe('archive codec', () => {
  let fs: AgentFS;

  beforeEach(() => {
    fs = AgentFS.create(createTestStorage(new Database(':memory:')));
  });

  async function seedTree(): Promise<void> {
    await fs.mkdir('/src');
    await fs.mkdir('/src/nested');
    await fs.mkdir('/src/empty');
    await fs.writeFile('/src/a.txt', Buffer.from('hello world'));
    await fs.writeFile('/src/nested/bin.dat', Buffer.from([0, 255, 1, 128, 42, 7]));
    await fs.symlink('../a.txt', '/src/nested/link');
  }

  it('round-trips nested directories, binary files, and symlinks', async () => {
    await seedTree();
    const bytes = await collect(encodeTree(fs, '/src'));

    await fs.mkdir('/dst');
    const summary = await extractTree(fs, '/dst', streamFromBytes(bytes, 64 * 1024));

    expect(summary).toEqual({ files: 2, directories: 2, symlinks: 1, bytes: 11 + 6 });
    expect(await fs.readFile('/dst/a.txt', 'utf8')).toBe('hello world');
    expect(new Uint8Array(await fs.readFile('/dst/nested/bin.dat'))).toEqual(
      Uint8Array.from([0, 255, 1, 128, 42, 7]),
    );
    expect(await fs.readlink('/dst/nested/link')).toBe('../a.txt');
    expect((await fs.stat('/dst/empty')).isDirectory()).toBe(true);
  });

  it('reassembles correctly across arbitrary stream chunk boundaries', async () => {
    await seedTree();
    const bytes = await collect(encodeTree(fs, '/src'));

    for (const chunkSize of [1, 2, 3, 5, 7, 13]) {
      await fs.rm('/out', { recursive: true, force: true }).catch(() => undefined);
      await fs.mkdir('/out');
      const summary = await extractTree(fs, '/out', streamFromBytes(bytes, chunkSize));
      expect(summary.files).toBe(2);
      expect(new Uint8Array(await fs.readFile('/out/nested/bin.dat'))).toEqual(
        Uint8Array.from([0, 255, 1, 128, 42, 7]),
      );
    }
  });

  it('rejects exporting a non-directory', async () => {
    await fs.writeFile('/file.txt', Buffer.from('x'));
    await expect(collect(encodeTree(fs, '/file.txt'))).rejects.toThrow(ArchiveError);
  });

  it('releases the lock when the export stream completes', async () => {
    await seedTree();
    let released = false;
    const stream = encodeTreeStream(fs, '/src', () => { released = true; });
    await collect(stream);
    expect(released).toBe(true);
  });

  it('releases the lock when the export stream is cancelled', async () => {
    await seedTree();
    let released = false;
    const stream = encodeTreeStream(fs, '/src', () => { released = true; });
    const reader = stream.getReader();
    await reader.read();
    await reader.cancel();
    expect(released).toBe(true);
  });

  describe('malformed input', () => {
    beforeEach(async () => {
      await fs.mkdir('/out');
    });

    const extract = (bytes: Uint8Array) => extractTree(fs, '/out', streamFromBytes(bytes, 8));
    const root = frame({ t: 'd', p: '' });

    it('rejects a bad magic header', async () => {
      const bytes = concat(Uint8Array.from([1, 2, 3, 4, 5, 6, 7]), END);
      await expect(extract(bytes)).rejects.toThrow(/magic/i);
    });

    it('rejects absolute paths', async () => {
      const bytes = concat(MAGIC, frame({ t: 'f', p: '/etc/passwd', s: 0 }), END);
      await expect(extract(bytes)).rejects.toThrow(/Absolute path/);
    });

    it('rejects parent-directory traversal', async () => {
      const bytes = concat(MAGIC, frame({ t: 'd', p: '../evil' }), END);
      await expect(extract(bytes)).rejects.toThrow(/Unsafe path/);
    });

    it('rejects duplicate paths', async () => {
      const bytes = concat(
        MAGIC,
        root,
        frame({ t: 'd', p: 'a' }),
        frame({ t: 'd', p: 'a' }),
        END,
      );
      await expect(extract(bytes)).rejects.toThrow(/Duplicate/);
    });

    it('rejects a file path nested under an existing file', async () => {
      const bytes = concat(
        MAGIC,
        root,
        frame({ t: 'f', p: 'a', s: 0 }),
        frame({ t: 'f', p: 'a/b', s: 0 }),
        END,
      );
      await expect(extract(bytes)).rejects.toThrow(/non-directory ancestor/);
    });

    it('rejects a truncated file body', async () => {
      const bytes = concat(
        MAGIC,
        root,
        frame({ t: 'f', p: 'a.txt', s: 10 }),
        Uint8Array.from([1, 2, 3]),
      );
      await expect(extract(bytes)).rejects.toThrow(/Truncated/);
    });

    it('rejects unreasonable header lengths', async () => {
      const header = new Uint8Array(4);
      new DataView(header.buffer).setUint32(0, MAX_HEADER_BYTES + 1, false);
      const bytes = concat(MAGIC, header);
      await expect(extract(bytes)).rejects.toThrow(/maximum header length/);
    });

    it('rejects unsafe file sizes', async () => {
      const bytes = concat(MAGIC, root, frame({ t: 'f', p: 'a', s: MAX_FILE_SIZE + 1 }), END);
      await expect(extract(bytes)).rejects.toThrow(/Unsafe or invalid file size/);
    });

    it('rejects trailing bytes after the end marker', async () => {
      const bytes = concat(MAGIC, root, frame({ t: 'd', p: 'a' }), END, Uint8Array.from([9, 9]));
      await expect(extract(bytes)).rejects.toThrow(/Trailing bytes/);
    });

    it('rejects duplicate root records', async () => {
      const bytes = concat(MAGIC, root, root, END);
      await expect(extract(bytes)).rejects.toThrow(/Duplicate/);
    });

    it('rejects an unknown entry type', async () => {
      const bytes = concat(MAGIC, frame({ t: 'x', p: 'a' }), END);
      await expect(extract(bytes)).rejects.toThrow(/Unknown entry type/);
    });
  });
});
