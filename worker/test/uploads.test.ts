// ABOUTME: Tests for resumable checksummed uploads and the direct sha256Path checksum helper.
// ABOUTME: Runs AgentFS + raw fs_upload SQL against in-memory SQLite via the shared test storage.

import { Buffer } from 'buffer';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { AgentFS, type CloudflareStorage } from 'agentfs-sdk/cloudflare';
import { initSchema } from '../src/schema';
import { HttpError, VolumeAccessCoordinator, parseV1Route } from '../src/files-api';
import { sha256Path } from '../src/checksum';
import {
  UPLOAD_TABLES,
  abortUpload,
  appendUpload,
  beginUpload,
  completeUpload,
  getUpload,
  readBoundedChunk,
  reapStaleUploads,
} from '../src/uploads';
import { createTestStorage } from './support/storage';

function sha256(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex');
}

function bytes(length: number, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) out[i] = (i * 31 + seed) & 0xff;
  return out;
}

function streamOf(data: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

async function expectHttpError(promise: Promise<unknown>, status: number, code: string): Promise<HttpError> {
  try {
    await promise;
  } catch (error) {
    expect(error).toBeInstanceOf(HttpError);
    const httpError = error as HttpError;
    expect(httpError.status).toBe(status);
    expect(httpError.code).toBe(code);
    return httpError;
  }
  throw new Error(`Expected an HttpError ${status} ${code} but the call resolved`);
}

describe('resumable uploads', () => {
  let db: Database.Database;
  let storage: CloudflareStorage;
  let fs: AgentFS;
  let access: VolumeAccessCoordinator;

  beforeEach(async () => {
    db = new Database(':memory:');
    storage = createTestStorage(db);
    initSchema(storage.sql as never, (cb) => storage.transactionSync(cb));
    fs = AgentFS.create(storage);
    access = new VolumeAccessCoordinator();
    await fs.mkdir('/data');
  });

  const sql = () => storage.sql as never;

  it('routes the uploads resource with a target path', () => {
    expect(parseV1Route('/v1/volumes/vol/uploads/data/big.bin')).toEqual({
      volume: 'vol', resource: 'uploads', path: '/data/big.bin',
    });
  });

  it('creates the additive fs_upload table', () => {
    const names = (db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as { name: string }[])
      .map((t) => t.name);
    for (const table of UPLOAD_TABLES) expect(names).toContain(table);
  });

  it('persists a session that survives a reconstructed filesystem/coordinator', async () => {
    const data = bytes(700_000, 3);
    const begin = await beginUpload(fs, sql(), access, '/data/f.bin', {
      size: data.byteLength, checksum: sha256(data),
    });
    expect(begin.created).toBe(true);
    expect(begin.session.offset).toBe(0);

    // Simulate a DO/Container restart: fresh AgentFS + coordinator, same DB.
    const fs2 = AgentFS.create(storage);
    const access2 = new VolumeAccessCoordinator();
    const status = getUpload(sql(), '/data/f.bin');
    expect(status.id).toBe(begin.session.id);
    expect(status.size).toBe(data.byteLength);

    await appendUpload(fs2, sql(), access2, '/data/f.bin', {
      offset: 0, chunkSha256: sha256(data), data,
    });
    const result = await completeUpload(fs2, sql(), access2, '/data/f.bin');
    expect(result.checksum).toBe(sha256(data));
    expect(new Uint8Array(await fs2.readFile('/data/f.bin'))).toEqual(data);
  });

  it('returns the same session on an idempotent resume and 409s a conflict', async () => {
    const first = await beginUpload(fs, sql(), access, '/data/f.bin', { size: 10, checksum: 'a'.repeat(64) });
    const second = await beginUpload(fs, sql(), access, '/data/f.bin', { size: 10, checksum: 'a'.repeat(64) });
    expect(second.created).toBe(false);
    expect(second.session.id).toBe(first.session.id);

    await expectHttpError(
      beginUpload(fs, sql(), access, '/data/f.bin', { size: 11, checksum: 'a'.repeat(64) }),
      409, 'UPLOAD_CONFLICT',
    );
    await expectHttpError(
      beginUpload(fs, sql(), access, '/data/f.bin', { size: 10, checksum: 'b'.repeat(64) }),
      409, 'UPLOAD_CONFLICT',
    );
  });

  it('validates the target path, size, and checksum', async () => {
    await expectHttpError(beginUpload(fs, sql(), access, '/', { size: 1, checksum: 'a'.repeat(64) }), 400, 'INVALID_PATH');
    await expectHttpError(beginUpload(fs, sql(), access, '/data/', { size: 1, checksum: 'a'.repeat(64) }), 400, 'INVALID_PATH');
    await expectHttpError(beginUpload(fs, sql(), access, '/data/f', { size: -1, checksum: 'a'.repeat(64) }), 400, 'INVALID_ARGUMENT');
    await expectHttpError(beginUpload(fs, sql(), access, '/data/f', { size: 1, checksum: 'A'.repeat(64) }), 400, 'INVALID_ARGUMENT');
    await expectHttpError(beginUpload(fs, sql(), access, '/data/f', { size: 1, checksum: 'abc' }), 400, 'INVALID_ARGUMENT');
  });

  it('canonicalizes dot segments before persisting the target', async () => {
    const result = await beginUpload(fs, sql(), access, '/data/sub/../f.bin', {
      size: 0,
      checksum: sha256(new Uint8Array()),
    });
    expect(result.session.path).toBe('/data/f.bin');
    expect(getUpload(sql(), '/data/./f.bin').id).toBe(result.session.id);
  });

  it('validates the parent directory', async () => {
    await expect(
      beginUpload(fs, sql(), access, '/missing/f.bin', { size: 1, checksum: 'a'.repeat(64) }),
    ).rejects.toMatchObject({ code: 'ENOENT' });
    await fs.writeFile('/data/afile', Buffer.from('x'));
    await expect(
      beginUpload(fs, sql(), access, '/data/afile/child', { size: 1, checksum: 'a'.repeat(64) }),
    ).rejects.toMatchObject({ code: 'ENOTDIR' });
  });

  it('appends chunks, advancing the offset with per-chunk checksums', async () => {
    const a = bytes(100, 1);
    const b = bytes(50, 2);
    const full = new Uint8Array([...a, ...b]);
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 150, checksum: sha256(full) });

    const s1 = await appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: sha256(a), data: a });
    expect(s1.offset).toBe(100);
    const s2 = await appendUpload(fs, sql(), access, '/data/f.bin', { offset: 100, chunkSha256: sha256(b), data: b });
    expect(s2.offset).toBe(150);
  });

  it('returns a stable offset mismatch when re-sending an accepted offset', async () => {
    const a = bytes(100, 1);
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 200, checksum: 'a'.repeat(64) });
    await appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: sha256(a), data: a });

    const error = await expectHttpError(
      appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: sha256(a), data: a }),
      409, 'UPLOAD_OFFSET_MISMATCH',
    );
    expect(new Headers(error.headers).get('Upload-Offset')).toBe('100');
    // Bytes were not duplicated: still at offset 100.
    expect(getUpload(sql(), '/data/f.bin').offset).toBe(100);
  });

  it('rejects a chunk with a mismatched checksum without advancing', async () => {
    const a = bytes(100, 1);
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 100, checksum: 'a'.repeat(64) });
    await expectHttpError(
      appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: 'a'.repeat(64), data: a }),
      400, 'CHUNK_CHECKSUM_MISMATCH',
    );
    expect(getUpload(sql(), '/data/f.bin').offset).toBe(0);
  });

  it('rejects an oversized chunk and one that overflows the declared size', async () => {
    const big = bytes(1024 * 1024 + 1);
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 5_000_000, checksum: 'a'.repeat(64) });
    await expectHttpError(
      appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: sha256(big), data: big }),
      413, 'CHUNK_TOO_LARGE',
    );

    await beginUpload(fs, sql(), access, '/data/small.bin', { size: 10, checksum: 'a'.repeat(64) });
    const over = bytes(20, 4);
    await expectHttpError(
      appendUpload(fs, sql(), access, '/data/small.bin', { offset: 0, chunkSha256: sha256(over), data: over }),
      400, 'UPLOAD_OVERFLOW',
    );
  });

  it('refuses to complete before all bytes arrive', async () => {
    const a = bytes(100, 1);
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 200, checksum: 'a'.repeat(64) });
    await appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: sha256(a), data: a });
    await expectHttpError(completeUpload(fs, sql(), access, '/data/f.bin'), 409, 'UPLOAD_INCOMPLETE');
  });

  it('retains a resumable session when the final checksum mismatches', async () => {
    const a = bytes(120, 9);
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 120, checksum: 'a'.repeat(64) });
    await appendUpload(fs, sql(), access, '/data/f.bin', { offset: 0, chunkSha256: sha256(a), data: a });
    await expectHttpError(completeUpload(fs, sql(), access, '/data/f.bin'), 409, 'UPLOAD_CHECKSUM_MISMATCH');

    // Session and temp survive; target was not created.
    expect(getUpload(sql(), '/data/f.bin').offset).toBe(120);
    await expect(fs.stat('/data/f.bin')).rejects.toBeTruthy();
    const temps = (await fs.readdir('/data')).filter((n) => n.startsWith('.airyfs-upload-'));
    expect(temps.length).toBe(1);
  });

  it('atomically publishes a completed upload and cleans up the session', async () => {
    const data = bytes(1_500_000, 7);
    await beginUpload(fs, sql(), access, '/data/big.bin', { size: data.byteLength, checksum: sha256(data) });
    let offset = 0;
    const chunk = 1024 * 1024;
    while (offset < data.byteLength) {
      const slice = data.subarray(offset, Math.min(offset + chunk, data.byteLength));
      const status = await appendUpload(fs, sql(), access, '/data/big.bin', {
        offset, chunkSha256: sha256(slice), data: slice,
      });
      offset = status.offset;
    }
    const result = await completeUpload(fs, sql(), access, '/data/big.bin');
    expect(result.path).toBe('/data/big.bin');
    expect(result.checksum).toBe(sha256(data));
    expect(result.type).toBe('file');
    expect(result.size).toBe(data.byteLength);
    expect(new Uint8Array(await fs.readFile('/data/big.bin'))).toEqual(data);
    expect(() => getUpload(sql(), '/data/big.bin')).toThrow(HttpError);
    expect((await fs.readdir('/data')).filter((n) => n.startsWith('.airyfs-upload-'))).toHaveLength(0);
  });

  it('publishes a zero-byte upload with the empty-content checksum', async () => {
    const empty = new Uint8Array(0);
    await beginUpload(fs, sql(), access, '/data/empty.bin', { size: 0, checksum: sha256(empty) });
    const result = await completeUpload(fs, sql(), access, '/data/empty.bin');
    expect(result.size).toBe(0);
    expect(result.checksum).toBe(sha256(empty));
    expect(new Uint8Array(await fs.readFile('/data/empty.bin'))).toEqual(empty);
  });

  it('aborts an upload, removing the temp file and session', async () => {
    await beginUpload(fs, sql(), access, '/data/f.bin', { size: 10, checksum: 'a'.repeat(64) });
    expect((await fs.readdir('/data')).some((n) => n.startsWith('.airyfs-upload-'))).toBe(true);
    await abortUpload(fs, sql(), access, '/data/f.bin');
    expect((await fs.readdir('/data')).some((n) => n.startsWith('.airyfs-upload-'))).toBe(false);
    expect(() => getUpload(sql(), '/data/f.bin')).toThrow(HttpError);
    await expectHttpError(abortUpload(fs, sql(), access, '/data/f.bin'), 404, 'UPLOAD_NOT_FOUND');
  });

  it('opportunistically reaps stale sessions older than the window', async () => {
    await beginUpload(fs, sql(), access, '/data/stale.bin', { size: 10, checksum: 'a'.repeat(64) });
    // Age the session well beyond the stale window.
    db.prepare("UPDATE fs_upload SET updated_at = unixepoch() - ? WHERE path = ?").run(48 * 3600, '/data/stale.bin');
    await reapStaleUploads(fs, sql());
    expect(() => getUpload(sql(), '/data/stale.bin')).toThrow(HttpError);
    expect((await fs.readdir('/data')).some((n) => n.startsWith('.airyfs-upload-'))).toBe(false);

    // begin also triggers reaping for other targets.
    await beginUpload(fs, sql(), access, '/data/keep.bin', { size: 10, checksum: 'a'.repeat(64) });
    db.prepare("UPDATE fs_upload SET updated_at = unixepoch() - ? WHERE path = ?").run(48 * 3600, '/data/keep.bin');
    await beginUpload(fs, sql(), access, '/data/other.bin', { size: 10, checksum: 'a'.repeat(64) });
    expect(() => getUpload(sql(), '/data/keep.bin')).toThrow(HttpError);
  });
});

describe('readBoundedChunk', () => {
  it('reads a bounded chunk and rejects one over the limit', async () => {
    const data = bytes(500, 2);
    expect(new Uint8Array(await readBoundedChunk(streamOf(data), 1024))).toEqual(data);

    const big = bytes(2048, 3);
    await expect(readBoundedChunk(streamOf(big), 1024)).rejects.toMatchObject({ code: 'CHUNK_TOO_LARGE' });
  });

  it('returns empty for a null body', async () => {
    expect((await readBoundedChunk(null, 1024)).byteLength).toBe(0);
  });
});

describe('sha256Path', () => {
  let storage: CloudflareStorage;
  let fs: AgentFS;
  let access: VolumeAccessCoordinator;

  beforeEach(() => {
    storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql as never, (cb) => storage.transactionSync(cb));
    fs = AgentFS.create(storage);
    access = new VolumeAccessCoordinator();
  });

  it('streams the SHA-256 of a regular file with size and inode', async () => {
    const data = bytes(400_000, 5);
    await fs.writeFile('/blob.bin', Buffer.from(data));
    const result = await sha256Path(fs, '/blob.bin', access);
    expect(result).toMatchObject({ algorithm: 'sha256', checksum: sha256(data), size: data.byteLength });
    expect(result.ino).toBeGreaterThan(0);
  });

  it('hashes an empty file to the well-known empty digest', async () => {
    await fs.writeFile('/empty.bin', Buffer.alloc(0));
    const result = await sha256Path(fs, '/empty.bin');
    expect(result.checksum).toBe(sha256(new Uint8Array(0)));
    expect(result.size).toBe(0);
  });

  it('rejects a directory target', async () => {
    await fs.mkdir('/dir');
    await expect(sha256Path(fs, '/dir')).rejects.toMatchObject({ code: 'EISDIR' });
  });
});
