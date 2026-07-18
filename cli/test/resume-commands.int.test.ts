// ABOUTME: End-to-end tests for `put --resume` and `get --resume` against a stateful mock AiryFS server.
// ABOUTME: The mock implements the uploads, files (range/HEAD), and checksum routes with real SHA-256.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

function sha256(buf: Buffer | Uint8Array): string {
  return createHash('sha256').update(buf).digest('hex');
}

interface StoredFile {
  content: Buffer;
  inode: number;
}

interface UploadSession {
  id: string;
  size: number;
  checksum: string;
  offset: number;
  buf: Buffer;
}

// Mutable server state, reset before each test.
let files: Map<string, StoredFile>;
let uploads: Map<string, UploadSession>;
let nextInode: number;
// Test-controlled fault injection keyed by target path.
let injectOffsetConflictOnce: Set<string>;
let overrideRangeInode: Map<string, number>;
let overrideChecksum: Map<string, string>;
const patchOffsets: number[] = [];
let lastRangeHeader: string | null = null;

const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;

const RESOURCE = '/v1/volumes/vol';

function decodePath(pathname: string, resource: string): string {
  const prefix = `${RESOURCE}/${resource}`;
  const rest = pathname.slice(prefix.length);
  if (rest === '' || rest === '/') return '/';
  return '/' + rest.split('/').filter(Boolean).map(decodeURIComponent).join('/');
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  const raw = await rawBody(request);
  await route(request, response, url, raw);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-resume-home-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

beforeEach(() => {
  files = new Map();
  uploads = new Map();
  nextInode = 100;
  injectOffsetConflictOnce = new Set();
  overrideRangeInode = new Map();
  overrideChecksum = new Map();
  patchOffsets.length = 0;
  lastRangeHeader = null;
});

describe('put --resume', () => {
  it('uploads a large file in 1 MiB chunks and publishes it', async () => {
    const dir = await tempDir();
    const local = join(dir, 'big.bin');
    const data = randomBytes(1_500_000);
    await writeFile(local, data);

    const result = await invoke(['put', '--resume', local, '/data/big.bin']);
    expect(result.code).toBe(0);

    // At least two chunks were sent, the second at the 1 MiB boundary.
    expect(patchOffsets).toContain(0);
    expect(patchOffsets).toContain(1024 * 1024);
    const published = files.get('/data/big.bin');
    expect(published).toBeDefined();
    expect(Buffer.compare(published!.content, data)).toBe(0);
    expect(sha256(published!.content)).toBe(sha256(data));
  });

  it('resumes from a partial server session without re-sending accepted bytes', async () => {
    const dir = await tempDir();
    const local = join(dir, 'resume.bin');
    const data = randomBytes(1_200_000);
    await writeFile(local, data);

    // Seed a session that already holds the first 1 MiB.
    const prefix = data.subarray(0, 1024 * 1024);
    uploads.set('/data/resume.bin', {
      id: 'seed', size: data.length, checksum: sha256(data), offset: prefix.length, buf: Buffer.from(prefix),
    });

    const result = await invoke(['put', '--resume', local, '/data/resume.bin']);
    expect(result.code).toBe(0);
    // The only PATCH resumes at the 1 MiB boundary, never at 0.
    expect(patchOffsets).toEqual([1024 * 1024]);
    expect(Buffer.compare(files.get('/data/resume.bin')!.content, data)).toBe(0);
  });

  it('reconciles a single offset conflict with one status GET and continues', async () => {
    const dir = await tempDir();
    const local = join(dir, 'conflict.bin');
    const data = randomBytes(300_000);
    await writeFile(local, data);
    injectOffsetConflictOnce.add('/data/conflict.bin');

    const result = await invoke(['put', '--resume', local, '/data/conflict.bin']);
    expect(result.code).toBe(0);
    // First PATCH at 0 is rejected once, then retried at 0 and accepted.
    expect(patchOffsets.filter((offset) => offset === 0).length).toBe(2);
    expect(Buffer.compare(files.get('/data/conflict.bin')!.content, data)).toBe(0);
  });

  it('uploads a zero-byte file', async () => {
    const dir = await tempDir();
    const local = join(dir, 'empty.bin');
    await writeFile(local, Buffer.alloc(0));

    const result = await invoke(['put', '--resume', local, '/data/empty.bin']);
    expect(result.code).toBe(0);
    expect(patchOffsets).toHaveLength(0);
    expect(files.get('/data/empty.bin')!.content.length).toBe(0);
  });

  it('returns the final result as JSON', async () => {
    const dir = await tempDir();
    const local = join(dir, 'j.bin');
    const data = randomBytes(2048);
    await writeFile(local, data);

    const result = await invoke(['--json', 'put', '--resume', local, '/data/j.bin']);
    expect(result.code).toBe(0);
    const parsed = JSON.parse(result.stdout);
    expect(parsed).toMatchObject({ ok: true, path: '/data/j.bin', checksum: sha256(data), type: 'file' });
  });
});

describe('get --resume', () => {
  it('downloads a file, verifies the checksum, and cleans up the sidecar', async () => {
    files.set('/data/get.bin', { content: randomBytes(500_000), inode: nextInode++ });
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    const result = await invoke(['get', '--resume', '/data/get.bin', local]);
    expect(result.code).toBe(0);
    expect(Buffer.compare(await readFile(local), files.get('/data/get.bin')!.content)).toBe(0);
    expect(await exists(`${local}.airyfs-partial`)).toBe(false);
    expect(await exists(`${local}.airyfs-partial.json`)).toBe(false);
  });

  it('resumes from a matching partial and sidecar with a byte-range request', async () => {
    const content = randomBytes(400_000);
    const inode = nextInode++;
    files.set('/data/resume.bin', { content, inode });
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    // Pre-stage a partial holding the first half plus a matching sidecar.
    const half = 200_000;
    await writeFile(`${local}.airyfs-partial`, content.subarray(0, half));
    await writeFile(`${local}.airyfs-partial.json`, JSON.stringify({ inode: String(inode), size: content.length }));

    const result = await invoke(['get', '--resume', '/data/resume.bin', local]);
    expect(result.code).toBe(0);
    expect(lastRangeHeader).toBe(`bytes=${half}-`);
    expect(Buffer.compare(await readFile(local), content)).toBe(0);
    expect(await exists(`${local}.airyfs-partial.json`)).toBe(false);
  });

  it('restarts safely when the sidecar inode no longer matches the remote', async () => {
    const content = randomBytes(300_000);
    files.set('/data/changed.bin', { content, inode: nextInode++ });
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    // Stale partial from a previous, different remote object.
    await writeFile(`${local}.airyfs-partial`, randomBytes(50_000));
    await writeFile(`${local}.airyfs-partial.json`, JSON.stringify({ inode: '999', size: 12345 }));

    const result = await invoke(['get', '--resume', '/data/changed.bin', local]);
    expect(result.code).toBe(0);
    // A full restart fetches from offset 0.
    expect(lastRangeHeader).toBe('bytes=0-');
    expect(Buffer.compare(await readFile(local), content)).toBe(0);
  });

  it('keeps partial state and fails when the inode changes mid-transfer', async () => {
    const content = randomBytes(200_000);
    const inode = nextInode++;
    files.set('/data/mid.bin', { content, inode });
    overrideRangeInode.set('/data/mid.bin', inode + 5000);
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    const result = await invoke(['get', '--resume', '/data/mid.bin', local]);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('changed during download');
    expect(await exists(local)).toBe(false);
    expect(await exists(`${local}.airyfs-partial.json`)).toBe(true);
  });

  it('keeps partial state and fails on a checksum mismatch', async () => {
    const content = randomBytes(150_000);
    files.set('/data/bad.bin', { content, inode: nextInode++ });
    overrideChecksum.set('/data/bad.bin', 'f'.repeat(64));
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    const result = await invoke(['get', '--resume', '/data/bad.bin', local]);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Checksum mismatch');
    expect(await exists(local)).toBe(false);
    expect(await exists(`${local}.airyfs-partial`)).toBe(true);
  });

  it('downloads a zero-byte file', async () => {
    files.set('/data/empty.bin', { content: Buffer.alloc(0), inode: nextInode++ });
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    const result = await invoke(['get', '--resume', '/data/empty.bin', local]);
    expect(result.code).toBe(0);
    expect((await readFile(local)).length).toBe(0);
    expect(await exists(`${local}.airyfs-partial`)).toBe(false);
  });

  it('refuses an existing destination unless --force', async () => {
    files.set('/data/keep.bin', { content: randomBytes(1000), inode: nextInode++ });
    const dir = await tempDir();
    const local = join(dir, 'out.bin');
    await writeFile(local, 'existing');

    const refused = await invoke(['get', '--resume', '/data/keep.bin', local]);
    expect(refused.code).toBe(1);
    expect(refused.stderr).toContain('already exists');

    const forced = await invoke(['get', '--resume', '--force', '/data/keep.bin', local]);
    expect(forced.code).toBe(0);
    expect(Buffer.compare(await readFile(local), files.get('/data/keep.bin')!.content)).toBe(0);
  });

  it('returns the final result as JSON', async () => {
    const content = randomBytes(2048);
    files.set('/data/j.bin', { content, inode: nextInode++ });
    const dir = await tempDir();
    const local = join(dir, 'out.bin');

    const result = await invoke(['--json', 'get', '--resume', '/data/j.bin', local]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, remote: '/data/j.bin', checksum: sha256(content) });
  });
});

// ---------------------------------------------------------------------------
// Mock server routing
// ---------------------------------------------------------------------------

async function route(
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
  raw: Buffer,
): Promise<void> {
  const method = request.method || 'GET';
  const pathname = url.pathname;

  if (pathname.startsWith(`${RESOURCE}/uploads`)) {
    return handleUploads(method, decodePath(pathname, 'uploads'), request, response, raw);
  }
  if (pathname.startsWith(`${RESOURCE}/files`)) {
    return handleFiles(method, decodePath(pathname, 'files'), request, response);
  }
  if (pathname === `${RESOURCE}/operations/checksum` && method === 'POST') {
    const { path } = JSON.parse(raw.toString() || '{}') as { path: string };
    const file = files.get(path);
    if (!file) return json(response, 404, { error: { code: 'ENOENT', message: 'not found' } });
    return json(response, 200, {
      algorithm: 'sha256',
      checksum: overrideChecksum.get(path) ?? sha256(file.content),
      size: file.content.length,
      ino: file.inode,
    });
  }
  json(response, 404, { error: { code: 'ENOENT', message: `Unhandled ${method} ${pathname}` } });
}

function handleUploads(
  method: string,
  path: string,
  request: IncomingMessage,
  response: ServerResponse,
  raw: Buffer,
): void {
  if (method === 'POST') {
    const body = JSON.parse(raw.toString() || '{}') as { size: number; checksum: string };
    const existing = uploads.get(path);
    if (existing) {
      if (existing.size !== body.size || existing.checksum !== body.checksum) {
        return json(response, 409, { error: { code: 'UPLOAD_CONFLICT', message: 'conflict' } });
      }
      return json(response, 200, status(path, existing));
    }
    const session: UploadSession = { id: `u-${path}`, size: body.size, checksum: body.checksum, offset: 0, buf: Buffer.alloc(0) };
    uploads.set(path, session);
    return json(response, 201, status(path, session));
  }
  if (method === 'GET') {
    const session = uploads.get(path);
    if (!session) return json(response, 404, { error: { code: 'UPLOAD_NOT_FOUND', message: 'none' } });
    return json(response, 200, status(path, session));
  }
  if (method === 'PATCH') {
    const session = uploads.get(path);
    if (!session) return json(response, 404, { error: { code: 'UPLOAD_NOT_FOUND', message: 'none' } });
    const offset = Number(request.headers['upload-offset']);
    const chunkSha = String(request.headers['x-airyfs-chunk-sha256'] || '');
    patchOffsets.push(offset);
    if (injectOffsetConflictOnce.has(path)) {
      injectOffsetConflictOnce.delete(path);
      response.writeHead(409, { 'Content-Type': 'application/json', 'Upload-Offset': String(session.offset) })
        .end(JSON.stringify({ error: { code: 'UPLOAD_OFFSET_MISMATCH', message: 'stale' } }));
      return;
    }
    if (offset !== session.offset) {
      response.writeHead(409, { 'Content-Type': 'application/json', 'Upload-Offset': String(session.offset) })
        .end(JSON.stringify({ error: { code: 'UPLOAD_OFFSET_MISMATCH', message: 'stale' } }));
      return;
    }
    if (sha256(raw) !== chunkSha) {
      return json(response, 400, { error: { code: 'CHUNK_CHECKSUM_MISMATCH', message: 'bad chunk' } });
    }
    session.buf = Buffer.concat([session.buf, raw]);
    session.offset += raw.length;
    return json(response, 200, status(path, session));
  }
  if (method === 'PUT') {
    const session = uploads.get(path);
    if (!session) return json(response, 404, { error: { code: 'UPLOAD_NOT_FOUND', message: 'none' } });
    if (session.offset !== session.size) {
      return json(response, 409, { error: { code: 'UPLOAD_INCOMPLETE', message: 'incomplete' } });
    }
    if (sha256(session.buf) !== session.checksum) {
      return json(response, 409, { error: { code: 'UPLOAD_CHECKSUM_MISMATCH', message: 'mismatch' } });
    }
    const inode = nextInode++;
    files.set(path, { content: session.buf, inode });
    uploads.delete(path);
    return json(response, 200, {
      path, checksum: session.checksum, size: session.size, type: 'file',
      ino: inode, mode: 0o100644, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0,
    });
  }
  if (method === 'DELETE') {
    uploads.delete(path);
    response.writeHead(204).end();
    return;
  }
  json(response, 405, { error: { code: 'METHOD_NOT_ALLOWED', message: 'no' } });
}

function handleFiles(method: string, path: string, request: IncomingMessage, response: ServerResponse): void {
  const file = files.get(path);
  if (!file) return json(response, 404, { error: { code: 'ENOENT', message: 'not found' } });

  if (method === 'HEAD') {
    response.writeHead(200, {
      'Content-Length': String(file.content.length),
      'Content-Type': 'application/octet-stream',
      'Accept-Ranges': 'bytes',
      'X-AiryFS-Inode': String(file.inode),
    }).end();
    return;
  }
  if (method === 'GET') {
    const range = request.headers['range'];
    const inodeHeader = String(overrideRangeInode.get(path) ?? file.inode);
    if (typeof range === 'string') {
      lastRangeHeader = range;
      const match = /^bytes=(\d+)-$/.exec(range);
      const start = match ? Number(match[1]) : 0;
      if (start >= file.content.length && file.content.length > 0) {
        response.writeHead(416, { 'Content-Range': `bytes */${file.content.length}` }).end();
        return;
      }
      const slice = file.content.subarray(start);
      response.writeHead(start === 0 ? 200 : 206, {
        'Content-Length': String(slice.length),
        'Content-Type': 'application/octet-stream',
        'X-AiryFS-Inode': inodeHeader,
        ...(start === 0 ? {} : { 'Content-Range': `bytes ${start}-${file.content.length - 1}/${file.content.length}` }),
      });
      response.end(slice);
      return;
    }
    response.writeHead(200, {
      'Content-Length': String(file.content.length),
      'Content-Type': 'application/octet-stream',
      'X-AiryFS-Inode': inodeHeader,
    }).end(file.content);
    return;
  }
  json(response, 405, { error: { code: 'METHOD_NOT_ALLOWED', message: 'no' } });
}

function status(path: string, session: UploadSession): Record<string, unknown> {
  return {
    id: session.id, path, size: session.size, offset: session.offset,
    checksum: session.checksum, createdAt: 1, updatedAt: 1,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function randomBytes(length: number): Buffer {
  const out = Buffer.alloc(length);
  for (let i = 0; i < length; i++) out[i] = (i * 2654435761) & 0xff;
  return out;
}

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'airyfs-resume-'));
  temporaryPaths.push(dir);
  return dir;
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

async function rawBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function json(response: ServerResponse, statusCode: number, value: unknown): void {
  response.writeHead(statusCode, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}

async function invoke(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const sink = (chunks: Buffer[]): Writable => new Writable({
    write(chunk, _enc, cb) {
      chunks.push(Buffer.from(chunk));
      cb();
    },
  });
  const code = await execute(['node', 'airyfs', '--session', 'test', ...args], {
    sessions,
    stdin: Readable.from(''),
    stdout: sink(stdout),
    stderr: sink(stderr),
    shellMode: true,
  });
  return { code, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString() };
}
