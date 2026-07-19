// ABOUTME: Integration tests for the binary and resource-oriented filesystem API.
// ABOUTME: Runs AgentFS against in-memory SQLite to verify real filesystem semantics.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import {
  errorResponse,
  fileResponse,
  handleFilesystemRequest,
  HttpError,
  latestFileVersion,
  parseV1Route,
  readCommandRequest,
  readVolumeCreateRequest,
  VolumeAccessCoordinator,
  writeFileStream,
  type V1Route,
} from '../src/files-api';
import { createTestStorage } from './support/storage';
import { MutationJournal } from '../src/mutation-journal';
import { initSchema } from '../src/schema';
import { FilesystemPrimitives } from '../src/filesystem-primitives';

function route(resource: V1Route['resource'], path: string): V1Route {
  return { volume: 'test', resource, path };
}

describe('filesystem HTTP API', () => {
  let fs: AgentFS;
  let sql: ReturnType<typeof createTestStorage>['sql'];
  let primitives: FilesystemPrimitives;

  beforeEach(() => {
    const storage = createTestStorage(new Database(':memory:'));
    sql = storage.sql;
    initSchema(sql);
    fs = AgentFS.create(storage);
    primitives = new FilesystemPrimitives(sql, (callback) => storage.transactionSync(callback));
  });

  it('parses resource routes and encoded path segments', () => {
    expect(parseV1Route('/v1/volumes/my%20volume/files/a%20b.txt')).toEqual({
      volume: 'my volume',
      resource: 'files',
      path: '/a b.txt',
    });
    expect(parseV1Route('/v1/volumes/my%20volume')).toEqual({
      volume: 'my volume', resource: 'volume', path: '/',
    });
    expect(parseV1Route('/v1/volumes/vol/snapshots')).toEqual({
      volume: 'vol', resource: 'snapshots', path: '/',
    });
    expect(parseV1Route('/v1/volumes/vol/snapshots/nightly')).toEqual({
      volume: 'vol', resource: 'snapshots', path: '/nightly',
    });
    expect(parseV1Route('/v1/volumes/vol/changes/src')).toEqual({
      volume: 'vol', resource: 'changes', path: '/src',
    });
    expect(parseV1Route('/v1/volumes/vol/browser-uploads/inbox/a%20b.txt')).toEqual({
      volume: 'vol', resource: 'browser-uploads', path: '/inbox/a b.txt',
    });
    expect(parseV1Route('/v1/volumes/vol/tree/src')).toEqual({
      volume: 'vol', resource: 'tree', path: '/src',
    });
    expect(parseV1Route('/v1/volumes/vol/trash/abc/restore')).toEqual({
      volume: 'vol', resource: 'trash', path: '/abc/restore',
    });
    expect(parseV1Route('/v1/volumes/vol/exec/pty')).toEqual({ volume: 'vol', resource: 'exec', path: '/pty' });
    expect(parseV1Route('/v1/volumes/vol/exec/pty-ticket')).toEqual({ volume: 'vol', resource: 'exec', path: '/pty-ticket' });
    expect(parseV1Route('/v1/volumes/vol/services/web/proxy/a')).toEqual({ volume: 'vol', resource: 'services', path: '/web/proxy/a' });
    expect(parseV1Route('/v1/volumes/source/forks')).toEqual({ volume: 'source', resource: 'forks', path: '/' });
    expect(parseV1Route('/v1/volumes/vol/sql')).toEqual({ volume: 'vol', resource: 'sql', path: '/' });
    expect(parseV1Route('/v1/volumes/vol/metrics')).toEqual({ volume: 'vol', resource: 'metrics', path: '/' });
    expect(parseV1Route('/v1/volumes/vol/usage-history')).toEqual({ volume: 'vol', resource: 'usage-history', path: '/' });
    expect(parseV1Route('/fs/read')).toBeNull();
    expect(() => parseV1Route('/v1/volumes/test/unknown')).toThrow(HttpError);
    expect(() => parseV1Route('/v1/volumes/test/usage/extra')).toThrow(HttpError);
    expect(() => parseV1Route('/v1/volumes/test/metrics/extra')).toThrow(HttpError);
    expect(() => parseV1Route('/v1/volumes/test/usage-history/extra')).toThrow(HttpError);
  });

  it('streams arbitrary binary content without UTF-8 conversion', async () => {
    const bytes = Uint8Array.from([0, 255, 1, 128, 42]);
    const request = new Request('http://localhost/v1/volumes/test/files/data.bin', {
      method: 'PUT',
      body: bytes,
    });

    const writeResponse = await handleFilesystemRequest(request, route('files', '/data.bin'), fs);
    expect(writeResponse?.status).toBe(204);

    const readResponse = await fileResponse(
      fs,
      '/data.bin',
      new Request('http://localhost/v1/volumes/test/files/data.bin')
    );
    expect(readResponse.status).toBe(200);
    expect(new Uint8Array(await readResponse.arrayBuffer())).toEqual(bytes);
    expect(readResponse.headers.get('Content-Type')).toBe('application/octet-stream');
  });

  it('streams browser POST uploads into their target path', async () => {
    await fs.mkdir('/inbox');
    const bytes = Uint8Array.from([0, 255, 7, 128]);
    const response = await handleFilesystemRequest(
      new Request('http://localhost/v1/volumes/test/browser-uploads/inbox/data.bin', {
        method: 'POST',
        body: bytes,
      }),
      route('browser-uploads', '/inbox/data.bin'),
      fs,
    );

    expect(response?.status).toBe(201);
    expect(await response?.json()).toMatchObject({ path: '/inbox/data.bin', size: bytes.byteLength, type: 'file' });
    expect(new Uint8Array(await fs.readFile('/inbox/data.bin'))).toEqual(bytes);
  });

  it('supports bounded, open-ended, and suffix byte ranges', async () => {
    await fs.writeFile('/range.txt', Buffer.from('0123456789'));

    for (const [header, expected, contentRange] of [
      ['bytes=2-5', '2345', 'bytes 2-5/10'],
      ['bytes=7-', '789', 'bytes 7-9/10'],
      ['bytes=-3', '789', 'bytes 7-9/10'],
    ]) {
      const response = await fileResponse(fs, '/range.txt', new Request('http://localhost', {
        headers: { Range: header },
      }));
      expect(response.status).toBe(206);
      expect(await response.text()).toBe(expected);
      expect(response.headers.get('Content-Range')).toBe(contentRange);
    }
  });

  it('returns stable validators and handles conditional reads', async () => {
    await fs.writeFile('/cached.txt', Buffer.from('cached'));
    const version = (ino: number) => latestFileVersion(sql, ino);
    const initial = await fileResponse(fs, '/cached.txt', new Request('http://localhost'), undefined, version);
    const etag = initial.headers.get('ETag')!;
    const lastModified = initial.headers.get('Last-Modified')!;
    await initial.body?.cancel();

    expect(etag).toMatch(/^"[0-9a-f]+-[0-9a-f]+-[0-9a-f]+"$/);
    expect((await fileResponse(fs, '/cached.txt', new Request('http://localhost', {
      headers: { 'If-None-Match': etag },
    }), undefined, version)).status).toBe(304);
    expect((await fileResponse(fs, '/cached.txt', new Request('http://localhost', {
      headers: { 'If-None-Match': `W/${etag}` },
    }), undefined, version)).status).toBe(304);
    expect((await fileResponse(fs, '/cached.txt', new Request('http://localhost', {
      headers: { 'If-None-Match': '"other"', 'If-Modified-Since': lastModified },
    }), undefined, version)).status).toBe(200);
    expect((await fileResponse(fs, '/cached.txt', new Request('http://localhost', {
      headers: { 'If-Modified-Since': lastModified },
    }), undefined, version)).status).toBe(304);

    await fs.writeFile('/cached.txt', Buffer.from('change'));
    const changed = await fileResponse(fs, '/cached.txt', new Request('http://localhost', {
      headers: { 'If-None-Match': etag },
    }), undefined, version);
    expect(changed.status).toBe(200);
    expect(changed.headers.get('ETag')).not.toBe(etag);
    await changed.body?.cancel();
  });

  it('uses If-Range to prevent combining stale partial content', async () => {
    await fs.writeFile('/range.txt', Buffer.from('0123456789'));
    const version = (ino: number) => latestFileVersion(sql, ino);
    const initial = await fileResponse(fs, '/range.txt', new Request('http://localhost'), undefined, version);
    const etag = initial.headers.get('ETag')!;
    await initial.body?.cancel();

    const matching = await fileResponse(fs, '/range.txt', new Request('http://localhost', {
      headers: { Range: 'bytes=2-4', 'If-Range': etag },
    }), undefined, version);
    expect(matching.status).toBe(206);
    expect(await matching.text()).toBe('234');

    const stale = await fileResponse(fs, '/range.txt', new Request('http://localhost', {
      headers: { Range: 'bytes=2-4', 'If-Range': '"stale"' },
    }), undefined, version);
    expect(stale.status).toBe(200);
    expect(stale.headers.get('Content-Range')).toBeNull();
    expect(await stale.text()).toBe('0123456789');
  });

  it('ignores unsupported ranges and Range on HEAD', async () => {
    await fs.writeFile('/range.txt', Buffer.from('0123456789'));

    for (const range of ['items=1-2', 'bytes=1-2,4-5']) {
      const response = await fileResponse(fs, '/range.txt', new Request('http://localhost', {
        headers: { Range: range },
      }));
      expect(response.status).toBe(200);
      expect(await response.text()).toBe('0123456789');
    }

    const head = await fileResponse(fs, '/range.txt', new Request('http://localhost', {
      method: 'HEAD',
      headers: { Range: 'bytes=1-2' },
    }));
    expect(head.status).toBe(200);
    expect(head.headers.get('Content-Length')).toBe('10');
  });

  it('rejects unsatisfiable ranges with the file size', async () => {
    await fs.writeFile('/range.txt', Buffer.from('123'));
    const response = await handleFilesystemRequest(
      new Request('http://localhost', { headers: { Range: 'bytes=9-10' } }),
      route('files', '/range.txt'),
      fs
    );
    expect(response?.status).toBe(416);
    expect(response?.headers.get('Content-Range')).toBe('bytes */3');
  });

  it('preserves the previous file when a streamed replacement fails', async () => {
    await fs.writeFile('/important.txt', Buffer.from('original'));
    const failedStream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(Buffer.from('partial'));
        controller.error(new Error('upload interrupted'));
      },
    });

    await expect(writeFileStream(fs, '/important.txt', failedStream)).rejects.toThrow('upload interrupted');
    expect(await fs.readFile('/important.txt', 'utf8')).toBe('original');
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-upload-'))).toEqual([]);
  });

  it('requires upload parent directories to exist', async () => {
    const failedStream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.error(new Error('upload interrupted'));
      },
    });

    await expect(writeFileStream(fs, '/new/nested/file.txt', failedStream)).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(fs.access('/new')).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('rejects trailing-slash file paths before creating directories', async () => {
    await expect(writeFileStream(
      fs,
      '/new/nested/',
      new Response(Buffer.from('data')).body
    )).rejects.toMatchObject({ status: 400, code: 'INVALID_PATH' });
    await expect(fs.access('/new')).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('allows unrelated path mutations while a stream is active', async () => {
    const access = new VolumeAccessCoordinator();
    const releaseRead = await access.acquireRead('/downloads/large.bin');
    const releaseWrite = await access.acquireWrite('/uploads/other.bin');
    releaseWrite();
    releaseRead();
  });

  it('treats equivalent AgentFS path aliases as conflicting locks', async () => {
    const access = new VolumeAccessCoordinator();
    const releaseRead = await access.acquireRead('a//b/');
    let acquired = false;
    const writer = access.acquireWrite('/a/b').then((release) => {
      acquired = true;
      release();
    });
    await Promise.resolve();
    expect(acquired).toBe(false);
    releaseRead();
    await writer;
    expect(acquired).toBe(true);
  });

  it('holds direct mutations until an active response stream finishes', async () => {
    const access = new VolumeAccessCoordinator();
    await fs.writeFile('/large.bin', Buffer.alloc(512 * 1024, 1));
    const response = await fileResponse(fs, '/large.bin', new Request('http://localhost'), access);
    const reader = response.body!.getReader();
    expect((await reader.read()).value?.byteLength).toBeGreaterThan(0);

    let replacementFinished = false;
    const replacement = writeFileStream(
      fs,
      '/large.bin',
      new Response(Buffer.from('replacement')).body,
      access
    ).then(() => { replacementFinished = true; });
    await Promise.resolve();
    expect(replacementFinished).toBe(false);

    while (!(await reader.read()).done) { /* drain the active download */ }
    await replacement;
    expect(await fs.readFile('/large.bin', 'utf8')).toBe('replacement');
  });

  it('supports directory metadata and mutation operations through AgentFS', async () => {
    expect((await handleFilesystemRequest(
      new Request('http://localhost', { method: 'PUT' }),
      route('directories', '/src'),
      fs
    ))?.status).toBe(204);
    await fs.writeFile('/src/a.txt', Buffer.from('a'));

    const listing = await handleFilesystemRequest(
      new Request('http://localhost'),
      route('directories', '/src'),
      fs
    );
    expect(await listing?.json()).toMatchObject([{ name: 'a.txt', type: 'file', size: 1 }]);

    const rename = await handleFilesystemRequest(
      new Request('http://localhost', {
        method: 'POST',
        body: JSON.stringify({ from: '/src/a.txt', to: '/src/b.txt' }),
      }),
      route('operations', '/rename'),
      fs
    );
    expect(rename?.status).toBe(204);
    expect(await fs.readFile('/src/b.txt', 'utf8')).toBe('a');
  });

  it('truncates a file in place through the operations API', async () => {
    await fs.writeFile('/data.txt', Buffer.from('1234567890'));
    const before = await fs.stat('/data.txt');

    const response = await handleFilesystemRequest(
      new Request('http://localhost', {
        method: 'POST',
        body: JSON.stringify({ path: '/data.txt', size: 4 }),
      }),
      route('operations', '/truncate'),
      fs
    );

    expect(response?.status).toBe(204);
    expect(await fs.readFile('/data.txt', 'utf8')).toBe('1234');
    expect((await fs.stat('/data.txt')).ino).toBe(before.ino);
  });

  it('touches existing paths and creates missing empty files', async () => {
    const create = await operationRequest('touch', { path: '/created.txt', atime: 10.25, mtime: 20.5 });
    expect(create.status).toBe(204);
    expect(await fs.stat('/created.txt')).toMatchObject({ atime: 10, mtime: 20, size: 0 });
    expect(await fs.readFile('/created.txt', 'utf8')).toBe('');

    const ino = (await fs.stat('/created.txt')).ino;
    expect((await operationRequest('touch', { path: '/created.txt' })).status).toBe(204);
    expect((await fs.stat('/created.txt')).ino).toBe(ino);
  });

  it('changes permission bits without changing the inode type', async () => {
    await fs.mkdir('/private');
    const response = await operationRequest('chmod', { path: '/private', mode: 0o700 });
    expect(response.status).toBe(204);
    expect((await fs.stat('/private')).mode).toBe(0o40700);
  });

  it('creates true hard links that share content and link counts', async () => {
    await fs.writeFile('/source.txt', Buffer.from('before'));
    expect((await operationRequest('link', { existing: '/source.txt', path: '/linked.txt' })).status).toBe(204);

    const source = await fs.stat('/source.txt');
    const linked = await fs.stat('/linked.txt');
    expect(linked.ino).toBe(source.ino);
    expect(linked.nlink).toBe(2);
    await fs.writeFile('/source.txt', Buffer.from('after'));
    expect(await fs.readFile('/linked.txt', 'utf8')).toBe('after');
    await fs.unlink('/source.txt');
    expect(await fs.readFile('/linked.txt', 'utf8')).toBe('after');
    expect((await fs.stat('/linked.txt')).nlink).toBe(1);
  });

  it('reports symlink metadata without following the link', async () => {
    await fs.writeFile('/target.txt', Buffer.from('target'));
    await fs.symlink('/target.txt', '/link');
    const response = await operationRequest('lstat', { path: '/link' });
    expect(await response.json()).toMatchObject({ type: 'symlink' });
  });

  it('appends binary data at the locked current file size', async () => {
    await fs.writeFile('/log.bin', Buffer.from([0, 1]));
    sql.exec('UPDATE fs_inode SET ctime = 1 WHERE ino = ?', (await fs.stat('/log.bin')).ino);
    const response = await operationRequest('append', { path: '/log.bin', data: Buffer.from([2, 255]).toString('base64') });
    expect(response.status).toBe(204);
    expect(new Uint8Array(await fs.readFile('/log.bin'))).toEqual(Uint8Array.from([0, 1, 2, 255]));
    expect((await fs.stat('/log.bin')).ctime).toBeGreaterThan(1);
  });

  it('bounds and validates append input before writing', async () => {
    await fs.writeFile('/log', Buffer.from('before'));
    expect((await operationRequest('append', { path: '/log', data: '!' })).status).toBe(400);
    expect((await operationRequest('append', { path: '/log', data: 'A'.repeat(1_398_108) })).status).toBe(413);
    expect(await fs.readFile('/log', 'utf8')).toBe('before');

    const missing = await operationRequest('append', { path: '/missing', data: '' });
    expect(missing.status).toBe(404);
  });

  it('rejects directory hard links, duplicate destinations, and metadata changes through symlinks', async () => {
    await fs.mkdir('/dir');
    await fs.writeFile('/file', Buffer.from('x'));
    await fs.symlink('/file', '/link');
    expect((await operationRequest('link', { existing: '/dir', path: '/dir-link' })).status).toBe(403);
    expect((await operationRequest('link', { existing: '/file', path: '/file' })).status).toBe(409);
    expect((await operationRequest('touch', { path: '/link' })).status).toBe(409);
    expect((await operationRequest('chmod', { path: '/link', mode: 0o600 })).status).toBe(409);
  });

  it('canonicalizes direct operation paths before mutation journaling', async () => {
    const mutations: string[][] = [];
    const response = await handleFilesystemRequest(
      new Request('http://localhost', { method: 'POST', body: JSON.stringify({ path: '/tmp/../note' }) }),
      route('operations', '/touch'),
      fs,
      undefined,
      async (paths) => { mutations.push(paths); },
      undefined,
      primitives,
    );
    expect(response?.status).toBe(204);
    expect(mutations).toEqual([['/note']]);
    expect((await fs.stat('/note')).isFile()).toBe(true);
  });

  it('counts logical subtree bytes once for hard-linked inodes', async () => {
    await fs.mkdir('/data');
    await fs.writeFile('/data/a', Buffer.from('12345'));
    await operationRequest('link', { existing: '/data/a', path: '/data/b' });
    const response = await operationRequest('du', { path: '/data' });
    expect(await response.json()).toEqual({ bytes: 5, inodes: 2 });
  });

  it('journals parent entries and current inodes for direct mutations', async () => {
    const db = new Database(':memory:');
    const storage = createTestStorage(db);
    const sql = (storage as unknown as { sql: Parameters<typeof initSchema>[0] }).sql;
    initSchema(sql);
    const journalFs = AgentFS.create(storage);
    const journal = new MutationJournal(sql);
    await journalFs.writeFile('/before.txt', Buffer.from('data'));
    await journalFs.rename('/before.txt', '/after.txt');

    await journal.record(journalFs, ['/before.txt', '/after.txt']);

    expect(db.prepare(
      'SELECT parent_ino, name, ino FROM fs_mutation_journal ORDER BY seq'
    ).all()).toEqual([
      { parent_ino: 1, name: 'before.txt', ino: null },
      { parent_ino: 1, name: 'after.txt', ino: 2 },
    ]);
  });

  it('maps AgentFS errors to stable JSON HTTP responses', async () => {
    const response = await handleFilesystemRequest(
      new Request('http://localhost'),
      route('files', '/missing.txt'),
      fs
    );
    expect(response?.status).toBe(404);
    expect(await response?.json()).toMatchObject({ error: { code: 'ENOENT', path: '/missing.txt' } });

    expect(errorResponse(new Error('unexpected')).status).toBe(500);
  });

  it('returns client errors for invalid methods, operations, and exec bodies', async () => {
    const method = await handleFilesystemRequest(
      new Request('http://localhost', { method: 'PATCH' }),
      route('files', '/x'),
      fs
    );
    expect(method?.status).toBe(405);
    expect(method?.headers.get('Allow')).toBe('GET, HEAD, PUT, DELETE');

    const operation = await handleFilesystemRequest(
      new Request('http://localhost', { method: 'POST', body: '{}' }),
      route('operations', '/unknown'),
      fs
    );
    expect(operation?.status).toBe(404);

    await expect(readCommandRequest(new Request('http://localhost', {
      method: 'POST',
      body: '{',
    }))).rejects.toMatchObject({ status: 400, code: 'INVALID_JSON' });
    await expect(readCommandRequest(new Request('http://localhost', {
      method: 'POST',
      body: '{}',
    }))).rejects.toMatchObject({ status: 400, code: 'INVALID_ARGUMENT' });
    await expect(readVolumeCreateRequest(new Request('http://localhost', {
      method: 'PUT', body: '{',
    }))).rejects.toMatchObject({ status: 400, code: 'INVALID_JSON' });
    await expect(readVolumeCreateRequest(new Request('http://localhost', {
      method: 'PUT', body: JSON.stringify({ chunkSize: 64 * 1024 }),
    }))).resolves.toBe(64 * 1024);
  });

  it('reports symlinks explicitly instead of treating them as directories', async () => {
    await fs.symlink('/target', '/link');
    const response = await handleFilesystemRequest(
      new Request('http://localhost'),
      route('files', '/link'),
      fs
    );
    expect(response?.status).toBe(409);
    expect(await response?.json()).toMatchObject({ error: { code: 'SYMLINK_NOT_RESOLVED' } });
  });

  async function operationRequest(name: string, body: Record<string, unknown>): Promise<Response> {
    return (await handleFilesystemRequest(
      new Request('http://localhost', { method: 'POST', body: JSON.stringify(body) }),
      route('operations', `/${name}`),
      fs,
      undefined,
      undefined,
      undefined,
      primitives,
    ))!;
  }
});

describe('open-handle lease retention', () => {
  let db: Database.Database;
  let leaseFs: AgentFS;

  const now = () => Math.floor(Date.now() / 1000);
  const inoOf = (name: string): number =>
    (db.prepare('SELECT ino FROM fs_dentry WHERE parent_ino = 1 AND name = ?').get(name) as { ino: number }).ino;
  const inodeExists = (ino: number): boolean =>
    db.prepare('SELECT 1 FROM fs_inode WHERE ino = ?').get(ino) !== undefined;
  const chunkCount = (ino: number): number =>
    (db.prepare('SELECT COUNT(*) AS c FROM fs_data WHERE ino = ?').get(ino) as { c: number }).c;
  const lease = (ino: number, expiresAt: number, openCount = 1) =>
    db.prepare(
      'INSERT INTO fs_open_inode (session_id, ino, open_count, expires_at) VALUES (?, ?, ?, ?)'
    ).run('fuse-session', ino, openCount, expiresAt);

  beforeEach(() => {
    db = new Database(':memory:');
    leaseFs = AgentFS.create(createTestStorage(db));
  });

  it('retains an unlinked inode and its data while a lease is unexpired', async () => {
    await leaseFs.writeFile('/leased.txt', Buffer.from('hello lease'));
    const ino = inoOf('leased.txt');
    lease(ino, now() + 120);

    await leaseFs.unlink('/leased.txt');

    // Pathname resolves away immediately, inode and data survive.
    await expect(leaseFs.stat('/leased.txt')).rejects.toMatchObject({ code: 'ENOENT' });
    expect(inodeExists(ino)).toBe(true);
    expect(chunkCount(ino)).toBeGreaterThan(0);
  });

  it('deletes an unlinked inode when no lease is held', async () => {
    await leaseFs.writeFile('/plain.txt', Buffer.from('bytes'));
    const ino = inoOf('plain.txt');

    await leaseFs.unlink('/plain.txt');

    expect(inodeExists(ino)).toBe(false);
    expect(chunkCount(ino)).toBe(0);
  });

  it('ignores an expired lease and deletes the inode', async () => {
    await leaseFs.writeFile('/stale.txt', Buffer.from('bytes'));
    const ino = inoOf('stale.txt');
    lease(ino, now() - 10);

    await leaseFs.unlink('/stale.txt');

    expect(inodeExists(ino)).toBe(false);
  });

  it('retains a replaced inode across a streaming rename-over', async () => {
    await leaseFs.writeFile('/target.txt', Buffer.from('original'));
    const victimIno = inoOf('target.txt');
    lease(victimIno, now() + 120);

    // Mimic writeFileStream: write a temp file and rename it over the target.
    await leaseFs.writeFile('/target.txt.tmp', Buffer.from('replacement'));
    const replacementIno = inoOf('target.txt.tmp');
    await leaseFs.rename('/target.txt.tmp', '/target.txt');

    // Path now resolves to the replacement; the leased victim survives.
    expect(inoOf('target.txt')).toBe(replacementIno);
    expect(inodeExists(victimIno)).toBe(true);
    expect(chunkCount(victimIno)).toBeGreaterThan(0);
  });

  it('cascades chunk and lease cleanup through the inode delete trigger', async () => {
    await leaseFs.writeFile('/cascade.txt', Buffer.from('bytes'));
    const ino = inoOf('cascade.txt');
    lease(ino, now() - 5); // expired, so unlink deletes the inode

    await leaseFs.unlink('/cascade.txt');

    expect(inodeExists(ino)).toBe(false);
    expect(chunkCount(ino)).toBe(0);
    expect(
      (db.prepare('SELECT COUNT(*) AS c FROM fs_open_inode WHERE ino = ?').get(ino) as { c: number }).c
    ).toBe(0);
  });
});
