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

function route(resource: V1Route['resource'], path: string): V1Route {
  return { volume: 'test', resource, path };
}

describe('filesystem HTTP API', () => {
  let fs: AgentFS;

  beforeEach(() => {
    fs = AgentFS.create(createTestStorage(new Database(':memory:')));
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
    expect(parseV1Route('/fs/read')).toBeNull();
    expect(() => parseV1Route('/v1/volumes/test/unknown')).toThrow(HttpError);
    expect(() => parseV1Route('/v1/volumes/test/usage/extra')).toThrow(HttpError);
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
});
