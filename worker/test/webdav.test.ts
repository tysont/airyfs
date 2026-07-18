// ABOUTME: Tests the dependency-free WebDAV adapter against a real in-memory AgentFS volume.
// ABOUTME: Covers discovery, streaming mutations, recoverable deletes, moves, copies, and locks.

import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { VolumeAccessCoordinator } from '../src/files-api';
import { initSchema } from '../src/schema';
import { listTrash } from '../src/trash';
import { handleWebDav } from '../src/webdav';
import { createTestStorage } from './support/storage';

describe('WebDAV', () => {
  let storage: ReturnType<typeof createTestStorage>;
  let fs: AgentFS;
  let access: VolumeAccessCoordinator;

  beforeEach(async () => {
    storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    fs = AgentFS.create(storage);
    access = new VolumeAccessCoordinator();
    await fs.mkdir('/docs');
    await fs.writeFile('/docs/a.txt', 'hello');
  });

  it('advertises class 2 and returns finite multistatus discovery', async () => {
    expect((await dav('OPTIONS', '/')).headers.get('DAV')).toBe('1, 2');
    const response = await dav('PROPFIND', '/docs', { Depth: '1' });
    expect(response.status).toBe(207);
    const xml = await response.text();
    expect(xml).toContain('/dav/vol/docs/');
    expect(xml).toContain('/dav/vol/docs/a.txt');
    expect((await dav('PROPFIND', '/', { Depth: 'infinity' })).status).toBe(403);
  });

  it('creates collections and streams file writes', async () => {
    expect((await dav('MKCOL', '/new')).status).toBe(201);
    expect((await dav('PUT', '/new/file', {}, 'data')).status).toBe(201);
    expect(await fs.readFile('/new/file', 'utf8')).toBe('data');
    expect((await dav('PUT', '/new/file', {}, 'changed')).status).toBe(204);
  });

  it('moves WebDAV deletes to trash', async () => {
    expect((await dav('DELETE', '/docs/a.txt')).status).toBe(204);
    expect(listTrash(storage.sql)[0].originalPath).toBe('/docs/a.txt');
  });

  it('moves, copies, and honors overwrite preconditions', async () => {
    const destination = 'http://example.com/dav/vol/docs/b.txt';
    expect((await dav('COPY', '/docs/a.txt', { Destination: destination })).status).toBe(201);
    expect(await fs.readFile('/docs/b.txt', 'utf8')).toBe('hello');
    await expect(dav('MOVE', '/docs/a.txt', { Destination: destination, Overwrite: 'F' }))
      .rejects.toMatchObject({ status: 412 });
    expect((await dav('MOVE', '/docs/a.txt', { Destination: 'http://example.com/dav/vol/docs/c.txt' })).status).toBe(201);
    expect((await dav('COPY', '/docs', { Destination: 'http://example.com/dav/vol/copied' })).status).toBe(201);
    expect(await fs.readFile('/copied/b.txt', 'utf8')).toBe('hello');
  });

  it('returns Finder-compatible lock tokens', async () => {
    const response = await dav('LOCK', '/docs/a.txt');
    expect(response.headers.get('Lock-Token')).toMatch(/^<opaquelocktoken:/);
    expect((await dav('UNLOCK', '/docs/a.txt')).status).toBe(204);
  });

  function dav(method: string, path: string, headers: HeadersInit = {}, body?: string): Promise<Response> {
    const request = new Request(`http://example.com/dav/vol${path}`, { method, headers, body });
    return handleWebDav({
      fs, sql: storage.sql, access, volume: 'vol', path, request, onMutation: async () => undefined,
    });
  }
});
