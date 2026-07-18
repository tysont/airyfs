// ABOUTME: Exercises site and share CLI commands against a local mock AiryFS server.
// ABOUTME: Verifies publish/status/unpublish requests and share link creation and URLs.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

interface CapturedRequest { method: string; path: string; body: string }

const requests: CapturedRequest[] = [];
const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  const body = await requestBody(request);
  requests.push({ method: request.method || 'GET', path: `${url.pathname}${url.search}`, body });
  await route(request, response, url);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-site-cmd-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe('site commands', () => {
  it('publishes the web root and prints the public URL', async () => {
    const result = await invoke(['site', 'publish', '/public', '--spa', '--listing', '--index', 'index.html']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain(`${endpoint}/s/vol/`);

    const publish = requests.slice().reverse().find((r) => r.method === 'PUT' && r.path === '/v1/volumes/vol/sites');
    expect(JSON.parse(publish?.body || '{}')).toMatchObject({ path: '/public', indexDocument: 'index.html', spa: true, directoryListing: true });
  });

  it('shows the published status', async () => {
    const result = await invoke(['--json', 'site', 'status']);
    const status = JSON.parse(result.stdout);
    expect(status.published).toBe(true);
    expect(status.url).toBe(`${endpoint}/s/vol/`);
  });

  it('snapshots then atomically deploys into the existing published root', async () => {
    const dist = join(temporaryPaths[0], 'dist');
    await mkdir(dist);
    await writeFile(join(dist, 'index.html'), '<h1>new</h1>');
    const result = await invoke(['--json', 'site', 'deploy', dist]);
    expect(result.code).toBe(0);
    const output = JSON.parse(result.stdout);
    expect(output.root).toBe('/public');
    expect(output.snapshot.name).toMatch(/^site-deploy-/);

    const snapshotIndex = requests.findIndex((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots');
    const importIndex = requests.findIndex((r) => r.method === 'PUT' && r.path === '/v1/volumes/vol/trees/public?replace=true');
    expect(snapshotIndex).toBeGreaterThanOrEqual(0);
    expect(importIndex).toBeGreaterThan(snapshotIndex);
  });

  it('rolls back through full-volume snapshot restore', async () => {
    const result = await invoke(['site', 'rollback', 'site-deploy-old']);
    expect(result.code).toBe(0);
    expect(requests.some((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots/site-deploy-old/restore')).toBe(true);
  });

  it('unpublishes the site', async () => {
    const result = await invoke(['site', 'unpublish']);
    expect(result.code).toBe(0);
    const del = requests.slice().reverse().find((r) => r.method === 'DELETE' && r.path === '/v1/volumes/vol/sites');
    expect(del).toBeTruthy();
  });
});

describe('share commands', () => {
  it('creates a share link and prints its URL', async () => {
    const result = await invoke(['share', '/report.pdf', '--expires', '1h']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain(`${endpoint}/d/vol/share-abc`);

    const create = requests.slice().reverse().find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/shares');
    expect(JSON.parse(create?.body || '{}')).toMatchObject({ path: '/report.pdf', expiresInSeconds: 3600 });
  });

  it('lists shares as JSON with resolved URLs', async () => {
    const result = await invoke(['--json', 'share', 'list']);
    const shares = JSON.parse(result.stdout);
    expect(shares[0].url).toBe(`${endpoint}/d/vol/share-abc`);
  });

  it('deletes a share', async () => {
    const result = await invoke(['share', 'rm', 'share-abc']);
    expect(result.code).toBe(0);
    const del = requests.slice().reverse().find((r) => r.method === 'DELETE' && r.path === '/v1/volumes/vol/shares/share-abc');
    expect(del).toBeTruthy();
  });
});

async function invoke(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const sink = (chunks: Buffer[]) => new Writable({
    write(chunk, _encoding, callback) { chunks.push(Buffer.from(chunk)); callback(); },
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

async function route(request: IncomingMessage, response: ServerResponse, url: URL): Promise<void> {
  if (url.pathname === '/v1/volumes/vol/sites') {
    if (request.method === 'PUT') {
      return json(response, 200, { pathPrefix: '/public', indexDocument: 'index.html', spa: true, directoryListing: true, cacheControl: null, createdAt: 1 });
    }
    if (request.method === 'GET') {
      return json(response, 200, { published: true, site: { pathPrefix: '/public', indexDocument: 'index.html', spa: true, directoryListing: true, cacheControl: null, createdAt: 1 } });
    }
    if (request.method === 'DELETE') {
      return json(response, 200, { removed: true });
    }
  }
  if (url.pathname === '/v1/volumes/vol/snapshots' && request.method === 'POST') {
    const input = JSON.parse((requests.at(-1)?.body) || '{}');
    return json(response, 201, {
      id: 'snapshot-1', name: input.name, note: input.note ?? null, createdAt: 1, chunkSize: 262144,
      inodeCount: 2, fileCount: 1, directoryCount: 1, symlinkCount: 0, byteCount: 12,
    });
  }
  if (url.pathname === '/v1/volumes/vol/trees/public' && url.searchParams.get('replace') === 'true' && request.method === 'PUT') {
    return json(response, 201, { files: 1, directories: 1, symlinks: 0, bytes: 12 });
  }
  if (url.pathname === '/v1/volumes/vol/snapshots/site-deploy-old/restore' && request.method === 'POST') {
    return json(response, 200, {
      id: 'snapshot-old', name: 'site-deploy-old', note: null, createdAt: 1, chunkSize: 262144,
      inodeCount: 2, fileCount: 1, directoryCount: 1, symlinkCount: 0, byteCount: 12,
    });
  }
  if (url.pathname === '/v1/volumes/vol/shares') {
    if (request.method === 'POST') {
      return json(response, 201, { id: 'share-abc', path: '/report.pdf', expiresAt: 9999999999, cacheControl: null, createdAt: 1 });
    }
    if (request.method === 'GET') {
      return json(response, 200, [{ id: 'share-abc', path: '/report.pdf', expiresAt: 9999999999, cacheControl: null, createdAt: 1 }]);
    }
  }
  if (request.method === 'DELETE' && url.pathname === '/v1/volumes/vol/shares/share-abc') {
    return json(response, 200, { id: 'share-abc', removed: true });
  }
  json(response, 404, { error: { code: 'ENOENT', message: `Unhandled ${request.method} ${url.pathname}` } });
}

function json(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}

async function requestBody(request: IncomingMessage): Promise<string> {
  let body = '';
  for await (const chunk of request) body += chunk.toString();
  return body;
}
