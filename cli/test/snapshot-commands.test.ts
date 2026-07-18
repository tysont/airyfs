// ABOUTME: Exercises snapshot CLI commands against a local mock AiryFS server.
// ABOUTME: Verifies routes/bodies, JSON/text rendering, confirmation guards, and root-only clone errors.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

interface CapturedRequest {
  method: string;
  path: string;
  body: string;
}

const requests: CapturedRequest[] = [];
const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;

function info(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'sid-1', name: 'nightly', note: null, createdAt: 1_700_000_000, chunkSize: 262144,
    inodeCount: 4, fileCount: 2, directoryCount: 3, symlinkCount: 1, byteCount: 17, ...overrides,
  };
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  const body = await requestBody(request);
  requests.push({ method: request.method || 'GET', path: `${url.pathname}${url.search}`, body });
  await route(request, response, url, body);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-snap-cmd-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

beforeEach(() => {
  requests.length = 0;
});

describe('snapshot create', () => {
  it('creates a snapshot with a name and note and reports metadata', async () => {
    const result = await invoke(['--json', 'snapshot', 'create', 'nightly', '--note', 'before refactor']);

    expect(result.code).toBe(0);
    const created = JSON.parse(result.stdout);
    expect(created).toMatchObject({ ok: true, name: 'nightly', directoryCount: 3, symlinkCount: 1 });

    const post = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots');
    expect(JSON.parse(post?.body || '{}')).toEqual({ name: 'nightly', note: 'before refactor' });
  });

  it('omits the name so the server generates a default', async () => {
    const result = await invoke(['snapshot', 'create']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Created snapshot snap-20260101-000000-abcdef');
    const post = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots');
    expect(JSON.parse(post?.body || '{}')).toEqual({});
  });

  it('accepts the snap alias', async () => {
    const result = await invoke(['snap', 'create', 'aliased']);
    expect(result.code).toBe(0);
    const post = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots');
    expect(JSON.parse(post?.body || '{}')).toEqual({ name: 'aliased' });
  });
});

describe('snapshot list', () => {
  it('renders a table with name/time/files/size', async () => {
    const result = await invoke(['snapshot', 'ls']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('nightly');
    expect(result.stdout).toContain('Files');
    expect(result.stdout).toContain('Size');
    expect(requests.some((r) => r.method === 'GET' && r.path === '/v1/volumes/vol/snapshots')).toBe(true);
  });

  it('emits a JSON array of snapshot DTOs', async () => {
    const result = await invoke(['--json', 'snapshot', 'list']);
    const list = JSON.parse(result.stdout);
    expect(Array.isArray(list)).toBe(true);
    expect(list[0]).toMatchObject({ name: 'nightly', directoryCount: 3, byteCount: 17 });
  });
});

describe('snapshot diff', () => {
  it('renders concise status/path lines by default and defaults against=live', async () => {
    const result = await invoke(['snapshot', 'diff', 'sid-1']);
    expect(result.code).toBe(0);
    expect(result.stdout).toBe('A /added.txt\nD /gone.txt\nM /dir/changed.txt\n');
    const diff = requests.find((r) => r.path.startsWith('/v1/volumes/vol/snapshots/sid-1/diff'));
    expect(diff?.path).toBe('/v1/volumes/vol/snapshots/sid-1/diff?against=live');
  });

  it('passes an explicit against target and emits JSON when asked', async () => {
    const result = await invoke(['--json', 'snapshot', 'diff', 'sid-1', 'other']);
    const entries = JSON.parse(result.stdout);
    expect(entries).toEqual([
      { path: '/added.txt', change: 'added', kind: 'file' },
      { path: '/gone.txt', change: 'removed', kind: 'file' },
      { path: '/dir/changed.txt', change: 'modified', kind: 'file' },
    ]);
    const diff = requests.find((r) => r.path.startsWith('/v1/volumes/vol/snapshots/sid-1/diff'));
    expect(diff?.path).toBe('/v1/volumes/vol/snapshots/sid-1/diff?against=other');
  });
});

describe('snapshot restore', () => {
  it('requires --force inside the shell (confirmation guard)', async () => {
    const result = await invoke(['snapshot', 'restore', 'sid-1']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('restore --force');
    expect(requests.some((r) => r.path.includes('/restore'))).toBe(false);
  });

  it('restores with --force', async () => {
    const result = await invoke(['snapshot', 'restore', 'sid-1', '--force']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Restored vol from snapshot nightly');
    const restore = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots/sid-1/restore');
    expect(restore).toBeDefined();
  });
});

describe('snapshot delete', () => {
  it('requires --force inside the shell (confirmation guard)', async () => {
    const result = await invoke(['snapshot', 'rm', 'sid-1']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('delete --force');
    expect(requests.some((r) => r.method === 'DELETE')).toBe(false);
  });

  it('deletes with --force via the rm alias', async () => {
    const result = await invoke(['snapshot', 'rm', 'sid-1', '--force']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Deleted snapshot nightly');
    const del = requests.find((r) => r.method === 'DELETE' && r.path === '/v1/volumes/vol/snapshots/sid-1');
    expect(del).toBeDefined();
  });
});

describe('snapshot clone', () => {
  it('clones into a target volume with --to', async () => {
    const result = await invoke(['snapshot', 'clone', 'sid-1', '--to', 'backup']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Cloned snapshot sid-1 to volume backup');
    const clone = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/snapshots/sid-1/clone');
    expect(JSON.parse(clone?.body || '{}')).toEqual({ targetVolume: 'backup' });
  });

  it('requires --to', async () => {
    const result = await invoke(['snapshot', 'clone', 'sid-1']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('--to');
  });

  it('surfaces a root-only clone authorization error', async () => {
    const result = await invoke(['snapshot', 'clone', 'forbidden', '--to', 'backup']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Only root or auth-disabled');
  });
});

async function invoke(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const sink = (chunks: Buffer[]) => new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.from(chunk));
      callback();
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

async function route(request: IncomingMessage, response: ServerResponse, url: URL, body: string): Promise<void> {
  const base = '/v1/volumes/vol/snapshots';
  if (request.method === 'POST' && url.pathname === base) {
    const parsed = JSON.parse(body || '{}') as { name?: string; note?: string };
    const name = parsed.name ?? 'snap-20260101-000000-abcdef';
    return json(response, 201, info({ name, note: parsed.note ?? null }));
  }
  if (request.method === 'GET' && url.pathname === base) {
    return json(response, 200, [info()]);
  }
  if (request.method === 'GET' && url.pathname === `${base}/sid-1/diff`) {
    return json(response, 200, [
      { path: '/added.txt', change: 'added', kind: 'file' },
      { path: '/gone.txt', change: 'removed', kind: 'file' },
      { path: '/dir/changed.txt', change: 'modified', kind: 'file' },
    ]);
  }
  if (request.method === 'POST' && url.pathname === `${base}/sid-1/restore`) {
    return json(response, 200, info());
  }
  if (request.method === 'POST' && url.pathname === `${base}/sid-1/clone`) {
    return json(response, 200, { files: 2, directories: 3, symlinks: 1, bytes: 17 });
  }
  if (request.method === 'POST' && url.pathname === `${base}/forbidden/clone`) {
    return json(response, 403, {
      error: { code: 'FORBIDDEN', message: 'Only root or auth-disabled callers may clone across volumes' },
    });
  }
  if (request.method === 'DELETE' && url.pathname === `${base}/sid-1`) {
    return json(response, 200, info());
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
