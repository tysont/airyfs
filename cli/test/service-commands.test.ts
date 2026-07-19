// ABOUTME: Exercises persistent preview service CLI management against a local mock server.
// ABOUTME: Verifies command, cwd, environment, public visibility, and lifecycle routes.

import { createServer, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

const requests: Array<{ method: string; path: string; body: string }> = [];
let endpoint: string;
let home: string;
let sessions: SessionManager;

const info = {
  name: 'web', command: "npm run 'dev server'", cwd: '/site', env: { NODE_ENV: 'development', EMPTY: '' },
  port: 5000, enabled: true, public: true, createdAt: 1,
};

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  let body = '';
  for await (const chunk of request) body += chunk.toString();
  requests.push({ method: request.method || 'GET', path: `${url.pathname}${url.search}`, body });
  if (url.pathname === '/v1/volumes/vol/services' && request.method === 'POST') return json(response, 201, info);
  if (url.pathname === '/v1/volumes/vol/services' && request.method === 'GET') return json(response, 200, [info]);
  if (url.pathname === '/v1/volumes/vol/services/web/start' && request.method === 'POST') return json(response, 200, info);
  if (url.pathname === '/v1/volumes/vol/services/web/stop' && request.method === 'POST') return json(response, 200, { ...info, enabled: false });
  if (url.pathname === '/v1/volumes/vol/services/web/logs' && request.method === 'GET') {
    const after = Number(url.searchParams.get('after') ?? 0);
    const entries = [
      { seq: 1, stream: 'stdout', data: Buffer.from('ready\n').toString('base64'), timestamp: 1 },
      { seq: 2, stream: 'stderr', data: Buffer.from('warning\n').toString('base64'), timestamp: 2 },
    ].filter((entry) => entry.seq > after);
    return json(response, 200, {
      entries,
      next: entries.at(-1)?.seq ?? null,
      generation: 'gen-1',
      earliestSeq: 1,
      reset: false,
      truncated: false,
    });
  }
  if (url.pathname === '/v1/volumes/vol/services/web' && request.method === 'DELETE') return json(response, 200, info);
  json(response, 404, { error: { code: 'NOT_FOUND', message: 'Unhandled request' } });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-service-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('service commands', () => {
  it('creates a public service with a remote cwd and environment', async () => {
    const result = await invoke([
      'service', 'create', 'web', '--cwd', '/site', '--env', 'NODE_ENV=development', '--env', 'EMPTY=', '--public',
      'npm', 'run', 'dev server',
    ]);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain(`/p/vol/web/`);
    const request = requests.find((entry) => entry.method === 'POST' && entry.path === '/v1/volumes/vol/services');
    expect(JSON.parse(request?.body || '{}')).toEqual({
      name: 'web', command: "npm run 'dev server'", cwd: '/site', env: { NODE_ENV: 'development', EMPTY: '' }, public: true,
    });
  });

  it('lists and manages services', async () => {
    expect((await invoke(['--json', 'service', 'list'])).stdout).toContain('web');
    expect((await invoke(['service', 'stop', 'web'])).code).toBe(0);
    expect((await invoke(['service', 'start', 'web'])).code).toBe(0);
    expect((await invoke(['service', 'delete', 'web'])).code).toBe(0);
  });

  it('prints service logs and forwards the cursor', async () => {
    const result = await invoke(['service', 'logs', 'web', '--after', '0']);
    expect(result.code).toBe(0);
    expect(result.stdout).toBe('ready\n');
    expect(result.stderr).toBe('warning\n');
    expect(requests.some((request) => request.path.endsWith('/services/web/logs?after=0'))).toBe(true);
  });

  it('returns structured logs in JSON mode', async () => {
    const result = await invoke(['--json', 'service', 'logs', 'web']);
    expect(JSON.parse(result.stdout)).toMatchObject({ next: 2, entries: [{ seq: 1 }, { seq: 2 }] });
  });

  it('follows with cursor deduplication until interrupted', async () => {
    const before = requests.filter((request) => request.path.includes('/services/web/logs')).length;
    const run = invoke(['service', 'logs', 'web', '--follow']);
    await waitFor(() => requests.filter((request) => request.path.includes('/services/web/logs')).length >= before + 2);
    process.emit('SIGINT');
    const result = await run;
    expect(result.code).toBe(130);
    expect(result.stdout).toBe('ready\n');
    expect(result.stderr).toBe('warning\n');
    expect(requests.some((request) => request.path.endsWith('/services/web/logs?after=2&generation=gen-1'))).toBe(true);
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

function json(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for service log poll');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
