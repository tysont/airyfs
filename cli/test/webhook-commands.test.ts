// ABOUTME: Exercises webhook create/list/delete CLI commands against a local mock server.
// ABOUTME: Verifies path/event requests and that signing secrets appear only on creation.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
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

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  let body = '';
  for await (const chunk of request) body += chunk.toString();
  requests.push({ method: request.method || 'GET', path: url.pathname, body });

  if (url.pathname === '/v1/volumes/vol/webhooks' && request.method === 'POST') {
    return json(response, 201, {
      id: 'hook-1', url: 'https://hooks.example.test/', pathPrefix: '/src',
      events: ['create', 'rename'], createdAt: 1, secret: 'signing-secret',
    });
  }
  if (url.pathname === '/v1/volumes/vol/webhooks' && request.method === 'GET') {
    return json(response, 200, [{
      id: 'hook-1', url: 'https://hooks.example.test/', pathPrefix: '/src',
      events: ['create', 'rename'], createdAt: 1,
    }]);
  }
  if (url.pathname === '/v1/volumes/vol/webhooks/hook-1' && request.method === 'DELETE') {
    return json(response, 200, { id: 'hook-1', removed: true });
  }
  json(response, 404, { error: { code: 'NOT_FOUND', message: 'Unhandled request' } });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-webhook-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('webhook commands', () => {
  it('creates a path- and event-filtered subscription and prints its secret', async () => {
    const result = await invoke(['webhook', 'create', 'https://hooks.example.test/', '--path', '/src', '--event', 'create', '--event', 'rename']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('signing-secret');
    const request = requests.find((entry) => entry.method === 'POST');
    expect(JSON.parse(request?.body || '{}')).toEqual({
      url: 'https://hooks.example.test/', pathPrefix: '/src', events: ['create', 'rename'],
    });
  });

  it('lists subscriptions without exposing secrets', async () => {
    const result = await invoke(['--json', 'webhook', 'list']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('hook-1');
    expect(result.stdout).not.toContain('signing-secret');
  });

  it('deletes a subscription and validates event names locally', async () => {
    expect((await invoke(['webhook', 'delete', 'hook-1'])).code).toBe(0);
    const invalid = await invoke(['webhook', 'create', 'https://hooks.example.test/', '--event', 'unknown']);
    expect(invalid.code).toBe(1);
    expect(invalid.stderr).toContain('Unknown webhook event');
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
