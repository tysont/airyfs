// ABOUTME: Exercises auth and capability CLI commands against a local mock AiryFS server.
// ABOUTME: Verifies login probing, token persistence, minting output, and revocation requests.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

interface CapturedRequest {
  method: string;
  path: string;
  auth: string | undefined;
  body: string;
}

const requests: CapturedRequest[] = [];
const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  const body = await requestBody(request);
  requests.push({
    method: request.method || 'GET',
    path: `${url.pathname}${url.search}`,
    auth: request.headers.authorization,
    body,
  });
  await route(request, response, url);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-auth-cmd-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe('auth commands', () => {
  it('logs in after a successful probe and stores the token', async () => {
    const result = await invoke(['auth', 'login', 'root-secret']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Logged in to vol as root');
    expect((await sessions.resolve('test')).session.token).toBe('root-secret');

    const probe = requests.slice().reverse().find((r) => r.method === 'GET' && r.path === '/v1/volumes/vol/capabilities');
    expect(probe?.auth).toBe('Bearer root-secret');
  });

  it('reports auth status and never echoes the token', async () => {
    await sessions.setToken('test', 'root-secret');
    const result = await invoke(['--json', 'auth', 'status']);

    const status = JSON.parse(result.stdout);
    expect(status).toMatchObject({ session: 'test', localToken: true, auth: 'root' });
    expect(result.stdout).not.toContain('root-secret');
  });

  it('rejects login when the probe fails and leaves the token unchanged', async () => {
    await sessions.setToken('test', 'root-secret');
    const result = await invoke(['auth', 'login', 'bad-token']);

    expect(result.code).toBe(1);
    expect((await sessions.resolve('test')).session.token).toBe('root-secret');
  });

  it('logs out by clearing the stored token', async () => {
    await sessions.setToken('test', 'root-secret');
    const result = await invoke(['auth', 'logout']);

    expect(result.code).toBe(0);
    expect((await sessions.resolve('test')).session.token).toBeUndefined();
  });

  it('logs in with a volume password and stores the minted token', async () => {
    const result = await invoke(['auth', 'login', '--password', 'hunter2pass']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('password-scoped token');
    expect((await sessions.resolve('test')).session.token).toBe('password-token');

    const login = requests.slice().reverse().find((r) => r.path === '/v1/volumes/vol/auth/login');
    expect(JSON.parse(login?.body || '{}')).toMatchObject({ password: 'hunter2pass' });
  });

  it('sets the volume password via passwd', async () => {
    await sessions.setToken('test', 'root-secret');
    const result = await invoke(['auth', 'passwd', 'a-strong-password']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Set the volume password');
    const set = requests.slice().reverse().find((r) => r.path === '/v1/volumes/vol/auth/password');
    expect(set?.auth).toBe('Bearer root-secret');
    expect(JSON.parse(set?.body || '{}')).toMatchObject({ password: 'a-strong-password' });
  });

  it('rejects a short password before contacting the server', async () => {
    const result = await invoke(['auth', 'passwd', 'short']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('at least 8 characters');
  });
});

describe('session portability', () => {
  it('exports and re-imports a session with its token', async () => {
    await sessions.setToken('test', 'portable-token');
    const exported = await invoke(['session', 'export', 'test']);
    expect(exported.code).toBe(0);
    const blob = exported.stdout.trim();
    expect(blob.startsWith('airyfs1:')).toBe(true);

    const imported = await invoke(['session', 'import', blob, 'copied']);
    expect(imported.code).toBe(0);
    const copied = await sessions.resolve('copied');
    expect(copied.session.endpoint).toBe(endpoint);
    expect(copied.session.volume).toBe('vol');
    expect(copied.session.token).toBe('portable-token');
    await sessions.use('test');
  });
});

describe('capability commands', () => {
  it('mints a capability and prints the token once', async () => {
    await sessions.setToken('test', 'root-secret');
    const result = await invoke([
      'capability', 'create', '--operation', 'read', '--operation', 'write',
      '--path', '/src', '--expires', '30m',
    ]);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Created capability generated-id');
    expect(result.stdout).toContain('minted-token');

    const mint = requests.slice().reverse().find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/capabilities');
    expect(mint?.auth).toBe('Bearer root-secret');
    expect(JSON.parse(mint?.body || '{}')).toEqual({
      operations: ['read', 'write'],
      pathPrefixes: ['/src'],
      expiresInSeconds: 1800,
    });
  });

  it('requires at least one operation', async () => {
    const result = await invoke(['capability', 'create']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('at least one --operation');
  });

  it('revokes a capability by id', async () => {
    await sessions.setToken('test', 'root-secret');
    const result = await invoke(['capability', 'revoke', 'cap-123']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Revoked capability cap-123');
    const revoke = requests.slice().reverse().find((r) => r.method === 'DELETE');
    expect(revoke?.path).toBe('/v1/volumes/vol/capabilities/cap-123');
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

async function route(request: IncomingMessage, response: ServerResponse, url: URL): Promise<void> {
  const auth = request.headers.authorization;
  if (request.method === 'GET' && url.pathname === '/v1/volumes/vol/capabilities') {
    if (auth === 'Bearer bad-token') {
      return json(response, 401, { error: { code: 'INVALID_TOKEN', message: 'Invalid token' } });
    }
    return json(response, 200, { auth: 'root', volume: 'vol' });
  }
  if (request.method === 'POST' && url.pathname === '/v1/volumes/vol/capabilities') {
    return json(response, 201, {
      token: 'minted-token',
      id: 'generated-id',
      volume: 'vol',
      operations: ['read', 'write'],
      pathPrefixes: ['/src'],
      expires: 9999999999,
    });
  }
  if (request.method === 'DELETE' && url.pathname.startsWith('/v1/volumes/vol/capabilities/')) {
    return json(response, 200, { id: url.pathname.split('/').pop(), revoked: true });
  }
  if (request.method === 'POST' && url.pathname === '/v1/volumes/vol/auth/login') {
    return json(response, 201, {
      token: 'password-token', id: 'pw-cap', volume: 'vol',
      operations: ['read', 'write', 'exec'], pathPrefixes: [], expires: 9999999999,
    });
  }
  if (request.method === 'POST' && url.pathname === '/v1/volumes/vol/auth/password') {
    return json(response, 201, { volume: 'vol', authEnabled: true, passwordSet: true });
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
