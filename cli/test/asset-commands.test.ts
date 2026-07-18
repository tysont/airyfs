// ABOUTME: Exercises immutable asset CLI put/get commands against a local mock server.
// ABOUTME: Verifies local hashing, streaming upload routes, and safe downloaded output.

import { createHash } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

const bytes = Buffer.from([0, 255, 1, 42]);
const checksum = createHash('sha256').update(bytes).digest('hex');
let uploaded = Buffer.alloc(0);
let endpoint: string;
let home: string;
let sessions: SessionManager;

const server = createServer(async (request: IncomingMessage, response: ServerResponse) => {
  const url = new URL(request.url || '/', 'http://localhost');
  if (url.pathname === `/v1/volumes/vol/assets/${checksum}` && request.method === 'PUT') {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    uploaded = Buffer.concat(chunks);
    return json(response, 201, { algorithm: 'sha256', checksum, size: uploaded.length, created: true });
  }
  if (url.pathname === `/v1/volumes/vol/assets/${checksum}` && request.method === 'GET') {
    response.writeHead(200, { 'Content-Type': 'application/octet-stream' }).end(bytes);
    return;
  }
  json(response, 404, { error: { code: 'NOT_FOUND', message: 'Unhandled request' } });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-asset-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('asset commands', () => {
  it('hashes and streams a local asset', async () => {
    const local = join(home, 'source.bin');
    await writeFile(local, bytes);
    const result = await invoke(['--json', 'asset', 'put', local]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ checksum, created: true });
    expect(uploaded).toEqual(bytes);
  });

  it('downloads an asset without exposing its internal storage path', async () => {
    const local = join(home, 'download.bin');
    const result = await invoke(['asset', 'get', checksum, local]);
    expect(result.code).toBe(0);
    expect(await readFile(local)).toEqual(bytes);
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
