// ABOUTME: Exercises deployment-wide volume listing through the CLI.
// ABOUTME: Verifies the registry endpoint and human-readable volume metadata.

import { createServer, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

let endpoint: string;
let home: string;
let sessions: SessionManager;
let requestedPath = '';

const server = createServer((request, response) => {
  requestedPath = request.url ?? '';
  json(response, 200, {
    volumes: [{ name: 'project', chunkSize: 262144, createdAt: 1_700_000_000 }],
    nextCursor: null,
  });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-volume-list-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'selected' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('volume list', () => {
  it('prints registered volume metadata', async () => {
    const result = await invoke(['volume', 'list']);
    expect(result.code).toBe(0);
    expect(requestedPath).toBe('/v1/volumes?limit=1000');
    expect(result.stdout).toContain('project');
    expect(result.stdout).toContain('256 KiB');
  });
});

async function invoke(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const sink = (chunks: Buffer[]) => new Writable({
    write(chunk, _encoding, callback) { chunks.push(Buffer.from(chunk)); callback(); },
  });
  const code = await execute(['node', 'airyfs', '--session', 'test', ...args], {
    sessions, stdin: Readable.from(''), stdout: sink(stdout), stderr: sink(stderr), shellMode: true,
  });
  return { code, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString() };
}

function json(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}
