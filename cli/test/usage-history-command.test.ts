// ABOUTME: Exercises human-readable and JSON output for usage history.
// ABOUTME: Verifies CLI pagination arguments reach the per-volume history endpoint.

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
let requestedUrl = '';

const server = createServer((request, response) => {
  requestedUrl = request.url ?? '';
  json(response, {
    samples: [{ sampledAt: 1_700_000_000, bytesUsed: 1024, inodes: 2, sqliteBytes: 4096, quotaBytes: null, quotaInodes: 10 }],
    next: 1_699_999_700,
  });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-usage-history-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('usage-history command', () => {
  it('renders samples and forwards pagination', async () => {
    const result = await invoke(['usage-history', '--before', '1700000300', '--limit', '10']);
    expect(result.code).toBe(0);
    expect(requestedUrl).toBe('/v1/volumes/vol/usage-history?before=1700000300&limit=10');
    expect(result.stdout).toContain('1.0 KiB');
    expect(result.stdout).toContain('4.0 KiB');
    expect(result.stdout).toContain('Next cursor: 1699999700');
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

function json(response: ServerResponse, value: unknown): void {
  response.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}
