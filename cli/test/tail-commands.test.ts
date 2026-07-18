// ABOUTME: Exercises remote tail line and byte windows through HTTP range reads.
// ABOUTME: Verifies raw output and mutually exclusive CLI option validation.

import { createServer, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

const content = Buffer.from('a\nb\nc\n');
let home: string;
let sessions: SessionManager;

const server = createServer((request, response) => {
  if (request.url?.includes('/changes')) {
    return json(response, { events: [], cursor: 1, latest: 1, oldest: 1, gap: false });
  }
  if (request.method === 'HEAD') {
    response.writeHead(200, { 'Content-Length': String(content.length) }).end();
    return;
  }
  const range = request.headers.range ?? `bytes=0-${content.length - 1}`;
  const suffix = range.match(/^bytes=-(\d+)$/);
  const start = suffix ? Math.max(0, content.length - Number(suffix[1])) : 0;
  response.writeHead(206, {
    'Content-Length': String(content.length - start),
    'Content-Range': `bytes ${start}-${content.length - 1}/${content.length}`,
  }).end(content.subarray(start));
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  home = await mkdtemp(join(tmpdir(), 'airyfs-tail-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint: `http://127.0.0.1:${address.port}`, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('tail command', () => {
  it('prints trailing lines and bytes', async () => {
    expect((await invoke(['tail', '/log', '--lines', '2'])).stdout).toBe('b\nc\n');
    expect((await invoke(['tail', '/log', '--bytes', '2'])).stdout).toBe('c\n');
  });

  it('rejects explicit line and byte windows together', async () => {
    const result = await invoke(['tail', '/log', '--lines', '1', '--bytes', '2']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('mutually exclusive');
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
