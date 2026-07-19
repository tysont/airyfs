// ABOUTME: Exercises scoped SQL CLI request construction and human-readable output.
// ABOUTME: Verifies JSON arguments, row tables, truncation notices, and local argument errors.

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
let body: Record<string, unknown> = {};

const server = createServer(async (request, response) => {
  let raw = '';
  for await (const chunk of request) raw += chunk.toString();
  body = JSON.parse(raw) as Record<string, unknown>;
  json(response, 200, {
    columns: ['id', 'body'], rows: [[1, 'hello']], rowsRead: 1001, rowsWritten: 0, truncated: true,
  });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-sql-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('sql command', () => {
  it('sends positional JSON arguments and renders rows', async () => {
    const result = await invoke(['sql', 'SELECT id, body FROM app_notes WHERE id = ?', '--arg', '1']);
    expect(result.code).toBe(0);
    expect(body).toEqual({ sql: 'SELECT id, body FROM app_notes WHERE id = ?', args: [1] });
    expect(result.stdout).toContain('hello');
    expect(result.stdout).toContain('Results truncated at 1,000 rows');
  });

  it('rejects invalid JSON arguments without a request', async () => {
    const result = await invoke(['sql', 'SELECT * FROM app_notes WHERE id = ?', '--arg', 'nope']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Invalid JSON argument: nope');
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
