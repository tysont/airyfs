// ABOUTME: Exercises soft deletion, trash listing, and undo through the CLI.
// ABOUTME: Verifies recoverable deletion output and the explicit permanent query flag.

import { createServer, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

const entry = { id: 'trash-1', originalPath: '/note', trashPath: '/.airyfs-trash/trash-1', type: 'file', size: 3, deletedAt: 1 };
const urls: string[] = [];
let home: string;
let sessions: SessionManager;

const server = createServer((request, response) => {
  urls.push(request.url ?? '');
  if (request.url?.includes('/directories')) return json(response, [{ name: 'note', type: 'file', size: 3, ino: 2, mode: 0, nlink: 1, uid: 0, gid: 0, atime: 1, mtime: 1, ctime: 1 }]);
  if (request.url?.endsWith('/trash')) return json(response, [entry]);
  if (request.url?.endsWith('/trash/undo')) return json(response, { ...entry, restoredPath: '/note' });
  if (request.method === 'DELETE' && request.url?.includes('/files/')) {
    if (request.url.includes('permanent=true')) return response.writeHead(204).end();
    return json(response, entry);
  }
  json(response, entry);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  home = await mkdtemp(join(tmpdir(), 'airyfs-trash-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint: `http://127.0.0.1:${address.port}`, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('trash commands', () => {
  it('soft deletes, lists trash, and restores the latest entry', async () => {
    expect((await invoke(['rm', '/note'])).stdout).toContain('Moved /note to trash');
    expect((await invoke(['trash', 'list'])).stdout).toContain('/note');
    expect((await invoke(['undo'])).stdout).toContain('Restored /note');
  });

  it('requests permanent deletion explicitly', async () => {
    expect((await invoke(['rm', '/note', '--permanent'])).stdout).toContain('Permanently removed');
    expect(urls.some((url) => url.includes('permanent=true'))).toBe(true);
  });
});

async function invoke(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const sink = (chunks: Buffer[]) => new Writable({ write(chunk, _encoding, callback) { chunks.push(Buffer.from(chunk)); callback(); } });
  const code = await execute(['node', 'airyfs', '--session', 'test', ...args], {
    sessions, stdin: Readable.from(''), stdout: sink(stdout), stderr: sink(stderr), shellMode: true,
  });
  return { code, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString() };
}

function json(response: ServerResponse, value: unknown): void {
  response.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}
