// ABOUTME: Exercises find, glob, and grep CLI request/output behavior against a mock server.
// ABOUTME: Verifies scoped paths, search options, line formatting, and local limit validation.

import { createServer, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

const bodies: Record<string, unknown>[] = [];
let endpoint: string;
let home: string;
let sessions: SessionManager;

const server = createServer(async (request, response) => {
  if (request.url?.includes('/tree/')) {
    return json(response, 200, {
      root: '/src',
      entries: [
        { path: '/src/lib', name: 'lib', depth: 1, type: 'directory', size: 0 },
        { path: '/src/lib/a.ts', name: 'a.ts', depth: 2, type: 'file', size: 1 },
      ],
      truncated: false,
    });
  }
  if (request.url?.endsWith('/quota')) {
    let body = '';
    for await (const chunk of request) body += chunk.toString();
    if (body) bodies.push(JSON.parse(body) as Record<string, unknown>);
    return json(response, 200, body ? JSON.parse(body) : { bytes: null, inodes: null });
  }
  let body = '';
  for await (const chunk of request) body += chunk.toString();
  const input = JSON.parse(body) as Record<string, unknown>;
  bodies.push(input);
  if (input.mode === 'grep') {
    return json(response, 200, {
      results: [{ path: '/src/a.ts', type: 'file', line: 2, column: 4, text: 'a needle' }],
      truncated: false, scannedEntries: 1, scannedBytes: 9,
    });
  }
  json(response, 200, {
    results: [{ path: '/src/a.ts', type: 'file' }], truncated: false, scannedEntries: 1, scannedBytes: 0,
  });
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind');
  endpoint = `http://127.0.0.1:${address.port}`;
  home = await mkdtemp(join(tmpdir(), 'airyfs-search-cmd-'));
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await rm(home, { recursive: true, force: true });
});

describe('search commands', () => {
  it('sends find and glob searches rooted at remote paths', async () => {
    expect((await invoke(['find', '/src', '--name', 'a'])).stdout).toBe('/src/a.ts\n');
    await invoke(['glob', '**/*.ts', '/src']);
    expect(bodies).toContainEqual({ mode: 'find', path: '/src', pattern: 'a', limit: 100 });
    expect(bodies).toContainEqual({ mode: 'glob', path: '/src', pattern: '**/*.ts', limit: 100 });
  });

  it('formats grep lines and validates limits locally', async () => {
    const result = await invoke(['grep', 'needle', '/src', '--ignore-case']);
    expect(result.stdout).toBe('/src/a.ts:2:4:a needle\n');
    expect(bodies.at(-1)).toMatchObject({ mode: 'grep', ignoreCase: true, regex: false });
    const invalid = await invoke(['find', '/', '--name', 'x', '--limit', '1001']);
    expect(invalid.code).toBe(1);
    expect(invalid.stderr).toContain('between 1 and 1000');
  });

  it('prints an indented directory tree', async () => {
    const result = await invoke(['tree', '/src', '--depth', '2']);
    expect(result.stdout).toBe('/src\nlib/\n  a.ts\n');
  });

  it('shows and configures byte and inode quotas', async () => {
    expect((await invoke(['volume', 'quota'])).stdout).toContain('unlimited');
    const result = await invoke(['volume', 'quota', '--bytes', '1m', '--inodes', '100']);
    expect(result.code).toBe(0);
    expect(bodies.at(-1)).toEqual({ bytes: 1_048_576, inodes: 100 });
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
