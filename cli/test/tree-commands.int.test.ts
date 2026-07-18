// ABOUTME: Integration tests for `airyfs push` and `airyfs pull` against a mock AiryFS server.
// ABOUTME: Verifies archive streaming, replace/refuse-overwrite, traversal defense, and auth headers.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdir, mkdtemp, readFile, readlink, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';
import { createLocalTreeStream, extractLocalTree, type TreeSummary } from '../src/api/archive.js';

interface CapturedPush {
  authorization?: string;
  replace: boolean;
  extractedTo: string;
  summary: TreeSummary;
}

const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;
let lastPush: CapturedPush | null = null;
let lastGetAuth: string | undefined;
let serveArchiveFrom: string | null = null;
let serveMalformed = false;

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  await route(request, response, url);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-tree-home-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
  await sessions.setToken('test', 'secret-token');
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

async function scratch(prefix: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), prefix));
  temporaryPaths.push(dir);
  return dir;
}

async function seed(dir: string): Promise<void> {
  await mkdir(join(dir, 'nested'), { recursive: true });
  await writeFile(join(dir, 'a.txt'), 'hello world');
  await writeFile(join(dir, 'nested', 'bin.dat'), Buffer.from([0, 255, 1, 128, 42, 7]));
  await symlink('../a.txt', join(dir, 'nested', 'link'));
}

describe('push', () => {
  it('streams a local directory tree and reports a summary', async () => {
    const source = await scratch('airyfs-push-src-');
    await seed(source);

    const result = await invoke(['--json', 'push', source, 'app']);
    expect(result.code).toBe(0);
    const summary = JSON.parse(result.stdout);
    expect(summary).toMatchObject({ ok: true, files: 2, directories: 1, symlinks: 1, bytes: 17, remote: '/app' });

    expect(lastPush?.replace).toBe(false);
    expect(lastPush?.authorization).toBe('Bearer secret-token');
    expect(await readFile(join(lastPush!.extractedTo, 'a.txt'), 'utf8')).toBe('hello world');
    expect(new Uint8Array(await readFile(join(lastPush!.extractedTo, 'nested', 'bin.dat')))).toEqual(
      Uint8Array.from([0, 255, 1, 128, 42, 7]),
    );
    expect(await readlink(join(lastPush!.extractedTo, 'nested', 'link'))).toBe('../a.txt');
  });

  it('passes replace=true through with --replace', async () => {
    const source = await scratch('airyfs-push-src-');
    await seed(source);
    const result = await invoke(['push', source, 'app', '--replace']);
    expect(result.code).toBe(0);
    expect(lastPush?.replace).toBe(true);
  });

  it('defaults the remote target to the local basename', async () => {
    const parent = await scratch('airyfs-push-parent-');
    const source = join(parent, 'myproject');
    await mkdir(source, { recursive: true });
    await writeFile(join(source, 'x.txt'), 'x');

    const result = await invoke(['--json', 'push', source]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout).remote).toBe('/myproject');
  });

  it('refuses to push a file', async () => {
    const dir = await scratch('airyfs-push-file-');
    const file = join(dir, 'plain.txt');
    await writeFile(file, 'nope');
    const result = await invoke(['push', file]);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('not a directory');
  });
});

describe('pull', () => {
  it('downloads and extracts a remote tree, preserving symlinks and bytes', async () => {
    const remoteSource = await scratch('airyfs-pull-remote-');
    await seed(remoteSource);
    serveArchiveFrom = remoteSource;

    const destParent = await scratch('airyfs-pull-dest-');
    const dest = join(destParent, 'out');

    const result = await invoke(['--json', 'pull', 'app', dest]);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, files: 2, symlinks: 1, local: dest });
    expect(lastGetAuth).toBe('Bearer secret-token');
    expect(await readFile(join(dest, 'a.txt'), 'utf8')).toBe('hello world');
    expect(new Uint8Array(await readFile(join(dest, 'nested', 'bin.dat')))).toEqual(
      Uint8Array.from([0, 255, 1, 128, 42, 7]),
    );
    expect(await readlink(join(dest, 'nested', 'link'))).toBe('../a.txt');
  });

  it('refuses to overwrite an existing local directory without --force', async () => {
    const remoteSource = await scratch('airyfs-pull-remote-');
    await seed(remoteSource);
    serveArchiveFrom = remoteSource;

    const destParent = await scratch('airyfs-pull-dest-');
    const dest = join(destParent, 'existing');
    await mkdir(dest, { recursive: true });
    await writeFile(join(dest, 'keep.txt'), 'keep');

    const refused = await invoke(['pull', 'app', dest]);
    expect(refused.code).toBe(1);
    expect(refused.stderr).toContain('already exists');
    expect(await readFile(join(dest, 'keep.txt'), 'utf8')).toBe('keep');

    const forced = await invoke(['pull', 'app', dest, '--force']);
    expect(forced.code).toBe(0);
    expect(await readFile(join(dest, 'a.txt'), 'utf8')).toBe('hello world');
    await expect(readFile(join(dest, 'keep.txt'), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('rejects a malformed/traversal archive and leaves no local target', async () => {
    serveArchiveFrom = null;
    serveMalformed = true;
    const destParent = await scratch('airyfs-pull-dest-');
    const dest = join(destParent, 'victim');

    const result = await invoke(['pull', 'app', dest]);
    serveMalformed = false;

    expect(result.code).toBe(1);
    expect(result.stderr).toMatch(/Unsafe path|archive/i);
    await expect(readFile(join(dest, 'a.txt'), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
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

function streamFromBytes(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

const encoder = new TextEncoder();
function frame(obj: unknown): Uint8Array {
  const json = encoder.encode(JSON.stringify(obj));
  const out = new Uint8Array(4 + json.byteLength);
  new DataView(out.buffer).setUint32(0, json.byteLength, false);
  out.set(json, 4);
  return out;
}
const MAGIC = Uint8Array.from([...'AIRYFS'].map((c) => c.charCodeAt(0)).concat(1));

async function rawBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

async function route(request: IncomingMessage, response: ServerResponse, url: URL): Promise<void> {
  if (request.method === 'PUT' && url.pathname.startsWith('/v1/volumes/vol/trees/')) {
    const body = await rawBody(request);
    const extractedTo = await mkdtemp(join(tmpdir(), 'airyfs-server-recv-'));
    temporaryPaths.push(extractedTo);
    const summary = await extractLocalTree(streamFromBytes(new Uint8Array(body)), extractedTo);
    lastPush = {
      authorization: request.headers.authorization,
      replace: url.searchParams.get('replace') === 'true',
      extractedTo,
      summary,
    };
    response.writeHead(201, { 'Content-Type': 'application/json' }).end(JSON.stringify(summary));
    return;
  }

  if (request.method === 'GET' && url.pathname.startsWith('/v1/volumes/vol/trees/')) {
    lastGetAuth = request.headers.authorization;
    response.writeHead(200, { 'Content-Type': 'application/x-airyfs-archive' });
    if (serveMalformed) {
      const bytes = Buffer.concat([Buffer.from(MAGIC), Buffer.from(frame({ t: 'd', p: '../evil' })), Buffer.alloc(4)]);
      response.end(bytes);
      return;
    }
    if (!serveArchiveFrom) {
      response.end();
      return;
    }
    await pipeline(createLocalTreeStream(serveArchiveFrom), response);
    return;
  }

  response.writeHead(404, { 'Content-Type': 'application/json' })
    .end(JSON.stringify({ error: { code: 'ENOENT', message: `Unhandled ${request.method} ${url.pathname}` } }));
}
