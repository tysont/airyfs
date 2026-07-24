// ABOUTME: Exercises CLI commands end to end against a local mock AiryFS HTTP server.
// ABOUTME: Covers session cwd, filesystem routes, streaming transfers, and exec exit codes.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
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
  body: string;
}

const requests: CapturedRequest[] = [];
const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;
let transientWarmFailures = 0;
let warmExecAttempts = 0;
let retryCommandAttempts = 0;
let ambiguousCommandAttempts = 0;
let busyCommandAttempts = 0;
let transportCommandAttempts = 0;
const cancelRequests: string[] = [];
let hangingResponse: ServerResponse | null = null;
let hangingId = '';
let nextJobId = 1;
const jobs = new Map<string, {
  command: string;
  status: 'running' | 'succeeded' | 'failed' | 'canceled';
  exitCode: number | null;
  logs: Array<{ seq: number; stream: 'stdout' | 'stderr'; data: string; timestamp: number }>;
}>();

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  const body = await requestBody(request);
  requests.push({ method: request.method || 'GET', path: `${url.pathname}${url.search}`, body });
  await route(request, response, url, body);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-commands-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe('commands', () => {
  it('lists directories and persists the remote cwd', async () => {
    const listed = await invoke(['--no-color', 'ls']);
    expect(listed.code).toBe(0);
    expect(listed.stdout).toContain('src/');

    expect((await invoke(['cd', 'src'])).code).toBe(0);
    const pwd = await invoke(['pwd']);
    expect(pwd.stdout.trim()).toBe('/src');
  });

  it('resolves relative mkdir and metadata requests against cwd', async () => {
    const result = await invoke(['mkdir', 'build']);

    expect(result.code).toBe(0);
    expect(requests).toContainEqual({
      method: 'PUT', path: '/v1/volumes/vol/directories/src/build', body: '',
    });
  });

  it('streams upload and download bodies', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'airyfs-transfer-'));
    temporaryPaths.push(directory);
    const source = join(directory, 'source.txt');
    const destination = join(directory, 'destination.txt');
    await writeFile(source, 'upload-body');

    expect((await invoke(['put', source, 'uploaded.txt'])).code).toBe(0);
    expect(requests).toContainEqual({
      method: 'PUT', path: '/v1/volumes/vol/files/src/uploaded.txt', body: 'upload-body',
    });

    expect((await invoke(['get', 'download.txt', destination])).code).toBe(0);
    expect(await readFile(destination, 'utf8')).toBe('download-body');
  });

  it('upload dispatches a file to a streaming put', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'airyfs-upload-'));
    temporaryPaths.push(directory);
    const source = join(directory, 'note.txt');
    await writeFile(source, 'upload-file');

    const result = await invoke(['upload', source, '/note.txt']);
    expect(result.code).toBe(0);
    expect(requests).toContainEqual({
      method: 'PUT', path: '/v1/volumes/vol/files/note.txt', body: 'upload-file',
    });
  });

  it('upload dispatches a directory to a tree push with -r', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'airyfs-uploadtree-'));
    temporaryPaths.push(directory);
    await writeFile(join(directory, 'a.txt'), 'a');

    const result = await invoke(['upload', '-r', directory, '/tree']);
    expect(result.code).toBe(0);
    expect(requests.some((request) =>
      request.method === 'PUT' && request.path.startsWith('/v1/volumes/vol/trees/tree'))).toBe(true);
  });

  it('upload refuses a directory without -r', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'airyfs-uploadguard-'));
    temporaryPaths.push(directory);

    const result = await invoke(['upload', directory, '/tree']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('is a directory');
  });

  it('download dispatches a file to get', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'airyfs-download-'));
    temporaryPaths.push(directory);
    const destination = join(directory, 'out.txt');

    const result = await invoke(['download', '/dl/file.txt', destination]);
    expect(result.code).toBe(0);
    expect(await readFile(destination, 'utf8')).toBe('file-body');
  });

  it('download refuses a directory without -r', async () => {
    const result = await invoke(['download', '/dl/nested']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('is a directory');
  });

  it('download rejects --resume on a directory', async () => {
    const result = await invoke(['download', '-r', '--resume', '/dl/nested']);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain('applies to files');
  });

  it('does not close shared stdout after cat', async () => {
    const chunks: Buffer[] = [];
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });

    const code = await execute(['node', 'airyfs', '--session', 'test', 'cat', 'cat.txt'], {
      sessions,
      stdin: Readable.from(''),
      stdout,
      stderr: stdout,
      shellMode: true,
    });
    stdout.write('after-cat');

    expect(code).toBe(0);
    expect(stdout.writableEnded).toBe(false);
    expect(Buffer.concat(chunks).toString()).toBe('cat-bodyafter-cat');
  });

  it('reads leading lines without starting an exec', async () => {
    const before = requests.length;
    const result = await invoke(['head', '-n', '2', '/head.txt']);
    expect(result).toMatchObject({ code: 0, stdout: 'one\ntwo\n' });
    expect(requests.slice(before).some((request) => request.path.endsWith('/exec'))).toBe(false);
  });

  it('dispatches direct metadata, link, usage, and directory primitives', async () => {
    expect((await invoke(['touch', '/note.txt'])).code).toBe(0);
    expect((await invoke(['chmod', '640', '/note.txt'])).code).toBe(0);
    expect((await invoke(['ln', '/note.txt', '/linked.txt'])).code).toBe(0);
    expect((await invoke(['lstat', '/note.txt'])).code).toBe(0);
    expect((await invoke(['du', '/'])).stdout).toContain('11\t/');
    expect((await invoke(['file', '/text.txt'])).stdout).toContain('/text.txt: text');
    expect((await invoke(['rmdir', '/empty'])).code).toBe(0);

    const operationBodies = requests
      .filter((request) => request.path.includes('/operations/'))
      .map((request) => [request.path.split('/').at(-1), JSON.parse(request.body)]);
    expect(operationBodies).toEqual(expect.arrayContaining([
      ['touch', { path: '/note.txt' }],
      ['chmod', { path: '/note.txt', mode: 0o640 }],
      ['link', { existing: '/note.txt', path: '/linked.txt' }],
    ]));
    expect(requests).toContainEqual({ method: 'DELETE', path: '/v1/volumes/vol/directories/empty?permanent=true', body: '' });
  });

  it('buffers a bounded stdin append into the direct operation', async () => {
    const result = await invokeOneShot(['append', '/log.bin'], 'more');
    expect(result.code).toBe(0);
    expect(requests).toContainEqual({
      method: 'POST',
      path: '/v1/volumes/vol/operations/append',
      body: JSON.stringify({ path: '/log.bin', data: Buffer.from('more').toString('base64') }),
    });
  });

  it('patches a file range in place with write --offset', async () => {
    const result = await invokeOneShot(['write', '--offset', '3', '/patch.bin'], 'BBBB');
    expect(result.code).toBe(0);
    expect(requests).toContainEqual({
      method: 'PATCH', path: '/v1/volumes/vol/files/patch.bin?offset=3', body: 'BBBB',
    });
  });

  it('feeds stdin to a command through the transient path with --stdin-file', async () => {
    const result = await invokeOneShot(['exec', '--no-stream', '--stdin-file', '-', 'cat'], 'piped stdin');
    expect(result.code).toBe(7);
    const exec = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/exec');
    expect(JSON.parse(exec?.body || '{}').stdin).toBe(Buffer.from('piped stdin').toString('base64'));
  });

  it('executes in the session cwd and propagates the remote exit code', async () => {
    const result = await invoke(['exec', '--no-stream', 'git', 'status']);

    expect(result.code).toBe(7);
    expect(result.stdout).toContain('remote stdout');
    const exec = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/jobs');
    expect(JSON.parse(exec?.body || '{}').command).toBe('cd -- /volume/src && git status');
  });

  it('does not optimize paths whose shell meaning differs', async () => {
    const before = requests.length;
    const result = await invoke(['exec', 'cat', '../cat.txt']);

    expect(result).toMatchObject({ code: 7, stdout: 'remote stdout\n' });
    expect(requests.slice(before).some((request) => request.path.endsWith('/jobs'))).toBe(true);
  });

  it('routes a safe relative read-only exec command directly', async () => {
    await invoke(['cd', '/']);
    const before = requests.length;
    const result = await invoke(['exec', 'cat', 'cat.txt']);

    expect(result).toMatchObject({ code: 0, stdout: 'cat-body' });
    expect(requests.slice(before).some((request) => request.path.endsWith('/jobs'))).toBe(false);
    expect(requests.slice(before)).toContainEqual({ method: 'GET', path: '/v1/volumes/vol/files/cat.txt', body: '' });
    await invoke(['cd', 'src']);
  });

  it('can force a recognized command through durable Container execution', async () => {
    const before = requests.length;
    const result = await invoke(['exec', '--container', 'cat', 'cat.txt']);

    expect(result.code).toBe(7);
    const submission = requests.slice(before).find((request) => request.path.endsWith('/jobs'));
    expect(JSON.parse(submission?.body || '{}').command).toBe('cd -- /volume/src && cat cat.txt');
  });

  it('permanently deletes the volume with --force', async () => {
    const before = requests.length;
    const result = await invoke(['volume', 'delete', '--force']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Deleted volume vol');
    expect(requests.slice(before)).toContainEqual({ method: 'DELETE', path: '/v1/volumes/vol', body: '' });
  });

  it('warms the Container with a no-op command', async () => {
    const result = await invoke(['warm', '--timeout', '1s']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Container is warm for vol');
    const warm = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/exec');
    expect(JSON.parse(warm?.body || '{}').command).toBe(':');
  });

  it('retries durable submission with one command', async () => {
    transientWarmFailures = 1;
    warmExecAttempts = 0;
    retryCommandAttempts = 0;

    const result = await invoke(['exec', '--no-stream', '--timeout', '2s', 'retry-command']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('recovered');
    expect(warmExecAttempts).toBe(2);
    expect(retryCommandAttempts).toBe(1);
  });

  it('bounds retries when durable submission keeps returning gateway errors', async () => {
    ambiguousCommandAttempts = 0;

    const result = await invoke(['exec', '--timeout', '2s', 'ambiguous-command']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Bad Gateway');
    expect(ambiguousCommandAttempts).toBe(3);
  });

  it('bounds retries when durable submission keeps losing transport', async () => {
    transportCommandAttempts = 0;

    const result = await invoke(['exec', '--timeout', '2s', 'transport-command']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Could not reach');
    expect(transportCommandAttempts).toBe(3);
  });

  it('retries a user command only when the server reports it was not admitted', async () => {
    busyCommandAttempts = 0;

    const result = await invoke(['exec', '--no-stream', '--timeout', '2s', 'busy-command']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('admitted');
    expect(busyCommandAttempts).toBe(2);
  });

  it('does not wait after EXEC_BUSY with --no-wait', async () => {
    busyCommandAttempts = 0;

    const result = await invoke(['exec', '--no-wait', 'busy-command']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('already running');
    expect(busyCommandAttempts).toBe(1);
  });

  it('passes command options through exec after the first positional argument', async () => {
    await invoke(['exec', '--no-stream', 'tool', '--json', '--timeout', 'remote-value']);

    const exec = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/jobs');
    expect(JSON.parse(exec?.body || '{}').command).toBe(
      "cd -- /volume/src && tool --json --timeout remote-value",
    );
  });

  it('streams live output and adopts the remote exit code by default', async () => {
    const result = await invoke(['exec', 'stream-cmd']);

    expect(result.code).toBe(5);
    expect(result.stdout).toContain('remote stream');
    expect(result.stderr).toContain('remote warn');
    const streamRequest = requests.slice().reverse()
      .find((request) => request.path === '/v1/volumes/vol/jobs');
    expect(JSON.parse(streamRequest?.body || '{}').command).toBe('cd -- /volume/src && stream-cmd');
  });

  it('decodes binary stdout bytes straight to the output stream', async () => {
    const result = await invokeBinary(['exec', 'stream-binary']);

    expect(result.code).toBe(0);
    expect(Uint8Array.from(result.stdout)).toEqual(Uint8Array.from([0, 255, 10, 13, 42, 128]));
  });

  it('keeps global --json buffered as a single ExecResult object', async () => {
    const before = requests.length;
    const result = await invoke(['--json', 'exec', 'git', 'status']);

    expect(result.code).toBe(7);
    expect(JSON.parse(result.stdout)).toMatchObject({ exitCode: 7, stdout: 'remote stdout\n', stderr: '' });
    const streamRequest = requests.slice(before)
      .find((request) => request.path === '/v1/volumes/vol/jobs');
    expect(streamRequest).toBeDefined();
  });

  it('cancels a streaming command once on interrupt and removes the SIGINT listener', async () => {
    cancelRequests.length = 0;
    hangingResponse = null;
    const baseline = process.listenerCount('SIGINT');

    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    const run = execute(['node', 'airyfs', '--session', 'test', 'exec', 'stream-hang'], {
      sessions,
      stdin: Readable.from(''),
      stdout: sink(stdout),
      stderr: sink(stderr),
      shellMode: true,
    });

    // Wait for the server to send the start event and the client to record its id.
    const added = await waitForNewSigintListener(baseline);
    await waitFor(() => hangingResponse !== null);
    await delay(50);

    added();
    const code = await run;

    // cancelExec is fire-and-forget, so the POST may still be in flight here.
    await waitFor(() => cancelRequests.length > 0);
    expect(code).toBe(130);
    expect(cancelRequests).toEqual(['hang-1']);
    expect(process.listenerCount('SIGINT')).toBe(baseline);
  });

  it('removes the SIGINT listener after a normal streaming exec', async () => {
    const baseline = process.listenerCount('SIGINT');
    const result = await invoke(['exec', 'stream-cmd']);
    expect(result.code).toBe(5);
    expect(process.listenerCount('SIGINT')).toBe(baseline);
  });

  it('emits structured errors in JSON mode', async () => {
    const result = await invoke(['--json', 'stat', 'missing.txt']);

    expect(result.code).toBe(1);
    expect(JSON.parse(result.stderr).error).toMatchObject({ code: 'ENOENT', status: 404 });
  });

  it('emits structured JSON for parser failures', async () => {
    const result = await invoke(['--json', 'ls', '--unknown-option']);

    expect(result.code).toBe(1);
    expect(JSON.parse(result.stderr).error.message).toContain('unknown option');
  });

  it('rejects JSON mode for the interactive shell', async () => {
    const result = await invoke(['--json', 'shell']);

    expect(result.code).toBe(1);
    expect(JSON.parse(result.stderr).error.message).toContain('cannot be combined with --json');
  });

  it('requires explicit session configuration outside a TTY', async () => {
    const result = await invoke(['session', 'create'], '', null);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Missing name');
  });

  it('selects created sessions and leaves no selection after deleting the current one', async () => {
    expect((await invoke([
      'session', 'create', 'other', '--endpoint', endpoint, '--volume', 'other-volume',
    ], '', null)).code).toBe(0);
    expect((await sessions.resolve()).name).toBe('other');

    expect((await invoke(['session', 'delete', 'other'], '', null)).code).toBe(0);
    expect((await sessions.list()).currentSession).toBeUndefined();
    const pwd = await invoke(['pwd'], '', null);
    expect(pwd.code).toBe(1);
    expect(pwd.stderr).toContain('No active session');

    expect((await invoke(['session', 'use', 'test'], '', null)).code).toBe(0);
  });

  it('marks the shell-local session active when listing sessions', async () => {
    await sessions.create('other-list', { endpoint, volume: 'other-list-volume' });

    const result = await invoke(['--json', 'session', 'list'], '', null, 'test');
    const listed = JSON.parse(result.stdout) as Array<{ name: string; active: boolean }>;

    expect(listed.find(({ name }) => name === 'test')?.active).toBe(true);
    expect(listed.find(({ name }) => name === 'other-list')?.active).toBe(false);
    await sessions.use('test');
  });
});

function sink(chunks: Buffer[]): Writable {
  return new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.from(chunk));
      callback();
    },
  });
}

async function invoke(
  args: string[],
  stdin = '',
  sessionName: string | null = 'test',
  sessionOverride?: string | null,
): Promise<{ code: number; stdout: string; stderr: string }> {
  const result = await invokeBinary(args, stdin, sessionName, sessionOverride);
  return { code: result.code, stdout: result.stdout.toString(), stderr: result.stderr.toString() };
}

async function invokeBinary(
  args: string[],
  stdin = '',
  sessionName: string | null = 'test',
  sessionOverride?: string | null,
): Promise<{ code: number; stdout: Buffer; stderr: Buffer }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const code = await execute(['node', 'airyfs', ...(sessionName ? ['--session', sessionName] : []), ...args], {
    sessions,
    stdin: Readable.from(stdin),
    stdout: sink(stdout),
    stderr: sink(stderr),
    shellMode: true,
    sessionOverride,
  });
  return { code, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) };
}

async function invokeOneShot(args: string[], stdin = ''): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const code = await execute(['node', 'airyfs', '--session', 'test', ...args], {
    sessions,
    stdin: Readable.from(stdin),
    stdout: sink(stdout),
    stderr: sink(stderr),
    shellMode: false,
  });
  return { code, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString() };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for condition');
    await delay(10);
  }
}

/** Return the SIGINT listener the running exec adds, without emitting a real signal. */
async function waitForNewSigintListener(baseline: number): Promise<() => void> {
  await waitFor(() => process.listenerCount('SIGINT') > baseline);
  const listeners = process.listeners('SIGINT');
  return listeners[listeners.length - 1] as () => void;
}

async function route(
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
  _body: string,
): Promise<void> {
  const directories: Record<string, unknown[]> = {
    '/v1/volumes/vol/directories': [entry('src', 'directory')],
    '/v1/volumes/vol/directories/src': [entry('file.txt', 'file', 12)],
    '/v1/volumes/vol/directories/dl': [entry('file.txt', 'file', 12), entry('nested', 'directory')],
  };
  if (request.method === 'GET' && url.pathname in directories) {
    return json(response, 200, directories[url.pathname]);
  }
  if (request.method === 'PUT' && url.pathname.startsWith('/v1/volumes/vol/trees/')) {
    return json(response, 201, { files: 1, directories: 0, symlinks: 0, bytes: 9 });
  }
  if (request.method === 'GET' && url.pathname === '/v1/volumes/vol/files/dl/file.txt') {
    response.writeHead(200, { 'Content-Type': 'application/octet-stream' }).end('file-body');
    return;
  }
  if (request.method === 'PUT' && url.pathname.startsWith('/v1/volumes/vol/directories/')) {
    response.writeHead(204).end();
    return;
  }
  if (request.method === 'PATCH' && url.pathname.startsWith('/v1/volumes/vol/files/')) {
    response.writeHead(204, { 'X-AiryFS-Bytes-Written': String(Buffer.byteLength(_body)) }).end();
    return;
  }
  if (request.method === 'PUT' && url.pathname.startsWith('/v1/volumes/vol/files/')) {
    response.writeHead(204).end();
    return;
  }
  if (request.method === 'GET' && url.pathname.endsWith('/download.txt')) {
    response.writeHead(200, { 'Content-Type': 'application/octet-stream' }).end('download-body');
    return;
  }
  if (request.method === 'GET' && url.pathname.endsWith('/cat.txt')) {
    response.writeHead(200, { 'Content-Type': 'application/octet-stream' }).end('cat-body');
    return;
  }
  if (request.method === 'HEAD' && url.pathname === '/v1/volumes/vol/files/head.txt') {
    response.writeHead(200, { 'Content-Length': '14' }).end();
    return;
  }
  if (request.method === 'GET' && url.pathname === '/v1/volumes/vol/files/head.txt') {
    response.writeHead(206, { 'Content-Length': '14', 'Content-Range': 'bytes 0-13/14' }).end('one\ntwo\nthree\n');
    return;
  }
  if (request.method === 'GET' && url.pathname === '/v1/volumes/vol/files/text.txt') {
    response.writeHead(206, { 'Content-Type': 'application/octet-stream' }).end('plain text\n');
    return;
  }
  if (request.method === 'POST' && url.pathname.startsWith('/v1/volumes/vol/operations/')) {
    if (url.pathname.endsWith('/lstat')) {
      return json(response, 200, { ino: 2, mode: 0o100644, nlink: 1, uid: 0, gid: 0, size: 11, atime: 0, mtime: 0, ctime: 0, type: 'file' });
    }
    if (url.pathname.endsWith('/du')) return json(response, 200, { bytes: 11, inodes: 2 });
    response.writeHead(204).end();
    return;
  }
  if (request.method === 'DELETE' && url.pathname.startsWith('/v1/volumes/vol/directories/')) {
    response.writeHead(204).end();
    return;
  }
  if (request.method === 'DELETE' && url.pathname === '/v1/volumes/vol') {
    return json(response, 200, { deleted: true });
  }
  if (request.method === 'POST' && url.pathname === '/v1/volumes/vol/jobs') {
    const command = (JSON.parse(_body) as { command: string }).command;
    if (command.includes('ambiguous-command')) {
      ambiguousCommandAttempts++;
      response.writeHead(502, { 'Content-Type': 'text/html' }).end('<html><head><title>502: Bad gateway</title></head></html>');
      return;
    }
    if (command.includes('transport-command')) {
      transportCommandAttempts++;
      response.destroy();
      return;
    }
    if (command.includes('busy-command')) {
      busyCommandAttempts++;
      if (busyCommandAttempts === 1) return json(response, 503, { error: { code: 'EXEC_BUSY', message: 'Another command is already running' } });
    }
    if (command.includes('retry-command')) {
      warmExecAttempts++;
      if (transientWarmFailures > 0) {
        transientWarmFailures--;
        response.writeHead(502, { 'Content-Type': 'text/html' }).end('<html><head><title>502: Bad gateway</title></head></html>');
        return;
      }
      retryCommandAttempts++;
    }

    const id = command.includes('stream-hang') ? 'hang-1' : `job-${nextJobId++}`;
    const output = command.includes('stream-binary')
      ? Buffer.from([0, 255, 10, 13, 42, 128])
      : Buffer.from(command.includes('stream-cmd') ? 'remote stream\n'
        : command.includes('retry-command') ? 'recovered\n'
          : command.includes('busy-command') ? 'admitted\n' : 'remote stdout\n');
    const stderr = command.includes('stream-cmd') ? Buffer.from('remote warn\n') : Buffer.alloc(0);
    const exitCode = command.includes('stream-cmd') ? 5
      : command.includes('stream-binary') || command.includes('retry-command') || command.includes('busy-command') ? 0 : 7;
    const running = command.includes('stream-hang');
    const logs = running ? [] : [
      { seq: 0, stream: 'stdout' as const, data: output.toString('base64'), timestamp: 1 },
      ...(stderr.length > 0 ? [{ seq: 1, stream: 'stderr' as const, data: stderr.toString('base64'), timestamp: 1 }] : []),
    ];
    jobs.set(id, { command, status: running ? 'running' : exitCode === 0 ? 'succeeded' : 'failed', exitCode: running ? null : exitCode, logs });
    if (running) {
      hangingId = id;
      hangingResponse = response;
    }
    return json(response, 200, jobDto(id, jobs.get(id)!));
  }
  const jobMatch = /^\/v1\/volumes\/vol\/jobs\/([^/]+)(?:\/(logs|cancel))?$/.exec(url.pathname);
  if (jobMatch) {
    const id = decodeURIComponent(jobMatch[1]);
    const job = jobs.get(id);
    if (!job) return json(response, 404, { error: { code: 'JOB_NOT_FOUND', message: `No job with id ${id}` } });
    if (jobMatch[2] === 'logs' && request.method === 'GET') {
      const after = Number(url.searchParams.get('after') ?? -1);
      return json(response, 200, { entries: job.logs.filter((entry) => entry.seq > after), next: null });
    }
    if (jobMatch[2] === 'cancel' && request.method === 'POST') {
      cancelRequests.push(id);
      job.status = 'canceled';
      job.exitCode = null;
      hangingResponse = null;
      return json(response, 200, jobDto(id, job));
    }
    if (!jobMatch[2] && request.method === 'GET') return json(response, 200, jobDto(id, job));
  }
  if (request.method === 'POST' && url.pathname === '/v1/volumes/vol/exec/cancel') {
    cancelRequests.push((JSON.parse(_body) as { id: string }).id);
    if (hangingResponse && !hangingResponse.writableEnded) {
      writeNdjson(hangingResponse, [{ type: 'exit', id: hangingId, exitCode: 143, signal: 'SIGTERM' }]);
      hangingResponse = null;
    }
    return json(response, 200, { ok: true, canceled: true });
  }
  if (request.method === 'POST' && url.pathname === '/v1/volumes/vol/exec') {
    const command = (JSON.parse(_body) as { command: string }).command;
    const stream = url.searchParams.get('stream') === 'true';
    if (command === ':') {
      warmExecAttempts++;
      if (transientWarmFailures > 0) {
        transientWarmFailures--;
        response.writeHead(502, { 'Content-Type': 'text/html' }).end(
          '<!DOCTYPE html><html><head><title>502: Bad gateway</title></head></html>',
        );
        return;
      }
      return json(response, 200, { exitCode: 0, stdout: '', stderr: '' });
    }
    if (command.includes('ambiguous-command')) {
      ambiguousCommandAttempts++;
      response.writeHead(502, { 'Content-Type': 'text/html' }).end(
        '<html><head><title>502: Bad gateway</title></head></html>',
      );
      return;
    }
    if (command.includes('transport-command')) {
      transportCommandAttempts++;
      response.destroy();
      return;
    }
    if (command.includes('busy-command')) {
      busyCommandAttempts++;
      if (busyCommandAttempts === 1) {
        return json(response, 503, { error: { code: 'EXEC_BUSY', message: 'Another command is already running' } });
      }
      return json(response, 200, { exitCode: 0, stdout: 'admitted\n', stderr: '' });
    }
    if (command.includes('retry-command')) {
      retryCommandAttempts++;
      return json(response, 200, { exitCode: 0, stdout: 'recovered\n', stderr: '' });
    }
    if (stream && command.includes('stream-hang')) {
      hangingId = 'hang-1';
      response.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
      response.write(`${JSON.stringify({ type: 'start', id: hangingId })}\n`);
      hangingResponse = response;
      return;
    }
    if (stream && command.includes('stream-binary')) {
      return writeNdjson(response, [
        { type: 'start', id: 'bin-1' },
        { type: 'stdout', id: 'bin-1', data: Buffer.from([0, 255, 10, 13, 42, 128]).toString('base64') },
        { type: 'exit', id: 'bin-1', exitCode: 0 },
      ]);
    }
    if (stream) {
      return writeNdjson(response, [
        { type: 'start', id: 'run-1' },
        { type: 'stdout', id: 'run-1', data: Buffer.from('remote stream\n').toString('base64') },
        { type: 'stderr', id: 'run-1', data: Buffer.from('remote warn\n').toString('base64') },
        { type: 'exit', id: 'run-1', exitCode: 5 },
      ]);
    }
    return json(response, 200, { exitCode: 7, stdout: 'remote stdout\n', stderr: '' });
  }
  if (request.method === 'GET' && url.pathname.endsWith('/missing.txt')) {
    return json(response, 404, { error: { code: 'ENOENT', message: 'No such file', path: '/src/missing.txt' } });
  }
  json(response, 404, { error: { code: 'ENOENT', message: `Unhandled ${request.method} ${url.pathname}` } });
}

function entry(name: string, type: 'file' | 'directory', size = 0): Record<string, unknown> {
  return { name, type, size, ino: 2, mode: type === 'directory' ? 0o40755 : 0o100644, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0 };
}

function jobDto(id: string, job: { command: string; status: string; exitCode: number | null; logs: unknown[] }): Record<string, unknown> {
  return {
    id, idempotencyKey: `key-${id}`, command: job.command, cwd: '/', status: job.status,
    execId: id, exitCode: job.exitCode, error: null, cancelRequested: job.status === 'canceled',
    outputBytes: 0, outputTruncated: false, createdAt: 1, updatedAt: 2, startedAt: 1,
    finishedAt: job.status === 'running' ? null : 2,
  };
}

function json(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}

function writeNdjson(response: ServerResponse, events: unknown[]): void {
  if (!response.headersSent) response.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
  response.end(events.map((event) => JSON.stringify(event)).join('\n') + '\n');
}

async function requestBody(request: IncomingMessage): Promise<string> {
  let body = '';
  for await (const chunk of request) body += chunk.toString();
  return body;
}
