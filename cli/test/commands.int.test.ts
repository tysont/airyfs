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

  it('executes in the session cwd and propagates the remote exit code', async () => {
    const result = await invoke(['exec', '--no-stream', 'git', 'status']);

    expect(result.code).toBe(7);
    expect(result.stdout).toContain('remote stdout');
    const exec = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/exec');
    expect(JSON.parse(exec?.body || '{}').command).toBe('cd -- /volume/src && git status');
  });

  it('warms the Container with a no-op command', async () => {
    const result = await invoke(['warm', '--timeout', '1s']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('Container is warm for vol');
    const warm = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/exec');
    expect(JSON.parse(warm?.body || '{}').command).toBe(':');
  });

  it('retries transient gateway errors during a safe preflight', async () => {
    transientWarmFailures = 1;
    warmExecAttempts = 0;
    retryCommandAttempts = 0;

    const result = await invoke(['exec', '--no-stream', '--timeout', '2s', 'retry-command']);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain('recovered');
    expect(warmExecAttempts).toBe(2);
    expect(retryCommandAttempts).toBe(1);
  });

  it('does not retry a user command after an ambiguous gateway error', async () => {
    ambiguousCommandAttempts = 0;

    const result = await invoke(['exec', '--timeout', '2s', 'ambiguous-command']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Bad Gateway');
    expect(ambiguousCommandAttempts).toBe(1);
  });

  it('does not retry a user command after an ambiguous transport failure', async () => {
    transportCommandAttempts = 0;

    const result = await invoke(['exec', '--timeout', '2s', 'transport-command']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Could not reach');
    expect(transportCommandAttempts).toBe(1);
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

    const exec = requests.slice().reverse().find((request) => request.path === '/v1/volumes/vol/exec');
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
      .find((request) => request.path === '/v1/volumes/vol/exec?stream=true');
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
    expect(JSON.parse(result.stdout)).toEqual({ exitCode: 7, stdout: 'remote stdout\n', stderr: '' });
    const streamRequest = requests.slice(before)
      .find((request) => request.path === '/v1/volumes/vol/exec?stream=true');
    expect(streamRequest).toBeUndefined();
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
