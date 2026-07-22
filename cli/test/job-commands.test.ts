// ABOUTME: Exercises the durable job CLI commands against a stateful mock AiryFS server.
// ABOUTME: Covers submit route/body/idempotency, list/status, binary logs, wait exit code, follow, and Ctrl-C cleanup.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';

interface CapturedRequest {
  method: string;
  path: string;
  body: string;
  idempotencyKey: string | null;
}

interface JobRecord {
  id: string;
  idempotencyKey: string;
  command: string;
  cwd: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'canceled' | 'unknown';
  execId: string | null;
  exitCode: number | null;
  error: string | null;
  cancelRequested: boolean;
  outputBytes: number;
  outputTruncated: boolean;
  createdAt: number;
  updatedAt: number;
  startedAt: number | null;
  finishedAt: number | null;
}

interface LogRow {
  seq: number;
  stream: 'stdout' | 'stderr';
  data: string;
  timestamp: number;
}

const requests: CapturedRequest[] = [];
const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;

// Test-controlled server state.
const jobs = new Map<string, JobRecord>();
const logs = new Map<string, LogRow[]>();
const cancelRequests: string[] = [];
let nextId = 1;

function makeJob(overrides: Partial<JobRecord>): JobRecord {
  const base: JobRecord = {
    id: `job-${nextId++}`,
    idempotencyKey: 'key',
    command: 'echo hi',
    cwd: '/',
    status: 'queued',
    execId: null,
    exitCode: null,
    error: null,
    cancelRequested: false,
    outputBytes: 0,
    outputTruncated: false,
    createdAt: 1_700_000_000,
    updatedAt: 1_700_000_000,
    startedAt: null,
    finishedAt: null,
  };
  return { ...base, ...overrides };
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  const body = await requestBody(request);
  requests.push({
    method: request.method || 'GET',
    path: `${url.pathname}${url.search}`,
    body,
    idempotencyKey: request.headers['idempotency-key'] as string | null ?? null,
  });
  await route(request, response, url, body);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-job-cmd-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  await sessions.create('test', { endpoint, volume: 'vol' });
  // Keep polling fast for --wait/--follow.
  process.env.AIRYFS_JOB_POLL_MS = '1';
});

afterAll(async () => {
  delete process.env.AIRYFS_JOB_POLL_MS;
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

beforeEach(() => {
  requests.length = 0;
  cancelRequests.length = 0;
  jobs.clear();
  logs.clear();
  nextId = 1;
});

describe('job submit', () => {
  it('submits with the joined command, session cwd, and an Idempotency-Key header', async () => {
    const result = await invoke(['job', 'submit', 'echo', 'hello world']);
    expect(result.code).toBe(0);
    expect(result.stdout.trim()).toBe('job-1');

    const post = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/jobs');
    expect(post).toBeDefined();
    expect(JSON.parse(post!.body)).toEqual({ command: "echo 'hello world'", cwd: '/' });
    expect(post!.idempotencyKey).toBeTruthy();
  });

  it('honors an explicit --cwd and --idempotency-key', async () => {
    const result = await invoke(['job', 'submit', '--cwd', '/data', '--idempotency-key', 'my-key', 'ls']);
    expect(result.code).toBe(0);
    const post = requests.find((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/jobs');
    expect(JSON.parse(post!.body)).toEqual({ command: 'ls', cwd: '/data' });
    expect(post!.idempotencyKey).toBe('my-key');
  });

  it('emits the full job object in JSON mode', async () => {
    const result = await invoke(['--json', 'job', 'submit', 'true']);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ id: 'job-1', status: 'queued' });
  });

  it('accepts the jobs alias', async () => {
    const result = await invoke(['jobs', 'submit', 'true']);
    expect(result.code).toBe(0);
    expect(result.stdout.trim()).toBe('job-1');
  });
});

describe('job submit --wait', () => {
  it('streams persisted output and adopts the remote exit code', async () => {
    const result = await invoke(['job', 'submit', '--wait', 'build']);
    // Exit code adopted from the terminal job below (7).
    expect(result.code).toBe(7);
    expect(result.stdout).toContain('out-line\n');
    expect(result.stderr).toContain('err-line\n');
  });

  it('reports success as exit code 0', async () => {
    const result = await invoke(['job', 'submit', '--wait', 'ok-build']);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain('done\n');
  });
});

describe('job list', () => {
  it('lists jobs as a table and filters by status', async () => {
    jobs.set('a', makeJob({ id: 'a', command: 'echo a', status: 'succeeded', exitCode: 0 }));
    jobs.set('b', makeJob({ id: 'b', command: 'echo b', status: 'queued' }));

    const table = await invoke(['job', 'list']);
    expect(table.code).toBe(0);
    expect(table.stdout).toContain('echo a');
    expect(table.stdout).toContain('echo b');

    const filtered = await invoke(['--json', 'job', 'list', '--status', 'queued']);
    const listReq = requests.find((r) => r.path === '/v1/volumes/vol/jobs?status=queued');
    expect(listReq).toBeDefined();
    expect(JSON.parse(filtered.stdout)).toHaveLength(2); // mock returns all; asserts route only
  });
});

describe('job status', () => {
  it('fetches a single job', async () => {
    jobs.set('job-x', makeJob({ id: 'job-x', status: 'running' }));
    const result = await invoke(['job', 'status', 'job-x']);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ id: 'job-x', status: 'running' });
    expect(requests.some((r) => r.path === '/v1/volumes/vol/jobs/job-x')).toBe(true);
  });
});

describe('job logs', () => {
  it('renders exact binary bytes to the matching streams', async () => {
    jobs.set('bin', makeJob({ id: 'bin', status: 'succeeded', exitCode: 0 }));
    logs.set('bin', [
      { seq: 0, stream: 'stdout', data: Buffer.from([0, 255, 10, 42]).toString('base64'), timestamp: 1 },
      { seq: 1, stream: 'stderr', data: Buffer.from([7, 8, 9]).toString('base64'), timestamp: 2 },
    ]);
    const result = await invokeBinary(['job', 'logs', 'bin']);
    expect(result.code).toBe(0);
    expect([...result.stdout]).toEqual([0, 255, 10, 42]);
    expect([...result.stderr]).toEqual([7, 8, 9]);
  });

  it('emits a machine-readable base64 DTO in JSON mode', async () => {
    jobs.set('j', makeJob({ id: 'j', status: 'succeeded', exitCode: 0 }));
    logs.set('j', [{ seq: 0, stream: 'stdout', data: Buffer.from('hi').toString('base64'), timestamp: 1 }]);
    const result = await invoke(['--json', 'job', 'logs', 'j']);
    expect(result.code).toBe(0);
    const page = JSON.parse(result.stdout);
    expect(page.entries[0].data).toBe(Buffer.from('hi').toString('base64'));
    expect(page.next).toBe(0);
  });

  it('passes the --after cursor through', async () => {
    jobs.set('j', makeJob({ id: 'j', status: 'succeeded', exitCode: 0 }));
    logs.set('j', [{ seq: 5, stream: 'stdout', data: Buffer.from('x').toString('base64'), timestamp: 1 }]);
    await invoke(['job', 'logs', 'j', '--after', '4']);
    expect(requests.some((r) => r.path.startsWith('/v1/volumes/vol/jobs/j/logs?after=4'))).toBe(true);
  });

  it('follows until the job reaches a terminal state', async () => {
    // Running for the first status poll, terminal afterward.
    jobs.set('f', makeJob({ id: 'f', status: 'running' }));
    logs.set('f', [{ seq: 0, stream: 'stdout', data: Buffer.from('tick\n').toString('base64'), timestamp: 1 }]);
    let polls = 0;
    statusHook = (job) => {
      if (job.id !== 'f') return;
      polls += 1;
      if (polls >= 2) job.status = 'succeeded';
    };
    try {
      const result = await invoke(['job', 'logs', 'f', '--follow']);
      expect(result.code).toBe(0);
      expect(result.stdout).toContain('tick\n');
    } finally {
      statusHook = null;
    }
  });
});

describe('job cancel', () => {
  it('requests cancellation via the cancel route', async () => {
    jobs.set('c', makeJob({ id: 'c', status: 'queued' }));
    const result = await invoke(['job', 'cancel', 'c']);
    expect(result.code).toBe(0);
    expect(requests.some((r) => r.method === 'POST' && r.path === '/v1/volumes/vol/jobs/c/cancel')).toBe(true);
    expect(cancelRequests).toEqual(['c']);
  });
});

describe('Ctrl-C during --follow', () => {
  it('stops locally without canceling the job and removes the SIGINT listener', async () => {
    jobs.set('hang', makeJob({ id: 'hang', status: 'running' }));
    logs.set('hang', []);
    const baseline = process.listenerCount('SIGINT');

    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    const run = execute(['node', 'airyfs', '--session', 'test', 'job', 'logs', 'hang', '--follow'], {
      sessions,
      stdin: Readable.from(''),
      stdout: sink(stdout),
      stderr: sink(stderr),
      shellMode: true,
    });

    const added = await waitForNewSigintListener(baseline);
    added();
    const code = await run;

    expect(code).toBe(0);
    expect(cancelRequests).toEqual([]);
    expect(process.listenerCount('SIGINT')).toBe(baseline);
  });
});

let statusHook: ((job: JobRecord) => void) | null = null;

async function route(
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
  body: string,
): Promise<void> {
  const method = request.method || 'GET';
  const parts = url.pathname.split('/').filter(Boolean); // v1, volumes, vol, jobs, [id], [action]

  if (parts[3] !== 'jobs') return json(response, 404, { error: { code: 'ENOENT', message: 'not found' } });

  // Collection
  if (parts.length === 4) {
    if (method === 'POST') {
      const parsed = JSON.parse(body) as { command: string; cwd?: string };
      const key = (request.headers['idempotency-key'] as string) || 'none';
      const job = makeJob({ command: parsed.command, cwd: parsed.cwd ?? '/', idempotencyKey: key });
      jobs.set(job.id, job);
      // Seed terminal state + logs for the --wait tests so polling converges.
      if (parsed.command.includes('ok-build')) {
        job.status = 'succeeded';
        job.exitCode = 0;
        logs.set(job.id, [{ seq: 0, stream: 'stdout', data: base64('done\n'), timestamp: 1 }]);
      } else if (parsed.command.includes('build')) {
        job.status = 'failed';
        job.exitCode = 7;
        logs.set(job.id, [
          { seq: 0, stream: 'stdout', data: base64('out-line\n'), timestamp: 1 },
          { seq: 1, stream: 'stderr', data: base64('err-line\n'), timestamp: 2 },
        ]);
      }
      return json(response, 201, job);
    }
    if (method === 'GET') return json(response, 200, [...jobs.values()]);
    return json(response, 405, { error: { code: 'METHOD_NOT_ALLOWED', message: 'no' } });
  }

  const id = parts[4];
  const job = jobs.get(id);

  // Item
  if (parts.length === 5) {
    if (method === 'GET') {
      if (!job) return json(response, 404, { error: { code: 'JOB_NOT_FOUND', message: 'no job' } });
      if (statusHook) statusHook(job);
      return json(response, 200, job);
    }
    return json(response, 405, { error: { code: 'METHOD_NOT_ALLOWED', message: 'no' } });
  }

  // Sub-actions
  if (parts.length === 6) {
    const action = parts[5];
    if (action === 'logs' && method === 'GET') {
      if (!job) return json(response, 404, { error: { code: 'JOB_NOT_FOUND', message: 'no job' } });
      const after = url.searchParams.get('after');
      const afterSeq = after === null ? -1 : Number(after);
      const rows = (logs.get(id) ?? []).filter((row) => row.seq > afterSeq);
      return json(response, 200, { entries: rows, next: rows.length > 0 ? rows[rows.length - 1].seq : null });
    }
    if (action === 'cancel' && method === 'POST') {
      if (!job) return json(response, 404, { error: { code: 'JOB_NOT_FOUND', message: 'no job' } });
      cancelRequests.push(id);
      job.status = 'canceled';
      job.cancelRequested = true;
      return json(response, 200, job);
    }
  }

  return json(response, 404, { error: { code: 'ENOENT', message: `Unhandled ${method} ${url.pathname}` } });
}

function base64(text: string): string {
  return Buffer.from(text).toString('base64');
}

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
): Promise<{ code: number; stdout: string; stderr: string }> {
  const result = await invokeBinary(args, stdin);
  return { code: result.code, stdout: result.stdout.toString(), stderr: result.stderr.toString() };
}

async function invokeBinary(
  args: string[],
  stdin = '',
): Promise<{ code: number; stdout: Buffer; stderr: Buffer }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const code = await execute(['node', 'airyfs', '--session', 'test', ...args], {
    sessions,
    stdin: Readable.from(stdin),
    stdout: sink(stdout),
    stderr: sink(stderr),
    shellMode: true,
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
    await delay(5);
  }
}

async function waitForNewSigintListener(baseline: number): Promise<() => void> {
  await waitFor(() => process.listenerCount('SIGINT') > baseline);
  const listeners = process.listeners('SIGINT');
  return listeners[listeners.length - 1] as () => void;
}

function json(response: ServerResponse, status: number, value: unknown): void {
  const payload = JSON.stringify(value);
  response.writeHead(status, { 'Content-Type': 'application/json' }).end(payload);
}

async function requestBody(request: IncomingMessage): Promise<string> {
  let body = '';
  for await (const chunk of request) body += chunk;
  return body;
}
