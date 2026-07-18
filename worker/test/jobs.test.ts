// ABOUTME: Tests the durable job queue: schema, DB state machine, runner, decoder, and scheduling helper.
// ABOUTME: Runs raw fs_job/fs_job_log SQL against in-memory SQLite via the shared test storage.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { CloudflareStorage } from 'agentfs-sdk/cloudflare';
import { initSchema, SCHEMA_TABLES } from '../src/schema';
import { HttpError, parseV1Route } from '../src/files-api';
import { buildCapability, capabilityAllows } from '../src/auth';
import {
  BoundedNdjsonDecoder,
  JOB_TABLES,
  JobNdjsonError,
  appendJobLog,
  claimNextJob,
  composeJobCommand,
  finalizeJob,
  getJob,
  getJobLogs,
  listJobs,
  recoverOrphans,
  requestCancel,
  runJob,
  scheduleJobRun,
  setExecId,
  submitJob,
  validateCwd,
  validateCommand,
  validateIdempotencyKey,
  validateStatusFilter,
  type ExecEvent,
} from '../src/jobs';
import { createTestStorage } from './support/storage';

let db: Database.Database;
let storage: CloudflareStorage;
let counter: number;

/** A monotonic clock so created_at ordering is deterministic across inserts. */
function now(): number {
  return counter++;
}

const tx = <T>(callback: () => T): T => storage.transactionSync(callback);
const sql = () => storage.sql as never;

let ids: number;
const idFactory = (): string => `job-${(ids++).toString().padStart(3, '0')}`;

beforeEach(() => {
  db = new Database(':memory:');
  storage = createTestStorage(db);
  initSchema(storage.sql as never, (cb) => storage.transactionSync(cb));
  counter = 1000;
  ids = 1;
});

function submit(command: string, cwd: string, key: string) {
  return submitJob(sql(), tx, { command, cwd, idempotencyKey: key }, idFactory, now);
}

function ndjsonStream(events: ExecEvent[], chunkSize?: number): ReadableStream<Uint8Array> {
  const text = events.map((event) => JSON.stringify(event)).join('\n') + '\n';
  const bytes = new TextEncoder().encode(text);
  const size = chunkSize ?? bytes.length;
  let offset = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset >= bytes.length) {
        controller.close();
        return;
      }
      controller.enqueue(bytes.subarray(offset, offset + size));
      offset += size;
    },
  });
}

describe('schema', () => {
  it('registers the job tables and creates them', () => {
    for (const table of JOB_TABLES) expect(SCHEMA_TABLES).toContain(table);
    const tables = (storage.sql as never as { exec: (q: string) => { toArray(): Array<{ name: string }> } })
      .exec("SELECT name FROM sqlite_master WHERE type='table'")
      .toArray()
      .map((row) => row.name);
    expect(tables).toContain('fs_job');
    expect(tables).toContain('fs_job_log');
  });
});

describe('validation', () => {
  it('rejects an empty command', () => {
    expect(() => validateCommand('')).toThrow(HttpError);
    expect(() => validateCommand('   ')).toThrow(HttpError);
    expect(validateCommand('echo hi')).toBe('echo hi');
  });

  it('canonicalizes an absolute cwd and rejects a relative one', () => {
    expect(validateCwd('/a/b/../c')).toBe('/a/c');
    expect(validateCwd('/a//b/./')).toBe('/a/b');
    expect(validateCwd('/')).toBe('/');
    expect(() => validateCwd('relative')).toThrow(HttpError);
    expect(() => validateCwd(42)).toThrow(HttpError);
  });

  it('requires a bounded idempotency key', () => {
    expect(() => validateIdempotencyKey('')).toThrow(HttpError);
    expect(() => validateIdempotencyKey('x'.repeat(256))).toThrow(HttpError);
    expect(validateIdempotencyKey('key-1')).toBe('key-1');
  });

  it('validates a status filter', () => {
    expect(validateStatusFilter(undefined)).toBeUndefined();
    expect(validateStatusFilter('running')).toBe('running');
    expect(() => validateStatusFilter('bogus')).toThrow(HttpError);
  });
});

describe('submitJob idempotency', () => {
  it('inserts once and dedupes by idempotency key', () => {
    const first = submit('echo a', '/', 'key-1');
    expect(first.created).toBe(true);
    expect(first.job.status).toBe('queued');
    expect(first.job.command).toBe('echo a');
    expect(first.job.cwd).toBe('/');

    const repeat = submit('echo different', '/other', 'key-1');
    expect(repeat.created).toBe(false);
    expect(repeat.job.id).toBe(first.job.id);
    // The original command/cwd are preserved; the duplicate submission is ignored.
    expect(repeat.job.command).toBe('echo a');

    expect(listJobs(sql())).toHaveLength(1);
  });

  it('creates distinct jobs for distinct keys', () => {
    const a = submit('echo a', '/', 'key-a');
    const b = submit('echo b', '/', 'key-b');
    expect(a.job.id).not.toBe(b.job.id);
    expect(listJobs(sql())).toHaveLength(2);
  });
});

describe('reads', () => {
  it('lists newest first and filters by status', () => {
    const a = submit('a', '/', 'k1');
    const b = submit('b', '/', 'k2');
    const all = listJobs(sql());
    expect(all.map((j) => j.id)).toEqual([b.job.id, a.job.id]);
    finalizeJob(sql(), a.job.id, { status: 'succeeded', exitCode: 0, error: null, outputBytes: 0, outputTruncated: false }, now);
    expect(listJobs(sql(), 'queued').map((j) => j.id)).toEqual([b.job.id]);
    expect(listJobs(sql(), 'succeeded').map((j) => j.id)).toEqual([a.job.id]);
  });

  it('throws a 404 for an unknown job', () => {
    expect(() => getJob(sql(), 'nope')).toThrow(HttpError);
  });
});

describe('claim ordering', () => {
  it('claims the oldest queued job and returns null when empty', () => {
    const first = submit('first', '/', 'k1');
    const second = submit('second', '/', 'k2');
    const claimedFirst = claimNextJob(sql(), tx, now);
    expect(claimedFirst?.id).toBe(first.job.id);
    expect(claimedFirst?.status).toBe('running');
    expect(claimedFirst?.started_at).not.toBeNull();

    const claimedSecond = claimNextJob(sql(), tx, now);
    expect(claimedSecond?.id).toBe(second.job.id);

    expect(claimNextJob(sql(), tx, now)).toBeNull();
  });
});

describe('orphan recovery', () => {
  it('marks running rows failed/interrupted without retrying', () => {
    const job = submit('long', '/', 'k1');
    claimNextJob(sql(), tx, now);
    const recovered = recoverOrphans(sql(), now);
    expect(recovered).toBe(1);
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('failed');
    expect(after.error).toBe('interrupted');
    // No new queued job is created — the command is never auto-retried.
    expect(listJobs(sql(), 'queued')).toHaveLength(0);
  });

  it('is a no-op when nothing is running', () => {
    submit('queued', '/', 'k1');
    expect(recoverOrphans(sql(), now)).toBe(0);
  });
});

describe('cancel states', () => {
  it('cancels a queued job immediately', () => {
    const job = submit('a', '/', 'k1');
    const result = requestCancel(sql(), tx, job.job.id, now);
    expect(result.changed).toBe(true);
    expect(result.execToCancel).toBeNull();
    expect(result.job.status).toBe('canceled');
    expect(getJob(sql(), job.job.id).status).toBe('canceled');
  });

  it('flags a running job and reports its exec id to signal', () => {
    const job = submit('a', '/', 'k1');
    claimNextJob(sql(), tx, now);
    setExecId(sql(), job.job.id, 'exec-42', now);
    const result = requestCancel(sql(), tx, job.job.id, now);
    expect(result.changed).toBe(true);
    expect(result.execToCancel).toBe('exec-42');
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('running');
    expect(after.cancelRequested).toBe(true);
  });

  it('is idempotent for terminal jobs and repeated running cancels', () => {
    const job = submit('a', '/', 'k1');
    claimNextJob(sql(), tx, now);
    requestCancel(sql(), tx, job.job.id, now);
    const second = requestCancel(sql(), tx, job.job.id, now);
    expect(second.changed).toBe(false);

    finalizeJob(sql(), job.job.id, { status: 'canceled', exitCode: 143, error: null, outputBytes: 0, outputTruncated: false }, now);
    const terminal = requestCancel(sql(), tx, job.job.id, now);
    expect(terminal.changed).toBe(false);
    expect(terminal.job.status).toBe('canceled');
  });

  it('throws a 404 canceling an unknown job', () => {
    expect(() => requestCancel(sql(), tx, 'missing', now)).toThrow(HttpError);
  });
});

describe('log ordering, pagination, and binary', () => {
  it('appends ordered rows and pages with a cursor', () => {
    const job = submit('a', '/', 'k1');
    for (let seq = 0; seq < 5; seq++) {
      appendJobLog(sql(), job.job.id, seq, seq % 2 === 0 ? 'stdout' : 'stderr', new Uint8Array([seq]), now);
    }
    const first = getJobLogs(sql(), job.job.id, undefined, 2);
    expect(first.entries.map((e) => e.seq)).toEqual([0, 1]);
    expect(first.next).toBe(1);
    const second = getJobLogs(sql(), job.job.id, first.next!, 2);
    expect(second.entries.map((e) => e.seq)).toEqual([2, 3]);
    const third = getJobLogs(sql(), job.job.id, second.next!, 2);
    expect(third.entries.map((e) => e.seq)).toEqual([4]);
    expect(third.next).toBeNull();
    expect(first.entries[0].stream).toBe('stdout');
    expect(first.entries[1].stream).toBe('stderr');
  });

  it('round-trips arbitrary binary bytes through base64', () => {
    const job = submit('a', '/', 'k1');
    const payload = new Uint8Array([0, 255, 10, 13, 42, 128, 0, 7]);
    appendJobLog(sql(), job.job.id, 0, 'stdout', payload, now);
    const page = getJobLogs(sql(), job.job.id);
    expect(Buffer.from(page.entries[0].data, 'base64')).toEqual(Buffer.from(payload));
  });

  it('throws a 404 reading logs for an unknown job', () => {
    expect(() => getJobLogs(sql(), 'missing')).toThrow(HttpError);
  });
});

describe('BoundedNdjsonDecoder', () => {
  it('parses events split across arbitrary chunk boundaries', () => {
    const decoder = new BoundedNdjsonDecoder();
    const text = `${JSON.stringify({ type: 'start', id: 'x' })}\n${JSON.stringify({ type: 'stdout', id: 'x', data: 'AA==' })}\n`;
    const bytes = new TextEncoder().encode(text);
    const events: ExecEvent[] = [];
    for (let i = 0; i < bytes.length; i++) {
      events.push(...decoder.push(bytes.subarray(i, i + 1)));
    }
    events.push(...decoder.flush());
    expect(events).toEqual([
      { type: 'start', id: 'x' },
      { type: 'stdout', id: 'x', data: 'AA==' },
    ]);
  });

  it('yields a trailing line without a newline on flush', () => {
    const decoder = new BoundedNdjsonDecoder();
    expect(decoder.push(new TextEncoder().encode('{"type":"exit","id":"x","exitCode":0}'))).toEqual([]);
    expect(decoder.flush()).toEqual([{ type: 'exit', id: 'x', exitCode: 0 }]);
  });

  it('rejects a malformed line and an over-long line', () => {
    expect(() => new BoundedNdjsonDecoder().push(new TextEncoder().encode('not json\n'))).toThrow(JobNdjsonError);
    const tiny = new BoundedNdjsonDecoder({ maxLineBytes: 4 });
    expect(() => tiny.push(new TextEncoder().encode('123456\n'))).toThrow(JobNdjsonError);
    const trailing = new BoundedNdjsonDecoder({ maxLineBytes: 1 });
    trailing.push(new Uint8Array([0xe2]));
    expect(() => trailing.flush()).toThrow(JobNdjsonError);
  });
});

describe('runJob', () => {
  it('runs to succeeded, persisting exec id and ordered output', async () => {
    const job = submit('echo hi', '/data', 'k1');
    claimNextJob(sql(), tx, now);
    const events: ExecEvent[] = [
      { type: 'start', id: 'exec-1' },
      { type: 'stdout', id: 'exec-1', data: Buffer.from('out\n').toString('base64') },
      { type: 'stderr', id: 'exec-1', data: Buffer.from('warn\n').toString('base64') },
      { type: 'exit', id: 'exec-1', exitCode: 0 },
    ];
    const execStream = vi.fn(async () => ndjsonStream(events, 3));
    await runJob({ sql: sql(), execStream, now }, job.job.id);

    expect(execStream).toHaveBeenCalledWith(composeJobCommand('echo hi', '/data'));
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('succeeded');
    expect(after.exitCode).toBe(0);
    expect(after.execId).toBe('exec-1');
    const logs = getJobLogs(sql(), job.job.id);
    expect(logs.entries.map((e) => [e.stream, Buffer.from(e.data, 'base64').toString()])).toEqual([
      ['stdout', 'out\n'],
      ['stderr', 'warn\n'],
    ]);
  });

  it('marks failed on a nonzero exit', async () => {
    const job = submit('false', '/', 'k1');
    claimNextJob(sql(), tx, now);
    await runJob({
      sql: sql(),
      execStream: async () => ndjsonStream([
        { type: 'start', id: 'e' },
        { type: 'exit', id: 'e', exitCode: 7 },
      ]),
      now,
    }, job.job.id);
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('failed');
    expect(after.exitCode).toBe(7);
  });

  it('maps a terminal exit to canceled when cancellation was requested', async () => {
    const job = submit('sleep', '/', 'k1');
    claimNextJob(sql(), tx, now);
    setExecId(sql(), job.job.id, 'e', now);
    requestCancel(sql(), tx, job.job.id, now);
    await runJob({
      sql: sql(),
      execStream: async () => ndjsonStream([
        { type: 'start', id: 'e' },
        { type: 'exit', id: 'e', exitCode: 143, signal: 'SIGTERM' },
      ]),
      now,
    }, job.job.id);
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('canceled');
    expect(after.exitCode).toBe(143);
  });

  it('signals cancellation requested before the stream exposes its exec id', async () => {
    const job = submit('sleep', '/', 'k1');
    claimNextJob(sql(), tx, now);
    requestCancel(sql(), tx, job.job.id, now);
    const cancelExec = vi.fn(async () => undefined);
    await runJob({
      sql: sql(),
      execStream: async () => ndjsonStream([
        { type: 'start', id: 'late-id' },
        { type: 'exit', id: 'late-id', exitCode: 143, signal: 'SIGTERM' },
      ]),
      cancelExec,
      now,
    }, job.job.id);
    expect(cancelExec).toHaveBeenCalledWith('late-id');
    expect(getJob(sql(), job.job.id).status).toBe('canceled');
  });

  it('marks failed with a concise error on a start/transport error', async () => {
    const job = submit('boom', '/', 'k1');
    claimNextJob(sql(), tx, now);
    await runJob({
      sql: sql(),
      execStream: async () => { throw new Error('CONTAINER_UNAVAILABLE'); },
      now,
    }, job.job.id);
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('failed');
    expect(after.exitCode).toBeNull();
    expect(after.error).toContain('CONTAINER_UNAVAILABLE');
  });

  it('caps persisted output and flags truncation while draining', async () => {
    const job = submit('yes', '/', 'k1');
    claimNextJob(sql(), tx, now);
    const big = Buffer.alloc(10, 0x61).toString('base64'); // 10 bytes 'a'
    const events: ExecEvent[] = [
      { type: 'start', id: 'e' },
      { type: 'stdout', id: 'e', data: big },
      { type: 'stdout', id: 'e', data: big },
      { type: 'stdout', id: 'e', data: big },
      { type: 'exit', id: 'e', exitCode: 0 },
    ];
    await runJob({ sql: sql(), execStream: async () => ndjsonStream(events), now, maxOutputBytes: 15 }, job.job.id);
    const after = getJob(sql(), job.job.id);
    expect(after.status).toBe('succeeded');
    expect(after.outputTruncated).toBe(true);
    expect(after.outputBytes).toBe(15);
    const logs = getJobLogs(sql(), job.job.id);
    const total = logs.entries.reduce((sum, e) => sum + Buffer.from(e.data, 'base64').length, 0);
    expect(total).toBe(15);
  });
});

describe('jobs route parsing and authorization', () => {
  it('parses the jobs collection, item, logs, and cancel routes', () => {
    expect(parseV1Route('/v1/volumes/vol/jobs')).toEqual({ volume: 'vol', resource: 'jobs', path: '/' });
    expect(parseV1Route('/v1/volumes/vol/jobs/job-1')).toEqual({ volume: 'vol', resource: 'jobs', path: '/job-1' });
    expect(parseV1Route('/v1/volumes/vol/jobs/job-1/logs')).toEqual({
      volume: 'vol', resource: 'jobs', path: '/job-1/logs',
    });
    expect(parseV1Route('/v1/volumes/vol/jobs/job-1/cancel')).toEqual({
      volume: 'vol', resource: 'jobs', path: '/job-1/cancel',
    });
  });

  it('requires the exec capability that job routes enforce', () => {
    // Job routes require operation 'exec' on '/'. A read-only capability is denied;
    // an exec capability is allowed.
    const readOnly = buildCapability('vol', ['read'], [], 3600);
    const execCap = buildCapability('vol', ['exec'], [], 3600);
    expect(capabilityAllows(readOnly, 'exec', ['/'])).toBe(false);
    expect(capabilityAllows(execCap, 'exec', ['/'])).toBe(true);
  });
});

describe('scheduleJobRun', () => {
  it('schedules the runNextJob callback with the given delay', async () => {
    const schedule = vi.fn(async () => undefined);
    await scheduleJobRun(schedule);
    await scheduleJobRun(schedule, 1);
    expect(schedule).toHaveBeenNthCalledWith(1, 0, 'runNextJob');
    expect(schedule).toHaveBeenNthCalledWith(2, 1, 'runNextJob');
  });
});
