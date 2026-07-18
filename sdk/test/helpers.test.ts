// ABOUTME: Tests SDK orchestration for change cursors, durable jobs, exec ids, and uploads.
// ABOUTME: Uses typed client doubles to isolate async-iterator behavior from HTTP transport.

import { describe, expect, it, vi } from 'vitest';
import {
  AiryFSClient,
  execStreamWithId,
  followJobLogs,
  resumableUploadBlob,
  waitForJob,
  watchChanges,
  type ChangePage,
  type ExecEvent,
  type Job,
  type JobLogPage,
  type UploadStatus,
} from '../src/index.js';

describe('watchChanges', () => {
  it('tails from latest, reports gaps, yields events, and stops on abort', async () => {
    const controller = new AbortController();
    const gap = vi.fn();
    const pages: ChangePage[] = [
      { events: [], cursor: 4, latest: 4, oldest: 1, gap: false },
      {
        events: [{ seq: 5, type: 'create', path: '/x', oldPath: null, ino: 2, timestamp: 1 }],
        cursor: 5, latest: 5, oldest: 5, gap: true,
      },
    ];
    const client = {
      getChanges: vi.fn(async () => pages.shift() ?? new Promise<ChangePage>(() => undefined)),
    } as unknown as AiryFSClient;

    const seen = [];
    for await (const event of watchChanges(client, { signal: controller.signal, onGap: gap })) {
      seen.push(event);
      controller.abort();
    }
    expect(seen.map((event) => event.seq)).toEqual([5]);
    expect(gap).toHaveBeenCalledOnce();
    expect((client.getChanges as ReturnType<typeof vi.fn>).mock.calls[1][0]).toMatchObject({ since: 4, wait: 25_000 });
  });
});

describe('durable job helpers', () => {
  const running = job('running');
  const succeeded = { ...job('succeeded'), exitCode: 0 };

  it('waits to terminal while draining logs exactly once', async () => {
    const logs: JobLogPage[] = [
      { entries: [{ seq: 1, stream: 'stdout', data: 'aGk=', timestamp: 1 }], next: null },
      { entries: [], next: null },
      { entries: [{ seq: 2, stream: 'stderr', data: 'IQ==', timestamp: 2 }], next: null },
      { entries: [], next: null },
    ];
    const client = {
      getJobLogs: vi.fn(async () => logs.shift() ?? { entries: [], next: null }),
      getJob: vi.fn().mockResolvedValueOnce(running).mockResolvedValueOnce(succeeded),
    } as unknown as AiryFSClient;
    const seen: number[] = [];
    const result = await waitForJob(client, 'j', { interval: 0, onLog: (entry) => seen.push(entry.seq) });
    expect(result.job.status).toBe('succeeded');
    expect(result.cursor).toBe(2);
    expect(seen).toEqual([1, 2]);
  });

  it('follows logs and returns the terminal job', async () => {
    const client = {
      getJobLogs: vi.fn()
        .mockResolvedValueOnce({ entries: [{ seq: 1, stream: 'stdout', data: '', timestamp: 1 }], next: null })
        .mockResolvedValue({ entries: [], next: null }),
      getJob: vi.fn().mockResolvedValue(succeeded),
    } as unknown as AiryFSClient;
    const seqs = [];
    for await (const entry of followJobLogs(client, 'j', { interval: 0 })) seqs.push(entry.seq);
    expect(seqs).toEqual([1]);
  });
});

describe('execStreamWithId', () => {
  it('resolves the admitted id without dropping stream events', async () => {
    const source: ExecEvent[] = [
      { type: 'start', id: 'run-1' },
      { type: 'stdout', id: 'run-1', data: 'aGk=' },
      { type: 'exit', id: 'run-1', exitCode: 0 },
    ];
    const client = { execStream: vi.fn(async () => iterable(source)) } as unknown as AiryFSClient;
    const stream = await execStreamWithId(client, 'true');
    const events = [];
    for await (const event of stream.events) events.push(event);
    expect(await stream.id).toBe('run-1');
    expect(events).toEqual(source);
  });
});

describe('resumableUploadBlob', () => {
  it('resumes from the server offset and publishes after checksummed chunks', async () => {
    const source = new Blob([new Uint8Array(1024 * 1024 + 2)]);
    const statuses: UploadStatus[] = [
      uploadStatus(1),
      uploadStatus(1024 * 1024 + 1),
      uploadStatus(source.size),
    ];
    const client = {
      beginUpload: vi.fn(async () => statuses.shift()),
      appendUpload: vi.fn(async () => statuses.shift()),
      completeUpload: vi.fn(async () => ({ path: '/big', checksum: 'a'.repeat(64) })),
    } as unknown as AiryFSClient;
    const progress: number[] = [];
    await resumableUploadBlob(client, '/big', source, 'a'.repeat(64), (sent) => progress.push(sent));
    expect(client.appendUpload).toHaveBeenCalledTimes(2);
    expect((client.appendUpload as ReturnType<typeof vi.fn>).mock.calls[0][1]).toBe(1);
    expect(progress).toEqual([1, 1024 * 1024 + 1, source.size]);
  });
});

function job(status: Job['status']): Job {
  return {
    id: 'j', idempotencyKey: 'k', command: 'true', cwd: '/', status, execId: null,
    exitCode: null, error: null, cancelRequested: false, outputBytes: 0,
    outputTruncated: false, createdAt: 1, updatedAt: 1, startedAt: null, finishedAt: null,
  };
}

function uploadStatus(offset: number): UploadStatus {
  return { id: 'u', path: '/big', size: 1024 * 1024 + 2, offset, checksum: 'a'.repeat(64), createdAt: 1, updatedAt: 1 };
}

async function* iterable<T>(values: T[]): AsyncGenerator<T> {
  yield* values;
}
