// ABOUTME: Verifies that SDK exec is durably identified, retry-safe, and cancelable.
// ABOUTME: Covers persisted output replay and typed ambiguous-outcome errors.

import { describe, expect, it, vi } from 'vitest';
import { AiryFSClient, AiryFSCommandOutcomeUnknownError } from '../src/index.js';

function job(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'command-1',
    idempotencyKey: 'key',
    command: 'printf hello',
    cwd: '/',
    status: 'succeeded',
    execId: 'command-1',
    exitCode: 0,
    error: null,
    cancelRequested: false,
    outputBytes: 5,
    outputTruncated: false,
    createdAt: 1,
    updatedAt: 2,
    startedAt: 1,
    finishedAt: 2,
    ...overrides,
  };
}

describe('durable exec', () => {
  it('retries submission with one key and reconstructs the persisted result', async () => {
    const keys: string[] = [];
    let submissions = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') {
        keys.push(new Headers(init.headers).get('Idempotency-Key')!);
        if (submissions++ === 0) throw new Error('connection reset');
        return Response.json(job({ status: 'running', exitCode: null, finishedAt: null }));
      }
      if (url.includes('/logs')) {
        if (url.includes('after=')) return Response.json({ entries: [], next: null });
        return Response.json({
          entries: [
            { seq: 0, stream: 'stdout', data: btoa('hello'), timestamp: 1 },
            { seq: 1, stream: 'stderr', data: btoa('note'), timestamp: 1 },
          ],
          next: null,
        });
      }
      if (url.endsWith('/jobs/command-1')) return Response.json(job());
      throw new Error(`Unexpected request: ${url}`);
    });
    const client = new AiryFSClient('https://example.com', 'volume', { fetch: fetchMock });

    const result = await client.exec('printf hello', { idempotencyKey: 'stable-key', pollInterval: 1 });

    expect(keys).toEqual(['stable-key', 'stable-key']);
    expect(result).toEqual({ commandId: 'command-1', exitCode: 0, stdout: 'hello', stderr: 'note' });
  });

  it('surfaces an interrupted command with its durable id', async () => {
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') {
        return Response.json(job({ status: 'running', exitCode: null, finishedAt: null }));
      }
      if (url.includes('/logs')) return Response.json({ entries: [], next: null });
      if (url.endsWith('/jobs/command-1')) {
        return Response.json(job({ status: 'unknown', exitCode: null, error: 'interrupted' }));
      }
      throw new Error(`Unexpected request: ${url}`);
    });
    const client = new AiryFSClient('https://example.com', 'volume', { fetch: fetchMock });

    await expect(client.exec('dangerous')).rejects.toMatchObject({
      name: 'AiryFSCommandOutcomeUnknownError',
      commandId: 'command-1',
    } satisfies Partial<AiryFSCommandOutcomeUnknownError>);
  });

  it('fetches logs again after observing terminal status', async () => {
    let logReads = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') {
        return Response.json(job({ status: 'running', exitCode: null, finishedAt: null }));
      }
      if (url.includes('/logs')) {
        if (logReads++ === 0) return Response.json({ entries: [], next: null });
        return Response.json({
          entries: [{ seq: 0, stream: 'stdout', data: btoa('late'), timestamp: 2 }],
          next: null,
        });
      }
      if (url.endsWith('/jobs/command-1')) return Response.json(job({ outputBytes: 4 }));
      throw new Error(`Unexpected request: ${url}`);
    });
    const client = new AiryFSClient('https://example.com', 'volume', { fetch: fetchMock });

    await expect(client.exec('printf late')).resolves.toMatchObject({ stdout: 'late', exitCode: 0 });
    expect(logReads).toBe(2);
  });

  it('cancels the durable command when a stream consumer detaches', async () => {
    let canceled = false;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') {
        return Response.json(job({ status: 'running', exitCode: null, finishedAt: null }));
      }
      if (url.endsWith('/jobs/command-1/cancel')) {
        canceled = true;
        return Response.json(job({ status: 'canceled', exitCode: null }));
      }
      throw new Error(`Unexpected request: ${url}`);
    });
    const client = new AiryFSClient('https://example.com', 'volume', { fetch: fetchMock });
    const events = await client.execStream('sleep 30');
    const iterator = events[Symbol.asyncIterator]();

    expect(await iterator.next()).toEqual({ done: false, value: { type: 'start', id: 'command-1' } });
    await iterator.return?.();

    expect(canceled).toBe(true);
  });
});
