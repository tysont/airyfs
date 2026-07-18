// ABOUTME: High-level async iterators and resumable workflows built on AiryFSClient.
// ABOUTME: Centralizes cursor, polling, cancellation, and idempotent transfer behavior.

import { AiryFSClient } from './client.js';
import { AiryFSApiError } from './errors.js';
import type {
  ChangeEvent,
  ExecEvent,
  ExecStreamResult,
  Job,
  JobLogEntry,
  WaitForJobOptions,
  WaitForJobResult,
  WatchChangesOptions,
  UploadCompleteResult,
} from './types.js';

const TERMINAL = new Set(['succeeded', 'failed', 'canceled']);
export const RESUMABLE_CHUNK_BYTES = 1024 * 1024;

/** Tail a volume's ordered change feed until the supplied signal aborts. */
export async function* watchChanges(
  client: AiryFSClient,
  options: WatchChangesOptions = {},
): AsyncGenerator<ChangeEvent> {
  let cursor: number;
  if (typeof options.since === 'number') cursor = options.since;
  else cursor = (await client.getChanges({
    path: options.path,
    since: options.since,
    limit: options.limit,
    signal: options.signal,
  })).cursor;

  while (!options.signal?.aborted) {
    let page;
    try {
      page = await client.getChanges({
        path: options.path,
        since: cursor,
        limit: options.limit,
        wait: options.wait ?? 25_000,
        signal: options.signal,
      });
    } catch (error) {
      if (options.signal?.aborted) return;
      throw error;
    }
    if (page.gap) options.onGap?.(page);
    if (page.cursor < cursor) throw new Error(`Change feed cursor moved backward from ${cursor} to ${page.cursor}`);
    for (const event of page.events) yield event;
    cursor = page.cursor;
  }
}

/** Yield every currently available persisted job log entry in sequence order. */
export async function* drainJobLogs(
  client: AiryFSClient,
  id: string,
  after?: number,
  limit?: number,
): AsyncGenerator<JobLogEntry, number | undefined> {
  let cursor = after;
  while (true) {
    const page = await client.getJobLogs(id, cursor, limit);
    const before = cursor;
    for (const entry of page.entries) {
      if (cursor !== undefined && entry.seq <= cursor) {
        throw new Error(`Job log cursor did not advance beyond ${cursor}`);
      }
      cursor = entry.seq;
      yield entry;
    }
    if (page.next === null) return cursor;
    if (cursor === before) throw new Error(`Job log page did not advance beyond ${String(before)}`);
  }
}

/** Wait for a durable job, draining persisted output on each poll. */
export async function waitForJob(
  client: AiryFSClient,
  id: string,
  options: WaitForJobOptions = {},
): Promise<WaitForJobResult> {
  let cursor = options.after;
  while (true) {
    cursor = await consumeLogs(client, id, cursor, options.onLog);
    const job = await client.getJob(id);
    if (TERMINAL.has(job.status)) {
      cursor = await consumeLogs(client, id, cursor, options.onLog);
      return { job, cursor };
    }
    await delay(options.interval ?? 500, options.signal);
  }
}

/** Follow a job's persisted logs until it reaches a terminal state. */
export async function* followJobLogs(
  client: AiryFSClient,
  id: string,
  options: Omit<WaitForJobOptions, 'onLog'> = {},
): AsyncGenerator<JobLogEntry, Job> {
  let cursor = options.after;
  while (true) {
    for await (const entry of drainJobLogs(client, id, cursor)) {
      cursor = entry.seq;
      yield entry;
    }
    const job = await client.getJob(id);
    if (TERMINAL.has(job.status)) {
      for await (const entry of drainJobLogs(client, id, cursor)) {
        cursor = entry.seq;
        yield entry;
      }
      return job;
    }
    await delay(options.interval ?? 500, options.signal);
  }
}

/** Expose a streaming exec's start id separately while retaining every event. */
export async function execStreamWithId(
  client: AiryFSClient,
  command: string,
  signal?: AbortSignal,
): Promise<ExecStreamResult> {
  const source = await client.execStream(command, signal);
  let resolveId!: (id: string) => void;
  let rejectId!: (error: unknown) => void;
  const id = new Promise<string>((resolve, reject) => {
    resolveId = resolve;
    rejectId = reject;
  });
  const events = (async function* (): AsyncGenerator<ExecEvent> {
    let started = false;
    try {
      for await (const event of source) {
        if (event.type === 'start' && !started) {
          started = true;
          resolveId(event.id);
        }
        yield event;
      }
      if (!started) rejectId(new Error('Exec stream ended before a start event'));
    } catch (error) {
      if (!started) rejectId(error);
      throw error;
    }
  })();
  return { id, events };
}

/**
 * Resume a Blob/File upload from the server's durable offset. The caller supplies
 * the full-file SHA-256 so large sources do not need to be buffered by the SDK.
 */
export async function resumableUploadBlob(
  client: AiryFSClient,
  remotePath: string,
  source: Blob,
  checksum: string,
  onProgress?: (transferred: number, total: number) => void,
): Promise<UploadCompleteResult> {
  let offset = (await client.beginUpload(remotePath, source.size, checksum)).offset;
  if (offset > source.size) throw new Error(`Server upload offset ${offset} exceeds source size ${source.size}`);
  onProgress?.(offset, source.size);

  let reconciledAt = -1;
  while (offset < source.size) {
    const chunk = new Uint8Array(await source.slice(offset, offset + RESUMABLE_CHUNK_BYTES).arrayBuffer());
    const chunkChecksum = await sha256(chunk);
    try {
      const status = await client.appendUpload(remotePath, offset, chunkChecksum, chunk);
      if (status.offset <= offset) throw new Error(`Upload offset did not advance beyond ${offset}`);
      offset = status.offset;
      reconciledAt = -1;
      onProgress?.(offset, source.size);
    } catch (error) {
      if (error instanceof AiryFSApiError && error.code === 'UPLOAD_OFFSET_MISMATCH' && reconciledAt !== offset) {
        reconciledAt = offset;
        offset = (await client.uploadStatus(remotePath)).offset;
        if (offset > source.size) {
          throw new Error(`Server upload offset ${offset} exceeds source size ${source.size}`);
        }
        onProgress?.(offset, source.size);
        continue;
      }
      throw error;
    }
  }
  return client.completeUpload(remotePath);
}

async function consumeLogs(
  client: AiryFSClient,
  id: string,
  after: number | undefined,
  onLog?: (entry: JobLogEntry) => void,
): Promise<number | undefined> {
  let cursor = after;
  for await (const entry of drainJobLogs(client, id, cursor)) {
    cursor = entry.seq;
    onLog?.(entry);
  }
  return cursor;
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const finish = (): void => {
      signal?.removeEventListener('abort', abort);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    const abort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      reject(signal?.reason ?? new DOMException('Aborted', 'AbortError'));
    };
    signal?.addEventListener('abort', abort, { once: true });
  });
}

async function sha256(data: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', data.slice().buffer);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}
