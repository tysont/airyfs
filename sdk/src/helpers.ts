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
  TailFileOptions,
  UploadCompleteResult,
} from './types.js';

const TERMINAL = new Set(['succeeded', 'failed', 'canceled', 'unknown']);
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

/** Yield a file's trailing bytes, optionally following appends via the change feed. */
export async function* tailFile(
  client: AiryFSClient,
  path: string,
  options: TailFileOptions = {},
): AsyncGenerator<Uint8Array> {
  if (options.lines !== undefined && options.bytes !== undefined) throw new Error('lines and bytes are mutually exclusive');
  const lines = options.lines ?? (options.bytes === undefined ? 10 : undefined);
  if (lines !== undefined && (!Number.isSafeInteger(lines) || lines < 0)) throw new Error('lines must be a non-negative integer');
  if (options.bytes !== undefined && (!Number.isSafeInteger(options.bytes) || options.bytes < 0)) {
    throw new Error('bytes must be a non-negative integer');
  }

  let cursor = (await client.getChanges({ path, since: 'latest', signal: options.signal })).cursor;
  let offset = 0;
  const initial = await trailingBytes(client, path, lines, options.bytes, options.signal);
  offset = initial.size;
  if (initial.data.byteLength > 0) yield initial.data;
  if (!options.follow) return;

  while (!options.signal?.aborted) {
    let page;
    try {
      page = await client.getChanges({ path, since: cursor, wait: options.wait ?? 25_000, signal: options.signal });
    } catch (error) {
      if (options.signal?.aborted) return;
      throw error;
    }
    if (page.gap) options.onGap?.(page);
    cursor = page.cursor;
    if (!page.events.some((event) => event.path === path || event.oldPath === path)) continue;
    try {
      const head = await client.headFile(path, options.signal);
      const size = responseSize(head);
      if (size < offset) offset = 0;
      if (size > offset) {
        const response = await client.readFile(path, `bytes=${offset}-`, options.signal);
        const data = new Uint8Array(await response.arrayBuffer());
        offset = responseTotal(response, offset + data.byteLength);
        if (data.byteLength > 0) yield data;
      }
    } catch (error) {
      if (error instanceof AiryFSApiError && error.code === 'ENOENT') {
        if (!options.retry) return;
        offset = 0;
        continue;
      }
      throw error;
    }
  }
}

async function trailingBytes(
  client: AiryFSClient,
  path: string,
  lines: number | undefined,
  bytes: number | undefined,
  signal?: AbortSignal,
): Promise<{ data: Uint8Array; size: number }> {
  const head = await client.headFile(path, signal);
  const size = responseSize(head);
  if (size === 0 || bytes === 0 || lines === 0) return { data: new Uint8Array(), size };
  if (bytes !== undefined) {
    const response = await client.readFile(path, `bytes=-${Math.min(bytes, size)}`, signal);
    return { data: new Uint8Array(await response.arrayBuffer()), size: responseTotal(response, size) };
  }

  let length = Math.min(size, 64 * 1024);
  let data = new Uint8Array();
  while (true) {
    const response = await client.readFile(path, `bytes=-${length}`, signal);
    data = new Uint8Array(await response.arrayBuffer());
    if (length >= size || newlineCount(data) > (lines ?? 10)) {
      return { data: lastLines(data, lines ?? 10), size: responseTotal(response, size) };
    }
    length = Math.min(size, length * 2);
  }
}

function responseSize(response: Response): number {
  const value = Number(response.headers.get('Content-Length'));
  if (!Number.isSafeInteger(value) || value < 0) throw new Error('File response is missing a valid Content-Length');
  return value;
}

function responseTotal(response: Response, fallback: number): number {
  const match = response.headers.get('Content-Range')?.match(/\/(\d+)$/);
  return match ? Number(match[1]) : fallback;
}

function newlineCount(data: Uint8Array): number {
  let count = 0;
  for (const byte of data) if (byte === 10) count++;
  return count;
}

function lastLines(data: Uint8Array, lines: number): Uint8Array {
  let remaining = lines;
  let index = data.length;
  if (index > 0 && data[index - 1] === 10) index--;
  while (index > 0 && remaining > 0) if (data[--index] === 10) remaining--;
  return data.subarray(remaining === 0 ? index + 1 : 0);
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
