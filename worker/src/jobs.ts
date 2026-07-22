// ABOUTME: Durable queued command execution: fs_job/fs_job_log tables, state machine, and the runner.
// ABOUTME: DB state-machine functions stay pure and unit-testable; the runner drives execStream + NDJSON.

import { Buffer } from 'buffer';
import { HttpError } from './files-api';
import type { SqlExec, TransactionSync } from './schema';

// ---------------------------------------------------------------------------
// Constants and types
// ---------------------------------------------------------------------------

/** Additive table names owned by this module, for schema verification/introspection. */
export const JOB_TABLES = ['fs_job', 'fs_job_log'] as const;

/** Terminal and non-terminal job states. */
export const JOB_STATUSES = ['queued', 'running', 'succeeded', 'failed', 'canceled', 'unknown'] as const;
export type JobStatus = (typeof JOB_STATUSES)[number];

/** States from which no further transition happens. */
export const TERMINAL_STATUSES: readonly JobStatus[] = ['succeeded', 'failed', 'canceled', 'unknown'];

export type JobStream = 'stdout' | 'stderr';

/** Persisted output is capped per job; the command still drains to completion. */
export const MAX_OUTPUT_BYTES = 50 * 1024 * 1024;

/** Bound the Idempotency-Key so a client cannot store an unbounded string. */
export const MAX_IDEMPOTENCY_KEY_BYTES = 255;

/** Default page size for log reads, and the hard ceiling a caller may request. */
export const DEFAULT_LOG_LIMIT = 500;
export const MAX_LOG_LIMIT = 2000;

/** Callback name the container scheduler invokes to advance the queue. */
export const RUN_NEXT_JOB_CALLBACK = 'runNextJob';

/** Raw fs_job row as stored; integer flags, nullable exit/error/timing columns. */
export interface JobRow {
  id: string;
  idempotency_key: string;
  command: string;
  cwd: string;
  status: JobStatus;
  exec_id: string | null;
  exit_code: number | null;
  error: string | null;
  cancel_requested: number;
  output_bytes: number;
  output_truncated: number;
  created_at: number;
  updated_at: number;
  started_at: number | null;
  finished_at: number | null;
}

/** Public job view returned by RPC/HTTP; camelCase and booleans normalized. */
export interface JobDto {
  id: string;
  idempotencyKey: string;
  command: string;
  cwd: string;
  status: JobStatus;
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

/** A single persisted log line; `data` is base64 so arbitrary bytes survive JSON. */
export interface JobLogEntry {
  seq: number;
  stream: JobStream;
  data: string;
  timestamp: number;
}

/** A page of log entries plus the cursor to resume after, or null at the end. */
export interface JobLogPage {
  entries: JobLogEntry[];
  next: number | null;
}

/** Result of a submission: the job plus whether it was newly created (vs deduped). */
export interface SubmitJobResult {
  job: JobDto;
  created: boolean;
}

export interface SubmitJobInput {
  command: string;
  cwd: string;
  idempotencyKey: string;
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/**
 * Create the additive fs_job and fs_job_log tables. Idempotent, so it is safe to
 * call on every schema init alongside the core AgentFS DDL.
 */
export function initJobSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_job (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    status TEXT NOT NULL,
    exec_id TEXT,
    exit_code INTEGER,
    error TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    output_bytes INTEGER NOT NULL DEFAULT 0,
    output_truncated INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER
  )`);
  sql.exec(`CREATE INDEX IF NOT EXISTS idx_fs_job_status_created ON fs_job(status, created_at)`);
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_job_log (
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    stream TEXT NOT NULL,
    data BLOB NOT NULL,
    timestamp INTEGER NOT NULL,
    PRIMARY KEY (job_id, seq)
  )`);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/** Validate a non-empty command string; the runner supplies the cwd separately. */
export function validateCommand(value: unknown): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "command" string');
  }
  return value;
}

/**
 * Validate and canonicalize an absolute cwd. The input must be an absolute path;
 * it is normalized (collapsing `.`/`..`/duplicate separators) and returned. A
 * relative path or non-string is rejected.
 */
export function validateCwd(value: unknown): string {
  if (typeof value !== 'string' || !value.startsWith('/')) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'cwd must be an absolute path');
  }
  const segments: string[] = [];
  for (const segment of value.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join('/')}`;
}

/** Validate a bounded, non-empty Idempotency-Key. */
export function validateIdempotencyKey(value: unknown): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "Idempotency-Key"');
  }
  if (Buffer.byteLength(value, 'utf8') > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `Idempotency-Key exceeds ${MAX_IDEMPOTENCY_KEY_BYTES} bytes`);
  }
  return value;
}

/** Validate an optional status filter against the known job states. */
export function validateStatusFilter(value: unknown): JobStatus | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value !== 'string' || !(JOB_STATUSES as readonly string[]).includes(value)) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `Unknown status filter: ${String(value)}`);
  }
  return value as JobStatus;
}

// ---------------------------------------------------------------------------
// Row access and normalization
// ---------------------------------------------------------------------------

function normalizeRow(row: Record<string, unknown>): JobRow {
  return {
    id: String(row.id),
    idempotency_key: String(row.idempotency_key),
    command: String(row.command),
    cwd: String(row.cwd),
    status: String(row.status) as JobStatus,
    exec_id: row.exec_id === null || row.exec_id === undefined ? null : String(row.exec_id),
    exit_code: row.exit_code === null || row.exit_code === undefined ? null : Number(row.exit_code),
    error: row.error === null || row.error === undefined ? null : String(row.error),
    cancel_requested: Number(row.cancel_requested ?? 0),
    output_bytes: Number(row.output_bytes ?? 0),
    output_truncated: Number(row.output_truncated ?? 0),
    created_at: Number(row.created_at),
    updated_at: Number(row.updated_at),
    started_at: row.started_at === null || row.started_at === undefined ? null : Number(row.started_at),
    finished_at: row.finished_at === null || row.finished_at === undefined ? null : Number(row.finished_at),
  };
}

/** Map a stored row to its public DTO. */
export function toJobDto(row: JobRow): JobDto {
  return {
    id: row.id,
    idempotencyKey: row.idempotency_key,
    command: row.command,
    cwd: row.cwd,
    status: row.status,
    execId: row.exec_id,
    exitCode: row.exit_code,
    error: row.error,
    cancelRequested: row.cancel_requested !== 0,
    outputBytes: row.output_bytes,
    outputTruncated: row.output_truncated !== 0,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
  };
}

function selectById(sql: SqlExec, id: string): JobRow | null {
  const rows = sql.exec('SELECT * FROM fs_job WHERE id = ?', id).toArray();
  return rows.length > 0 ? normalizeRow(rows[0]) : null;
}

function selectByKey(sql: SqlExec, key: string): JobRow | null {
  const rows = sql.exec('SELECT * FROM fs_job WHERE idempotency_key = ?', key).toArray();
  return rows.length > 0 ? normalizeRow(rows[0]) : null;
}

// ---------------------------------------------------------------------------
// Submission
// ---------------------------------------------------------------------------

/**
 * Insert a queued job, deduplicating on idempotency key. A repeated key returns
 * the existing job unchanged (created=false); a new key inserts exactly once
 * (created=true). The whole read-then-insert runs in one transaction so a
 * concurrent duplicate cannot create two rows.
 */
export function submitJob(
  sql: SqlExec,
  transaction: TransactionSync,
  input: SubmitJobInput,
  idFactory: () => string = () => crypto.randomUUID(),
  now: () => number = defaultNow,
): SubmitJobResult {
  const command = validateCommand(input.command);
  const cwd = validateCwd(input.cwd);
  const idempotencyKey = validateIdempotencyKey(input.idempotencyKey);

  return transaction(() => {
    const existing = selectByKey(sql, idempotencyKey);
    if (existing) {
      if (existing.command !== command || existing.cwd !== cwd) {
        throw new HttpError(409, 'IDEMPOTENCY_CONFLICT', 'Idempotency-Key is already associated with a different command');
      }
      return { job: toJobDto(existing), created: false };
    }

    const id = idFactory();
    const timestamp = now();
    sql.exec(
      `INSERT INTO fs_job
        (id, idempotency_key, command, cwd, status, cancel_requested, output_bytes, output_truncated, created_at, updated_at)
       VALUES (?, ?, ?, ?, 'queued', 0, 0, 0, ?, ?)`,
      id, idempotencyKey, command, cwd, timestamp, timestamp,
    );
    const created = selectById(sql, id);
    if (!created) throw new Error('Job row vanished immediately after creation');
    return { job: toJobDto(created), created: true };
  });
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/** List jobs newest-first, optionally filtered by status. */
export function listJobs(sql: SqlExec, status?: JobStatus): JobDto[] {
  const rows = status
    ? sql.exec('SELECT * FROM fs_job WHERE status = ? ORDER BY created_at DESC, rowid DESC', status).toArray()
    : sql.exec('SELECT * FROM fs_job ORDER BY created_at DESC, rowid DESC').toArray();
  return rows.map((row) => toJobDto(normalizeRow(row)));
}

/** Fetch one job by id, or throw a stable 404. */
export function getJob(sql: SqlExec, id: string): JobDto {
  const row = selectById(sql, id);
  if (!row) throw new HttpError(404, 'JOB_NOT_FOUND', `No job with id ${id}`);
  return toJobDto(row);
}

/**
 * Read a page of a job's logs in seq order. `after` is an exclusive cursor
 * (return seq > after); `limit` is clamped to [1, MAX_LOG_LIMIT]. `next` is the
 * last returned seq when a full page came back, else null.
 */
export function getJobLogs(sql: SqlExec, id: string, after?: number, limit?: number): JobLogPage {
  if (!selectById(sql, id)) throw new HttpError(404, 'JOB_NOT_FOUND', `No job with id ${id}`);
  const afterSeq = after === undefined ? -1 : after;
  if (!Number.isSafeInteger(afterSeq) || afterSeq < -1) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'after must be a non-negative integer');
  }
  const pageSize = clampLimit(limit);
  const rows = sql
    .exec(
      'SELECT seq, stream, data, timestamp FROM fs_job_log WHERE job_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?',
      id, afterSeq, pageSize,
    )
    .toArray();
  const entries: JobLogEntry[] = rows.map((row) => ({
    seq: Number(row.seq),
    stream: String(row.stream) as JobStream,
    data: toBase64(row.data),
    timestamp: Number(row.timestamp),
  }));
  const next = entries.length === pageSize && entries.length > 0
    ? entries[entries.length - 1].seq
    : null;
  return { entries, next };
}

function clampLimit(limit?: number): number {
  if (limit === undefined) return DEFAULT_LOG_LIMIT;
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'limit must be a positive integer');
  }
  return Math.min(limit, MAX_LOG_LIMIT);
}

// ---------------------------------------------------------------------------
// State machine (claim, orphan recovery, terminal transitions, cancel)
// ---------------------------------------------------------------------------

/**
 * Mark every row still `running` as failed/interrupted. Called at the start of a
 * fresh DO generation before any new claim: such a row can only belong to a
 * previous, crashed generation whose command outcome is ambiguous. Never
 * auto-retries — an ambiguously admitted command must not run twice.
 */
export function recoverOrphans(sql: SqlExec, now: () => number = defaultNow): number {
  const running = sql.exec("SELECT id FROM fs_job WHERE status = 'running'").toArray();
  const timestamp = now();
  for (const row of running) {
    sql.exec(
      `UPDATE fs_job
       SET status = 'unknown', error = 'interrupted', finished_at = ?, updated_at = ?
       WHERE id = ? AND status = 'running'`,
      timestamp, timestamp, String(row.id),
    );
  }
  return running.length;
}

/**
 * Atomically claim the oldest queued job, transitioning it to `running` and
 * stamping started_at. Returns the claimed row or null when the queue is empty.
 */
export function claimNextJob(
  sql: SqlExec,
  transaction: TransactionSync,
  now: () => number = defaultNow,
): JobRow | null {
  return transaction(() => {
    const rows = sql
      .exec("SELECT * FROM fs_job WHERE status = 'queued' ORDER BY created_at ASC, rowid ASC LIMIT 1")
      .toArray();
    if (rows.length === 0) return null;
    const row = normalizeRow(rows[0]);
    const timestamp = now();
    sql.exec(
      "UPDATE fs_job SET status = 'running', started_at = ?, updated_at = ? WHERE id = ? AND status = 'queued'",
      timestamp, timestamp, row.id,
    );
    const claimed = selectById(sql, row.id);
    return claimed && claimed.status === 'running' ? claimed : null;
  });
}

/** Persist the container-side exec id once the runner sees the `start` event. */
export function setExecId(sql: SqlExec, id: string, execId: string, now: () => number = defaultNow): void {
  sql.exec('UPDATE fs_job SET exec_id = ?, updated_at = ? WHERE id = ?', execId, now(), id);
}

/** Append one ordered log row (BLOB) for a running job. */
export function appendJobLog(
  sql: SqlExec,
  jobId: string,
  seq: number,
  stream: JobStream,
  data: Uint8Array,
  now: () => number = defaultNow,
): void {
  sql.exec(
    'INSERT INTO fs_job_log (job_id, seq, stream, data, timestamp) VALUES (?, ?, ?, ?, ?)',
    jobId, seq, stream, Buffer.from(data), now(),
  );
}

export interface FinalizeInput {
  status: Extract<JobStatus, 'succeeded' | 'failed' | 'canceled' | 'unknown'>;
  exitCode: number | null;
  error: string | null;
  outputBytes: number;
  outputTruncated: boolean;
}

/** Atomically transition a job to a terminal state with its exit/output fields. */
export function finalizeJob(
  sql: SqlExec,
  id: string,
  input: FinalizeInput,
  now: () => number = defaultNow,
): void {
  const timestamp = now();
  sql.exec(
    `UPDATE fs_job
       SET status = ?, exit_code = ?, error = ?, output_bytes = ?, output_truncated = ?,
           finished_at = ?, updated_at = ?
     WHERE id = ?`,
    input.status,
    input.exitCode,
    input.error,
    input.outputBytes,
    input.outputTruncated ? 1 : 0,
    timestamp,
    timestamp,
    id,
  );
}

/** Outcome of a cancel request; `changed` is false for idempotent terminal cancels. */
export interface CancelResult {
  job: JobDto;
  changed: boolean;
  /** exec id to signal when a running job was asked to cancel, else null. */
  execToCancel: string | null;
}

/**
 * Request cancellation.
 *  - queued  -> canceled immediately (terminal), no command ever ran.
 *  - running -> cancel_requested set; the runner maps its terminal to canceled.
 *               Returns exec id (when known) so the caller can signal it.
 *  - terminal -> idempotent no-op.
 * Never duplicates command execution.
 */
export function requestCancel(
  sql: SqlExec,
  transaction: TransactionSync,
  id: string,
  now: () => number = defaultNow,
): CancelResult {
  return transaction(() => {
    const row = selectById(sql, id);
    if (!row) throw new HttpError(404, 'JOB_NOT_FOUND', `No job with id ${id}`);

    if (row.status === 'queued') {
      const timestamp = now();
      sql.exec(
        `UPDATE fs_job SET status = 'canceled', cancel_requested = 1, finished_at = ?, updated_at = ?
         WHERE id = ? AND status = 'queued'`,
        timestamp, timestamp, id,
      );
      const updated = selectById(sql, id);
      return { job: toJobDto(updated ?? row), changed: true, execToCancel: null };
    }

    if (row.status === 'running') {
      const already = row.cancel_requested !== 0;
      if (!already) {
        const timestamp = now();
        sql.exec('UPDATE fs_job SET cancel_requested = 1, updated_at = ? WHERE id = ?', timestamp, id);
      }
      const updated = selectById(sql, id);
      return { job: toJobDto(updated ?? row), changed: !already, execToCancel: (updated ?? row).exec_id };
    }

    // Terminal: idempotent.
    return { job: toJobDto(row), changed: false, execToCancel: null };
  });
}

// ---------------------------------------------------------------------------
// Bounded NDJSON decoder (Worker-side, incremental)
// ---------------------------------------------------------------------------

/** ExecEvent contract emitted by the container's /exec/stream endpoint. */
export type ExecEvent =
  | { type: 'start'; id: string }
  | { type: 'stdout'; id: string; data: string }
  | { type: 'stderr'; id: string; data: string }
  | { type: 'exit'; id: string; exitCode: number; signal?: string; timedOut?: boolean };

export class JobNdjsonError extends Error {}

const DEFAULT_MAX_LINE_BYTES = 4 * 1024 * 1024;

/**
 * Incremental NDJSON decoder tolerant of arbitrary chunk boundaries. Bounds a
 * single line so a runaway container cannot exhaust Worker memory. `push`
 * returns every complete parsed line; `flush` returns any trailing line.
 */
export class BoundedNdjsonDecoder {
  private readonly decoder = new TextDecoder();
  private readonly maxLineBytes: number;
  private buffer = '';

  constructor(options: { maxLineBytes?: number } = {}) {
    this.maxLineBytes = options.maxLineBytes ?? DEFAULT_MAX_LINE_BYTES;
  }

  push(chunk: Uint8Array): ExecEvent[] {
    this.buffer += this.decoder.decode(chunk, { stream: true });
    const events: ExecEvent[] = [];
    let newline = this.buffer.indexOf('\n');
    while (newline >= 0) {
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      this.bound(line);
      const parsed = this.parse(line);
      if (parsed) events.push(parsed);
      newline = this.buffer.indexOf('\n');
    }
    this.bound(this.buffer);
    return events;
  }

  flush(): ExecEvent[] {
    this.buffer += this.decoder.decode();
    const trailing = this.buffer;
    this.buffer = '';
    this.bound(trailing);
    const parsed = this.parse(trailing);
    return parsed ? [parsed] : [];
  }

  private bound(line: string): void {
    if (Buffer.byteLength(line, 'utf8') > this.maxLineBytes) {
      throw new JobNdjsonError(`NDJSON line exceeds ${this.maxLineBytes} bytes`);
    }
  }

  private parse(line: string): ExecEvent | null {
    if (line.trim() === '') return null;
    try {
      return JSON.parse(line) as ExecEvent;
    } catch {
      throw new JobNdjsonError(`Malformed NDJSON line: ${line.slice(0, 120)}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

export interface JobRunnerDeps {
  sql: SqlExec;
  /** Start a streaming command; resolves once the container accepts it. */
  execStream: (command: string, signal?: AbortSignal) => Promise<ReadableStream<Uint8Array>>;
  /** Cancel a command that was requested before its start event exposed the exec id. */
  cancelExec?: (id: string) => Promise<void>;
  now?: () => number;
  /** Persisted-output cap; defaults to {@link MAX_OUTPUT_BYTES}. Injectable for tests. */
  maxOutputBytes?: number;
}

/** Compose the shell command that runs a job in its remote cwd under /volume. */
export function composeJobCommand(command: string, cwd: string): string {
  const mountPath = cwd === '/' ? '/volume' : `/volume${cwd}`;
  return `cd -- ${shellQuote(mountPath)} && ${command}`;
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(value)) return value;
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function conciseError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 500 ? `${message.slice(0, 497)}...` : message;
}

function isUnknownOutcome(error: unknown): boolean {
  return error instanceof HttpError && error.code === 'COMMAND_OUTCOME_UNKNOWN'
    || conciseError(error).toLowerCase().includes('outcome is unknown');
}

/**
 * Drive a claimed (`running`) job to a terminal state through execStream.
 *
 * On start, persists exec_id. stdout/stderr events are base64-decoded and
 * appended as ordered BLOB rows, capped at MAX_OUTPUT_BYTES with a truncation
 * flag while the command keeps draining. On exit, transitions atomically to
 * succeeded/failed, or to canceled when cancellation was requested. A transport
 * or start error marks the job failed with a concise message.
 */
export async function runJob(deps: JobRunnerDeps, jobId: string): Promise<void> {
  const { sql } = deps;
  const now = deps.now ?? defaultNow;
  const maxOutputBytes = deps.maxOutputBytes ?? MAX_OUTPUT_BYTES;

  const job = selectById(sql, jobId);
  if (!job || job.status !== 'running') return;

  let stream: ReadableStream<Uint8Array>;
  try {
    stream = await deps.execStream(composeJobCommand(job.command, job.cwd));
  } catch (error) {
    finalizeJob(sql, jobId, {
      status: isUnknownOutcome(error) ? 'unknown' : 'failed',
      exitCode: null,
      error: conciseError(error),
      outputBytes: 0,
      outputTruncated: false,
    }, now);
    return;
  }

  const decoder = new BoundedNdjsonDecoder();
  const reader = stream.getReader();
  let seq = 0;
  let outputBytes = 0;
  let truncated = false;
  let exitCode: number | null = null;

  const handle = async (event: ExecEvent): Promise<void> => {
    if (event.type === 'start') {
      setExecId(sql, jobId, event.id, now);
      if (selectById(sql, jobId)?.cancel_requested && deps.cancelExec) {
        await deps.cancelExec(event.id);
      }
    } else if (event.type === 'stdout' || event.type === 'stderr') {
      const bytes = new Uint8Array(Buffer.from(event.data, 'base64'));
      if (bytes.length === 0) return;
      if (truncated) return;
      const remaining = maxOutputBytes - outputBytes;
      if (bytes.length <= remaining) {
        appendJobLog(sql, jobId, seq++, event.type, bytes, now);
        outputBytes += bytes.length;
      } else {
        if (remaining > 0) {
          appendJobLog(sql, jobId, seq++, event.type, bytes.subarray(0, remaining), now);
          outputBytes += remaining;
        }
        truncated = true;
      }
    } else if (event.type === 'exit') {
      exitCode = event.exitCode;
    }
  };

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) for (const event of decoder.push(value)) await handle(event);
    }
    for (const event of decoder.flush()) await handle(event);
  } catch (error) {
    finalizeJob(sql, jobId, {
      status: isUnknownOutcome(error) ? 'unknown' : 'failed',
      exitCode,
      error: conciseError(error),
      outputBytes,
      outputTruncated: truncated,
    }, now);
    return;
  } finally {
    reader.releaseLock();
  }

  const fresh = selectById(sql, jobId);
  const canceled = (fresh ?? job).cancel_requested !== 0;
  if (canceled) {
    finalizeJob(sql, jobId, {
      status: 'canceled',
      exitCode,
      error: null,
      outputBytes,
      outputTruncated: truncated,
    }, now);
  } else if (exitCode === 0) {
    finalizeJob(sql, jobId, {
      status: 'succeeded',
      exitCode: 0,
      error: null,
      outputBytes,
      outputTruncated: truncated,
    }, now);
  } else {
    finalizeJob(sql, jobId, {
      status: exitCode === null ? 'unknown' : 'failed',
      exitCode,
      error: exitCode === null ? 'Command outcome is unknown because the stream ended without an exit event' : null,
      outputBytes,
      outputTruncated: truncated,
    }, now);
  }
}

// ---------------------------------------------------------------------------
// Scheduling helper
// ---------------------------------------------------------------------------

export type ScheduleFn = (when: number, callback: string) => Promise<unknown>;

/**
 * Schedule the queue runner via the inherited Container scheduler. `delaySeconds`
 * is 0 to advance immediately after a submission or a terminal state, or a small
 * positive value to defer while an interactive exec/destroy holds the container.
 */
export async function scheduleJobRun(schedule: ScheduleFn, delaySeconds = 0): Promise<void> {
  await schedule(delaySeconds, RUN_NEXT_JOB_CALLBACK);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function defaultNow(): number {
  return Math.floor(Date.now() / 1000);
}

/** Encode a BLOB column value (Buffer | ArrayBuffer | typed array) to base64. */
function toBase64(data: unknown): string {
  if (data === null || data === undefined) return '';
  if (typeof data === 'string') return Buffer.from(data, 'binary').toString('base64');
  if (data instanceof ArrayBuffer) return Buffer.from(new Uint8Array(data)).toString('base64');
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return Buffer.from(new Uint8Array(view.buffer, view.byteOffset, view.byteLength)).toString('base64');
  }
  return Buffer.from(data as never).toString('base64');
}
