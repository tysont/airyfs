// ABOUTME: Worker entrypoint and AiryFS Durable Object class.
// ABOUTME: Routes HTTP requests to named volumes, each backed by a DO with persistent SQLite.

import { Container, getContainer } from '@cloudflare/containers';
import { AgentFS, type CloudflareStorage } from 'agentfs-sdk/cloudflare';
import {
  ChunkSizeConflictError,
  configureChunkSize,
  initSchema,
  InvalidChunkSizeError,
  SCHEMA_TABLES,
} from './schema';
import { HranaServer, wrapSqlStorage } from './hrana-server';
import { MutationJournal } from './mutation-journal';
import {
  errorResponse,
  fileResponse,
  handleFilesystemRequest,
  HttpError,
  parseV1Route,
  readCommandRequest,
  readJsonObject,
  readVolumeCreateRequest,
  toStatsDto,
  VolumeAccessCoordinator,
  writeFileStream,
  type StatsDto,
  type V1Route,
} from './files-api';
import { encodeTreeStream, type TreeSummary } from './archive';
import { holdStreamUntilDone } from './exec-stream';
import { sseToNdjson } from './sse-stream';
import { postContainerHttpStream } from './container-http-stream';
import { importTree } from './tree-import';
import {
  createSnapshot as createSnapshotRow,
  deleteSnapshot as deleteSnapshotRow,
  diffSnapshot as diffSnapshotRows,
  encodeSnapshotArchiveStream,
  listSnapshots as listSnapshotRows,
  resolveSnapshot,
  restoreSnapshot as restoreSnapshotRow,
  SnapshotNotFoundError,
  type SnapshotDiffEntry,
  type SnapshotInfo,
  type SnapshotStorage,
} from './snapshots';
import {
  isRootTarget,
  mapSnapshotError,
  readOptionalJsonObject,
  snapshotAccess,
} from './snapshot-routing';
import {
  abortUpload,
  appendUpload,
  beginUpload,
  completeUpload,
  getUpload,
  MAX_UPLOAD_CHUNK_BYTES,
  readBoundedChunk,
  type UploadBeginResult,
  type UploadCompleteResult,
  type UploadStatus,
} from './uploads';
import { sha256Path, type ChecksumResult } from './checksum';
import {
  claimNextJob,
  getJob as getJobRow,
  getJobLogs as getJobLogRows,
  listJobs as listJobRows,
  recoverOrphans,
  requestCancel,
  runJob,
  scheduleJobRun,
  submitJob as submitJobRow,
  validateStatusFilter,
  type JobDto,
  type JobLogPage,
  type JobStatus,
  type SubmitJobResult,
} from './jobs';
import {
  ChangeFeedError,
  getChanges as getChangeRows,
  type ChangePage,
} from './change-feed';
import {
  authenticate,
  buildCapability,
  capabilityAllows,
  isCapabilityRevoked,
  OPERATIONS,
  revokeCapability,
  signCapability,
  type AccessRequirement,
  type Identity,
  type Operation,
} from './auth';

interface Env {
  AiryFS: DurableObjectNamespace<AiryFS>;
  /** When set, HTTP access requires a root or capability bearer credential. */
  AIRYFS_AUTH_SECRET?: string;
}

interface WorkerSocket {
  opened: Promise<unknown>;
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  close(): Promise<void>;
}

const STARTUP_TIMEOUT_MS = 60_000;
const CONTAINER_EXEC_TIMEOUT_MS = 310_000;
const DESTROY_TIMEOUT_MS = 10_000;
const CHANGE_LONG_POLL_MAX_MS = 25_000;
const CHANGE_LONG_POLL_INTERVAL_MS = 200;

async function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

export class AiryFS extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = '30m';

  private fs: AgentFS | null = null;
  private startupPromise: Promise<void> | null = null;
  private startupAbort: AbortController | null = null;
  private destroyPromise: Promise<void> | null = null;
  private activeServePromise: Promise<void> | null = null;
  private invalidationServePromise: Promise<void> | null = null;
  private activeExec: symbol | null = null;
  /** In-memory single-flight guard for the scheduled queue runner. */
  private jobRunning = false;
  private dataSocket: WorkerSocket | null = null;
  private invalidationSocket: WorkerSocket | null = null;
  private hranaServer: HranaServer | null = null;
  private readonly access = new VolumeAccessCoordinator();
  private readonly mutations: MutationJournal;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx as never, env);
    this.mutations = new MutationJournal(this.ctx.storage.sql);
    this.ctx.blockConcurrencyWhile(async () => {
      initSchema(this.ctx.storage.sql, (callback) => this.ctx.storage.transactionSync(callback));
    });
  }

  private filesystem(requestedChunkSize?: unknown): AgentFS {
    const configuration = this.ctx.storage.transactionSync(() =>
      configureChunkSize(this.ctx.storage.sql, requestedChunkSize)
    );
    if (!this.fs || this.fs.getChunkSize() !== configuration.chunkSize) {
      this.fs = AgentFS.create(this.ctx.storage as unknown as CloudflareStorage);
    }
    return this.fs;
  }

  private createVolume(requestedChunkSize: unknown): { chunkSize: number } {
    try {
      const fs = this.filesystem(requestedChunkSize);
      return { chunkSize: fs.getChunkSize() };
    } catch (error) {
      if (error instanceof InvalidChunkSizeError) {
        throw new HttpError(400, 'INVALID_CHUNK_SIZE', error.message);
      }
      if (error instanceof ChunkSizeConflictError) {
        throw new HttpError(409, 'CHUNK_SIZE_IMMUTABLE', error.message);
      }
      throw error;
    }
  }

  override onStop() {
    // Socket and request completion own cleanup. A delayed lifecycle callback
    // must not clear state belonging to a replacement Container generation.
  }

  // ---------------------------------------------------------------------------
  // Core API
  // ---------------------------------------------------------------------------

  /** Execute a shell command in the Container against the FUSE-mounted volume. */
  async exec(command: string, signal?: AbortSignal): Promise<unknown> {
    if (this.activeExec || this.destroyPromise) {
      throw new HttpError(503, 'EXEC_BUSY', 'Another command or Container lifecycle operation is already running');
    }
    const execution = Symbol('exec');
    this.activeExec = execution;
    try {
      await this.ensureContainer(signal);
      signal?.throwIfAborted();

      const commandSignals = [AbortSignal.timeout(
        command === ':' ? STARTUP_TIMEOUT_MS : CONTAINER_EXEC_TIMEOUT_MS
      )];
      if (command === ':' && signal) commandSignals.push(signal);
      let resp: Response;
      try {
        resp = await this.containerFetch(
          new Request('http://localhost/exec', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command }),
            signal: AbortSignal.any(commandSignals),
          }),
          4000
        );
      } catch (error) {
        if (command !== ':') throw error;
        this.clearRuntimeConnections();
        await this.destroyBounded();
        throw new HttpError(503, 'CONTAINER_UNAVAILABLE', 'Container preflight timed out; retry startup');
      }

      if (!resp.ok) {
        const message = await resp.text();
        if (resp.status === 503 && message.includes('FUSE unavailable')) {
          this.clearRuntimeConnections();
          await this.destroyBounded();
          throw new HttpError(503, 'CONTAINER_UNAVAILABLE', message);
        }
        if (resp.status === 503) {
          throw new HttpError(503, 'EXEC_BUSY', 'Another command is already running');
        }
        throw new Error(`Container exec failed (${resp.status}): ${message}`);
      }

      return resp.json();
    } finally {
      if (this.activeExec === execution) this.activeExec = null;
    }
  }

  /**
   * Stream a shell command in the Container as NDJSON events over Workers RPC.
   *
   * Reuses the same single-flight token and container startup as {@link exec}.
   * The token is held until the returned stream completes, errors, or is
   * canceled — a caller that only awaits this method still holds it while the
   * command runs. The real-command abort `signal` is propagated to the container
   * so aborting it (or the returned stream) terminates the process there.
   */
  async execStream(command: string, signal?: AbortSignal): Promise<ReadableStream<Uint8Array>> {
    if (this.activeExec || this.destroyPromise) {
      throw new HttpError(503, 'EXEC_BUSY', 'Another command or Container lifecycle operation is already running');
    }
    const execution = Symbol('exec-stream');
    this.activeExec = execution;
    const release = (): void => {
      if (this.activeExec === execution) this.activeExec = null;
    };
    try {
      await this.ensureContainer(signal);
      signal?.throwIfAborted();

      const id = crypto.randomUUID();
      const commandSignals = [AbortSignal.timeout(CONTAINER_EXEC_TIMEOUT_MS)];
      if (signal) commandSignals.push(signal);

      const commandSignal = AbortSignal.any(commandSignals);
      const socket = this.ctx.container!.getTcpPort(4000).connect('0.0.0.0:4000');
      const resp = await postContainerHttpStream(socket, '/exec/stream', { command, id }, commandSignal);

      if (resp.status < 200 || resp.status >= 300) {
        const message = await new Response(resp.body).text();
        if (resp.status === 503 && message.includes('FUSE unavailable')) {
          this.clearRuntimeConnections();
          await this.destroyBounded();
          throw new HttpError(503, 'CONTAINER_UNAVAILABLE', message);
        }
        if (resp.status === 503) {
          throw new HttpError(503, 'EXEC_BUSY', 'Another command is already running');
        }
        throw new Error(`Container exec failed (${resp.status}): ${message}`);
      }

      return holdStreamUntilDone(sseToNdjson(resp.body), release);
    } catch (error) {
      release();
      throw error;
    }
  }

  /**
   * Terminate a streaming command by its id. Best-effort: it neither starts nor
   * recycles the Container, since a missing Container means nothing to cancel.
   */
  async cancelExec(id: string): Promise<void> {
    try {
      await this.containerFetch(
        new Request('http://localhost/exec/cancel', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id }),
          signal: AbortSignal.timeout(STARTUP_TIMEOUT_MS),
        }),
        4000
      );
    } catch {
      // Container gone or unreachable; the command is already not running.
    }
  }

  /** Read a file from the volume (direct DO access, no Container needed). */
  async readFile(path: string): Promise<string> {
    const fs = this.filesystem();
    const release = await this.access.acquireRead(path);
    try {
      return await fs.readFile(path, 'utf8');
    } finally {
      release();
    }
  }

  /** Write a file to the volume (direct DO access, no Container needed). */
  async writeFile(path: string, content: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.writeFile(path, content);
      await this.mutations.record(fs, [path]);
    } finally {
      release();
    }
  }

  /** List a directory on the volume (direct DO access, no Container needed). */
  async listDir(path: string): Promise<string[]> {
    const release = await this.access.acquireRead(path);
    try {
      return await this.filesystem().readdir(path);
    } finally {
      release();
    }
  }

  /** Get serializable metadata for a file, directory, or symlink target. */
  async statPath(path: string): Promise<StatsDto> {
    const release = await this.access.acquireRead(path);
    try {
      return toStatsDto(await this.filesystem().stat(path));
    } finally {
      release();
    }
  }

  /** List a directory with metadata in one AgentFS query. */
  async listDirDetailed(path: string): Promise<Array<{ name: string } & StatsDto>> {
    const release = await this.access.acquireRead(path);
    try {
      return (await this.filesystem().readdirPlus(path)).map((entry) => ({
        name: entry.name,
        ...toStatsDto(entry.stats),
      }));
    } finally {
      release();
    }
  }

  async makeDir(path: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.mkdir(path);
      await this.mutations.record(fs, [path]);
    } finally { release(); }
  }

  async removePath(path: string, recursive = false): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.rm(path, { recursive });
      await this.mutations.record(fs, [path]);
    } finally { release(); }
  }

  async renamePath(from: string, to: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite([from, to]);
    try {
      await fs.rename(from, to);
      await this.mutations.record(fs, [from, to]);
    } finally { release(); }
  }

  async copyPath(from: string, to: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite([from, to]);
    try {
      await fs.copyFile(from, to);
      await this.mutations.record(fs, [to]);
    } finally { release(); }
  }

  async createSymlink(target: string, path: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.symlink(target, path);
      await this.mutations.record(fs, [path]);
    } finally { release(); }
  }

  async readSymlink(path: string): Promise<string> {
    const release = await this.access.acquireRead(path);
    try {
      return await this.filesystem().readlink(path);
    } finally {
      release();
    }
  }

  /** Stream a binary file over Workers RPC. */
  async readFileStream(path: string): Promise<ReadableStream<Uint8Array>> {
    const response = await fileResponse(
      this.filesystem(),
      path,
      new Request('http://localhost', { method: 'GET' }),
      this.access
    );
    if (!response.body) {
      return new ReadableStream({
        type: 'bytes',
        start: (controller) => controller.close(),
      });
    }
    return response.body;
  }

  /** Atomically replace a file from a byte stream over Workers RPC. */
  async writeFileStream(path: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    const fs = this.filesystem();
    await writeFileStream(fs, path, stream, this.access);
    await this.mutations.record(fs, [path]);
  }

  /**
   * Export a directory subtree as a AiryFS archive stream over Workers RPC.
   *
   * Holds the existing path read lock for the full lifetime of the stream so the
   * archive is a point-in-time-consistent snapshot: the lock conflicts with the
   * FUSE whole-volume ('*') write lock and with any direct write under `path`.
   */
  async exportTreeStream(path: string): Promise<ReadableStream<Uint8Array>> {
    const fs = this.filesystem();
    const release = await this.access.acquireRead(path);
    try {
      const stats = await fs.stat(path);
      if (!stats.isDirectory()) {
        throw new HttpError(409, 'ENOTDIR', `Export target is not a directory: ${path}`);
      }
    } catch (error) {
      release();
      throw error;
    }
    return encodeTreeStream(fs, path, release);
  }

  /**
   * Import a AiryFS archive stream into a non-root directory, transactionally.
   * Delegates staging and the observer-safe swap to {@link importTree}.
   */
  async importTreeStream(
    path: string,
    stream: ReadableStream<Uint8Array> | null,
    options?: { replace?: boolean; allowRoot?: boolean }
  ): Promise<TreeSummary> {
    const fs = this.filesystem();
    const summary = await importTree(
      fs,
      path,
      stream,
      { replace: options?.replace, allowRoot: options?.allowRoot },
      {
        acquireWrite: (target) => this.access.acquireWrite(target),
        record: (paths) => this.mutations.record(fs, paths),
      }
    );
    // A root replace rebuilds the whole tree; drop the cached AgentFS so the
    // next access re-reads the fresh inode/dentry state.
    if (options?.allowRoot && isRootTarget(path)) this.fs = null;
    return summary;
  }

  // ---------------------------------------------------------------------------
  // Resumable uploads and direct checksums (trusted RPC surface)
  // ---------------------------------------------------------------------------

  /** Compute the streaming SHA-256 of a regular file without buffering it. */
  async checksum(path: string): Promise<ChecksumResult> {
    return sha256Path(this.filesystem(), path, this.access);
  }

  /** Create or resume a resumable upload session addressed by its target path. */
  async beginUpload(
    path: string,
    body: { size: number; checksum: string }
  ): Promise<UploadBeginResult> {
    return beginUpload(this.filesystem(), this.ctx.storage.sql, this.access, path, body);
  }

  /** Return the status of an active upload session. */
  async uploadStatus(path: string): Promise<UploadStatus> {
    return getUpload(this.ctx.storage.sql, path);
  }

  /** Append one bounded, checksummed chunk at the stored offset. */
  async appendUpload(
    path: string,
    offset: number,
    chunkSha256: string,
    data: Uint8Array
  ): Promise<UploadStatus> {
    return appendUpload(this.filesystem(), this.ctx.storage.sql, this.access, path, {
      offset,
      chunkSha256,
      data,
    });
  }

  /** Publish a fully-received upload over its target and journal the change. */
  async completeUpload(path: string): Promise<UploadCompleteResult> {
    const fs = this.filesystem();
    const result = await completeUpload(fs, this.ctx.storage.sql, this.access, path);
    await this.mutations.record(fs, [path]);
    return result;
  }

  /** Abort an upload, removing its temp file and session. */
  async abortUpload(path: string): Promise<void> {
    await abortUpload(this.filesystem(), this.ctx.storage.sql, this.access, path);
  }

  // ---------------------------------------------------------------------------
  // Snapshots (trusted RPC surface; HTTP routing enforces auth separately)
  // ---------------------------------------------------------------------------

  /** Structural view of this DO's SQLite storage for the raw snapshot SQL module. */
  private snapshotStorage(): SnapshotStorage {
    return this.ctx.storage as unknown as SnapshotStorage;
  }

  /** Capture a full-volume snapshot. An omitted name generates a timestamped default. */
  async createSnapshot(name?: string, note?: string): Promise<SnapshotInfo> {
    const release = await this.access.acquireRead('*');
    try {
      return createSnapshotRow(this.snapshotStorage(), name, note);
    } finally {
      release();
    }
  }

  /** List every snapshot, oldest first. */
  async listSnapshots(): Promise<SnapshotInfo[]> {
    return listSnapshotRows(this.snapshotStorage());
  }

  /** Diff a snapshot against the live volume (default) or another snapshot. */
  async diffSnapshot(id: string, against: string = 'live'): Promise<SnapshotDiffEntry[]> {
    const release = await this.access.acquireRead('*');
    try {
      const target = against === 'live' ? ('live' as const) : { snapshot: against };
      return diffSnapshotRows(this.snapshotStorage(), { snapshot: id }, target);
    } finally {
      release();
    }
  }

  /**
   * Restore a snapshot over the live volume. Rejects while an exec or Container
   * lifecycle operation is active, destroys/recycles the Container first so it
   * remounts fresh against the restored data, then swaps the tables under the
   * whole-volume write lock and drops the cached AgentFS. The snapshot itself is
   * preserved.
   */
  async restoreSnapshot(id: string): Promise<SnapshotInfo> {
    if (this.activeExec || this.destroyPromise) {
      throw new HttpError(503, 'EXEC_BUSY', 'Another command or Container lifecycle operation is already running');
    }
    if (!resolveSnapshot(this.snapshotStorage(), id)) {
      throw new SnapshotNotFoundError(id);
    }
    await this.destroyContainer();
    const release = await this.access.acquireWrite('*');
    try {
      const info = restoreSnapshotRow(this.snapshotStorage(), id);
      this.fs = null;
      return info;
    } finally {
      release();
    }
  }

  /** Delete a snapshot and all its payload rows. */
  async deleteSnapshot(id: string): Promise<SnapshotInfo> {
    const release = await this.access.acquireWrite('*');
    try {
      return deleteSnapshotRow(this.snapshotStorage(), id);
    } finally {
      release();
    }
  }

  /**
   * Stream a snapshot's contents as a AIRYFS archive over Workers RPC. Holds a
   * whole-volume read lock for the stream's lifetime so it cannot race a restore.
   */
  async exportSnapshotStream(id: string): Promise<ReadableStream<Uint8Array>> {
    const release = await this.access.acquireRead('/');
    try {
      if (!resolveSnapshot(this.snapshotStorage(), id)) {
        throw new SnapshotNotFoundError(id);
      }
    } catch (error) {
      release();
      throw error;
    }
    return encodeSnapshotArchiveStream(this.snapshotStorage(), id, release);
  }

  /**
   * Clone a snapshot's exact contents into another volume's root. Streams the
   * snapshot's AIRYFS archive straight into the target DO's transactional root
   * import (replace + allowRoot). The target must differ from the source volume;
   * cloning onto the source is refused. This RPC surface is trusted.
   */
  async cloneSnapshot(id: string, targetVolume: string): Promise<TreeSummary> {
    if (typeof targetVolume !== 'string' || targetVolume.trim() === '') {
      throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing target volume');
    }
    if (!resolveSnapshot(this.snapshotStorage(), id)) {
      throw new SnapshotNotFoundError(id);
    }
    if (this.env.AiryFS.idFromName(targetVolume).equals(this.ctx.id)) {
      throw new HttpError(409, 'CLONE_SELF', 'Clone target volume must differ from the source volume');
    }
    const stream = await this.exportSnapshotStream(id);
    return getContainer<AiryFS>(this.env.AiryFS, targetVolume).importTreeStream('/', stream, {
      replace: true,
      allowRoot: true,
    });
  }

  // ---------------------------------------------------------------------------
  // Durable job queue (trusted RPC surface; HTTP routing enforces auth separately)
  // ---------------------------------------------------------------------------

  /**
   * Enqueue a command to run in the Container. Persists the job (deduplicating on
   * the idempotency key) before scheduling the queue runner, so a submission is
   * durable even if scheduling or this DO generation fails immediately after.
   */
  async submitJob(command: string, cwd: string, idempotencyKey: string): Promise<SubmitJobResult> {
    const result = submitJobRow(
      this.ctx.storage.sql,
      (callback) => this.ctx.storage.transactionSync(callback),
      { command, cwd, idempotencyKey },
    );
    if (result.job.status === 'queued') {
      await scheduleJobRun((when, cb) => this.schedule(when, cb));
    }
    return result;
  }

  /** List jobs, newest first, optionally filtered by status. */
  listJobs(status?: JobStatus): JobDto[] {
    return listJobRows(this.ctx.storage.sql, status);
  }

  /** Fetch a single job by id. */
  getJob(id: string): JobDto {
    return getJobRow(this.ctx.storage.sql, id);
  }

  /** Read a page of a job's persisted logs. */
  getJobLogs(id: string, after?: number, limit?: number): JobLogPage {
    return getJobLogRows(this.ctx.storage.sql, id, after, limit);
  }

  /**
   * Cancel a job. Queued jobs cancel immediately; a running job is flagged and
   * its container exec is signaled so the runner records a canceled terminal.
   * Terminal jobs are returned unchanged (idempotent).
   */
  async cancelJob(id: string): Promise<JobDto> {
    const result = requestCancel(
      this.ctx.storage.sql,
      (callback) => this.ctx.storage.transactionSync(callback),
      id,
    );
    if (result.execToCancel) await this.cancelExec(result.execToCancel);
    return result.job;
  }

  /**
   * Scheduled queue runner. Single-flight in memory; recovers orphaned `running`
   * rows from a prior generation, defers while an interactive exec/destroy holds
   * the Container, otherwise claims and runs the oldest queued job to a terminal
   * state and schedules the next run.
   */
  async runNextJob(): Promise<void> {
    if (this.jobRunning) return;
    this.jobRunning = true;
    try {
      const recovered = recoverOrphans(this.ctx.storage.sql);
      if (recovered > 0) {
        // The previous DO generation may have left an admitted command alive in
        // the Container. Recycle it before admitting another queued command.
        await this.destroyContainer();
      }

      if (this.activeExec || this.destroyPromise) {
        await scheduleJobRun((when, cb) => this.schedule(when, cb), 1);
        return;
      }

      const claimed = claimNextJob(
        this.ctx.storage.sql,
        (callback) => this.ctx.storage.transactionSync(callback),
      );
      if (!claimed) return;

      await runJob(
        {
          sql: this.ctx.storage.sql,
          execStream: (command, signal) => this.execStream(command, signal),
          cancelExec: (execId) => this.cancelExec(execId),
        },
        claimed.id,
      );
      // Advance the queue after reaching a terminal state.
      await scheduleJobRun((when, cb) => this.schedule(when, cb));
    } finally {
      this.jobRunning = false;
    }
  }

  /** Read filesystem changes after an exclusive cursor; exposed through RPC. */
  getChanges(
    since: number | 'latest' = 'latest',
    limit?: number,
    pathPrefix = '/',
  ): ChangePage {
    try {
      return getChangeRows(this.ctx.storage.sql, { since, limit, pathPrefix });
    } catch (error) {
      if (error instanceof ChangeFeedError) {
        throw new HttpError(400, 'INVALID_ARGUMENT', error.message);
      }
      throw error;
    }
  }

  /** Hold a bounded poll while allowing concurrent filesystem mutations. */
  private async waitForChanges(
    since: number | 'latest',
    limit: number | undefined,
    pathPrefix: string,
    waitMs: number,
    signal: AbortSignal,
  ): Promise<ChangePage> {
    let page = this.getChanges(since, limit, pathPrefix);
    if (page.events.length > 0 || page.gap || waitMs === 0) return page;

    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      const delay = Math.min(CHANGE_LONG_POLL_INTERVAL_MS, deadline - Date.now());
      await abortable(new Promise<void>((resolve) => setTimeout(resolve, delay)), signal);
      page = this.getChanges(page.cursor, limit, pathPrefix);
      if (page.events.length > 0 || page.gap) return page;
    }
    return page;
  }

  /** Get volume metadata: table row counts. */
  dbInfo(): Record<string, number> {
    const result: Record<string, number> = {};
    for (const table of SCHEMA_TABLES) {
      try {
        const row = this.ctx.storage.sql
          .exec<{ count: number }>(`SELECT count(*) as count FROM ${table}`)
          .one();
        result[table] = row.count;
      } catch {
        // Table may not exist if AgentFS.create() hasn't run yet
      }
    }
    return result;
  }

  /** Return logical filesystem usage, physical SQLite size, and runtime health. */
  async usage(): Promise<Record<string, unknown>> {
    const filesystem = await this.filesystem().statfs();
    const runtimeState = await this.getState();
    let container: Record<string, unknown> = {
      state: runtimeState.status,
      hranaConnected: Boolean(this.activeServePromise),
    };

    if (runtimeState.status === 'healthy' && this.activeServePromise && !this.destroyPromise) {
      try {
        const response = await this.ctx.container!.getTcpPort(4000).fetch(
          new Request('http://localhost/health', { signal: AbortSignal.timeout(5_000) }),
        );
        container = {
          state: runtimeState.status,
          hranaConnected: true,
          ...await response.json<Record<string, unknown>>(),
        };
      } catch (error) {
        container = {
          state: runtimeState.status,
          hranaConnected: Boolean(this.activeServePromise),
          health: 'unhealthy',
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }

    return {
      filesystem,
      sqliteBytes: this.ctx.storage.sql.databaseSize,
      container,
      hrana: {
        pipelineRequests: this.hranaServer?.pipelineCount ?? 0,
        sqlStatements: this.hranaServer?.statementCount ?? 0,
      },
    };
  }

  /** Destroy the Container. Volume data persists in DO SQLite. */
  async destroyContainer(): Promise<void> {
    if (this.destroyPromise) return this.destroyPromise;
    const operation = (async (): Promise<void> => {
      this.startupAbort?.abort();
      await this.startupPromise?.catch(() => undefined);
      await this.destroyBounded();
      this.clearRuntimeState();
    })();
    this.destroyPromise = operation;
    try {
      await operation;
    } finally {
      if (this.destroyPromise === operation) this.destroyPromise = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Container lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Start the Container and set up bridge + FUSE mount via HTTP calls.
   *
   * The DO drives the sequence:
   *   1. Container starts with command-server.js (port 4000)
   *   2. POST /setup → bridge starts in-process (ports 9000 + 8080)
   *   3. DO connects TCP to :9000, starts HranaServer
   *   4. POST /mount → agentfs FUSE daemon at /volume
   *
   * If the Hrana TCP connection drops (container eviction, crash),
   * activeServePromise resets to null so the next call reconnects.
   */
  private async ensureContainer(requestSignal?: AbortSignal): Promise<void> {
    this.filesystem();
    if (this.startupPromise) return this.startupPromise;

    const startupAbort = new AbortController();
    const signals = [startupAbort.signal, AbortSignal.timeout(STARTUP_TIMEOUT_MS)];
    if (requestSignal) signals.push(requestSignal);
    const signal = AbortSignal.any(signals);
    const startup = this.prepareContainer(signal);
    this.startupAbort = startupAbort;
    this.startupPromise = startup;
    try {
      await startup;
    } catch (error) {
      startupAbort.abort();
      this.closeRuntimeSockets();
      // An explicit destroy owns cleanup after it has waited for this startup.
      const alreadyRecycled = error instanceof HttpError && error.code === 'CONTAINER_RECYCLED';
      if (!alreadyRecycled && !this.destroyPromise) await this.destroyBounded();
      throw error;
    } finally {
      if (this.startupPromise === startup) this.startupPromise = null;
      if (this.startupAbort === startupAbort) this.startupAbort = null;
    }
  }

  private async prepareContainer(signal: AbortSignal): Promise<void> {
    const state = await this.getState();
    if (state.status === 'stopped' || state.status === 'stopped_with_code') {
      await this.startContainer(signal);
      return;
    }
    if (state.status === 'stopping') {
      await this.destroyBounded();
      this.clearRuntimeConnections();
      throw new HttpError(
        503,
        'CONTAINER_RECYCLED',
        'Recycled an orphaned Container; retry startup',
        { 'Retry-After': '1' }
      );
    }

    await this.setupBridge(signal);
    await this.connectData(signal);
    await this.connectInvalidation(signal);
    try {
      const response = await this.containerFetch(
        new Request('http://localhost/health', { signal }),
        4000
      );
      const health = await response.json<{ fuseMounted?: boolean }>();
      if (health.fuseMounted) return;
    } catch {
      // Recycle below. A healthy Container without a usable mount cannot serve exec.
    }
    this.clearRuntimeConnections();
    await this.destroyBounded();
    throw new HttpError(
      503,
      'CONTAINER_RECYCLED',
      'Recycled a Container with an unavailable FUSE mount; retry startup',
      { 'Retry-After': '1' }
    );
  }

  private async startContainer(signal: AbortSignal): Promise<void> {

    this.entrypoint = ['node', '/app/dist/command-server.js'];

    // 1. Wait for command server
    await this.startAndWaitForPorts({
      ports: [4000],
      cancellationOptions: {
        abort: signal,
        instanceGetTimeoutMS: STARTUP_TIMEOUT_MS,
        portReadyTimeoutMS: STARTUP_TIMEOUT_MS,
      },
    });

    // 2. Start bridge in-process
    await this.setupBridge(signal);

    // 3. Connect FUSE data and invalidation traffic.
    const { socket, servePromise } = await this.connectData(signal);
    await this.connectInvalidation(signal);

    // 4. Mount FUSE — fire-and-forget, then poll for readiness.
    // Can't await because the mount blocks while FUSE queries flow through serve().
    this.containerFetch(
      new Request('http://localhost/mount', { method: 'POST', signal }),
      4000
    ).catch(() => {
      // Mount request itself failing is not fatal — the health poll below
      // will detect whether the mount actually succeeded.
    });

    // Poll until FUSE is mounted or we give up. The health endpoint checks
    // mountpoint -q and reports fuseExitCode if the daemon crashed.
    let mounted = false;
    let mountError: Error | null = null;
    for (let i = 0; i < 30; i++) {
      await abortable(new Promise<void>((resolve) => setTimeout(resolve, 1000)), signal);
      try {
        const healthResp = await this.containerFetch(
          new Request('http://localhost/health', { signal }),
          4000
        );
        const health = (await healthResp.json()) as {
          fuseMounted?: boolean;
          fuseExitCode?: number | null;
        };
        if (health.fuseMounted) {
          mounted = true;
          break;
        }
        // If daemon exited, stop polling
        if (health.fuseExitCode !== null && health.fuseExitCode !== undefined) {
          mountError = new Error(`FUSE daemon exited with code ${health.fuseExitCode}`);
          break;
        }
      } catch (err) {
        // Container not ready yet — keep polling
      }
    }

    if (!mounted) {
      await socket.close().catch(() => undefined);
      await this.invalidationSocket?.close().catch(() => undefined);
      if (this.activeServePromise === servePromise) this.activeServePromise = null;
      throw mountError ?? new Error('FUSE mount did not complete within 30 seconds');
    }
  }

  private async setupBridge(signal: AbortSignal): Promise<void> {
    const setupResp = await this.containerFetch(
      new Request('http://localhost/setup', { method: 'POST', signal }),
      4000
    );
    if (!setupResp.ok) {
      throw new Error(`Bridge setup failed (${setupResp.status}): ${await setupResp.text()}`);
    }
    const setupResult = (await setupResp.json()) as { ok: boolean; error?: string };
    if (!setupResult.ok) {
      throw new Error(`Bridge setup failed: ${setupResult.error}`);
    }
  }

  private async connectData(signal: AbortSignal): Promise<{
    socket: WorkerSocket;
    servePromise: Promise<void>;
  }> {
    const previousSocket = this.dataSocket;
    const socket = this.ctx.container!.getTcpPort(9000).connect('0.0.0.0:9000');
    this.dataSocket = socket;
    try {
      await abortable(socket.opened, signal);
    } catch (error) {
      if (this.dataSocket === socket) this.dataSocket = null;
      await socket.close().catch(() => undefined);
      throw error;
    }
    await previousSocket?.close().catch(() => undefined);

    this.hranaServer = new HranaServer({
      readable: socket.readable,
      writable: socket.writable,
      sql: wrapSqlStorage(this.ctx.storage.sql),
      writeLock: () => this.access.acquireWrite('*'),
    });

    const servePromise = this.hranaServer.serve();
    this.activeServePromise = servePromise;
    this.ctx.waitUntil(servePromise);
    const clearServePromise = (): void => {
      if (this.activeServePromise === servePromise) {
        this.activeServePromise = null;
        if (this.dataSocket === socket) this.dataSocket = null;
      }
    };
    void servePromise.then(clearServePromise, clearServePromise);
    return { socket, servePromise };
  }

  private async connectInvalidation(signal: AbortSignal): Promise<void> {
    if (this.invalidationServePromise) return;
    const socket = this.ctx.container!.getTcpPort(9001).connect('0.0.0.0:9001');
    this.invalidationSocket = socket;
    try {
      await abortable(socket.opened, signal);
    } catch (error) {
      if (this.invalidationSocket === socket) this.invalidationSocket = null;
      await socket.close().catch(() => undefined);
      throw error;
    }
    const server = new HranaServer({
      readable: socket.readable,
      writable: socket.writable,
      sql: wrapSqlStorage(this.ctx.storage.sql),
    });
    const servePromise = server.serve();
    this.invalidationServePromise = servePromise;
    this.ctx.waitUntil(servePromise);
    const clear = (): void => {
      if (this.invalidationServePromise === servePromise) {
        this.invalidationServePromise = null;
        if (this.invalidationSocket === socket) this.invalidationSocket = null;
      }
    };
    void servePromise.then(clear, clear);
  }

  private closeRuntimeSockets(): void {
    const sockets = [this.dataSocket, this.invalidationSocket];
    this.dataSocket = null;
    this.invalidationSocket = null;
    for (const socket of sockets) void socket?.close().catch(() => undefined);
  }

  private clearRuntimeConnections(): void {
    this.activeServePromise = null;
    this.invalidationServePromise = null;
    this.hranaServer = null;
    this.closeRuntimeSockets();
  }

  private clearRuntimeState(): void {
    this.startupAbort?.abort();
    this.startupAbort = null;
    this.startupPromise = null;
    this.activeExec = null;
    this.clearRuntimeConnections();
  }

  private async destroyBounded(): Promise<void> {
    try {
      await abortable(this.destroy(), AbortSignal.timeout(DESTROY_TIMEOUT_MS));
    } catch (error) {
      this.ctx.abort('Container destruction did not complete before its deadline');
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Authentication and authorization
  // ---------------------------------------------------------------------------

  /**
   * Resolve the caller's identity and authorize the request. Auth is enforced
   * only when AIRYFS_AUTH_SECRET is configured; otherwise local/test behavior is
   * preserved. Workers RPC bypasses this path and stays trusted.
   */
  private async authorize(
    request: Request,
    url: URL,
    v1Route: V1Route | null,
    volume: string
  ): Promise<Identity> {
    const secret = this.env.AIRYFS_AUTH_SECRET;
    if (!secret) return { kind: 'disabled' };

    const identity = await authenticate(secret, request.headers.get('Authorization'), volume);
    if (identity.kind === 'capability') {
      if (isCapabilityRevoked(this.ctx.storage.sql, identity.capability.id)) {
        throw new HttpError(403, 'TOKEN_REVOKED', 'Capability token has been revoked');
      }
      const requirement = await requiredAccess(request, url, v1Route);
      if (
        requirement.operation &&
        !capabilityAllows(identity.capability, requirement.operation, requirement.paths)
      ) {
        const scope = requirement.paths.join(', ') || '/';
        throw new HttpError(
          403,
          'FORBIDDEN',
          `Capability does not permit ${requirement.operation} on ${scope}`
        );
      }
    }
    return identity;
  }

  private async handleCapabilities(
    request: Request,
    route: V1Route,
    identity: Identity
  ): Promise<Response> {
    const secret = this.env.AIRYFS_AUTH_SECRET;

    if (request.method === 'GET' && route.path === '/') {
      return Response.json(authStatus(identity, route.volume));
    }

    if (request.method === 'POST' && route.path === '/') {
      if (!secret) {
        throw new HttpError(409, 'AUTH_DISABLED', 'Set AIRYFS_AUTH_SECRET to mint capability tokens');
      }
      const body = await readJsonObject(request);
      const capability = buildCapability(
        route.volume,
        parseOperations(body.operations),
        parsePathPrefixes(body.pathPrefixes),
        parseExpiry(body.expiresInSeconds)
      );
      const token = await signCapability(secret, capability);
      return Response.json({ token, ...capability }, { status: 201 });
    }

    if (request.method === 'DELETE' && route.path !== '/') {
      if (!secret) {
        throw new HttpError(409, 'AUTH_DISABLED', 'Set AIRYFS_AUTH_SECRET to manage capability tokens');
      }
      const id = route.path.slice(1);
      revokeCapability(this.ctx.storage.sql, id);
      return Response.json({ id, revoked: true });
    }

    throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', {
      Allow: 'GET, POST, DELETE',
    });
  }

  /**
   * Route the `snapshots` resource. Collection: GET (list), POST (create).
   * Item: DELETE. Item sub-actions: GET `/:id/diff`, POST `/:id/restore`,
   * POST `/:id/clone`. SnapshotError codes are mapped to stable HTTP statuses.
   */
  private async handleSnapshots(
    request: Request,
    url: URL,
    route: V1Route,
    identity: Identity
  ): Promise<Response> {
    try {
      const segments = route.path.split('/').filter(Boolean);
      const method = request.method;

      if (segments.length === 0) {
        if (method === 'GET') return Response.json(await this.listSnapshots());
        if (method === 'POST') {
          const body = await readOptionalJsonObject(request);
          if (body.name !== undefined && typeof body.name !== 'string') {
            throw new HttpError(400, 'INVALID_ARGUMENT', 'name must be a string');
          }
          if (body.note !== undefined && typeof body.note !== 'string') {
            throw new HttpError(400, 'INVALID_ARGUMENT', 'note must be a string');
          }
          const info = await this.createSnapshot(
            body.name as string | undefined,
            body.note as string | undefined
          );
          return Response.json(info, { status: 201 });
        }
        throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST' });
      }

      const id = segments[0];

      if (segments.length === 1) {
        if (method === 'DELETE') return Response.json(await this.deleteSnapshot(id));
        throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'DELETE' });
      }

      if (segments.length === 2) {
        const action = segments[1];
        if (action === 'diff') {
          if (method !== 'GET') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
          }
          const against = url.searchParams.get('against') ?? 'live';
          return Response.json(await this.diffSnapshot(id, against));
        }
        if (action === 'restore') {
          if (method !== 'POST') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
          }
          return Response.json(await this.restoreSnapshot(id));
        }
        if (action === 'clone') {
          if (method !== 'POST') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
          }
          // Cross-volume clone is privileged: a scoped capability, which is
          // bound to a single volume, may never target another one.
          if (identity.kind === 'capability') {
            throw new HttpError(403, 'FORBIDDEN', 'Only root or auth-disabled callers may clone across volumes');
          }
          const body = await readOptionalJsonObject(request);
          if (typeof body.targetVolume !== 'string' || body.targetVolume.trim() === '') {
            throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "targetVolume" string');
          }
          return Response.json(await this.cloneSnapshot(id, body.targetVolume));
        }
        throw new HttpError(404, 'INVALID_ROUTE', `Unknown snapshot action: ${action}`);
      }

      throw new HttpError(404, 'INVALID_ROUTE', 'Invalid snapshot route');
    } catch (error) {
      throw mapSnapshotError(error);
    }
  }

  /**
   * Route the `uploads` resource, where the route path is the final target.
   * POST creates or resumes, GET reports status, PATCH appends one bounded
   * chunk, PUT completes, and DELETE aborts. The uploads module throws stable
   * HttpErrors that the outer fetch handler renders.
   */
  private async handleUploads(request: Request, route: V1Route): Promise<Response> {
    const path = route.path;
    switch (request.method) {
      case 'POST': {
        const body = await readJsonObject(request);
        const { session, created } = await this.beginUpload(path, {
          size: body.size as number,
          checksum: body.checksum as string,
        });
        return Response.json(session, { status: created ? 201 : 200 });
      }
      case 'GET':
        return Response.json(await this.uploadStatus(path));
      case 'PATCH': {
        const offset = parseUploadOffset(request.headers.get('Upload-Offset'));
        const chunkSha256 = request.headers.get('X-AiryFS-Chunk-SHA256');
        if (!chunkSha256) {
          throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing X-AiryFS-Chunk-SHA256 header');
        }
        const data = await readBoundedChunk(request.body, MAX_UPLOAD_CHUNK_BYTES);
        return Response.json(await this.appendUpload(path, offset, chunkSha256, data));
      }
      case 'PUT':
        return Response.json(await this.completeUpload(path));
      case 'DELETE':
        await this.abortUpload(path);
        return new Response(null, { status: 204 });
      default:
        throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', {
          Allow: 'POST, GET, PATCH, PUT, DELETE',
        });
    }
  }

  /**
   * Route the plural `jobs` resource. Collection: POST (submit, requires an
   * Idempotency-Key header), GET (list, optional ?status). Item: GET `/:id`,
   * GET `/:id/logs?after=&limit=`, POST `/:id/cancel`. All job routes require the
   * `exec` capability because command text and output are execution-capable.
   */
  private async handleJobs(request: Request, url: URL, route: V1Route): Promise<Response> {
    const segments = route.path.split('/').filter(Boolean);
    const method = request.method;

    if (segments.length === 0) {
      if (method === 'POST') {
        const idempotencyKey = request.headers.get('Idempotency-Key') ?? '';
        const body = await readJsonObject(request);
        const cwd = body.cwd === undefined ? '/' : body.cwd;
        if (typeof cwd !== 'string') {
          throw new HttpError(400, 'INVALID_ARGUMENT', 'cwd must be a string');
        }
        const result = await this.submitJob(body.command as string, cwd, idempotencyKey);
        return Response.json(result.job, { status: result.created ? 201 : 200 });
      }
      if (method === 'GET') {
        const status = validateStatusFilter(url.searchParams.get('status') ?? undefined);
        return Response.json(this.listJobs(status));
      }
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST' });
    }

    const id = segments[0];

    if (segments.length === 1) {
      if (method === 'GET') return Response.json(this.getJob(id));
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
    }

    if (segments.length === 2) {
      const action = segments[1];
      if (action === 'logs') {
        if (method !== 'GET') {
          throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
        }
        const after = parseOptionalInteger(url.searchParams.get('after'), 'after');
        const limit = parseOptionalInteger(url.searchParams.get('limit'), 'limit');
        return Response.json(this.getJobLogs(id, after, limit));
      }
      if (action === 'cancel') {
        if (method !== 'POST') {
          throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
        }
        return Response.json(await this.cancelJob(id));
      }
      throw new HttpError(404, 'INVALID_ROUTE', `Unknown job action: ${action}`);
    }

    throw new HttpError(404, 'INVALID_ROUTE', 'Invalid job route');
  }

  // ---------------------------------------------------------------------------
  // HTTP routing
  // ---------------------------------------------------------------------------

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    try {
      const v1Route = parseV1Route(url.pathname);
      const routeVolume = v1Route?.volume ?? url.searchParams.get('volume') ?? '';
      const identity = await this.authorize(request, url, v1Route, routeVolume);

      if (v1Route) {
        if (v1Route.resource === 'capabilities') {
          return await this.handleCapabilities(request, v1Route, identity);
        }

        if (v1Route.resource === 'volume') {
          if (request.method === 'GET') {
            return Response.json({ chunkSize: this.filesystem().getChunkSize() });
          }
          if (request.method === 'PUT') {
            return Response.json(this.createVolume(await readVolumeCreateRequest(request)), { status: 201 });
          }
          throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, PUT' });
        }

        if (v1Route.resource === 'changes') {
          if (request.method !== 'GET') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
          }
          const rawSince = url.searchParams.get('since');
          const since = rawSince === null || rawSince === '' || rawSince === 'latest'
            ? 'latest'
            : parseOptionalInteger(rawSince, 'since')!;
          const limit = parseOptionalInteger(url.searchParams.get('limit'), 'limit');
          const wait = parseOptionalInteger(url.searchParams.get('wait'), 'wait') ?? 0;
          if (wait < 0 || wait > CHANGE_LONG_POLL_MAX_MS) {
            throw new HttpError(
              400,
              'INVALID_ARGUMENT',
              `wait must be between 0 and ${CHANGE_LONG_POLL_MAX_MS} milliseconds`,
            );
          }
          return Response.json(
            await this.waitForChanges(since, limit, v1Route.path, wait, request.signal),
            { headers: { 'Cache-Control': 'no-store' } },
          );
        }

        const fs = this.filesystem();
        const filesystemResponse = await handleFilesystemRequest(
          request,
          v1Route,
          fs,
          this.access,
          (paths) => this.mutations.record(fs, paths)
        );
        if (filesystemResponse) return filesystemResponse;

        if (v1Route.resource === 'trees') {
          if (request.method === 'GET') {
            const body = await this.exportTreeStream(v1Route.path);
            return new Response(body, {
              headers: {
                'Content-Type': 'application/x-airyfs-archive',
                'X-AiryFS-Archive-Root': v1Route.path,
              },
            });
          }
          if (request.method === 'PUT') {
            const replace = url.searchParams.get('replace') === 'true';
            const summary = await this.importTreeStream(v1Route.path, request.body, { replace });
            return Response.json(summary, { status: 201 });
          }
          throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, PUT' });
        }

        if (v1Route.resource === 'snapshots') {
          return await this.handleSnapshots(request, url, v1Route, identity);
        }

        if (v1Route.resource === 'uploads') {
          return await this.handleUploads(request, v1Route);
        }

        if (v1Route.resource === 'jobs') {
          return await this.handleJobs(request, url, v1Route);
        }

        if (v1Route.resource === 'exec') {
          if (request.method !== 'POST') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
          }
          if (v1Route.path === '/cancel') {
            const body = await readJsonObject(request);
            if (typeof body.id !== 'string' || !body.id) {
              throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "id" string');
            }
            await this.cancelExec(body.id);
            return Response.json({ ok: true });
          }
          const command = await readCommandRequest(request);
          if (url.searchParams.get('stream') === 'true') {
            return new Response(await this.execStream(command, request.signal), {
              headers: {
                'Content-Type': 'application/x-ndjson',
                'Content-Encoding': 'Identity',
                'Cache-Control': 'no-cache',
              },
            });
          }
          return Response.json(await this.exec(command, request.signal));
        }

        if (v1Route.resource === 'usage' && request.method === 'GET') {
          return Response.json(await this.usage());
        }

        throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
      }

      if (url.pathname === '/exec' && request.method === 'POST') {
        return Response.json(await this.exec(await readCommandRequest(request), request.signal));
      }

      if (url.pathname === '/destroy' && request.method === 'POST') {
        await this.destroyContainer();
        return new Response('ok');
      }

      if (url.pathname === '/fs/write' && request.method === 'POST') {
        const path = url.searchParams.get('path');
        if (!path) return new Response('Missing ?path=', { status: 400 });
        await writeFileStream(this.filesystem(), path, request.body, this.access);
        await this.mutations.record(this.filesystem(), [path]);
        return new Response('ok');
      }

      if (url.pathname === '/fs/read') {
        const path = url.searchParams.get('path');
        if (!path) return new Response('Missing ?path=', { status: 400 });
        return fileResponse(this.filesystem(), path, request, this.access);
      }

      if (url.pathname === '/fs/ls') {
        const path = url.searchParams.get('path') ?? '/';
        return Response.json(await this.listDir(path));
      }

      if (url.pathname === '/kv/set' && request.method === 'POST') {
        const key = url.searchParams.get('key');
        if (!key) return new Response('Missing ?key=', { status: 400 });
        const value = await request.text();
        this.ctx.storage.sql.exec(
          "INSERT OR REPLACE INTO kv_store (key, value, created_at, updated_at) VALUES (?, ?, unixepoch(), unixepoch())",
          key,
          value
        );
        return new Response('ok');
      }

      if (url.pathname === '/kv/get') {
        const key = url.searchParams.get('key');
        if (!key) return new Response('Missing ?key=', { status: 400 });
        const rows = this.ctx.storage.sql
          .exec<{ value: string }>("SELECT value FROM kv_store WHERE key = ?", key)
          .toArray();
        if (rows.length === 0) return new Response('Not found', { status: 404 });
        return new Response(rows[0].value);
      }

      if (url.pathname === '/perf') {
        return Response.json({
          pipelineRequests: this.hranaServer?.pipelineCount ?? 0,
          sqlStatements: this.hranaServer?.statementCount ?? 0,
        });
      }

      if (url.pathname === '/db-info') {
        return Response.json(this.dbInfo());
      }

      if (url.pathname === '/usage') {
        return Response.json(await this.usage());
      }

      return new Response('Not found', { status: 404 });
    } catch (error) {
      return errorResponse(error);
    }
  }
}

/** Describe the operation and paths a request requires, for capability authorization. */
async function requiredAccess(
  request: Request,
  url: URL,
  route: V1Route | null
): Promise<AccessRequirement> {
  const method = request.method;
  if (route) {
    switch (route.resource) {
      case 'volume':
        return { operation: method === 'GET' ? 'read' : 'write', paths: ['/'] };
      case 'files':
        return {
          operation: method === 'GET' || method === 'HEAD' ? 'read' : 'write',
          paths: [route.path],
        };
      case 'directories':
        return { operation: method === 'GET' ? 'read' : 'write', paths: [route.path] };
      case 'trees':
        return { operation: method === 'GET' ? 'read' : 'write', paths: [route.path] };
      case 'uploads':
        // The route path is the final target; status reads, all mutating upload
        // methods write, scoped to that path.
        return { operation: method === 'GET' ? 'read' : 'write', paths: [route.path] };
      case 'operations':
        return method === 'POST'
          ? operationAccess(await safeJson(request), route.path.slice(1))
          : { operation: 'write', paths: [route.path] };
      case 'snapshots':
        return snapshotAccess(method, route.path);
      case 'exec':
        return { operation: 'exec', paths: ['/'] };
      case 'jobs':
        // Command text and output are execution-capable and sensitive: every job
        // route requires the exec capability on the volume root.
        return { operation: 'exec', paths: ['/'] };
      case 'changes':
        return { operation: 'read', paths: [route.path] };
      case 'usage':
        return { operation: 'read', paths: ['/'] };
      case 'capabilities':
        return method === 'GET'
          ? { operation: null, paths: [] }
          : { operation: 'admin', paths: [] };
    }
  }

  switch (url.pathname) {
    case '/exec':
      return { operation: 'exec', paths: ['/'] };
    case '/destroy':
      return { operation: 'admin', paths: [] };
    case '/fs/write':
      return { operation: 'write', paths: queryPaths(url) };
    case '/fs/read':
      return { operation: 'read', paths: queryPaths(url) };
    case '/fs/ls':
      return { operation: 'read', paths: [url.searchParams.get('path') ?? '/'] };
    case '/kv/set':
      return { operation: 'write', paths: ['/'] };
    case '/kv/get':
      return { operation: 'read', paths: ['/'] };
    case '/perf':
    case '/db-info':
    case '/usage':
      return { operation: 'read', paths: ['/'] };
    default:
      return { operation: null, paths: [] };
  }
}

/** Required access for a filesystem operations-body request; covers all path operands. */
function operationAccess(body: Record<string, unknown> | null, operation: string): AccessRequirement {
  const pick = (key: string): string[] =>
    typeof body?.[key] === 'string' ? [body[key] as string] : [];
  switch (operation) {
    case 'readlink':
    case 'checksum':
      return { operation: 'read', paths: pick('path') };
    case 'rename':
    case 'copy':
      return { operation: 'write', paths: [...pick('from'), ...pick('to')] };
    case 'symlink':
      return { operation: 'write', paths: [...pick('path'), ...pick('target')] };
    case 'truncate':
      return { operation: 'write', paths: pick('path') };
    default:
      return { operation: 'write', paths: [] };
  }
}

async function safeJson(request: Request): Promise<Record<string, unknown> | null> {
  try {
    return await request.clone().json();
  } catch {
    return null;
  }
}

function queryPaths(url: URL): string[] {
  const path = url.searchParams.get('path');
  return path ? [path] : [];
}

function authStatus(identity: Identity, volume: string): Record<string, unknown> {
  if (identity.kind === 'root') return { auth: 'root', volume };
  if (identity.kind === 'capability') {
    const capability = identity.capability;
    return {
      auth: 'capability',
      volume,
      capability: {
        id: capability.id,
        volume: capability.volume,
        operations: capability.operations,
        pathPrefixes: capability.pathPrefixes,
        expires: capability.expires,
      },
    };
  }
  return { auth: 'disabled', volume };
}

function parseOperations(value: unknown): Operation[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'operations must be a non-empty array');
  }
  const operations: Operation[] = [];
  for (const entry of value) {
    if (!(OPERATIONS as readonly string[]).includes(entry as string)) {
      throw new HttpError(400, 'INVALID_ARGUMENT', `Unknown operation: ${String(entry)}`);
    }
    if (!operations.includes(entry as Operation)) operations.push(entry as Operation);
  }
  return operations;
}

function parsePathPrefixes(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === 'string')) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'pathPrefixes must be an array of strings');
  }
  return value as string[];
}

function parseUploadOffset(value: string | null): number {
  if (value === null) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing Upload-Offset header');
  }
  const offset = Number(value);
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Upload-Offset must be a non-negative integer');
  }
  return offset;
}

function parseOptionalInteger(value: string | null, name: string): number | undefined {
  if (value === null || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `${name} must be an integer`);
  }
  return parsed;
}

function parseExpiry(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'expiresInSeconds must be a positive integer');
  }
  return value;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    let volume: string | null;
    try {
      volume = parseV1Route(url.pathname)?.volume ?? url.searchParams.get('volume');
    } catch (error) {
      if (error instanceof URIError) {
        return Response.json(
          { error: { code: 'INVALID_PATH', message: 'Path contains invalid URL encoding' } },
          { status: 400 }
        );
      }
      return errorResponse(error);
    }

    if (!volume) {
      return new Response('Missing ?volume= parameter', { status: 400 });
    }

    const stub = getContainer<AiryFS>(env.AiryFS, volume);
    return stub.fetch(request);
  },
};
