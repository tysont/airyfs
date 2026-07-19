// ABOUTME: Worker entrypoint and AiryFS Durable Object class.
// ABOUTME: Routes HTTP requests to named volumes, each backed by a DO with persistent SQLite.

import { Container, getContainer } from '@cloudflare/containers';
import { AgentFS, type CloudflareStorage } from 'agentfs-sdk/cloudflare';
import {
  ChunkSizeConflictError,
  configureChunkSize,
  configureQuota,
  initSchema,
  InvalidChunkSizeError,
  readQuota,
  SCHEMA_TABLES,
} from './schema';
import { HranaServer, wrapSqlStorage } from './hrana-server';
import { MutationJournal } from './mutation-journal';
import {
  errorResponse,
  fileResponse,
  latestFileVersion,
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
  hashPassword,
  isCapabilityRevoked,
  normalizePath,
  OPERATIONS,
  readPasswordRecord,
  revokeCapability,
  signCapability,
  verifyPassword,
  writePasswordRecord,
  type AccessRequirement,
  type Identity,
  type Operation,
} from './auth';
import {
  createShare,
  deleteShare,
  deleteSite,
  listShares,
  parsePublicVolume,
  readShare,
  readSite,
  serveShare,
  serveSite,
  subdomainVolume,
  writeSite,
} from './sites';
import {
  createWebhook,
  deleteWebhook,
  deliverWebhooks,
  hasPendingWebhookDeliveries,
  listWebhooks,
  nextWebhookDelay,
} from './webhooks';
import { assetPath, getAsset, putAsset } from './assets';
import {
  advanceSchedule,
  createSchedule,
  deleteSchedule,
  listDueSchedules,
  listSchedules,
  nextScheduleDelay,
  setScheduleEnabled,
} from './schedules';
import { search as searchVolume } from './search';
import { readTree } from './tree';
import { listTrash, moveToTrash, purgeTrash, restoreTrash, undoTrash } from './trash';
import { handleWebDav, parseWebDavDestination } from './webdav';
import { consumePtyTicket, createPtyTicket } from './pty-tickets';
import { relayPty } from './pty-relay';
import { createService, deleteService, listServices, readService, setServiceEnabled, type ServiceRecord } from './services';
import { handleS3Request, parseS3Route } from './s3';
import { executeScopedSql } from './scoped-sql';
import { VolumeRegistry } from './volume-registry';
import { handleVolumeRegistryRequest } from './volume-registry-api';
import { renderPrometheusMetrics, type MetricsSnapshot } from './metrics';

export { VolumeRegistry };

interface Env {
  AiryFS: DurableObjectNamespace<AiryFS>;
  VolumeRegistry: DurableObjectNamespace<VolumeRegistry>;
  /** When set, HTTP access requires a root or capability bearer credential. */
  AIRYFS_AUTH_SECRET?: string;
  /** When set, `<volume>.<SITES_ZONE>` hostnames serve that volume's published site. */
  SITES_ZONE?: string;
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
const METRICS_CACHE_MS = 5_000;
const LEGACY_VOLUME_PATHS = new Set([
  '/exec', '/destroy', '/fs/write', '/fs/read', '/fs/ls', '/kv/set', '/kv/get', '/perf', '/db-info', '/usage',
]);

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
  private webhookDeliveryRunning = false;
  private scheduleRunnerActive = false;
  private dataSocket: WorkerSocket | null = null;
  private invalidationSocket: WorkerSocket | null = null;
  private hranaServer: HranaServer | null = null;
  private readonly access = new VolumeAccessCoordinator();
  private metricsCache: { expiresAt: number; text: string } | null = null;
  private metricsPromise: Promise<string> | null = null;
  private readonly mutations: MutationJournal;
  private registeredVolume: string | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx as never, env);
    this.mutations = new MutationJournal(this.ctx.storage.sql);
    this.ctx.blockConcurrencyWhile(async () => {
      initSchema(this.ctx.storage.sql, (callback) => this.ctx.storage.transactionSync(callback));
      await this.scheduleWebhookDelivery();
      await this.scheduleNextCronRun();
    });
  }

  private async recordMutations(fs: AgentFS, paths: string[]): Promise<void> {
    await this.mutations.record(fs, paths);
    await this.scheduleWebhookDelivery();
  }

  private async scheduleWebhookDelivery(): Promise<void> {
    if (!hasPendingWebhookDeliveries(this.ctx.storage.sql)) return;
    await this.schedule(nextWebhookDelay(this.ctx.storage.sql) ?? 0, 'deliverWebhookQueue');
  }

  async deliverWebhookQueue(): Promise<void> {
    if (this.webhookDeliveryRunning) return;
    this.webhookDeliveryRunning = true;
    try {
      const rows = this.ctx.storage.sql.exec("SELECT value FROM fs_config WHERE key = 'volume_name'").toArray();
      const volume = rows[0]?.value === undefined ? '' : String(rows[0].value);
      await deliverWebhooks(this.ctx.storage.sql, volume);
      await this.scheduleWebhookDelivery();
    } finally {
      this.webhookDeliveryRunning = false;
    }
  }

  private async scheduleNextCronRun(): Promise<void> {
    const delay = nextScheduleDelay(this.ctx.storage.sql);
    if (delay !== null) await this.schedule(delay, 'runScheduledJobs');
  }

  async runScheduledJobs(): Promise<void> {
    if (this.scheduleRunnerActive) return;
    this.scheduleRunnerActive = true;
    try {
      for (const schedule of listDueSchedules(this.ctx.storage.sql)) {
        await this.submitJob(
          schedule.command,
          schedule.cwd,
          `schedule:${schedule.id}:${schedule.scheduledFor}`,
        );
        advanceSchedule(this.ctx.storage.sql, schedule);
      }
      await this.scheduleNextCronRun();
    } finally {
      this.scheduleRunnerActive = false;
    }
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

  private async ensureVolumeRegistered(volume: string, refresh = false): Promise<void> {
    if (!refresh && this.registeredVolume === volume) return;
    const stored = this.ctx.storage.sql.exec(
      "SELECT value FROM fs_config WHERE key = 'registry_volume_name'",
    ).toArray()[0]?.value;
    if (!refresh && stored === volume) {
      this.registeredVolume = volume;
      return;
    }

    await this.env.VolumeRegistry.getByName('global').register(volume, this.filesystem().getChunkSize());
    this.ctx.storage.sql.exec(
      `INSERT INTO fs_config(key, value) VALUES ('volume_name', ?), ('registry_volume_name', ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      volume,
      volume,
    );
    this.registeredVolume = volume;
  }

  private async registerSuccessfulVolume(volume: string | null, response: Promise<Response>): Promise<Response> {
    const result = await response;
    if (volume && result.status < 400) await this.ensureVolumeRegistered(volume);
    return result;
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

  private async execPty(): Promise<Response> {
    if (this.activeExec || this.destroyPromise) {
      throw new HttpError(503, 'EXEC_BUSY', 'Another command or Container lifecycle operation is already running');
    }
    const execution = Symbol('exec-pty');
    this.activeExec = execution;
    try {
      await this.ensureContainer();
      const socket = this.ctx.container!.getTcpPort(4001).connect('0.0.0.0:4001') as WorkerSocket;
      await socket.opened;
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      this.ctx.waitUntil(relayPty(server, socket).finally(() => {
        if (this.activeExec === execution) this.activeExec = null;
      }));
      return new Response(null, { status: 101, webSocket: client } as ResponseInit & { webSocket: WebSocket });
    } catch (error) {
      if (this.activeExec === execution) this.activeExec = null;
      throw error;
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
      await this.recordMutations(fs, [path]);
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
      await this.recordMutations(fs, [path]);
    } finally { release(); }
  }

  async removePath(path: string, recursive = false): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.rm(path, { recursive });
      await this.recordMutations(fs, [path]);
    } finally { release(); }
  }

  async renamePath(from: string, to: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite([from, to]);
    try {
      await fs.rename(from, to);
      await this.recordMutations(fs, [from, to]);
    } finally { release(); }
  }

  async copyPath(from: string, to: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite([from, to]);
    try {
      await fs.copyFile(from, to);
      await this.recordMutations(fs, [to]);
    } finally { release(); }
  }

  async createSymlink(target: string, path: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.symlink(target, path);
      await this.recordMutations(fs, [path]);
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
    await this.recordMutations(fs, [path]);
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
        record: (paths) => this.recordMutations(fs, paths),
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
    await this.recordMutations(fs, [path]);
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
    return getContainer<AiryFS>(this.env.AiryFS, targetVolume).importCloneStream(stream, targetVolume);
  }

  /** Trusted target-side clone import followed by one-time registry publication. */
  async importCloneStream(stream: ReadableStream<Uint8Array>, volume: string): Promise<TreeSummary> {
    const summary = await this.importTreeStream('/', stream, {
      replace: true,
      allowRoot: true,
    });
    await this.ensureVolumeRegistered(volume);
    return summary;
  }

  /** Stream a consistent live-volume copy into a new, empty target volume. */
  async forkVolume(targetVolume: string): Promise<TreeSummary> {
    if (typeof targetVolume !== 'string' || targetVolume.trim() === '') {
      throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing target volume');
    }
    if (this.env.AiryFS.idFromName(targetVolume).equals(this.ctx.id)) {
      throw new HttpError(409, 'FORK_SELF', 'Fork target volume must differ from the source volume');
    }
    const source = this.filesystem();
    const stream = await this.exportTreeStream('/');
    return getContainer<AiryFS>(this.env.AiryFS, targetVolume)
      .importForkStream(stream, source.getChunkSize(), targetVolume);
  }

  /** Trusted target-side fork import. Existing filesystem contents are never replaced. */
  async importForkStream(stream: ReadableStream<Uint8Array>, chunkSize: number, volume: string): Promise<TreeSummary> {
    this.createVolume(chunkSize);
    const summary = await this.importTreeStream('/', stream, { replace: false, allowRoot: true });
    await this.ensureVolumeRegistered(volume);
    return summary;
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
  async usage(): Promise<MetricsSnapshot> {
    const filesystem = await this.filesystem().statfs();
    const quota = readQuota(this.ctx.storage.sql);
    const runtimeState = await this.getState();
    let container: MetricsSnapshot['container'] = {
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
      filesystem: {
        ...filesystem,
        quotaBytes: quota.bytes,
        quotaInodes: quota.inodes,
        bytesAvailable: quota.bytes === null ? null : Math.max(0, quota.bytes - filesystem.bytesUsed),
        inodesAvailable: quota.inodes === null ? null : Math.max(0, quota.inodes - filesystem.inodes),
      },
      sqliteBytes: this.ctx.storage.sql.databaseSize,
      container,
      hrana: {
        pipelineRequests: this.hranaServer?.pipelineCount ?? 0,
        sqlStatements: this.hranaServer?.statementCount ?? 0,
      },
    };
  }

  /** Render a short-lived scrape snapshot to bound repeated row-count work. */
  async metrics(): Promise<string> {
    const now = Date.now();
    if (this.metricsCache && this.metricsCache.expiresAt > now) return this.metricsCache.text;
    if (this.metricsPromise) return this.metricsPromise;
    this.metricsPromise = (async () => {
      const text = renderPrometheusMetrics(await this.usage(), this.dbInfo());
      this.metricsCache = { expiresAt: Date.now() + METRICS_CACHE_MS, text };
      return text;
    })();
    try {
      return await this.metricsPromise;
    } finally {
      this.metricsPromise = null;
    }
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
      onWrite: () => this.scheduleWebhookDelivery(),
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
   * Route the per-volume `auth` resource. This runs before the normal bearer
   * check so `/login` is reachable without a token: it exchanges the volume
   * password for a scoped capability. `GET /` reports whether a password is set,
   * and `POST /password` sets or rotates it (root credential, an admin
   * capability, or the current password authorizes the change).
   */
  private async handleAuth(request: Request, route: V1Route): Promise<Response> {
    const secret = this.env.AIRYFS_AUTH_SECRET;
    const sql = this.ctx.storage.sql;
    const path = route.path;
    const method = request.method;

    if (method === 'GET' && path === '/') {
      return Response.json({
        volume: route.volume,
        authEnabled: Boolean(secret),
        passwordSet: readPasswordRecord(sql) !== null,
      });
    }

    if (method === 'POST' && path === '/login') {
      if (!secret) {
        throw new HttpError(409, 'AUTH_DISABLED', 'Set AIRYFS_AUTH_SECRET to enable password login');
      }
      const record = readPasswordRecord(sql);
      const body = await readJsonObject(request);
      const password = typeof body.password === 'string' ? body.password : '';
      // Verify even when no password is set so timing does not disclose that fact.
      const ok = record ? await verifyPassword(password, record) : false;
      if (!record || !password || !ok) {
        throw new HttpError(401, 'INVALID_PASSWORD', 'Incorrect volume password');
      }
      const expiresInSeconds = body.expiresInSeconds === undefined
        ? DEFAULT_LOGIN_TTL_SECONDS
        : parseExpiry(body.expiresInSeconds);
      const capability = buildCapability(route.volume, ['read', 'write', 'exec'], [], expiresInSeconds);
      const token = await signCapability(secret, capability);
      return Response.json({ token, ...capability }, { status: 201 });
    }

    if (method === 'POST' && path === '/password') {
      if (!secret) {
        throw new HttpError(409, 'AUTH_DISABLED', 'Set AIRYFS_AUTH_SECRET to manage volume passwords');
      }
      const body = await readJsonObject(request);
      const newPassword = typeof body.password === 'string' ? body.password : '';
      if (newPassword.length < 8) {
        throw new HttpError(400, 'WEAK_PASSWORD', 'Password must be at least 8 characters');
      }
      const existing = readPasswordRecord(sql);
      const authorized = await this.authorizePasswordChange(request, route.volume, secret, existing, body);
      if (!authorized) {
        throw new HttpError(
          403,
          'FORBIDDEN',
          existing
            ? 'Root credential, admin capability, or current password required'
            : 'Root credential or admin capability required to set the initial password'
        );
      }
      writePasswordRecord(sql, await hashPassword(newPassword));
      return Response.json({ volume: route.volume, passwordSet: true }, { status: existing ? 200 : 201 });
    }

    throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST' });
  }

  /** True when the caller may set/rotate the volume password: root, admin capability, or current password. */
  private async authorizePasswordChange(
    request: Request,
    volume: string,
    secret: string,
    existing: ReturnType<typeof readPasswordRecord>,
    body: Record<string, unknown>
  ): Promise<boolean> {
    try {
      const identity = await authenticate(secret, request.headers.get('Authorization'), volume);
      if (identity.kind === 'root') return true;
      if (
        identity.kind === 'capability' &&
        identity.capability.operations.includes('admin') &&
        !isCapabilityRevoked(this.ctx.storage.sql, identity.capability.id)
      ) {
        return true;
      }
    } catch {
      // No usable bearer credential; fall back to the current-password path.
    }
    if (existing && typeof body.currentPassword === 'string' && body.currentPassword) {
      return verifyPassword(body.currentPassword, existing);
    }
    return false;
  }

  /**
   * Serve a public site request (`/s/<volume>/<path...>`). Unauthenticated and
   * read-only; the volume's published web root resolves index documents and,
   * when enabled, falls back to the index for single-page-app routes.
   */
  private async handleSiteRequest(url: URL, request: Request): Promise<Response> {
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, HEAD' });
    }
    const site = readSite(this.ctx.storage.sql);
    if (!site) throw new HttpError(404, 'NOT_PUBLISHED', 'This volume has no published site');
    const segments = url.pathname.split('/').filter(Boolean); // ['s', volume, ...rest]
    const subPath = `/${segments.slice(2).map(decodeURIComponent).join('/')}`;
    return serveSite(
      this.filesystem(),
      this.access,
      site,
      subPath,
      request,
      (ino) => latestFileVersion(this.ctx.storage.sql, ino)
    );
  }

  /**
   * Serve a share link (`/d/<volume>/<id>`). Unauthenticated and read-only;
   * enforces the share's expiry before streaming its single file.
   */
  private async handleShareRequest(url: URL, request: Request): Promise<Response> {
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, HEAD' });
    }
    const segments = url.pathname.split('/').filter(Boolean); // ['d', volume, id]
    const id = segments[2] ? decodeURIComponent(segments[2]) : '';
    const share = id ? readShare(this.ctx.storage.sql, id) : null;
    if (!share) throw new HttpError(404, 'NOT_FOUND', 'Unknown share link');
    return serveShare(
      this.filesystem(),
      this.access,
      share,
      request,
      (ino) => latestFileVersion(this.ctx.storage.sql, ino)
    );
  }

  private async handleWebDavRequest(url: URL, request: Request): Promise<Response> {
    const segments = url.pathname.split('/').filter(Boolean);
    const volume = segments[1] ? decodeURIComponent(segments[1]) : '';
    const path = normalizePath(`/${segments.slice(2).map(decodeURIComponent).join('/')}`);
    if (!volume) throw new HttpError(404, 'INVALID_ROUTE', 'Missing WebDAV volume');
    if (request.method !== 'OPTIONS') await this.authorizeWebDav(request, volume, path);
    return handleWebDav({
      fs: this.filesystem(), sql: this.ctx.storage.sql, access: this.access, volume, path, request,
      onMutation: (paths) => this.recordMutations(this.filesystem(), paths),
    });
  }

  private async ensureServiceRunning(service: ServiceRecord): Promise<void> {
    if (!service.enabled) throw new HttpError(409, 'SERVICE_STOPPED', `Preview service is stopped: ${service.name}`);
    await this.ensureContainer();
    const response = await this.containerFetch(new Request('http://localhost/services/start', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
        name: service.name, command: service.command, port: service.port,
        cwd: service.cwd === '/' ? '/volume' : `/volume${service.cwd}`, env: service.env,
      }),
    }), 4002);
    if (!response.ok) throw new HttpError(503, 'SERVICE_START_FAILED', await response.text());
  }

  private async proxyService(service: ServiceRecord, path: string, request: Request): Promise<Response> {
    await this.ensureServiceRunning(service);
    const target = new URL(request.url);
    target.protocol = 'http:';
    target.hostname = 'localhost';
    target.port = String(service.port);
    target.pathname = path;
    try {
      return await this.ctx.container!.getTcpPort(service.port).fetch(new Request(target, request));
    } catch (error) {
      throw new HttpError(503, 'SERVICE_WARMING', error instanceof Error ? error.message : String(error), { 'Retry-After': '1' });
    }
  }

  private async handlePublicService(url: URL, request: Request): Promise<Response> {
    const segments = url.pathname.split('/').filter(Boolean);
    const name = segments[2] ? decodeURIComponent(segments[2]) : '';
    const service = name ? readService(this.ctx.storage.sql, name) : null;
    if (!service || !service.public) throw new HttpError(404, 'NOT_PUBLISHED', 'Preview service is not public');
    const path = `/${segments.slice(3).map(decodeURIComponent).join('/')}`;
    return this.proxyService(service, path, request);
  }

  private async handleServices(request: Request, route: V1Route): Promise<Response> {
    const segments = route.path.split('/').filter(Boolean);
    if (segments.length === 0) {
      if (request.method === 'GET') return Response.json(listServices(this.ctx.storage.sql));
      if (request.method === 'POST') return Response.json(createService(this.ctx.storage.sql, await readJsonObject(request)), { status: 201 });
    }
    const name = segments[0];
    if (segments.length === 1) {
      if (request.method === 'GET') return Response.json(readService(this.ctx.storage.sql, name));
      if (request.method === 'DELETE') {
        const service = deleteService(this.ctx.storage.sql, name);
        await this.containerFetch(new Request('http://localhost/services/stop', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name }),
        }), 4002).catch(() => undefined);
        return Response.json(service);
      }
    }
    if (segments.length === 2 && (segments[1] === 'start' || segments[1] === 'stop') && request.method === 'POST') {
      const enabled = segments[1] === 'start';
      const service = setServiceEnabled(this.ctx.storage.sql, name, enabled);
      if (enabled) await this.ensureServiceRunning(service);
      else await this.containerFetch(new Request('http://localhost/services/stop', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name }),
      }), 4002).catch(() => undefined);
      return Response.json(service);
    }
    if (segments.length === 2 && segments[1] === 'logs' && request.method === 'GET') {
      readService(this.ctx.storage.sql, name);
      const url = new URL(request.url);
      const after = parseOptionalInteger(url.searchParams.get('after'), 'after') ?? 0;
      const generation = url.searchParams.get('generation') || null;
      if (after < 0) throw new HttpError(400, 'INVALID_ARGUMENT', 'after must be non-negative');
      await this.ensureContainer();
      const response = await this.containerFetch(
        new Request(`http://localhost/services/${encodeURIComponent(name)}/logs?${new URLSearchParams({
          after: String(after),
          ...(generation ? { generation } : {}),
        })}`),
        4002,
      );
      if (response.status === 404) {
        return Response.json({ entries: [], next: null, generation: null, earliestSeq: null, reset: generation !== null, truncated: false });
      }
      if (!response.ok) throw new HttpError(503, 'SERVICE_LOGS_UNAVAILABLE', await response.text());
      const page = await response.json<{
        entries: Array<{ seq: number; stream: 'stdout' | 'stderr'; data: string; timestamp: number }>;
        generation: string;
        earliestSeq: number | null;
        reset: boolean;
        truncated: boolean;
      }>();
      return Response.json({ ...page, next: page.entries.at(-1)?.seq ?? null });
    }
    if (segments[1] === 'proxy') {
      const path = `/${segments.slice(2).map(decodeURIComponent).join('/')}`;
      return this.proxyService(readService(this.ctx.storage.sql, name), path, request);
    }
    throw new HttpError(404, 'INVALID_ROUTE', 'Invalid preview service route');
  }

  private async authorizeWebDav(request: Request, volume: string, path: string): Promise<void> {
    const secret = this.env.AIRYFS_AUTH_SECRET;
    if (!secret) return;
    let authorization = request.headers.get('Authorization');
    const basic = authorization?.match(/^Basic\s+(.+)$/i);
    if (basic) {
      try {
        const decoded = new TextDecoder().decode(Uint8Array.from(atob(basic[1]), (character) => character.charCodeAt(0)));
        const password = decoded.slice(decoded.indexOf(':') + 1);
        const record = readPasswordRecord(this.ctx.storage.sql);
        if (record && await verifyPassword(password, record)) return;
        authorization = `Bearer ${password}`;
      } catch {
        authorization = null;
      }
    }
    let identity: Identity;
    try {
      identity = await authenticate(secret, authorization, volume);
    } catch {
      throw new HttpError(401, 'UNAUTHENTICATED', 'WebDAV credentials required', {
        'WWW-Authenticate': `Basic realm="AiryFS ${volume}"`,
      });
    }
    if (identity.kind !== 'capability') return;
    if (isCapabilityRevoked(this.ctx.storage.sql, identity.capability.id)) throw new HttpError(403, 'TOKEN_REVOKED', 'Capability revoked');
    const read = request.method === 'GET' || request.method === 'HEAD' || request.method === 'PROPFIND';
    const paths = [path];
    if (request.method === 'MOVE' || request.method === 'COPY') paths.push(parseWebDavDestination(request, volume));
    if (!capabilityAllows(identity.capability, read ? 'read' : 'write', paths)) {
      throw new HttpError(403, 'FORBIDDEN', 'Capability does not permit this WebDAV operation');
    }
  }

  /** Manage the published site: GET status, PUT publish/update, DELETE unpublish. */
  private async handleSites(request: Request, route: V1Route): Promise<Response> {
    const sql = this.ctx.storage.sql;
    if (route.path !== '/') throw new HttpError(404, 'INVALID_ROUTE', 'Invalid sites route');
    if (request.method === 'GET') {
      const site = readSite(sql);
      return Response.json({ published: site !== null, site });
    }
    if (request.method === 'PUT') {
      const body = await readOptionalJsonObject(request);
      const pathPrefix = typeof body.path === 'string' && body.path ? body.path : '/';
      const indexDocument = typeof body.indexDocument === 'string' && body.indexDocument
        ? body.indexDocument
        : 'index.html';
      const spa = body.spa === true;
      const directoryListing = body.directoryListing === true;
      const cacheControl = typeof body.cacheControl === 'string' ? body.cacheControl : null;
      const site = writeSite(sql, { pathPrefix, indexDocument, spa, directoryListing, cacheControl });
      return Response.json(site);
    }
    if (request.method === 'DELETE') {
      return Response.json({ removed: deleteSite(sql) });
    }
    throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, PUT, DELETE' });
  }

  /** Manage share links: GET list, POST create, DELETE by id. */
  private async handleShares(request: Request, route: V1Route): Promise<Response> {
    const sql = this.ctx.storage.sql;
    const segments = route.path.split('/').filter(Boolean);
    if (segments.length === 0) {
      if (request.method === 'GET') return Response.json(listShares(sql));
      if (request.method === 'POST') {
        const body = await readJsonObject(request);
        if (typeof body.path !== 'string' || !body.path) {
          throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "path" string');
        }
        const expiresAt = body.expiresInSeconds === undefined
          ? null
          : Math.floor(Date.now() / 1000) + parseExpiry(body.expiresInSeconds);
        const cacheControl = typeof body.cacheControl === 'string' ? body.cacheControl : null;
        const share = createShare(sql, body.path, expiresAt, cacheControl);
        return Response.json(share, { status: 201 });
      }
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST' });
    }
    if (segments.length === 1 && request.method === 'DELETE') {
      return Response.json({ id: segments[0], removed: deleteShare(sql, segments[0]) });
    }
    throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST, DELETE' });
  }

  private async handleWebhooks(request: Request, route: V1Route): Promise<Response> {
    const segments = route.path.split('/').filter(Boolean);
    if (segments.length === 0) {
      if (request.method === 'GET') return Response.json(listWebhooks(this.ctx.storage.sql));
      if (request.method === 'POST') {
        const body = await readJsonObject(request);
        this.ctx.storage.sql.exec(
          "INSERT OR REPLACE INTO fs_config (key, value) VALUES ('volume_name', ?)",
          route.volume,
        );
        return Response.json(createWebhook(this.ctx.storage.sql, {
          url: body.url,
          pathPrefix: body.pathPrefix,
          events: body.events,
        }), { status: 201 });
      }
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST' });
    }
    if (segments.length === 1 && request.method === 'DELETE') {
      return Response.json({ id: segments[0], removed: deleteWebhook(this.ctx.storage.sql, segments[0]) });
    }
    throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST, DELETE' });
  }

  private async handleAssets(request: Request, route: V1Route): Promise<Response> {
    const segments = route.path.split('/').filter(Boolean);
    if (segments.length !== 1) throw new HttpError(404, 'INVALID_ROUTE', 'An asset SHA-256 is required');
    const hash = segments[0];
    if (request.method === 'GET' || request.method === 'HEAD') {
      return getAsset(
        this.filesystem(),
        this.access,
        hash,
        request,
        (ino) => latestFileVersion(this.ctx.storage.sql, ino),
      );
    }
    if (request.method === 'PUT') {
      const result = await putAsset(this.filesystem(), this.access, hash, request.body);
      if (result.created) await this.recordMutations(this.filesystem(), [assetPath(hash)]);
      return Response.json(result, { status: result.created ? 201 : 200 });
    }
    throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, HEAD, PUT' });
  }

  private async handleSchedules(request: Request, route: V1Route): Promise<Response> {
    const segments = route.path.split('/').filter(Boolean);
    if (segments.length === 0) {
      if (request.method === 'GET') return Response.json(listSchedules(this.ctx.storage.sql));
      if (request.method === 'POST') {
        const schedule = createSchedule(this.ctx.storage.sql, await readJsonObject(request));
        await this.scheduleNextCronRun();
        return Response.json(schedule, { status: 201 });
      }
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, POST' });
    }
    if (segments.length === 1 && request.method === 'DELETE') {
      return Response.json({ id: segments[0], removed: deleteSchedule(this.ctx.storage.sql, segments[0]) });
    }
    if (segments.length === 2 && request.method === 'POST' && (segments[1] === 'enable' || segments[1] === 'disable')) {
      const schedule = setScheduleEnabled(this.ctx.storage.sql, segments[0], segments[1] === 'enable');
      await this.scheduleNextCronRun();
      return Response.json(schedule);
    }
    throw new HttpError(404, 'INVALID_ROUTE', 'Invalid schedule route');
  }

  private async handleSearch(request: Request, route: V1Route): Promise<Response> {
    if (route.path !== '/') throw new HttpError(404, 'INVALID_ROUTE', 'Search does not accept a path suffix');
    if (request.method !== 'POST') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
    }
    const body = await readJsonObject(request);
    return Response.json(await searchVolume(this.filesystem(), this.ctx.storage.sql, this.access, {
      mode: body.mode,
      path: body.path,
      pattern: body.pattern,
      regex: body.regex,
      ignoreCase: body.ignoreCase,
      limit: body.limit,
    }));
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
    const browserUploadRequest = url.pathname.includes('/browser-uploads/');

    try {
      // Public web hosting is served before any bearer check.
      if (url.pathname === '/s' || url.pathname.startsWith('/s/')) {
        return this.registerSuccessfulVolume(parsePublicVolume(url.pathname), this.handleSiteRequest(url, request));
      }
      if (url.pathname.startsWith('/d/')) {
        return this.registerSuccessfulVolume(parsePublicVolume(url.pathname), this.handleShareRequest(url, request));
      }
      if (url.pathname.startsWith('/dav/')) {
        return this.registerSuccessfulVolume(parsePublicVolume(url.pathname), this.handleWebDavRequest(url, request));
      }
      if (url.pathname.startsWith('/p/')) {
        return this.registerSuccessfulVolume(parsePublicVolume(url.pathname), this.handlePublicService(url, request));
      }
      const s3Route = parseS3Route(url.pathname);
      if (s3Route) {
        const response = await handleS3Request({
          request,
          route: s3Route,
          fs: this.filesystem(),
          access: this.access,
          authSecret: this.env.AIRYFS_AUTH_SECRET,
          onMutation: (paths) => this.recordMutations(this.filesystem(), paths),
          versionForInode: (ino) => latestFileVersion(this.ctx.storage.sql, ino),
        });
        if (response.status < 400) await this.ensureVolumeRegistered(s3Route.volume);
        return response;
      }

      const v1Route = parseV1Route(url.pathname);

      if (v1Route?.resource === 'browser-uploads' && request.method === 'OPTIONS') {
        return withBrowserUploadCors(new Response(null, { status: 204 }));
      }

      // The per-volume auth resource is handled before the bearer check so that
      // `/auth/login` can exchange the volume password for a token unauthenticated.
      if (v1Route?.resource === 'auth') {
        return await this.handleAuth(request, v1Route);
      }

      const routeVolume = v1Route?.volume ?? url.searchParams.get('volume') ?? '';
      const ptyUpgrade = v1Route?.resource === 'exec' && v1Route.path === '/pty';
      if (ptyUpgrade && this.env.AIRYFS_AUTH_SECRET) {
        const ticket = url.searchParams.get('ticket') ?? '';
        if (!ticket || !consumePtyTicket(this.ctx.storage.sql, ticket)) {
          throw new HttpError(401, 'INVALID_PTY_TICKET', 'A valid single-use PTY ticket is required');
        }
      }
      const identity = ptyUpgrade
        ? { kind: 'disabled' } as Identity
        : await this.authorize(request, url, v1Route, routeVolume);

      if (v1Route && !(v1Route.resource === 'volume' && request.method === 'PUT')) {
        await this.ensureVolumeRegistered(v1Route.volume);
      } else if (!v1Route && routeVolume && LEGACY_VOLUME_PATHS.has(url.pathname)) {
        await this.ensureVolumeRegistered(routeVolume);
      }

      if (v1Route) {
        if (v1Route.resource === 'capabilities') {
          return await this.handleCapabilities(request, v1Route, identity);
        }

        if (v1Route.resource === 'volume') {
          if (request.method === 'GET') {
            return Response.json({ chunkSize: this.filesystem().getChunkSize() });
          }
          if (request.method === 'PUT') {
            const info = this.createVolume(await readVolumeCreateRequest(request));
            await this.ensureVolumeRegistered(v1Route.volume, true);
            return Response.json(info, { status: 201 });
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

        if (
          request.method === 'DELETE'
          && (v1Route.resource === 'files' || v1Route.resource === 'directories')
          && url.searchParams.get('permanent') !== 'true'
        ) {
          const entry = await moveToTrash(this.filesystem(), this.ctx.storage.sql, this.access, v1Route.path);
          await this.recordMutations(this.filesystem(), [v1Route.path, entry.trashPath]);
          return Response.json(entry, { headers: { 'X-AiryFS-Trash-Id': entry.id } });
        }

        if (v1Route.resource !== 'trash' && (v1Route.path === '/.airyfs-trash' || v1Route.path.startsWith('/.airyfs-trash/'))) {
          throw new HttpError(404, 'ENOENT', 'Path not found');
        }

        const fs = this.filesystem();
        const filesystemResponse = await handleFilesystemRequest(
          request,
          v1Route,
          fs,
          this.access,
          (paths) => this.recordMutations(fs, paths),
          (ino) => latestFileVersion(this.ctx.storage.sql, ino)
        );
        if (filesystemResponse) {
          return v1Route.resource === 'browser-uploads'
            ? withBrowserUploadCors(filesystemResponse)
            : filesystemResponse;
        }

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

        if (v1Route.resource === 'forks') {
          if (v1Route.path !== '/') throw new HttpError(404, 'INVALID_ROUTE', 'Forks do not accept a path suffix');
          if (request.method !== 'POST') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
          }
          if (identity.kind === 'capability') {
            throw new HttpError(403, 'FORBIDDEN', 'Only root or auth-disabled callers may fork across volumes');
          }
          const body = await readJsonObject(request);
          if (typeof body.targetVolume !== 'string' || body.targetVolume.trim() === '') {
            throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "targetVolume" string');
          }
          return Response.json(await this.forkVolume(body.targetVolume), { status: 201 });
        }

        if (v1Route.resource === 'sql') {
          if (v1Route.path !== '/') throw new HttpError(404, 'INVALID_ROUTE', 'SQL does not accept a path suffix');
          if (request.method !== 'POST') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
          }
          const body = await readJsonObject(request);
          return Response.json(executeScopedSql(this.ctx.storage.sql, body.sql, body.args));
        }

        if (v1Route.resource === 'uploads') {
          return await this.handleUploads(request, v1Route);
        }

        if (v1Route.resource === 'jobs') {
          return await this.handleJobs(request, url, v1Route);
        }

        if (v1Route.resource === 'sites') {
          return await this.handleSites(request, v1Route);
        }

        if (v1Route.resource === 'shares') {
          return await this.handleShares(request, v1Route);
        }

        if (v1Route.resource === 'webhooks') {
          return await this.handleWebhooks(request, v1Route);
        }

        if (v1Route.resource === 'assets') {
          return await this.handleAssets(request, v1Route);
        }

        if (v1Route.resource === 'schedules') {
          return await this.handleSchedules(request, v1Route);
        }

        if (v1Route.resource === 'search') {
          return await this.handleSearch(request, v1Route);
        }

        if (v1Route.resource === 'tree') {
          if (request.method !== 'GET') {
            throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
          }
          return Response.json(await readTree(this.filesystem(), this.access, {
            path: v1Route.path,
            depth: optionalQueryNumber(url, 'depth'),
            limit: optionalQueryNumber(url, 'limit'),
          }));
        }

        if (v1Route.resource === 'quota') {
          if (v1Route.path !== '/') throw new HttpError(404, 'INVALID_ROUTE', 'Quota does not accept a path suffix');
          if (request.method === 'GET') return Response.json(readQuota(this.ctx.storage.sql));
          if (request.method === 'PUT') {
            const body = await readJsonObject(request);
            try {
              return Response.json(this.ctx.storage.transactionSync(() => configureQuota(this.ctx.storage.sql, body)));
            } catch (error) {
              throw new HttpError(400, 'INVALID_QUOTA', error instanceof Error ? error.message : String(error));
            }
          }
          throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, PUT' });
        }

        if (v1Route.resource === 'trash') {
          const segments = v1Route.path.split('/').filter(Boolean);
          if (segments.length === 0 && request.method === 'GET') {
            return Response.json(listTrash(this.ctx.storage.sql));
          }
          if (segments.length === 1 && segments[0] === 'undo' && request.method === 'POST') {
            return Response.json(await undoTrash(this.filesystem(), this.ctx.storage.sql, this.access));
          }
          if (segments.length === 1 && request.method === 'DELETE') {
            return Response.json(await purgeTrash(this.filesystem(), this.ctx.storage.sql, this.access, segments[0]));
          }
          if (segments.length === 2 && segments[1] === 'restore' && request.method === 'POST') {
            const body = await readOptionalJsonObject(request);
            if (body.to !== undefined && typeof body.to !== 'string') {
              throw new HttpError(400, 'INVALID_ARGUMENT', 'to must be a string');
            }
            return Response.json(await restoreTrash(
              this.filesystem(), this.ctx.storage.sql, this.access, segments[0], body.to as string | undefined,
            ));
          }
          throw new HttpError(404, 'INVALID_ROUTE', 'Invalid trash route');
        }

        if (v1Route.resource === 'services') return this.handleServices(request, v1Route);

        if (v1Route.resource === 'exec') {
          if (v1Route.path === '/pty-ticket') {
            if (request.method !== 'POST') {
              throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
            }
            return Response.json(createPtyTicket(this.ctx.storage.sql), { status: 201 });
          }
          if (v1Route.path === '/pty') {
            if (request.method !== 'GET' || request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
              throw new HttpError(426, 'UPGRADE_REQUIRED', 'PTY execution requires a WebSocket upgrade', { Upgrade: 'websocket' });
            }
            return this.execPty();
          }
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

        if (v1Route.resource === 'metrics' && request.method === 'GET') {
          return new Response(await this.metrics(), {
            headers: { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8' },
          });
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
        await this.recordMutations(this.filesystem(), [path]);
        return new Response('ok');
      }

      if (url.pathname === '/fs/read') {
        const path = url.searchParams.get('path');
        if (!path) return new Response('Missing ?path=', { status: 400 });
        return fileResponse(
          this.filesystem(),
          path,
          request,
          this.access,
          (ino) => latestFileVersion(this.ctx.storage.sql, ino)
        );
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
      const response = errorResponse(error);
      return browserUploadRequest ? withBrowserUploadCors(response) : response;
    }
  }
}

function withBrowserUploadCors(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  headers.set('Access-Control-Max-Age', '86400');
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function optionalQueryNumber(url: URL, name: string): number | undefined {
  const value = url.searchParams.get(name);
  if (value === null) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new HttpError(400, 'INVALID_ARGUMENT', `${name} must be a number`);
  return parsed;
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
      case 'forks':
        return { operation: 'admin', paths: ['/'] };
      case 'sql':
        return { operation: 'sql', paths: [] };
      case 'files':
        return {
          operation: method === 'GET' || method === 'HEAD' ? 'read' : 'write',
          paths: [route.path],
        };
      case 'directories':
        return { operation: method === 'GET' ? 'read' : 'write', paths: [route.path] };
      case 'trees':
        return { operation: method === 'GET' ? 'read' : 'write', paths: [route.path] };
      case 'tree':
        return { operation: 'read', paths: [route.path] };
      case 'uploads':
        // The route path is the final target; status reads, all mutating upload
        // methods write, scoped to that path.
        return { operation: method === 'GET' ? 'read' : 'write', paths: [route.path] };
      case 'browser-uploads':
        return { operation: 'write', paths: [route.path] };
      case 'assets':
        return {
          operation: method === 'GET' || method === 'HEAD' ? 'read' : 'write',
          paths: [assetPath(route.path.slice(1))],
        };
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
      case 'schedules':
        // Schedules persist command execution beyond the caller's token lifetime.
        return { operation: 'admin', paths: [] };
      case 'search': {
        const body = await safeJson(request);
        return { operation: 'read', paths: [typeof body?.path === 'string' ? body.path : '/'] };
      }
      case 'changes':
        return { operation: 'read', paths: [route.path] };
      case 'webhooks':
        return { operation: 'admin', paths: [] };
      case 'usage':
      case 'metrics':
        return { operation: 'read', paths: ['/'] };
      case 'quota':
        return method === 'GET'
          ? { operation: 'read', paths: ['/'] }
          : { operation: 'admin', paths: [] };
      case 'trash':
        return method === 'GET'
          ? { operation: 'read', paths: ['/'] }
          : { operation: 'admin', paths: [] };
      case 'services': {
        const segments = route.path.split('/').filter(Boolean);
        if (segments[1] === 'proxy' || (segments.length === 2 && segments[1] === 'logs')) {
          return { operation: 'exec', paths: ['/'] };
        }
        return method === 'GET' ? { operation: 'read', paths: ['/'] } : { operation: 'admin', paths: [] };
      }
      case 'capabilities':
        return method === 'GET'
          ? { operation: null, paths: [] }
          : { operation: 'admin', paths: [] };
      case 'auth':
        // Handled before the bearer check; never authorized through this path.
        return { operation: null, paths: [] };
      case 'sites':
      case 'shares':
        // Publishing exposes volume content publicly: reads status, admin mutates.
        return method === 'GET'
          ? { operation: 'read', paths: ['/'] }
          : { operation: 'admin', paths: ['/'] };
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

/** Default lifetime of a token minted by password login: 24 hours. */
const DEFAULT_LOGIN_TTL_SECONDS = 24 * 60 * 60;

function parseExpiry(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'expiresInSeconds must be a positive integer');
  }
  return value;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Subdomain hosting: rewrite `<volume>.<zone>/<path>` to the volume's site route.
    const label = subdomainVolume(request.headers.get('Host') ?? url.hostname, env.SITES_ZONE);
    if (label) {
      const target = new URL(url);
      target.pathname = `/s/${encodeURIComponent(label)}${url.pathname === '/' ? '' : url.pathname}`;
      return getContainer<AiryFS>(env.AiryFS, label).fetch(new Request(target, request));
    }

    if (url.pathname === '/v1/volumes' || url.pathname === '/v1/volumes/') {
      return handleVolumeRegistryRequest(
        request,
        env.AIRYFS_AUTH_SECRET,
        (after, limit) => env.VolumeRegistry.getByName('global').list(after, limit),
      );
    }

    let volume: string | null;
    try {
      volume = parsePublicVolume(url.pathname)
        ?? parseS3Route(url.pathname)?.volume
        ?? parseV1Route(url.pathname)?.volume
        ?? url.searchParams.get('volume');
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
