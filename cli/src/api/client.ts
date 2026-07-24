// ABOUTME: Provides a typed, streaming client for the AiryFS Worker HTTP API.
// ABOUTME: Uses v1 filesystem routes and legacy diagnostic routes behind one interface.

import { AiryFSApiError, AiryFSCommandOutcomeUnknownError, AiryFSTransportError, responseError } from './errors.js';
import { decodeNdjsonStream } from './ndjson.js';
import { encodeRemotePath } from './paths.js';
import { connectPty, type PtySession } from './pty.js';
import type {
  AuthStatus,
  AssetInfo,
  ChangePage,
  ChangeQuery,
  CreatedWebhook,
  CreateWebhookInput,
  ChecksumResult,
  DatabaseInfo,
  DirectoryEntry,
  DiskUsage,
  ExecEvent,
  ExecOptions,
  ExecResult,
  Job,
  JobLogPage,
  JobSchedule,
  JobStatus,
  MintCapabilityInput,
  MintedCapability,
  MountInfo,
  MountList,
  CreateMountInput,
  PasswordStatus,
  PublishSiteInput,
  QuotaInfo,
  CreateShareInput,
  SiteInfo,
  SiteStatus,
  SearchInput,
  SearchResponse,
  ShareInfo,
  PerfInfo,
  SnapshotDiffEntry,
  SnapshotInfo,
  TreeSummary,
  TreeViewResponse,
  TrashEntry,
  RestoredTrashEntry,
  ServiceRecord,
  ServiceLogPage,
  CreateServiceInput,
  SqlResult,
  SqlValue,
  UploadCompleteResult,
  UploadStatus,
  UsageInfo,
  UsageHistoryPage,
  VolumeInfo,
  VolumeRecord,
  VolumePage,
  WebhookInfo,
} from './types.js';

type Fetch = typeof fetch;

export class AiryFSClient {
  private readonly volumeBase: string;

  constructor(
    readonly endpoint: string,
    readonly volume: string,
    private readonly fetchImpl: Fetch = fetch,
    private readonly token?: string,
  ) {
    this.volumeBase = `/v1/volumes/${encodeURIComponent(volume)}`;
  }

  async getVolume(): Promise<VolumeInfo> {
    return this.json<VolumeInfo>(this.volumeBase);
  }

  async listVolumes(): Promise<VolumeRecord[]> {
    const volumes: VolumeRecord[] = [];
    let cursor: string | null = null;
    do {
      const query = new URLSearchParams({ limit: '1000' });
      if (cursor) query.set('cursor', cursor);
      const page = await this.json<VolumePage>(`/v1/volumes?${query}`);
      volumes.push(...page.volumes);
      cursor = page.nextCursor;
    } while (cursor);
    return volumes;
  }

  async quota(): Promise<QuotaInfo> {
    return this.json<QuotaInfo>(`${this.volumeBase}/quota`);
  }

  async setQuota(input: Partial<QuotaInfo>): Promise<QuotaInfo> {
    return this.json<QuotaInfo>(`${this.volumeBase}/quota`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async createVolume(chunkSize?: number): Promise<VolumeInfo> {
    return this.json<VolumeInfo>(this.volumeBase, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(chunkSize === undefined ? {} : { chunkSize }),
    });
  }

  /** Permanently delete the volume and all of its data; requires root access. */
  async deleteVolume(): Promise<{ deleted: boolean }> {
    return this.json<{ deleted: boolean }>(this.volumeBase, { method: 'DELETE' });
  }

  async listDirectory(path: string): Promise<DirectoryEntry[]> {
    return this.json<DirectoryEntry[]>(this.resourcePath('directories', path));
  }

  async tree(path: string, options: { depth?: number; limit?: number } = {}): Promise<TreeViewResponse> {
    const url = new URL(this.resourcePath('tree', path), 'http://airyfs.local');
    if (options.depth !== undefined) url.searchParams.set('depth', String(options.depth));
    if (options.limit !== undefined) url.searchParams.set('limit', String(options.limit));
    return this.json<TreeViewResponse>(`${url.pathname}${url.search}`);
  }

  async readFile(path: string, range?: string): Promise<Response> {
    return this.request(this.resourcePath('files', path), {
      headers: range ? { Range: range } : undefined,
    });
  }

  async headFile(path: string): Promise<Response> {
    return this.request(this.resourcePath('files', path), { method: 'HEAD' });
  }

  async readFileBytes(path: string, range?: string): Promise<Uint8Array> {
    return new Uint8Array(await (await this.readFile(path, range)).arrayBuffer());
  }

  async writeFile(path: string, body: NonNullable<RequestInit['body']>): Promise<void> {
    await this.request(this.resourcePath('files', path), {
      method: 'PUT',
      body,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
  }

  /**
   * Patch bytes into an existing file starting at `offset`, extending it when
   * the write runs past the end. Returns the number of bytes written.
   */
  async writeFileRange(path: string, offset: number, body: NonNullable<RequestInit['body']>): Promise<number> {
    const url = new URL(this.url(this.resourcePath('files', path)));
    url.searchParams.set('offset', String(offset));
    const response = await this.requestUrl(url, {
      method: 'PATCH',
      body,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
    const written = response.headers.get('X-AiryFS-Bytes-Written');
    return written === null ? 0 : Number(written);
  }

  async deleteFile(path: string, permanent = false): Promise<TrashEntry | undefined> {
    const url = new URL(this.url(this.resourcePath('files', path)));
    if (permanent) url.searchParams.set('permanent', 'true');
    const response = await this.requestUrl(url, { method: 'DELETE' });
    return permanent ? undefined : await response.json() as TrashEntry;
  }

  async makeDirectory(path: string): Promise<void> {
    await this.request(this.resourcePath('directories', path), { method: 'PUT' });
  }

  async removeDirectory(path: string, recursive = false, permanent = false): Promise<TrashEntry | undefined> {
    const url = new URL(this.url(this.resourcePath('directories', path)));
    if (recursive) url.searchParams.set('recursive', 'true');
    if (permanent) url.searchParams.set('permanent', 'true');
    const response = await this.requestUrl(url, { method: 'DELETE' });
    return permanent ? undefined : await response.json() as TrashEntry;
  }

  async listTrash(): Promise<TrashEntry[]> {
    return this.json<TrashEntry[]>(`${this.volumeBase}/trash`);
  }

  async restoreTrash(id: string, to?: string): Promise<RestoredTrashEntry> {
    return this.json<RestoredTrashEntry>(`${this.volumeBase}/trash/${encodeURIComponent(id)}/restore`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(to ? { to } : {}),
    });
  }

  async purgeTrash(id: string): Promise<TrashEntry> {
    return this.json<TrashEntry>(`${this.volumeBase}/trash/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  async undoTrash(): Promise<RestoredTrashEntry> {
    return this.json<RestoredTrashEntry>(`${this.volumeBase}/trash/undo`, { method: 'POST' });
  }

  async rename(from: string, to: string): Promise<void> {
    await this.operation('rename', { from, to });
  }

  async copy(from: string, to: string): Promise<void> {
    await this.operation('copy', { from, to });
  }

  async symlink(target: string, path: string): Promise<void> {
    await this.operation('symlink', { target, path });
  }

  async readlink(path: string): Promise<string> {
    const result = await this.operation<{ target: string }>('readlink', { path });
    return result.target;
  }

  async truncate(path: string, size: number): Promise<void> {
    await this.operation('truncate', { path, size });
  }

  async lstat(path: string): Promise<import('./types.js').FileStats> {
    return this.operation('lstat', { path });
  }

  async touch(path: string, options: { atime?: number; mtime?: number } = {}): Promise<void> {
    await this.operation('touch', { path, ...options });
  }

  async chmod(path: string, mode: number): Promise<void> {
    await this.operation('chmod', { path, mode });
  }

  async link(existing: string, path: string): Promise<void> {
    await this.operation('link', { existing, path });
  }

  async appendFile(path: string, data: Uint8Array): Promise<void> {
    await this.operation('append', { path, data: Buffer.from(data).toString('base64') });
  }

  async diskUsage(path: string): Promise<DiskUsage> {
    return this.operation<DiskUsage>('du', { path });
  }

  /** Compute the server-side streaming SHA-256 of a remote file. */
  async checksum(path: string): Promise<ChecksumResult> {
    return this.operation<ChecksumResult>('checksum', { path });
  }

  async putAsset(checksum: string, body: NonNullable<RequestInit['body']>): Promise<AssetInfo> {
    return this.json<AssetInfo>(`${this.volumeBase}/assets/${encodeURIComponent(checksum)}`, {
      method: 'PUT',
      body,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
  }

  async getAsset(checksum: string): Promise<Response> {
    return this.request(`${this.volumeBase}/assets/${encodeURIComponent(checksum)}`);
  }

  // --- Resumable uploads (the route path is the final target) -------------

  /** Create or resume a resumable upload session for `path`. */
  async beginUpload(path: string, size: number, checksum: string): Promise<UploadStatus> {
    return this.json<UploadStatus>(this.resourcePath('uploads', path), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ size, checksum }),
    });
  }

  /** Return the current status of an upload session. */
  async uploadStatus(path: string): Promise<UploadStatus> {
    return this.json<UploadStatus>(this.resourcePath('uploads', path));
  }

  /** Append one bounded, checksummed chunk at `offset`; returns the new status. */
  async appendUpload(
    path: string,
    offset: number,
    chunkSha256: string,
    data: Uint8Array,
  ): Promise<UploadStatus> {
    return this.json<UploadStatus>(this.resourcePath('uploads', path), {
      method: 'PATCH',
      headers: {
        'Upload-Offset': String(offset),
        'X-AiryFS-Chunk-SHA256': chunkSha256,
        'Content-Type': 'application/octet-stream',
      },
      body: data,
    });
  }

  /** Complete a fully-received upload, publishing it over its target. */
  async completeUpload(path: string): Promise<UploadCompleteResult> {
    return this.json<UploadCompleteResult>(this.resourcePath('uploads', path), { method: 'PUT' });
  }

  /** Abort an upload, removing its temp file and session. */
  async abortUpload(path: string): Promise<void> {
    await this.request(this.resourcePath('uploads', path), { method: 'DELETE' });
  }

  /** Stream a directory subtree as a AiryFS archive (pull). */
  async exportTree(path: string): Promise<Response> {
    return this.request(this.resourcePath('trees', path));
  }

  /** Import a AiryFS archive into a target directory (push); returns a summary. */
  async importTree(
    path: string,
    body: NonNullable<RequestInit['body']>,
    replace = false,
  ): Promise<TreeSummary> {
    const url = new URL(this.url(this.resourcePath('trees', path)));
    if (replace) url.searchParams.set('replace', 'true');
    const response = await this.requestUrl(url, {
      method: 'PUT',
      body,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
    return response.json() as Promise<TreeSummary>;
  }

  async exec(command: string, signalOrOptions?: AbortSignal | ExecOptions): Promise<ExecResult> {
    const options = execOptions(signalOrOptions);
    let commandId: string | undefined;
    let stdout = '';
    let stderr = '';
    let exitCode = 1;
    let outputTruncated = false;
    const stdoutDecoder = new TextDecoder();
    const stderrDecoder = new TextDecoder();
    for await (const event of await this.execStream(command, options)) {
      if (event.type === 'start') commandId = event.id;
      else if (event.type === 'stdout') stdout += stdoutDecoder.decode(fromBase64(event.data), { stream: true });
      else if (event.type === 'stderr') stderr += stderrDecoder.decode(fromBase64(event.data), { stream: true });
      else if (event.type === 'exit') {
        exitCode = event.exitCode;
        outputTruncated = event.outputTruncated ?? false;
      }
    }
    stdout += stdoutDecoder.decode();
    stderr += stderrDecoder.decode();
    return { commandId, exitCode, stdout, stderr, ...(outputTruncated ? { outputTruncated: true } : {}) };
  }

  /**
   * Run a command and stream its NDJSON events. Awaiting this resolves once the
   * server accepts the request (connection or admission errors throw here); the
   * returned iterable then yields events until the terminal `exit`.
   */
  async execStream(command: string, signalOrOptions?: AbortSignal | ExecOptions): Promise<AsyncIterable<ExecEvent>> {
    const options = execOptions(signalOrOptions);
    const key = options.idempotencyKey ?? crypto.randomUUID();
    const job = await this.submitJobWithRetry(command, '/', key, options.signal);
    return this.followCommand(job, options);
  }

  async execTransient(command: string, signalOrOptions?: AbortSignal | ExecTransientOptions): Promise<ExecResult> {
    const options = execTransientOptions(signalOrOptions);
    return this.json<ExecResult>(`${this.volumeBase}/exec`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(execBody(command, options.stdin)),
      signal: options.signal,
    });
  }

  async execStreamTransient(command: string, signalOrOptions?: AbortSignal | ExecTransientOptions): Promise<AsyncIterable<ExecEvent>> {
    const options = execTransientOptions(signalOrOptions);
    const url = new URL(this.url(`${this.volumeBase}/exec`));
    url.searchParams.set('stream', 'true');
    const response = await this.requestUrl(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(execBody(command, options.stdin)),
      signal: options.signal,
    });
    if (!response.body) return emptyTransientEvents();
    return decodeNdjsonStream<ExecEvent>(response.body);
  }

  /** Request cancellation of a streaming command by its start-event id. */
  async cancelExec(id: string): Promise<void> {
    try {
      await this.cancelJob(id);
    } catch (error) {
      if (!(error instanceof AiryFSApiError) || error.code !== 'JOB_NOT_FOUND') throw error;
      await this.request(`${this.volumeBase}/exec/cancel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id }),
      });
    }
  }

  async openPty(WebSocketImpl: typeof WebSocket = WebSocket): Promise<PtySession> {
    const { ticket } = await this.json<{ ticket: string }>(`${this.volumeBase}/exec/pty-ticket`, { method: 'POST' });
    const url = new URL(this.endpoint);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.pathname = `${this.volumeBase}/exec/pty`;
    url.search = new URLSearchParams({ ticket }).toString();
    return connectPty(url, WebSocketImpl);
  }

  async listServices(): Promise<ServiceRecord[]> { return this.json<ServiceRecord[]>(`${this.volumeBase}/services`); }
  async getServiceLogs(
    name: string,
    options: { after?: number; generation?: string; signal?: AbortSignal } = {},
  ): Promise<ServiceLogPage> {
    const query = new URLSearchParams();
    if (options.after !== undefined) query.set('after', String(options.after));
    if (options.generation) query.set('generation', options.generation);
    const suffix = query.size > 0 ? `?${query}` : '';
    return this.json<ServiceLogPage>(`${this.volumeBase}/services/${encodeURIComponent(name)}/logs${suffix}`, {
      signal: options.signal,
    });
  }
  async createService(input: CreateServiceInput): Promise<ServiceRecord> {
    return this.json<ServiceRecord>(`${this.volumeBase}/services`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input),
    });
  }
  async startService(name: string): Promise<ServiceRecord> { return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}/start`, { method: 'POST' }); }
  async stopService(name: string): Promise<ServiceRecord> { return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}/stop`, { method: 'POST' }); }
  async deleteService(name: string): Promise<ServiceRecord> { return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}`, { method: 'DELETE' }); }

  // --- Durable jobs -------------------------------------------------------

  /**
   * Submit a durable job. An Idempotency-Key makes retries safe: repeating the
   * same key returns the existing job rather than enqueuing a duplicate. One is
   * generated by default.
   */
  async submitJob(
    command: string,
    cwd?: string,
    idempotencyKey: string = crypto.randomUUID(),
    signal?: AbortSignal,
  ): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(cwd === undefined ? { command } : { command, cwd }),
      signal,
    });
  }

  /** List jobs newest-first, optionally filtered by status. */
  async listJobs(status?: JobStatus): Promise<Job[]> {
    const url = new URL(this.url(`${this.volumeBase}/jobs`));
    if (status) url.searchParams.set('status', status);
    return (await this.requestUrl(url)).json() as Promise<Job[]>;
  }

  /** Fetch a single job by id. */
  async getJob(id: string, signal?: AbortSignal): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs/${encodeURIComponent(id)}`, { signal });
  }

  /** Read a page of a job's persisted logs; `data` in each entry is base64. */
  async getJobLogs(id: string, after?: number, limit?: number, signal?: AbortSignal): Promise<JobLogPage> {
    const url = new URL(this.url(`${this.volumeBase}/jobs/${encodeURIComponent(id)}/logs`));
    if (after !== undefined) url.searchParams.set('after', String(after));
    if (limit !== undefined) url.searchParams.set('limit', String(limit));
    return (await this.requestUrl(url, { signal })).json() as Promise<JobLogPage>;
  }

  /** Request cancellation of a job by id; idempotent for terminal jobs. */
  async cancelJob(id: string, signal?: AbortSignal): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs/${encodeURIComponent(id)}/cancel`, { method: 'POST', signal });
  }

  private async submitJobWithRetry(
    command: string,
    cwd: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ): Promise<Job> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      signal?.throwIfAborted();
      try {
        return await this.submitJob(command, cwd, idempotencyKey, signal);
      } catch (error) {
        lastError = error;
        const retryable = error instanceof AiryFSTransportError
          || (error instanceof AiryFSApiError && [502, 503, 504].includes(error.status));
        if (!retryable || attempt === 2) throw error;
        await abortableDelay(200 * (2 ** attempt), signal);
      }
    }
    throw lastError;
  }

  private followCommand(initial: Job, options: ExecOptions): AsyncIterable<ExecEvent> {
    const client = this;
    return (async function* (): AsyncGenerator<ExecEvent> {
      const id = initial.id;
      let cursor: number | undefined;
      let terminal = false;
      let pollFailed = false;
      try {
        yield { type: 'start', id };
        let job = initial;
        while (true) {
          options.signal?.throwIfAborted();
          let page = await client.retryCommandRead(() => client.getJobLogs(id, cursor, undefined, options.signal), options.signal);
          cursor = client.advanceLogCursor(page, cursor);
          for (const entry of page.entries) {
            yield { type: entry.stream, id, data: entry.data };
          }
          job = await client.retryCommandRead(() => client.getJob(id, options.signal), options.signal);
          if (job.status === 'succeeded' || job.status === 'failed' || job.status === 'canceled' || job.status === 'unknown') {
            do {
              page = await client.retryCommandRead(() => client.getJobLogs(id, cursor, undefined, options.signal), options.signal);
              cursor = client.advanceLogCursor(page, cursor);
              for (const entry of page.entries) yield { type: entry.stream, id, data: entry.data };
            } while (page.next !== null);
            terminal = true;
            if (job.status === 'unknown') {
              throw new AiryFSCommandOutcomeUnknownError(id, job.error ?? undefined);
            }
            if (job.exitCode === null && job.status === 'failed') {
              throw new AiryFSApiError(503, 'COMMAND_FAILED', job.error ?? 'Command failed before reporting an exit code');
            }
            yield {
              type: 'exit', id,
              exitCode: job.exitCode ?? (job.status === 'canceled' ? 130 : 1),
              ...(job.outputTruncated ? { outputTruncated: true } : {}),
              ...(job.status === 'canceled' ? { signal: 'SIGTERM' } : {}),
            };
            return;
          }
          await abortableDelay(options.pollInterval ?? 250, options.signal);
        }
      } catch (error) {
        pollFailed = true;
        throw error;
      } finally {
        if (!terminal && (!pollFailed || options.signal?.aborted)) {
          await client.cancelJob(id, AbortSignal.timeout(5_000));
        }
      }
    })();
  }

  private advanceLogCursor(page: JobLogPage, cursor: number | undefined): number | undefined {
    for (const entry of page.entries) {
      if (cursor !== undefined && entry.seq <= cursor) throw new Error(`Command log cursor did not advance beyond ${cursor}`);
      cursor = entry.seq;
    }
    if (page.next !== null && page.entries.length === 0) throw new Error('Command log page did not advance');
    return cursor;
  }

  private async retryCommandRead<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      signal?.throwIfAborted();
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        const retryable = error instanceof AiryFSTransportError
          || error instanceof AiryFSApiError && [502, 503, 504].includes(error.status);
        if (!retryable || attempt === 2) throw error;
        await abortableDelay(200 * (2 ** attempt), signal);
      }
    }
    throw lastError;
  }

  async createSchedule(input: { name: string; cron: string; command: string; cwd: string }): Promise<JobSchedule> {
    return this.json<JobSchedule>(`${this.volumeBase}/schedules`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async listSchedules(): Promise<JobSchedule[]> {
    return this.json<JobSchedule[]>(`${this.volumeBase}/schedules`);
  }

  async setScheduleEnabled(id: string, enabled: boolean): Promise<JobSchedule> {
    return this.json<JobSchedule>(
      `${this.volumeBase}/schedules/${encodeURIComponent(id)}/${enabled ? 'enable' : 'disable'}`,
      { method: 'POST' },
    );
  }

  async deleteSchedule(id: string): Promise<{ id: string; removed: boolean }> {
    return this.json<{ id: string; removed: boolean }>(
      `${this.volumeBase}/schedules/${encodeURIComponent(id)}`,
      { method: 'DELETE' },
    );
  }

  /** Read or long-poll filesystem changes after an exclusive sequence cursor. */
  async getChanges(options: ChangeQuery = {}): Promise<ChangePage> {
    const url = new URL(this.url(this.resourcePath('changes', options.path ?? '/')));
    if (options.since !== undefined) url.searchParams.set('since', String(options.since));
    if (options.limit !== undefined) url.searchParams.set('limit', String(options.limit));
    if (options.wait !== undefined) url.searchParams.set('wait', String(options.wait));
    return (await this.requestUrl(url, { signal: options.signal })).json() as Promise<ChangePage>;
  }

  async search(input: SearchInput): Promise<SearchResponse> {
    return this.json<SearchResponse>(`${this.volumeBase}/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async createWebhook(input: CreateWebhookInput): Promise<CreatedWebhook> {
    return this.json<CreatedWebhook>(`${this.volumeBase}/webhooks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async listWebhooks(): Promise<WebhookInfo[]> {
    return this.json<WebhookInfo[]>(`${this.volumeBase}/webhooks`);
  }

  async deleteWebhook(id: string): Promise<{ id: string; removed: boolean }> {
    return this.json<{ id: string; removed: boolean }>(
      `${this.volumeBase}/webhooks/${encodeURIComponent(id)}`,
      { method: 'DELETE' },
    );
  }

  async usage(): Promise<UsageInfo> {
    return this.json<UsageInfo>(`${this.volumeBase}/usage`);
  }

  async usageHistory(options: { before?: number; limit?: number } = {}): Promise<UsageHistoryPage> {
    const query = new URLSearchParams();
    if (options.before !== undefined) query.set('before', String(options.before));
    if (options.limit !== undefined) query.set('limit', String(options.limit));
    const suffix = query.size > 0 ? `?${query}` : '';
    return this.json<UsageHistoryPage>(`${this.volumeBase}/usage-history${suffix}`);
  }

  async metrics(): Promise<string> {
    return (await this.request(`${this.volumeBase}/metrics`)).text();
  }

  async perf(): Promise<PerfInfo> {
    return this.legacyJson<PerfInfo>('/perf');
  }

  async databaseInfo(): Promise<DatabaseInfo> {
    return this.legacyJson<DatabaseInfo>('/db-info');
  }

  async destroyContainer(): Promise<void> {
    await this.legacy('/destroy', { method: 'POST' });
  }

  async setKv(key: string, value: string): Promise<void> {
    const url = this.legacyUrl('/kv/set');
    url.searchParams.set('key', key);
    await this.requestUrl(url, { method: 'POST', body: value });
  }

  async getKv(key: string): Promise<string> {
    const url = this.legacyUrl('/kv/get');
    url.searchParams.set('key', key);
    return (await this.requestUrl(url)).text();
  }

  async authStatus(): Promise<AuthStatus> {
    return this.json<AuthStatus>(`${this.volumeBase}/capabilities`);
  }

  /** Report whether deployment auth is enabled and a volume password is set. */
  async passwordStatus(): Promise<PasswordStatus> {
    return this.json<PasswordStatus>(`${this.volumeBase}/auth`);
  }

  /** Return the published-site status for the volume. */
  async getSite(): Promise<SiteStatus> {
    return this.json<SiteStatus>(`${this.volumeBase}/sites`);
  }

  /** Publish or update the volume's web root. */
  async publishSite(input: PublishSiteInput): Promise<SiteInfo> {
    return this.json<SiteInfo>(`${this.volumeBase}/sites`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  /** Remove the volume's published site. */
  async unpublishSite(): Promise<{ removed: boolean }> {
    return this.json<{ removed: boolean }>(`${this.volumeBase}/sites`, { method: 'DELETE' });
  }

  /** List share links for the volume. */
  async listShares(): Promise<ShareInfo[]> {
    return this.json<ShareInfo[]>(`${this.volumeBase}/shares`);
  }

  /** Create a share link for a single file, with optional expiry. */
  async createShare(input: CreateShareInput): Promise<ShareInfo> {
    return this.json<ShareInfo>(`${this.volumeBase}/shares`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  /** Delete a share link by id. */
  async deleteShare(id: string): Promise<{ id: string; removed: boolean }> {
    return this.json<{ id: string; removed: boolean }>(
      `${this.volumeBase}/shares/${encodeURIComponent(id)}`,
      { method: 'DELETE' },
    );
  }

  /** Set or rotate the volume password. Authorized by root, admin, or currentPassword. */
  async setVolumePassword(password: string, currentPassword?: string): Promise<PasswordStatus> {
    const body: Record<string, string> = { password };
    if (currentPassword !== undefined) body.currentPassword = currentPassword;
    return this.json<PasswordStatus>(`${this.volumeBase}/auth/password`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  /** Exchange the volume password for a scoped capability token. */
  async loginWithPassword(password: string, expiresInSeconds?: number): Promise<MintedCapability> {
    const body: Record<string, unknown> = { password };
    if (expiresInSeconds !== undefined) body.expiresInSeconds = expiresInSeconds;
    return this.json<MintedCapability>(`${this.volumeBase}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  async createCapability(input: MintCapabilityInput): Promise<MintedCapability> {
    return this.json<MintedCapability>(`${this.volumeBase}/capabilities`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async revokeCapability(id: string): Promise<void> {
    await this.request(`${this.volumeBase}/capabilities/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  async listMounts(): Promise<MountList> {
    return this.json<MountList>(`${this.volumeBase}/mounts`);
  }

  async createMount(mountpoint: string, input: CreateMountInput): Promise<MountInfo> {
    return this.json<MountInfo>(this.resourcePath('mounts', mountpoint), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async deleteMount(mountpoint: string): Promise<MountInfo & { removed: boolean }> {
    return this.json<MountInfo & { removed: boolean }>(this.resourcePath('mounts', mountpoint), { method: 'DELETE' });
  }

  /** Capture a full-volume snapshot; an omitted name generates a server default. */
  async createSnapshot(name?: string, note?: string): Promise<SnapshotInfo> {
    const body: Record<string, string> = {};
    if (name !== undefined) body.name = name;
    if (note !== undefined) body.note = note;
    return this.json<SnapshotInfo>(`${this.volumeBase}/snapshots`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  async listSnapshots(): Promise<SnapshotInfo[]> {
    return this.json<SnapshotInfo[]>(`${this.volumeBase}/snapshots`);
  }

  /** Diff a snapshot against the live volume (default) or another snapshot id/name. */
  async diffSnapshot(id: string, against = 'live'): Promise<SnapshotDiffEntry[]> {
    const url = new URL(this.url(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}/diff`));
    url.searchParams.set('against', against);
    return (await this.requestUrl(url)).json() as Promise<SnapshotDiffEntry[]>;
  }

  async restoreSnapshot(id: string): Promise<SnapshotInfo> {
    return this.json<SnapshotInfo>(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}/restore`, {
      method: 'POST',
    });
  }

  async cloneSnapshot(id: string, targetVolume: string): Promise<TreeSummary> {
    return this.json<TreeSummary>(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}/clone`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetVolume }),
    });
  }

  async forkVolume(targetVolume: string): Promise<TreeSummary> {
    return this.json<TreeSummary>(`${this.volumeBase}/forks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetVolume }),
    });
  }

  async sql(statement: string, args: SqlValue[] = []): Promise<SqlResult> {
    return this.json<SqlResult>(`${this.volumeBase}/sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: statement, args }),
    });
  }

  async deleteSnapshot(id: string): Promise<SnapshotInfo> {
    return this.json<SnapshotInfo>(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  private async operation<T = void>(name: string, body: Record<string, unknown>): Promise<T> {
    const response = await this.request(`${this.volumeBase}/operations/${name}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  }

  private resourcePath(resource: 'files' | 'directories' | 'trees' | 'tree' | 'uploads' | 'changes' | 'mounts', path: string): string {
    const encoded = encodeRemotePath(path);
    return `${this.volumeBase}/${resource}${encoded ? `/${encoded}` : ''}`;
  }

  private async json<T>(path: string, init?: RequestInit): Promise<T> {
    return (await this.request(path, init)).json() as Promise<T>;
  }

  private async legacyJson<T>(path: string, init?: RequestInit): Promise<T> {
    return (await this.legacy(path, init)).json() as Promise<T>;
  }

  private async legacy(path: string, init?: RequestInit): Promise<Response> {
    return this.requestUrl(this.legacyUrl(path), init);
  }

  private legacyUrl(path: string): URL {
    const url = new URL(this.url(path));
    url.searchParams.set('volume', this.volume);
    return url;
  }

  private request(path: string, init?: RequestInit): Promise<Response> {
    return this.requestUrl(new URL(this.url(path)), init);
  }

  private async requestUrl(url: URL, init?: RequestInit): Promise<Response> {
    let response: Response;
    try {
      response = await this.fetchImpl(url, this.withAuth(init));
    } catch (error) {
      throw new AiryFSTransportError(url.origin, error instanceof Error ? error.message : String(error));
    }
    if (!response.ok) throw await responseError(response);
    return response;
  }

  /** Attach the session bearer credential to every request when one is configured. */
  private withAuth(init?: RequestInit): RequestInit | undefined {
    if (!this.token) return init;
    const headers = new Headers(init?.headers);
    headers.set('Authorization', `Bearer ${this.token}`);
    return { ...init, headers };
  }

  private url(path: string): string {
    return `${this.endpoint.replace(/\/$/, '')}${path}`;
  }
}

function execOptions(value?: AbortSignal | ExecOptions): ExecOptions {
  if (!value) return {};
  return 'aborted' in value && typeof value.addEventListener === 'function'
    ? { signal: value as AbortSignal }
    : value as ExecOptions;
}

function fromBase64(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function toBase64(value: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < value.byteLength; i++) binary += String.fromCharCode(value[i]);
  return btoa(binary);
}

/** Options for a transient (non-durable) exec: abort signal plus optional stdin. */
export interface ExecTransientOptions {
  signal?: AbortSignal;
  stdin?: Uint8Array | string;
}

function execTransientOptions(value?: AbortSignal | ExecTransientOptions): ExecTransientOptions {
  if (!value) return {};
  return 'aborted' in value && typeof value.addEventListener === 'function'
    ? { signal: value as AbortSignal }
    : value as ExecTransientOptions;
}

/** Build an exec request body, base64-encoding stdin when present. */
function execBody(command: string, stdin?: Uint8Array | string): Record<string, string> {
  if (stdin === undefined) return { command };
  const bytes = typeof stdin === 'string' ? new TextEncoder().encode(stdin) : stdin;
  return { command, stdin: toBase64(bytes) };
}

function abortableDelay(ms: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

async function* emptyTransientEvents(): AsyncGenerator<ExecEvent> {}
