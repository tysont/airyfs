// ABOUTME: Complete typed HTTP client for every AiryFS volume resource.
// ABOUTME: Keeps transport web-standard and leaves orchestration to reusable helpers.

import { AiryFSApiError, AiryFSCommandOutcomeUnknownError, AiryFSTransportError, responseError } from './errors.js';
import { decodeNdjsonStream } from './ndjson.js';
import { encodeRemotePath } from './paths.js';
import type {
  AuthStatus,
  ChangePage,
  ChangeQuery,
  ChecksumResult,
  DatabaseInfo,
  DiskUsage,
  DirectoryEntry,
  AiryFSClientOptions,
  ExecEvent,
  ExecOptions,
  ExecResult,
  Job,
  JobLogPage,
  JobStatus,
  MintCapabilityInput,
  MintedCapability,
  PerfInfo,
  QuotaInfo,
  TrashEntry,
  RestoredTrashEntry,
  SnapshotDiffEntry,
  SnapshotInfo,
  ServiceRecord,
  ServiceLogPage,
  CreateServiceInput,
  SqlResult,
  SqlValue,
  TreeSummary,
  TreeViewResponse,
  UploadCompleteResult,
  UploadStatus,
  UsageInfo,
  UsageHistoryPage,
  VolumeInfo,
  VolumeRecord,
  VolumePage,
} from './types.js';

export class AiryFSClient {
  private readonly volumeBase: string;
  private readonly fetchImpl: typeof fetch;
  private readonly token?: string;
  private readonly defaultHeaders: HeadersInit;

  constructor(
    readonly endpoint: string,
    readonly volume: string,
    options: AiryFSClientOptions = {},
  ) {
    this.volumeBase = `/v1/volumes/${encodeURIComponent(volume)}`;
    this.fetchImpl = options.fetch ?? fetch;
    this.token = options.token;
    this.defaultHeaders = options.headers ?? {};
  }

  getVolume(): Promise<VolumeInfo> {
    return this.json<VolumeInfo>(this.volumeBase);
  }

  /**
   * Permanently delete the volume and all of its data. Requires root (or an
   * auth-disabled deployment). Idempotent: deleting a missing volume succeeds.
   */
  deleteVolume(): Promise<{ deleted: boolean }> {
    return this.json<{ deleted: boolean }>(this.volumeBase, { method: 'DELETE' });
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

  quota(): Promise<QuotaInfo> {
    return this.json<QuotaInfo>(`${this.volumeBase}/quota`);
  }

  setQuota(input: Partial<QuotaInfo>): Promise<QuotaInfo> {
    return this.json<QuotaInfo>(`${this.volumeBase}/quota`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  createVolume(chunkSize?: number): Promise<VolumeInfo> {
    return this.json<VolumeInfo>(this.volumeBase, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(chunkSize === undefined ? {} : { chunkSize }),
    });
  }

  listDirectory(path: string): Promise<DirectoryEntry[]> {
    return this.json<DirectoryEntry[]>(this.resourcePath('directories', path));
  }

  tree(path: string, options: { depth?: number; limit?: number } = {}): Promise<TreeViewResponse> {
    const query = new URLSearchParams();
    if (options.depth !== undefined) query.set('depth', String(options.depth));
    if (options.limit !== undefined) query.set('limit', String(options.limit));
    const suffix = query.size > 0 ? `?${query}` : '';
    return this.json<TreeViewResponse>(`${this.resourcePath('tree', path)}${suffix}`);
  }

  readFile(path: string, range?: string, signal?: AbortSignal): Promise<Response> {
    return this.request(this.resourcePath('files', path), {
      headers: range ? { Range: range } : undefined,
      signal,
    });
  }

  headFile(path: string, signal?: AbortSignal): Promise<Response> {
    return this.request(this.resourcePath('files', path), { method: 'HEAD', signal });
  }

  async readFileBytes(path: string, range?: string, signal?: AbortSignal): Promise<Uint8Array> {
    return new Uint8Array(await (await this.readFile(path, range, signal)).arrayBuffer());
  }

  async readFileText(path: string, range?: string, signal?: AbortSignal): Promise<string> {
    return (await this.readFile(path, range, signal)).text();
  }

  async writeFile(path: string, body: BodyInit, signal?: AbortSignal): Promise<void> {
    await this.request(this.resourcePath('files', path), {
      method: 'PUT',
      body,
      signal,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
  }

  /**
   * Write `body` into an existing file starting at `offset`, extending the file
   * when the write runs past its end. Patches bytes in place instead of
   * replacing the whole file. Resolves to the number of bytes written.
   */
  async writeFileRange(path: string, offset: number, body: BodyInit, signal?: AbortSignal): Promise<number> {
    const url = this.resourceUrl('files', path);
    url.searchParams.set('offset', String(offset));
    const response = await this.requestUrl(url, {
      method: 'PATCH',
      body,
      signal,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
    const written = response.headers.get('X-AiryFS-Bytes-Written');
    return written === null ? 0 : Number(written);
  }

  async deleteFile(path: string, permanent = false): Promise<TrashEntry | undefined> {
    const url = this.resourceUrl('files', path);
    if (permanent) url.searchParams.set('permanent', 'true');
    const response = await this.requestUrl(url, { method: 'DELETE' });
    return permanent ? undefined : await response.json() as TrashEntry;
  }

  async makeDirectory(path: string): Promise<void> {
    await this.request(this.resourcePath('directories', path), { method: 'PUT' });
  }

  async removeDirectory(path: string, recursive = false, permanent = false): Promise<TrashEntry | undefined> {
    const url = this.resourceUrl('directories', path);
    if (recursive) url.searchParams.set('recursive', 'true');
    if (permanent) url.searchParams.set('permanent', 'true');
    const response = await this.requestUrl(url, { method: 'DELETE' });
    return permanent ? undefined : await response.json() as TrashEntry;
  }

  listTrash(): Promise<TrashEntry[]> {
    return this.json<TrashEntry[]>(`${this.volumeBase}/trash`);
  }

  restoreTrash(id: string, to?: string): Promise<RestoredTrashEntry> {
    return this.json<RestoredTrashEntry>(`${this.volumeBase}/trash/${encodeURIComponent(id)}/restore`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(to ? { to } : {}),
    });
  }

  purgeTrash(id: string): Promise<TrashEntry> {
    return this.json<TrashEntry>(`${this.volumeBase}/trash/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  undoTrash(): Promise<RestoredTrashEntry> {
    return this.json<RestoredTrashEntry>(`${this.volumeBase}/trash/undo`, { method: 'POST' });
  }

  rename(from: string, to: string): Promise<void> {
    return this.operation('rename', { from, to });
  }

  copy(from: string, to: string): Promise<void> {
    return this.operation('copy', { from, to });
  }

  symlink(target: string, path: string): Promise<void> {
    return this.operation('symlink', { target, path });
  }

  async readlink(path: string): Promise<string> {
    return (await this.operation<{ target: string }>('readlink', { path })).target;
  }

  truncate(path: string, size: number): Promise<void> {
    return this.operation('truncate', { path, size });
  }

  lstat(path: string): Promise<import('./types.js').FileStats> {
    return this.operation('lstat', { path });
  }

  touch(path: string, options: { atime?: number; mtime?: number } = {}): Promise<void> {
    return this.operation('touch', { path, ...options });
  }

  chmod(path: string, mode: number): Promise<void> {
    return this.operation('chmod', { path, mode });
  }

  link(existing: string, path: string): Promise<void> {
    return this.operation('link', { existing, path });
  }

  appendFile(path: string, data: Uint8Array): Promise<void> {
    return this.operation('append', { path, data: encodeBase64(data) });
  }

  diskUsage(path: string): Promise<DiskUsage> {
    return this.operation<DiskUsage>('du', { path });
  }

  checksum(path: string): Promise<ChecksumResult> {
    return this.operation<ChecksumResult>('checksum', { path });
  }

  exportTree(path: string, signal?: AbortSignal): Promise<Response> {
    return this.request(this.resourcePath('trees', path), { signal });
  }

  async importTree(path: string, body: BodyInit, replace = false, signal?: AbortSignal): Promise<TreeSummary> {
    const url = this.resourceUrl('trees', path);
    if (replace) url.searchParams.set('replace', 'true');
    return (await this.requestUrl(url, {
      method: 'PUT',
      body,
      signal,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' })).json() as Promise<TreeSummary>;
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

  async execStream(command: string, signalOrOptions?: AbortSignal | ExecOptions): Promise<AsyncIterable<ExecEvent>> {
    const options = execOptions(signalOrOptions);
    const key = options.idempotencyKey ?? crypto.randomUUID();
    const job = await this.submitJobWithRetry(command, '/', key, options.signal);
    return this.followCommand(job, options);
  }

  /**
   * Execute without durable tracking. Prefer {@link exec} unless immediate
   * admission is required. Optional `stdin` is fed to the process on its
   * standard input, which is then closed (EOF).
   */
  execTransient(command: string, signalOrOptions?: AbortSignal | ExecTransientOptions): Promise<ExecResult> {
    const options = execTransientOptions(signalOrOptions);
    return this.json<ExecResult>(`${this.volumeBase}/exec`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(execBody(command, options.stdin)),
      signal: options.signal,
    });
  }

  /** Stream without durable tracking. Prefer {@link execStream} for recoverable execution. */
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
    return response.body ? decodeNdjsonStream<ExecEvent>(response.body) : emptyTransientEvents();
  }

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

  createPtyTicket(): Promise<{ ticket: string; expiresAt: number }> {
    return this.json<{ ticket: string; expiresAt: number }>(`${this.volumeBase}/exec/pty-ticket`, { method: 'POST' });
  }

  listServices(): Promise<ServiceRecord[]> {
    return this.json<ServiceRecord[]>(`${this.volumeBase}/services`);
  }

  getService(name: string): Promise<ServiceRecord> {
    return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}`);
  }

  getServiceLogs(
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

  createService(input: CreateServiceInput): Promise<ServiceRecord> {
    return this.json<ServiceRecord>(`${this.volumeBase}/services`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  startService(name: string): Promise<ServiceRecord> {
    return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}/start`, { method: 'POST' });
  }

  stopService(name: string): Promise<ServiceRecord> {
    return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}/stop`, { method: 'POST' });
  }

  deleteService(name: string): Promise<ServiceRecord> {
    return this.json<ServiceRecord>(`${this.volumeBase}/services/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  beginUpload(path: string, size: number, checksum: string): Promise<UploadStatus> {
    return this.json<UploadStatus>(this.resourcePath('uploads', path), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ size, checksum }),
    });
  }

  uploadStatus(path: string): Promise<UploadStatus> {
    return this.json<UploadStatus>(this.resourcePath('uploads', path));
  }

  appendUpload(
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
      body: data.slice().buffer,
    });
  }

  completeUpload(path: string): Promise<UploadCompleteResult> {
    return this.json<UploadCompleteResult>(this.resourcePath('uploads', path), { method: 'PUT' });
  }

  async abortUpload(path: string): Promise<void> {
    await this.request(this.resourcePath('uploads', path), { method: 'DELETE' });
  }

  submitJob(command: string, cwd = '/', idempotencyKey: string = crypto.randomUUID(), signal?: AbortSignal): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify({ command, cwd }),
      signal,
    });
  }

  async listJobs(status?: JobStatus): Promise<Job[]> {
    const url = new URL(this.url(`${this.volumeBase}/jobs`));
    if (status) url.searchParams.set('status', status);
    return (await this.requestUrl(url)).json() as Promise<Job[]>;
  }

  getJob(id: string, signal?: AbortSignal): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs/${encodeURIComponent(id)}`, { signal });
  }

  async getJobLogs(id: string, after?: number, limit?: number, signal?: AbortSignal): Promise<JobLogPage> {
    const url = new URL(this.url(`${this.volumeBase}/jobs/${encodeURIComponent(id)}/logs`));
    if (after !== undefined) url.searchParams.set('after', String(after));
    if (limit !== undefined) url.searchParams.set('limit', String(limit));
    return (await this.requestUrl(url, { signal })).json() as Promise<JobLogPage>;
  }

  cancelJob(id: string, signal?: AbortSignal): Promise<Job> {
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
              type: 'exit',
              id,
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

  async getChanges(options: ChangeQuery = {}): Promise<ChangePage> {
    const url = this.resourceUrl('changes', options.path ?? '/');
    if (options.since !== undefined) url.searchParams.set('since', String(options.since));
    if (options.limit !== undefined) url.searchParams.set('limit', String(options.limit));
    if (options.wait !== undefined) url.searchParams.set('wait', String(options.wait));
    return (await this.requestUrl(url, { signal: options.signal })).json() as Promise<ChangePage>;
  }

  createSnapshot(name?: string, note?: string): Promise<SnapshotInfo> {
    const body: Record<string, string> = {};
    if (name !== undefined) body.name = name;
    if (note !== undefined) body.note = note;
    return this.json<SnapshotInfo>(`${this.volumeBase}/snapshots`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  listSnapshots(): Promise<SnapshotInfo[]> {
    return this.json<SnapshotInfo[]>(`${this.volumeBase}/snapshots`);
  }

  async diffSnapshot(id: string, against = 'live'): Promise<SnapshotDiffEntry[]> {
    const url = new URL(this.url(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}/diff`));
    url.searchParams.set('against', against);
    return (await this.requestUrl(url)).json() as Promise<SnapshotDiffEntry[]>;
  }

  restoreSnapshot(id: string): Promise<SnapshotInfo> {
    return this.json<SnapshotInfo>(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}/restore`, { method: 'POST' });
  }

  cloneSnapshot(id: string, targetVolume: string): Promise<TreeSummary> {
    return this.json<TreeSummary>(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}/clone`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetVolume }),
    });
  }

  forkVolume(targetVolume: string): Promise<TreeSummary> {
    return this.json<TreeSummary>(`${this.volumeBase}/forks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetVolume }),
    });
  }

  sql(statement: string, args: SqlValue[] = []): Promise<SqlResult> {
    return this.json<SqlResult>(`${this.volumeBase}/sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: statement, args }),
    });
  }

  deleteSnapshot(id: string): Promise<SnapshotInfo> {
    return this.json<SnapshotInfo>(`${this.volumeBase}/snapshots/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  authStatus(): Promise<AuthStatus> {
    return this.json<AuthStatus>(`${this.volumeBase}/capabilities`);
  }

  createCapability(input: MintCapabilityInput): Promise<MintedCapability> {
    return this.json<MintedCapability>(`${this.volumeBase}/capabilities`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  async revokeCapability(id: string): Promise<void> {
    await this.request(`${this.volumeBase}/capabilities/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  usage(): Promise<UsageInfo> {
    return this.json<UsageInfo>(`${this.volumeBase}/usage`);
  }

  usageHistory(options: { before?: number; limit?: number } = {}): Promise<UsageHistoryPage> {
    const query = new URLSearchParams();
    if (options.before !== undefined) query.set('before', String(options.before));
    if (options.limit !== undefined) query.set('limit', String(options.limit));
    const suffix = query.size > 0 ? `?${query}` : '';
    return this.json<UsageHistoryPage>(`${this.volumeBase}/usage-history${suffix}`);
  }

  async metrics(): Promise<string> {
    return (await this.request(`${this.volumeBase}/metrics`)).text();
  }

  perf(): Promise<PerfInfo> {
    return this.legacyJson<PerfInfo>('/perf');
  }

  databaseInfo(): Promise<DatabaseInfo> {
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

  private async operation<T = void>(name: string, body: Record<string, unknown>): Promise<T> {
    const response = await this.request(`${this.volumeBase}/operations/${name}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return response.status === 204 ? undefined as T : response.json() as Promise<T>;
  }

  private resourcePath(resource: string, path: string): string {
    const encoded = encodeRemotePath(path);
    return `${this.volumeBase}/${resource}${encoded ? `/${encoded}` : ''}`;
  }

  private resourceUrl(resource: string, path: string): URL {
    return new URL(this.url(this.resourcePath(resource, path)));
  }

  private async json<T>(path: string, init?: RequestInit): Promise<T> {
    return (await this.request(path, init)).json() as Promise<T>;
  }

  private legacyJson<T>(path: string, init?: RequestInit): Promise<T> {
    return this.legacy(path, init).then((response) => response.json() as Promise<T>);
  }

  private legacy(path: string, init?: RequestInit): Promise<Response> {
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
      response = await this.fetchImpl(url, this.withDefaults(init));
    } catch (error) {
      throw new AiryFSTransportError(url.origin, error instanceof Error ? error.message : String(error));
    }
    if (!response.ok) throw await responseError(response);
    return response;
  }

  private withDefaults(init?: RequestInit): RequestInit {
    const headers = new Headers(this.defaultHeaders);
    new Headers(init?.headers).forEach((value, key) => headers.set(key, value));
    if (this.token) headers.set('Authorization', `Bearer ${this.token}`);
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

function encodeBase64(data: Uint8Array): string {
  let binary = '';
  for (let offset = 0; offset < data.length; offset += 32_768) {
    binary += String.fromCharCode(...data.subarray(offset, offset + 32_768));
  }
  return btoa(binary);
}
