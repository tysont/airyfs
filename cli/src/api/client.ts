// ABOUTME: Provides a typed, streaming client for the AiryFS Worker HTTP API.
// ABOUTME: Uses v1 filesystem routes and legacy diagnostic routes behind one interface.

import { AiryFSTransportError, responseError } from './errors.js';
import { decodeNdjsonStream } from './ndjson.js';
import { encodeRemotePath } from './paths.js';
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
  ExecEvent,
  ExecResult,
  Job,
  JobLogPage,
  JobSchedule,
  JobStatus,
  MintCapabilityInput,
  MintedCapability,
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
  UploadCompleteResult,
  UploadStatus,
  UsageInfo,
  VolumeInfo,
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

  async writeFile(path: string, body: NonNullable<RequestInit['body']>): Promise<void> {
    await this.request(this.resourcePath('files', path), {
      method: 'PUT',
      body,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
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

  async exec(command: string, signal?: AbortSignal): Promise<ExecResult> {
    return this.json<ExecResult>(`${this.volumeBase}/exec`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command }),
      signal,
    });
  }

  /**
   * Run a command and stream its NDJSON events. Awaiting this resolves once the
   * server accepts the request (connection or admission errors throw here); the
   * returned iterable then yields events until the terminal `exit`.
   */
  async execStream(command: string, signal?: AbortSignal): Promise<AsyncIterable<ExecEvent>> {
    const url = new URL(this.url(`${this.volumeBase}/exec`));
    url.searchParams.set('stream', 'true');
    const response = await this.requestUrl(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command }),
      signal,
    });
    if (!response.body) return emptyEvents();
    return decodeNdjsonStream<ExecEvent>(response.body);
  }

  /** Request cancellation of a streaming command by its start-event id. */
  async cancelExec(id: string): Promise<void> {
    await this.request(`${this.volumeBase}/exec/cancel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id }),
    });
  }

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
  ): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(cwd === undefined ? { command } : { command, cwd }),
    });
  }

  /** List jobs newest-first, optionally filtered by status. */
  async listJobs(status?: JobStatus): Promise<Job[]> {
    const url = new URL(this.url(`${this.volumeBase}/jobs`));
    if (status) url.searchParams.set('status', status);
    return (await this.requestUrl(url)).json() as Promise<Job[]>;
  }

  /** Fetch a single job by id. */
  async getJob(id: string): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs/${encodeURIComponent(id)}`);
  }

  /** Read a page of a job's persisted logs; `data` in each entry is base64. */
  async getJobLogs(id: string, after?: number, limit?: number): Promise<JobLogPage> {
    const url = new URL(this.url(`${this.volumeBase}/jobs/${encodeURIComponent(id)}/logs`));
    if (after !== undefined) url.searchParams.set('after', String(after));
    if (limit !== undefined) url.searchParams.set('limit', String(limit));
    return (await this.requestUrl(url)).json() as Promise<JobLogPage>;
  }

  /** Request cancellation of a job by id; idempotent for terminal jobs. */
  async cancelJob(id: string): Promise<Job> {
    return this.json<Job>(`${this.volumeBase}/jobs/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
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

  private resourcePath(resource: 'files' | 'directories' | 'trees' | 'tree' | 'uploads' | 'changes', path: string): string {
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

async function* emptyEvents(): AsyncGenerator<ExecEvent> {
  // A response with no body yields no events; the caller treats it as no output.
}
