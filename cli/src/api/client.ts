// ABOUTME: Provides a typed, streaming client for the AiryFS Worker HTTP API.
// ABOUTME: Uses v1 filesystem routes and legacy diagnostic routes behind one interface.

import { AiryFSTransportError, responseError } from './errors.js';
import { encodeRemotePath } from './paths.js';
import type {
  DatabaseInfo,
  DirectoryEntry,
  ExecResult,
  PerfInfo,
  UsageInfo,
  VolumeInfo,
} from './types.js';

type Fetch = typeof fetch;

export class AiryFSClient {
  private readonly volumeBase: string;

  constructor(
    readonly endpoint: string,
    readonly volume: string,
    private readonly fetchImpl: Fetch = fetch,
  ) {
    this.volumeBase = `/v1/volumes/${encodeURIComponent(volume)}`;
  }

  async getVolume(): Promise<VolumeInfo> {
    return this.json<VolumeInfo>(this.volumeBase);
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

  async deleteFile(path: string): Promise<void> {
    await this.request(this.resourcePath('files', path), { method: 'DELETE' });
  }

  async makeDirectory(path: string): Promise<void> {
    await this.request(this.resourcePath('directories', path), { method: 'PUT' });
  }

  async removeDirectory(path: string, recursive = false): Promise<void> {
    const url = new URL(this.url(this.resourcePath('directories', path)));
    if (recursive) url.searchParams.set('recursive', 'true');
    await this.requestUrl(url, { method: 'DELETE' });
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

  async exec(command: string, signal?: AbortSignal): Promise<ExecResult> {
    return this.json<ExecResult>(`${this.volumeBase}/exec`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command }),
      signal,
    });
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

  private async operation<T = void>(name: string, body: Record<string, unknown>): Promise<T> {
    const response = await this.request(`${this.volumeBase}/operations/${name}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  }

  private resourcePath(resource: 'files' | 'directories', path: string): string {
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
      response = await this.fetchImpl(url, init);
    } catch (error) {
      throw new AiryFSTransportError(url.origin, error instanceof Error ? error.message : String(error));
    }
    if (!response.ok) throw await responseError(response);
    return response;
  }

  private url(path: string): string {
    return `${this.endpoint.replace(/\/$/, '')}${path}`;
  }
}
