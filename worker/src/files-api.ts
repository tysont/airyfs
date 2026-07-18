// ABOUTME: Resource-oriented HTTP adapter for the AgentFS-backed volume.
// ABOUTME: Provides binary streaming, ranges, metadata, and POSIX error responses.

import { Buffer } from 'buffer';
import type { FileHandle, FileSystem, Stats } from 'agentfs-sdk/cloudflare';
import { sha256Path } from './checksum';
import type { SqlExec } from './schema';

const READ_CHUNK_SIZE = 256 * 1024;
const WRITE_CHUNK_SIZE = 256 * 1024;

export interface StatsDto {
  ino: number;
  mode: number;
  nlink: number;
  uid: number;
  gid: number;
  size: number;
  atime: number;
  mtime: number;
  ctime: number;
  type: 'file' | 'directory' | 'symlink' | 'other';
}

export interface V1Route {
  volume: string;
  resource: 'volume' | 'files' | 'directories' | 'trees' | 'tree' | 'operations' | 'exec' | 'usage' | 'quota' | 'trash' | 'services' | 'capabilities' | 'snapshots' | 'uploads' | 'browser-uploads' | 'assets' | 'jobs' | 'schedules' | 'search' | 'changes' | 'webhooks' | 'auth' | 'sites' | 'shares';
  path: string;
}

interface ErrnoLike extends Error {
  code?: string;
  path?: string;
}

export class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly headers?: HeadersInit
  ) {
    super(message);
  }
}

type LockWaiter = {
  mode: 'read' | 'write';
  paths: string[];
  resolve: (release: () => void) => void;
};

/** Fair path lock; '*' is used for FUSE SQL mutations that cannot identify paths. */
export class VolumeAccessCoordinator {
  private readonly active: LockWaiter[] = [];
  private readonly waiters: LockWaiter[] = [];

  acquireRead(paths: string | string[] = '*'): Promise<() => void> {
    return this.acquire('read', paths);
  }

  acquireWrite(paths: string | string[] = '*'): Promise<() => void> {
    return this.acquire('write', paths);
  }

  private acquire(mode: LockWaiter['mode'], paths: string | string[]): Promise<() => void> {
    return new Promise((resolve) => {
      const requestedPaths = Array.isArray(paths) ? paths : [paths];
      this.waiters.push({ mode, paths: requestedPaths.map(this.normalizeLockPath), resolve });
      this.drain();
    });
  }

  private normalizeLockPath(path: string): string {
    if (path === '*') return path;
    const normalized = `/${path.split('/').filter(Boolean).join('/')}`;
    return normalized === '' ? '/' : normalized;
  }

  private drain(): void {
    for (let index = 0; index < this.waiters.length; index++) {
      const waiter = this.waiters[index];
      const conflictsWithActive = this.active.some((active) => this.conflicts(waiter, active));
      const conflictsWithEarlierWaiter = this.waiters
        .slice(0, index)
        .some((earlier) => this.conflicts(waiter, earlier));
      if (conflictsWithActive || conflictsWithEarlierWaiter) continue;

      this.waiters.splice(index, 1);
      this.active.push(waiter);
      waiter.resolve(this.releaseOnce(() => {
        const activeIndex = this.active.indexOf(waiter);
        if (activeIndex >= 0) this.active.splice(activeIndex, 1);
        this.drain();
      }));
      index = -1;
    }
  }

  private conflicts(a: LockWaiter, b: LockWaiter): boolean {
    if (a.mode === 'read' && b.mode === 'read') return false;
    return a.paths.some((aPath) => b.paths.some((bPath) => this.pathsOverlap(aPath, bPath)));
  }

  private pathsOverlap(a: string, b: string): boolean {
    if (a === '*' || b === '*') return true;
    const normalize = (path: string) => path === '/' ? '/' : path.replace(/\/+$/, '');
    const left = normalize(a);
    const right = normalize(b);
    return left === right || left === '/' || right === '/'
      || left.startsWith(`${right}/`) || right.startsWith(`${left}/`);
  }

  private releaseOnce(release: () => void): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      release();
    };
  }
}

export function parseV1Route(pathname: string): V1Route | null {
  const parts = pathname.split('/');
  if (parts[1] !== 'v1') return null;
  if (parts[2] !== 'volumes' || !parts[3]) {
    throw new HttpError(404, 'INVALID_ROUTE', 'Invalid v1 volume route');
  }

  if (!parts[4]) {
    if (parts.length > 5 || (parts.length === 5 && parts[4] !== '')) {
      throw new HttpError(404, 'INVALID_ROUTE', 'Invalid v1 volume route');
    }
    return { volume: decodeURIComponent(parts[3]), resource: 'volume', path: '/' };
  }

  const resource = parts[4];
  if (!['files', 'directories', 'trees', 'tree', 'operations', 'exec', 'usage', 'quota', 'trash', 'services', 'capabilities', 'snapshots', 'uploads', 'browser-uploads', 'assets', 'jobs', 'schedules', 'search', 'changes', 'webhooks', 'auth', 'sites', 'shares'].includes(resource)) {
    throw new HttpError(404, 'INVALID_ROUTE', `Unknown volume resource: ${resource}`);
  }

  if (resource === 'usage' && parts.length > 5) {
    throw new HttpError(404, 'INVALID_ROUTE', `${resource} does not accept a path suffix`);
  }
  // exec accepts no suffix (run), cancellation, PTY tickets, or a PTY upgrade.
  if (resource === 'exec' && parts.length > 5 && !(parts.length === 6 && ['cancel', 'pty', 'pty-ticket'].includes(parts[5]))) {
    throw new HttpError(404, 'INVALID_ROUTE', 'Invalid exec route suffix');
  }

  return {
    volume: decodeURIComponent(parts[3]),
    resource: resource as V1Route['resource'],
    path: parts.length > 5
      ? `/${parts.slice(5).map(decodeURIComponent).join('/')}`
      : '/',
  };
}

export function toStatsDto(stats: Stats): StatsDto {
  let type: StatsDto['type'] = 'other';
  if (stats.isFile()) type = 'file';
  else if (stats.isDirectory()) type = 'directory';
  else if (stats.isSymbolicLink()) type = 'symlink';

  return {
    ino: stats.ino,
    mode: stats.mode,
    nlink: stats.nlink,
    uid: stats.uid,
    gid: stats.gid,
    size: stats.size,
    atime: stats.atime,
    mtime: stats.mtime,
    ctime: stats.ctime,
    type,
  };
}

export function errorResponse(error: unknown): Response {
  if (error instanceof HttpError) {
    return Response.json(
      { error: { code: error.code, message: error.message } },
      { status: error.status, headers: error.headers }
    );
  }

  const errno = error instanceof Error ? error as ErrnoLike : null;
  const quotaExceeded = errno?.message.includes('AIRYFS_ENOSPC_') ?? false;
  const code = quotaExceeded ? 'ENOSPC' : errno?.code ?? 'INTERNAL_ERROR';
  const statusByCode: Record<string, number> = {
    ENOENT: 404,
    EEXIST: 409,
    ENOTEMPTY: 409,
    EISDIR: 409,
    ENOTDIR: 409,
    EINVAL: 400,
    EPERM: 403,
    ENOSPC: 507,
  };

  return Response.json(
    {
      error: {
        code,
        message: error instanceof Error ? error.message : String(error),
        ...(errno?.path ? { path: errno.path } : {}),
      },
    },
    { status: statusByCode[code] ?? 500 }
  );
}

function parseRange(value: string | null, size: number): { start: number; end: number } | null {
  if (!value) return null;
  if (!value.startsWith('bytes=') || value.includes(',')) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(value.trim());
  if (!match || (!match[1] && !match[2])) return null;
  if (size === 0) {
    throw new HttpError(416, 'INVALID_RANGE', 'Range is not satisfiable', {
      'Content-Range': `bytes */${size}`,
    });
  }

  let start: number;
  let end: number;
  if (!match[1]) {
    const suffixLength = Number(match[2]);
    if (!Number.isSafeInteger(suffixLength) || suffixLength <= 0) {
      throw new HttpError(416, 'INVALID_RANGE', 'Range is not satisfiable', {
        'Content-Range': `bytes */${size}`,
      });
    }
    start = Math.max(0, size - suffixLength);
    end = size - 1;
  } else {
    start = Number(match[1]);
    end = match[2] ? Number(match[2]) : size - 1;
  }

  if (
    !Number.isSafeInteger(start) || !Number.isSafeInteger(end) ||
    start < 0 || start >= size || end < start
  ) {
    throw new HttpError(416, 'INVALID_RANGE', 'Range is not satisfiable', {
      'Content-Range': `bytes */${size}`,
    });
  }

  return { start, end: Math.min(end, size - 1) };
}

function createReadStream(
  handle: FileHandle,
  start: number,
  end: number,
  release: () => void
): ReadableStream<Uint8Array> {
  let offset = start;
  return new ReadableStream({
    type: 'bytes',
    async pull(controller) {
      if (offset > end) {
        controller.close();
        release();
        return;
      }

      try {
        const chunk = await handle.pread(offset, Math.min(READ_CHUNK_SIZE, end - offset + 1));
        if (chunk.byteLength === 0) {
          controller.error(new Error('File changed while it was being streamed'));
          release();
          return;
        }
        offset += chunk.byteLength;
        controller.enqueue(new Uint8Array(chunk));
        if (offset > end) {
          controller.close();
          release();
        }
      } catch (error) {
        controller.error(error);
        release();
      }
    },
    cancel() {
      release();
    },
  });
}

export function latestFileVersion(sql: SqlExec, ino: number): number {
  const rows = sql.exec('SELECT max(seq) AS version FROM fs_change_feed WHERE ino = ?', ino).toArray();
  return rows[0]?.version === null || rows[0]?.version === undefined ? 0 : Number(rows[0].version);
}

function entityTag(stats: Stats, version: number): string {
  return `"${stats.ino.toString(16)}-${version.toString(16)}-${stats.size.toString(16)}"`;
}

function weakTagValue(value: string): string {
  return value.trim().replace(/^W\//, '');
}

function isNotModified(request: Request, etag: string, mtime: number): boolean {
  const ifNoneMatch = request.headers.get('If-None-Match');
  if (ifNoneMatch !== null) {
    return ifNoneMatch.trim() === '*'
      || ifNoneMatch.split(',').some((candidate) => weakTagValue(candidate) === etag);
  }

  const ifModifiedSince = request.headers.get('If-Modified-Since');
  if (!ifModifiedSince) return false;
  const since = Date.parse(ifModifiedSince);
  return !Number.isNaN(since) && Math.floor(mtime) * 1000 <= since;
}

function canServeRange(request: Request, etag: string, mtime: number): boolean {
  const ifRange = request.headers.get('If-Range');
  if (!ifRange) return true;
  if (ifRange.startsWith('W/')) return false;
  if (ifRange.startsWith('"')) return ifRange === etag;
  const since = Date.parse(ifRange);
  return !Number.isNaN(since) && Math.floor(mtime) * 1000 <= since;
}

export async function fileResponse(
  fs: FileSystem,
  path: string,
  request: Request,
  access?: VolumeAccessCoordinator,
  versionForInode: (ino: number) => number = () => 0
): Promise<Response> {
  const release = access ? await access.acquireRead(path) : () => undefined;
  try {
    const stats = await fs.stat(path);
    if (!stats.isFile()) {
      if (stats.isSymbolicLink()) {
        throw new HttpError(409, 'SYMLINK_NOT_RESOLVED', 'Use the readlink operation for symbolic links');
      }
      const error = new Error(`EISDIR: illegal operation on a directory, open '${path}'`) as ErrnoLike;
      error.code = 'EISDIR';
      error.path = path;
      throw error;
    }

    const etag = entityTag(stats, versionForInode(stats.ino));
    const lastModified = new Date(stats.mtime * 1000).toUTCString();
    const baseHeaders = new Headers({
      'Accept-Ranges': 'bytes',
      'Content-Type': 'application/octet-stream',
      'ETag': etag,
      'Last-Modified': lastModified,
      'X-AiryFS-Inode': String(stats.ino),
    });
    if (isNotModified(request, etag, stats.mtime)) {
      release();
      return new Response(null, { status: 304, headers: baseHeaders });
    }

    const range = request.method === 'GET' && canServeRange(request, etag, stats.mtime)
      ? parseRange(request.headers.get('Range'), stats.size)
      : null;
    const start = range?.start ?? 0;
    const end = range?.end ?? Math.max(0, stats.size - 1);
    const length = stats.size === 0 ? 0 : end - start + 1;
    const headers = baseHeaders;
    headers.set('Content-Length', String(length));
    if (range) headers.set('Content-Range', `bytes ${start}-${end}/${stats.size}`);

    if (request.method === 'HEAD' || stats.size === 0) {
      release();
      return new Response(null, { status: range ? 206 : 200, headers });
    }

    return new Response(createReadStream(await fs.open(path), start, end, release), {
      status: range ? 206 : 200,
      headers,
    });
  } catch (error) {
    release();
    throw error;
  }
}

function temporaryPath(path: string): string {
  if (path === '/' || path.endsWith('/')) {
    throw new HttpError(400, 'INVALID_PATH', 'A file path without a trailing slash is required');
  }
  const separator = path.lastIndexOf('/');
  const parent = separator <= 0 ? '' : path.slice(0, separator);
  return `${parent}/.airyfs-upload-${crypto.randomUUID()}`;
}

function parentPath(path: string): string {
  const separator = path.lastIndexOf('/');
  return separator <= 0 ? '/' : path.slice(0, separator);
}

export async function writeFileStream(
  fs: FileSystem,
  path: string,
  stream: ReadableStream<Uint8Array> | null,
  access?: VolumeAccessCoordinator
): Promise<void> {
  let temp: string | null = null;

  try {
    temp = temporaryPath(path);
    const parent = parentPath(path);
    const releaseParent = access ? await access.acquireWrite(temp) : () => undefined;
    try {
      const parentStats = await fs.stat(parent);
      if (!parentStats.isDirectory()) {
        const error = new Error(`ENOTDIR: not a directory, open '${parent}'`) as ErrnoLike;
        error.code = 'ENOTDIR';
        error.path = parent;
        throw error;
      }
      await fs.writeFile(temp, Buffer.alloc(0));
    } finally {
      releaseParent();
    }
    if (stream) {
      const handle = await fs.open(temp);
      const reader = stream.getReader();
      let offset = 0;
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value.byteLength === 0) continue;
          for (let chunkOffset = 0; chunkOffset < value.byteLength; chunkOffset += WRITE_CHUNK_SIZE) {
            const chunk = value.subarray(chunkOffset, chunkOffset + WRITE_CHUNK_SIZE);
            await handle.pwrite(offset, Buffer.from(chunk));
            offset += chunk.byteLength;
          }
        }
        await handle.fsync();
      } catch (error) {
        await reader.cancel(error).catch(() => undefined);
        throw error;
      } finally {
        reader.releaseLock();
      }
    }
    const release = access ? await access.acquireWrite(path) : () => undefined;
    try {
      await fs.rename(temp, path);
    } finally {
      release();
    }
  } catch (error) {
    if (temp) await fs.rm(temp, { force: true }).catch(() => undefined);
    throw error;
  }
}

async function readJson<T>(request: Request): Promise<T> {
  try {
    return await request.json<T>();
  } catch {
    throw new HttpError(400, 'INVALID_JSON', 'Request body must be valid JSON');
  }
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `Missing "${name}" string`);
  }
  return value;
}

function requireNonNegativeInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `Expected non-negative integer "${name}"`);
  }
  return value;
}

export async function readCommandRequest(request: Request): Promise<string> {
  const body = await readJson<Record<string, unknown>>(request);
  return requireString(body.command, 'command');
}

/** Parse a JSON object request body, rejecting malformed JSON with a stable error. */
export async function readJsonObject(request: Request): Promise<Record<string, unknown>> {
  return readJson<Record<string, unknown>>(request);
}

export async function readVolumeCreateRequest(request: Request): Promise<unknown> {
  if (!request.body) return undefined;
  return (await readJson<Record<string, unknown>>(request)).chunkSize;
}

async function withWrite<T>(
  access: VolumeAccessCoordinator | undefined,
  paths: string | string[],
  operation: () => Promise<T>
): Promise<T> {
  const release = access ? await access.acquireWrite(paths) : () => undefined;
  try {
    return await operation();
  } finally {
    release();
  }
}

async function withRead<T>(
  access: VolumeAccessCoordinator | undefined,
  path: string,
  operation: () => Promise<T>
): Promise<T> {
  const release = access ? await access.acquireRead(path) : () => undefined;
  try {
    return await operation();
  } finally {
    release();
  }
}

export async function handleFilesystemRequest(
  request: Request,
  route: V1Route,
  fs: FileSystem,
  access?: VolumeAccessCoordinator,
  onMutation?: (paths: string[]) => Promise<void>,
  versionForInode?: (ino: number) => number
): Promise<Response | null> {
  try {
    if (route.resource === 'files') {
      if (request.method === 'GET' || request.method === 'HEAD') {
        return await fileResponse(fs, route.path, request, access, versionForInode);
      }
      if (request.method === 'PUT') {
        await writeFileStream(fs, route.path, request.body, access);
        await onMutation?.([route.path]);
        return new Response(null, { status: 204 });
      }
      if (request.method === 'DELETE') {
        await withWrite(access, route.path, () => fs.unlink(route.path));
        await onMutation?.([route.path]);
        return new Response(null, { status: 204 });
      }
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, HEAD, PUT, DELETE' });
    }

    if (route.resource === 'browser-uploads') {
      if (request.method !== 'POST') {
        throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST, OPTIONS' });
      }
      await writeFileStream(fs, route.path, request.body, access);
      await onMutation?.([route.path]);
      return Response.json({ path: route.path, ...toStatsDto(await fs.stat(route.path)) }, { status: 201 });
    }

    if (route.resource === 'directories') {
      if (request.method === 'GET') {
        const entries = await withRead(access, route.path, () => fs.readdirPlus(route.path));
        return Response.json(entries.filter((entry) => route.path !== '/' || entry.name !== '.airyfs-trash').map((entry) => ({
          name: entry.name,
          ...toStatsDto(entry.stats),
        })));
      }
      if (request.method === 'PUT') {
        await withWrite(access, route.path, () => fs.mkdir(route.path));
        await onMutation?.([route.path]);
        return new Response(null, { status: 204 });
      }
      if (request.method === 'DELETE') {
        const recursive = new URL(request.url).searchParams.get('recursive') === 'true';
        await withWrite(access, route.path, () => fs.rm(route.path, { recursive }));
        await onMutation?.([route.path]);
        return new Response(null, { status: 204 });
      }
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, PUT, DELETE' });
    }

    if (route.resource === 'operations' && request.method === 'POST') {
      const operation = route.path.slice(1);
      const body = await readJson<Record<string, unknown>>(request);
      if (operation === 'rename') {
        const from = requireString(body.from, 'from');
        const to = requireString(body.to, 'to');
        await withWrite(access, [from, to], () => fs.rename(from, to));
        await onMutation?.([from, to]);
        return new Response(null, { status: 204 });
      }
      if (operation === 'copy') {
        const from = requireString(body.from, 'from');
        const to = requireString(body.to, 'to');
        await withWrite(access, [from, to], () => fs.copyFile(from, to));
        await onMutation?.([to]);
        return new Response(null, { status: 204 });
      }
      if (operation === 'symlink') {
        const target = requireString(body.target, 'target');
        const path = requireString(body.path, 'path');
        await withWrite(access, path, () => fs.symlink(target, path));
        await onMutation?.([path]);
        return new Response(null, { status: 204 });
      }
      if (operation === 'readlink') {
        return Response.json({ target: await fs.readlink(requireString(body.path, 'path')) });
      }
      if (operation === 'checksum') {
        return Response.json(await sha256Path(fs, requireString(body.path, 'path'), access));
      }
      if (operation === 'truncate') {
        const path = requireString(body.path, 'path');
        const size = requireNonNegativeInteger(body.size, 'size');
        await withWrite(access, path, async () => {
          const handle = await fs.open(path);
          await handle.truncate(size);
          await handle.fsync();
        });
        await onMutation?.([path]);
        return new Response(null, { status: 204 });
      }
      throw new HttpError(404, 'UNKNOWN_OPERATION', `Unknown filesystem operation: ${operation}`);
    }

    if (route.resource === 'operations') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'POST' });
    }

    return null;
  } catch (error) {
    return errorResponse(error);
  }
}
