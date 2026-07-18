// ABOUTME: Translates the WebDAV protocol into direct AgentFS operations for remote mounts.
// ABOUTME: Implements finite property discovery, streaming I/O, mutations, and Finder-compatible locks.

import type { FileSystem, Stats } from 'agentfs-sdk/cloudflare';
import { normalizePath } from './auth';
import { fileResponse, HttpError, latestFileVersion, VolumeAccessCoordinator, writeFileStream } from './files-api';
import type { SqlExec } from './schema';
import { contentTypeFor } from './sites';
import { moveToTrash, TRASH_ROOT } from './trash';

const METHODS = 'OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, MOVE, COPY, LOCK, UNLOCK';
const MAX_COPY_ENTRIES = 100_000;

interface DavOptions {
  fs: FileSystem;
  sql: SqlExec;
  access: VolumeAccessCoordinator;
  volume: string;
  path: string;
  request: Request;
  onMutation: (paths: string[]) => Promise<void>;
}

export async function handleWebDav(options: DavOptions): Promise<Response> {
  const { fs, sql, access, path, request } = options;
  if (path === TRASH_ROOT || path.startsWith(`${TRASH_ROOT}/`)) throw new HttpError(404, 'ENOENT', 'Path not found');
  switch (request.method) {
    case 'OPTIONS': return new Response(null, { headers: davHeaders() });
    case 'GET':
    case 'HEAD': return fileResponse(fs, path, request, access, (ino) => latestFileVersion(sql, ino));
    case 'PROPFIND': return propfind(options);
    case 'PROPPATCH': return propPatch(options);
    case 'PUT': {
      let existed = true;
      try { await fs.lstat(path); } catch { existed = false; }
      await writeFileStream(fs, path, request.body, access);
      await options.onMutation([path]);
      return new Response(null, { status: existed ? 204 : 201, headers: davHeaders() });
    }
    case 'MKCOL': {
      if (request.body) throw new HttpError(415, 'UNSUPPORTED_MEDIA_TYPE', 'MKCOL request bodies are unsupported');
      const release = await access.acquireWrite(path);
      try { await fs.mkdir(path); } finally { release(); }
      await options.onMutation([path]);
      return new Response(null, { status: 201, headers: davHeaders() });
    }
    case 'DELETE': {
      const entry = await moveToTrash(fs, sql, access, path);
      await options.onMutation([path, entry.trashPath]);
      return new Response(null, { status: 204, headers: davHeaders() });
    }
    case 'MOVE':
    case 'COPY': return moveOrCopy(options);
    case 'LOCK': return lockResponse(path, request);
    case 'UNLOCK': return new Response(null, { status: 204, headers: davHeaders() });
    default: throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'WebDAV method not allowed', { Allow: METHODS });
  }
}

async function propfind(options: DavOptions): Promise<Response> {
  const depth = options.request.headers.get('Depth') ?? 'infinity';
  if (depth !== '0' && depth !== '1') {
    return xmlResponse('<D:error xmlns:D="DAV:"><D:propfind-finite-depth/></D:error>', 403);
  }
  const target = await options.fs.lstat(options.path);
  const resources: Array<{ path: string; stats: Stats }> = [{ path: options.path, stats: target }];
  if (depth === '1' && target.isDirectory()) {
    const entries = await options.fs.readdirPlus(options.path);
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      if (options.path === '/' && entry.name === '.airyfs-trash') continue;
      resources.push({ path: options.path === '/' ? `/${entry.name}` : `${options.path}/${entry.name}`, stats: entry.stats });
    }
  }
  const responses = resources.map(({ path, stats }) => propertyResponse(options.volume, path, stats, latestFileVersion(options.sql, stats.ino))).join('');
  return xmlResponse(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">${responses}</D:multistatus>`, 207);
}

function propertyResponse(volume: string, path: string, stats: Stats, version: number): string {
  const directory = stats.isDirectory();
  const hrefPath = path.split('/').map(encodeURIComponent).join('/');
  const href = `/dav/${encodeURIComponent(volume)}${hrefPath}${directory && path !== '/' ? '/' : ''}`;
  const name = path === '/' ? volume : path.split('/').pop()!;
  const etag = `&quot;${stats.ino.toString(16)}-${version.toString(16)}-${stats.size.toString(16)}&quot;`;
  return `<D:response><D:href>${escapeXml(href)}</D:href><D:propstat><D:prop>`
    + `<D:displayname>${escapeXml(name)}</D:displayname>`
    + `<D:resourcetype>${directory ? '<D:collection/>' : ''}</D:resourcetype>`
    + `${directory ? '' : `<D:getcontentlength>${stats.size}</D:getcontentlength><D:getcontenttype>${escapeXml(contentTypeFor(path))}</D:getcontenttype>`}`
    + `<D:getlastmodified>${new Date(stats.mtime * 1000).toUTCString()}</D:getlastmodified>`
    + `<D:creationdate>${new Date(stats.ctime * 1000).toISOString()}</D:creationdate><D:getetag>${etag}</D:getetag>`
    + `</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`;
}

async function propPatch(options: DavOptions): Promise<Response> {
  const stats = await options.fs.lstat(options.path);
  const response = propertyResponse(options.volume, options.path, stats, latestFileVersion(options.sql, stats.ino));
  return xmlResponse(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">${response}</D:multistatus>`, 207);
}

async function moveOrCopy(options: DavOptions): Promise<Response> {
  const destination = parseWebDavDestination(options.request, options.volume);
  const sourceStats = await options.fs.lstat(options.path);
  if (destination === options.path || (sourceStats.isDirectory() && destination.startsWith(`${options.path}/`))) {
    throw new HttpError(403, 'INVALID_DESTINATION', 'Destination cannot be the source or its descendant');
  }
  let existed = true;
  try { await options.fs.lstat(destination); } catch { existed = false; }
  if (existed && (options.request.headers.get('Overwrite') ?? 'T').toUpperCase() === 'F') {
    throw new HttpError(412, 'PRECONDITION_FAILED', 'Destination exists');
  }
  const release = await options.access.acquireWrite([options.path, destination]);
  try {
    if (options.request.method === 'MOVE') await options.fs.rename(options.path, destination);
    else if (sourceStats.isDirectory()) {
      if (existed) await options.fs.rm(destination, { recursive: true });
      await copyCollection(options.fs, options.path, destination);
    } else await options.fs.copyFile(options.path, destination);
  } finally { release(); }
  await options.onMutation(options.request.method === 'MOVE' ? [options.path, destination] : [destination]);
  return new Response(null, { status: existed ? 204 : 201, headers: davHeaders() });
}

async function copyCollection(fs: FileSystem, source: string, destination: string): Promise<void> {
  let copied = 0;
  await fs.mkdir(destination);
  const walk = async (from: string, to: string): Promise<void> => {
    const entries = await fs.readdirPlus(from);
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      if (++copied > MAX_COPY_ENTRIES) throw new HttpError(413, 'COPY_TOO_LARGE', `Collection COPY exceeds ${MAX_COPY_ENTRIES} entries`);
      const childFrom = `${from === '/' ? '' : from}/${entry.name}`;
      const childTo = `${to === '/' ? '' : to}/${entry.name}`;
      if (entry.stats.isDirectory()) {
        await fs.mkdir(childTo);
        await walk(childFrom, childTo);
      } else if (entry.stats.isSymbolicLink()) {
        await fs.symlink(await fs.readlink(childFrom), childTo);
      } else {
        await fs.copyFile(childFrom, childTo);
      }
    }
  };
  await walk(source, destination);
}

export function parseWebDavDestination(request: Request, volume: string): string {
  const raw = request.headers.get('Destination');
  if (!raw) throw new HttpError(400, 'INVALID_DESTINATION', 'Destination header is required');
  let destination: URL;
  try { destination = new URL(raw, request.url); } catch { throw new HttpError(400, 'INVALID_DESTINATION', 'Invalid Destination URL'); }
  const source = new URL(request.url);
  if (destination.origin !== source.origin) throw new HttpError(403, 'CROSS_ORIGIN_DESTINATION', 'Destination must use the same origin');
  const segments = destination.pathname.split('/').filter(Boolean).map(decodeURIComponent);
  if (segments[0] !== 'dav' || segments[1] !== volume) throw new HttpError(403, 'CROSS_VOLUME_DESTINATION', 'Destination must use the same volume');
  const path = normalizePath(`/${segments.slice(2).join('/')}`);
  if (path === TRASH_ROOT || path.startsWith(`${TRASH_ROOT}/`)) throw new HttpError(403, 'INVALID_DESTINATION', 'Trash is reserved');
  return path;
}

function lockResponse(path: string, request: Request): Response {
  const token = `opaquelocktoken:${crypto.randomUUID()}`;
  const timeout = request.headers.get('Timeout') ?? 'Second-3600';
  const body = `<?xml version="1.0" encoding="utf-8"?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock>`
    + `<D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth>`
    + `<D:timeout>${escapeXml(timeout)}</D:timeout><D:locktoken><D:href>${token}</D:href></D:locktoken>`
    + `<D:lockroot><D:href>${escapeXml(path)}</D:href></D:lockroot></D:activelock></D:lockdiscovery></D:prop>`;
  const response = xmlResponse(body, 200);
  response.headers.set('Lock-Token', `<${token}>`);
  return response;
}

function davHeaders(): Headers {
  return new Headers({ DAV: '1, 2', Allow: METHODS, 'MS-Author-Via': 'DAV', 'Accept-Ranges': 'bytes' });
}

function xmlResponse(body: string, status: number): Response {
  const headers = davHeaders();
  headers.set('Content-Type', 'application/xml; charset=utf-8');
  return new Response(body, { status, headers });
}

function escapeXml(value: string): string {
  return value.replace(/[&<>"']/g, (character) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' })[character]!);
}
