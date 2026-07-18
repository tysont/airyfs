// ABOUTME: Public web hosting for AiryFS volumes: static site + file-share serving from DO SQLite.
// ABOUTME: Adds MIME inference, index-document and SPA fallback, and per-volume publish/share records.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { normalizePath } from './auth';
import { fileResponse, HttpError, VolumeAccessCoordinator } from './files-api';
import type { SqlExec } from './schema';

// ---------------------------------------------------------------------------
// MIME inference
// ---------------------------------------------------------------------------

const MIME_TYPES: Record<string, string> = {
  html: 'text/html; charset=utf-8',
  htm: 'text/html; charset=utf-8',
  css: 'text/css; charset=utf-8',
  js: 'text/javascript; charset=utf-8',
  mjs: 'text/javascript; charset=utf-8',
  json: 'application/json; charset=utf-8',
  map: 'application/json; charset=utf-8',
  txt: 'text/plain; charset=utf-8',
  md: 'text/markdown; charset=utf-8',
  xml: 'application/xml; charset=utf-8',
  svg: 'image/svg+xml',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
  avif: 'image/avif',
  ico: 'image/x-icon',
  woff: 'font/woff',
  woff2: 'font/woff2',
  ttf: 'font/ttf',
  otf: 'font/otf',
  pdf: 'application/pdf',
  wasm: 'application/wasm',
  zip: 'application/zip',
  webmanifest: 'application/manifest+json',
};

/** Extract the volume for a public hosting request: `/s/<volume>/...` or `/d/<volume>/...`. */
export function parsePublicVolume(pathname: string): string | null {
  const segments = pathname.split('/').filter(Boolean);
  if ((segments[0] === 's' || segments[0] === 'd') && segments[1]) {
    return decodeURIComponent(segments[1]);
  }
  return null;
}

/** Resolve a `<volume>.<zone>` host into its volume label, or null when it does not match. */
export function subdomainVolume(host: string | null, zone: string | undefined): string | null {
  if (!zone || !host) return null;
  const bare = host.split(':')[0].toLowerCase();
  const suffix = `.${zone.toLowerCase()}`;
  if (bare === zone.toLowerCase() || !bare.endsWith(suffix)) return null;
  const label = bare.slice(0, bare.length - suffix.length);
  return label && !label.includes('.') ? label : null;
}

/** Infer a Content-Type from a path extension, defaulting to octet-stream. */
export function contentTypeFor(path: string): string {
  const dot = path.lastIndexOf('.');
  const slash = path.lastIndexOf('/');
  if (dot <= slash) return 'application/octet-stream';
  const ext = path.slice(dot + 1).toLowerCase();
  return MIME_TYPES[ext] ?? 'application/octet-stream';
}

// ---------------------------------------------------------------------------
// Records and persistence (per-volume, in the volume's own SQLite)
// ---------------------------------------------------------------------------

export interface SiteRecord {
  /** Volume subtree that becomes the web root. */
  pathPrefix: string;
  /** Document served for directory requests (default index.html). */
  indexDocument: string;
  /** Serve indexDocument with 200 for unmatched paths (single-page apps). */
  spa: boolean;
  /** Optional Cache-Control header value applied to served files. */
  cacheControl: string | null;
  createdAt: number;
}

export interface ShareRecord {
  id: string;
  /** File path within the volume served by the share. */
  path: string;
  /** Absolute expiry in Unix seconds, or null for no expiry. */
  expiresAt: number | null;
  cacheControl: string | null;
  createdAt: number;
}

const SITE_ID = 'web';

export function readSite(sql: SqlExec): SiteRecord | null {
  const rows = sql
    .exec("SELECT path, index_document, spa, cache_control, created_at FROM site_config WHERE kind = 'site' AND id = ?", SITE_ID)
    .toArray();
  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    pathPrefix: String(row.path),
    indexDocument: String(row.index_document),
    spa: Number(row.spa) === 1,
    cacheControl: row.cache_control === null ? null : String(row.cache_control),
    createdAt: Number(row.created_at),
  };
}

export function writeSite(sql: SqlExec, record: Omit<SiteRecord, 'createdAt'>): SiteRecord {
  sql.exec(
    `INSERT INTO site_config (id, kind, path, index_document, spa, cache_control, expires_at, created_at)
       VALUES (?, 'site', ?, ?, ?, ?, NULL, unixepoch())
     ON CONFLICT(id) DO UPDATE SET
       path = excluded.path,
       index_document = excluded.index_document,
       spa = excluded.spa,
       cache_control = excluded.cache_control`,
    SITE_ID,
    normalizePath(record.pathPrefix),
    record.indexDocument,
    record.spa ? 1 : 0,
    record.cacheControl
  );
  return readSite(sql)!;
}

export function deleteSite(sql: SqlExec): boolean {
  const existed = readSite(sql) !== null;
  sql.exec("DELETE FROM site_config WHERE kind = 'site' AND id = ?", SITE_ID);
  return existed;
}

export function createShare(
  sql: SqlExec,
  path: string,
  expiresAt: number | null,
  cacheControl: string | null
): ShareRecord {
  const id = crypto.randomUUID().replace(/-/g, '');
  sql.exec(
    `INSERT INTO site_config (id, kind, path, index_document, spa, cache_control, expires_at, created_at)
       VALUES (?, 'share', ?, '', 0, ?, ?, unixepoch())`,
    id,
    normalizePath(path),
    cacheControl,
    expiresAt
  );
  return readShare(sql, id)!;
}

export function readShare(sql: SqlExec, id: string): ShareRecord | null {
  const rows = sql
    .exec("SELECT id, path, cache_control, expires_at, created_at FROM site_config WHERE kind = 'share' AND id = ?", id)
    .toArray();
  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    id: String(row.id),
    path: String(row.path),
    expiresAt: row.expires_at === null ? null : Number(row.expires_at),
    cacheControl: row.cache_control === null ? null : String(row.cache_control),
    createdAt: Number(row.created_at),
  };
}

export function listShares(sql: SqlExec): ShareRecord[] {
  return sql
    .exec("SELECT id, path, cache_control, expires_at, created_at FROM site_config WHERE kind = 'share' ORDER BY created_at DESC")
    .toArray()
    .map((row) => ({
      id: String(row.id),
      path: String(row.path),
      expiresAt: row.expires_at === null ? null : Number(row.expires_at),
      cacheControl: row.cache_control === null ? null : String(row.cache_control),
      createdAt: Number(row.created_at),
    }));
}

export function deleteShare(sql: SqlExec, id: string): boolean {
  const existed = readShare(sql, id) !== null;
  sql.exec("DELETE FROM site_config WHERE kind = 'share' AND id = ?", id);
  return existed;
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/** Join a normalized root and a request-relative subpath, preventing traversal above root. */
function resolveUnder(root: string, subPath: string): string {
  return normalizePath(`${root}/${subPath}`);
}

async function isFile(fs: FileSystem, path: string): Promise<boolean> {
  try {
    return (await fs.stat(path)).isFile();
  } catch {
    return false;
  }
}

/** Re-wrap a fileResponse with a public Content-Type and optional Cache-Control. */
function withPublicHeaders(response: Response, contentType: string, cacheControl: string | null): Response {
  const headers = new Headers(response.headers);
  headers.set('Content-Type', contentType);
  if (cacheControl) headers.set('Cache-Control', cacheControl);
  return new Response(response.body, { status: response.status, headers });
}

/**
 * Serve a request against a published site. Directory or empty paths resolve to
 * the index document; unmatched paths fall back to the index document when SPA
 * mode is enabled, otherwise return 404.
 */
export async function serveSite(
  fs: FileSystem,
  access: VolumeAccessCoordinator,
  site: SiteRecord,
  subPath: string,
  request: Request
): Promise<Response> {
  const trimmed = subPath.replace(/^\/+/, '');
  const wantsIndex = trimmed === '' || subPath.endsWith('/');
  const primary = wantsIndex
    ? resolveUnder(site.pathPrefix, `${trimmed}/${site.indexDocument}`)
    : resolveUnder(site.pathPrefix, trimmed);

  let target: string | null = null;
  if (await isFile(fs, primary)) {
    target = primary;
  } else if (!wantsIndex) {
    // A path without a trailing slash may still name a directory with an index.
    const asDirIndex = resolveUnder(site.pathPrefix, `${trimmed}/${site.indexDocument}`);
    if (await isFile(fs, asDirIndex)) target = asDirIndex;
  }

  if (!target && site.spa) {
    const fallback = resolveUnder(site.pathPrefix, site.indexDocument);
    if (await isFile(fs, fallback)) {
      const response = await fileResponse(fs, fallback, request, access);
      return withPublicHeaders(response, contentTypeFor(fallback), site.cacheControl);
    }
  }

  if (!target) {
    throw new HttpError(404, 'NOT_FOUND', 'No published file for this path');
  }
  const response = await fileResponse(fs, target, request, access);
  return withPublicHeaders(response, contentTypeFor(target), site.cacheControl);
}

/** Serve a share by id, enforcing expiry, then streaming its single file. */
export async function serveShare(
  fs: FileSystem,
  access: VolumeAccessCoordinator,
  share: ShareRecord,
  request: Request
): Promise<Response> {
  if (share.expiresAt !== null && share.expiresAt <= Math.floor(Date.now() / 1000)) {
    throw new HttpError(410, 'SHARE_EXPIRED', 'This share link has expired');
  }
  if (!(await isFile(fs, share.path))) {
    throw new HttpError(404, 'NOT_FOUND', 'The shared file no longer exists');
  }
  const response = await fileResponse(fs, share.path, request, access);
  return withPublicHeaders(response, contentTypeFor(share.path), share.cacheControl);
}
