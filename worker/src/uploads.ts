// ABOUTME: Resumable, checksummed large-file uploads addressed by their final target path.
// ABOUTME: Persists sessions in the additive fs_upload table; stages a hidden temp file in the target parent.

import { Buffer } from 'buffer';
import { createHash } from 'node:crypto';
import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { sha256Path } from './checksum';
import { HttpError, toStatsDto, type StatsDto, type VolumeAccessCoordinator } from './files-api';
import type { SqlExec } from './schema';

/** Maximum bytes accepted in a single PATCH chunk. */
export const MAX_UPLOAD_CHUNK_BYTES = 1024 * 1024;
/** Sessions untouched for this long are opportunistically reaped on the next begin. */
export const STALE_UPLOAD_SECONDS = 24 * 60 * 60;

/** Additive table names owned by this module, for schema verification/introspection. */
export const UPLOAD_TABLES = ['fs_upload'] as const;

interface ErrnoLike extends Error {
  code?: string;
  path?: string;
}

/** Public status of an upload session; excludes the hidden temp path. */
export interface UploadStatus {
  id: string;
  path: string;
  size: number;
  offset: number;
  checksum: string;
  createdAt: number;
  updatedAt: number;
}

/** Result of a begin: the session plus whether it was newly created (vs resumed). */
export interface UploadBeginResult {
  session: UploadStatus;
  created: boolean;
}

/** Metadata returned when an upload is published over its target. */
export interface UploadCompleteResult extends StatsDto {
  path: string;
  checksum: string;
}

export interface AppendChunk {
  offset: number;
  chunkSha256: string;
  data: Uint8Array;
}

interface UploadRow {
  id: string;
  path: string;
  size: number;
  offset: number;
  checksum: string;
  temp_path: string;
  created_at: number;
  updated_at: number;
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/**
 * Create the additive fs_upload table. One active upload per target path (the
 * primary key). Idempotent, so it is safe to call on every schema init.
 */
export function initUploadSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_upload (
    path TEXT PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    offset INTEGER NOT NULL DEFAULT 0,
    temp_path TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`);
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function normalizePath(path: string): string {
  const segments: string[] = [];
  for (const segment of path.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join('/')}`;
}

/** Validate and normalize an upload target path; the root and trailing slashes are rejected. */
export function validateUploadPath(path: string): string {
  if (typeof path !== 'string' || path === '' || path === '/' || path.endsWith('/')) {
    throw new HttpError(400, 'INVALID_PATH', 'A file path without a trailing slash is required');
  }
  const normalized = normalizePath(path);
  if (normalized === '/') {
    throw new HttpError(400, 'INVALID_PATH', 'A file path without a trailing slash is required');
  }
  return normalized;
}

function validateSize(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Expected non-negative integer "size"');
  }
  return value;
}

function validateChecksum(value: unknown, field = 'checksum'): string {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `Expected lowercase hex SHA-256 "${field}"`);
  }
  return value;
}

function parentPath(path: string): string {
  const separator = path.lastIndexOf('/');
  return separator <= 0 ? '/' : path.slice(0, separator);
}

function temporaryPath(path: string): string {
  const parent = parentPath(path);
  const base = parent === '/' ? '' : parent;
  return `${base}/.airyfs-upload-${crypto.randomUUID()}`;
}

// ---------------------------------------------------------------------------
// Row access
// ---------------------------------------------------------------------------

function selectRow(sql: SqlExec, path: string): UploadRow | null {
  const rows = sql.exec('SELECT * FROM fs_upload WHERE path = ?', path).toArray() as unknown as UploadRow[];
  return rows.length > 0 ? normalizeRow(rows[0]) : null;
}

function normalizeRow(row: UploadRow): UploadRow {
  return {
    id: String(row.id),
    path: String(row.path),
    size: Number(row.size),
    offset: Number(row.offset),
    checksum: String(row.checksum),
    temp_path: String(row.temp_path),
    created_at: Number(row.created_at),
    updated_at: Number(row.updated_at),
  };
}

function toStatus(row: UploadRow): UploadStatus {
  return {
    id: row.id,
    path: row.path,
    size: row.size,
    offset: row.offset,
    checksum: row.checksum,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

// ---------------------------------------------------------------------------
// Begin / status
// ---------------------------------------------------------------------------

/**
 * Create or resume an upload session for `path`. An existing session with the
 * same size and checksum is returned (resume); a mismatch is a 409 conflict.
 * Stale sessions older than 24h are opportunistically reaped first. The parent
 * directory must exist. Serialized on the target path so concurrent begins
 * cannot orphan a temp file.
 */
export async function beginUpload(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  rawPath: string,
  body: { size: unknown; checksum: unknown }
): Promise<UploadBeginResult> {
  const path = validateUploadPath(rawPath);
  const size = validateSize(body.size);
  const checksum = validateChecksum(body.checksum);

  await reapStaleUploads(fs, sql);

  const release = await access.acquireWrite(path);
  try {
    const existing = selectRow(sql, path);
    if (existing) {
      if (existing.size !== size || existing.checksum !== checksum) {
        throw new HttpError(
          409,
          'UPLOAD_CONFLICT',
          `An upload with different size/checksum is already active for ${path}`
        );
      }
      return { session: toStatus(existing), created: false };
    }

    const parent = parentPath(path);
    let parentStats;
    try {
      parentStats = await fs.stat(parent);
    } catch (error) {
      throw asErrno(error, parent);
    }
    if (!parentStats.isDirectory()) {
      const error = new Error(`ENOTDIR: not a directory, open '${parent}'`) as ErrnoLike;
      error.code = 'ENOTDIR';
      error.path = parent;
      throw error;
    }

    const id = crypto.randomUUID();
    const tempPath = temporaryPath(path);
    try {
      await fs.writeFile(tempPath, Buffer.alloc(0));
      sql.exec(
        `INSERT INTO fs_upload (path, id, size, checksum, offset, temp_path, created_at, updated_at)
         VALUES (?, ?, ?, ?, 0, ?, unixepoch(), unixepoch())`,
        path, id, size, checksum, tempPath
      );
      const created = selectRow(sql, path);
      if (!created) throw new Error('Upload session vanished immediately after creation');
      return { session: toStatus(created), created: true };
    } catch (error) {
      sql.exec('DELETE FROM fs_upload WHERE path = ? AND id = ?', path, id);
      await fs.rm(tempPath, { force: true }).catch(() => undefined);
      throw error;
    }
  } finally {
    release();
  }
}

/** Return the status of an active upload session, or 404 if none exists. */
export function getUpload(sql: SqlExec, rawPath: string): UploadStatus {
  const path = validateUploadPath(rawPath);
  const row = selectRow(sql, path);
  if (!row) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);
  return toStatus(row);
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

/**
 * Append one bounded chunk at the stored offset. The request `offset` must match
 * the stored offset exactly; a mismatch (including a retried, already-accepted
 * offset) returns a stable UPLOAD_OFFSET_MISMATCH carrying the current offset so
 * the client can GET and continue without duplicating bytes. The chunk's
 * SHA-256 is verified before it is written; the offset advances atomically under
 * the temp-path write lock.
 */
export async function appendUpload(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  rawPath: string,
  chunk: AppendChunk
): Promise<UploadStatus> {
  const path = validateUploadPath(rawPath);
  const chunkSha256 = validateChecksum(chunk.chunkSha256, 'X-AiryFS-Chunk-SHA256');
  if (!Number.isSafeInteger(chunk.offset) || chunk.offset < 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Upload-Offset must be a non-negative integer');
  }
  if (chunk.data.byteLength === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Chunk must not be empty');
  }
  if (chunk.data.byteLength > MAX_UPLOAD_CHUNK_BYTES) {
    throw new HttpError(413, 'CHUNK_TOO_LARGE', `Chunk exceeds ${MAX_UPLOAD_CHUNK_BYTES} bytes`);
  }

  const preview = selectRow(sql, path);
  if (!preview) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);

  const release = await access.acquireWrite(preview.temp_path);
  try {
    // Re-read under the lock so the offset comparison and advance are atomic.
    const row = selectRow(sql, path);
    if (!row) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);

    if (chunk.offset !== row.offset) {
      throw new HttpError(
        409,
        'UPLOAD_OFFSET_MISMATCH',
        `Expected Upload-Offset ${row.offset}, got ${chunk.offset}`,
        { 'Upload-Offset': String(row.offset) }
      );
    }
    if (row.offset + chunk.data.byteLength > row.size) {
      throw new HttpError(
        400,
        'UPLOAD_OVERFLOW',
        `Chunk would exceed the declared size ${row.size}`
      );
    }

    const actual = createHash('sha256').update(chunk.data).digest('hex');
    if (actual !== chunkSha256) {
      throw new HttpError(400, 'CHUNK_CHECKSUM_MISMATCH', 'Chunk SHA-256 does not match X-AiryFS-Chunk-SHA256');
    }

    const handle = await fs.open(row.temp_path);
    await handle.pwrite(row.offset, Buffer.from(chunk.data));
    const newOffset = row.offset + chunk.data.byteLength;
    sql.exec(
      'UPDATE fs_upload SET offset = ?, updated_at = unixepoch() WHERE path = ?',
      newOffset, path
    );
    const updated = selectRow(sql, path);
    if (!updated) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);
    return toStatus(updated);
  } finally {
    release();
  }
}

// ---------------------------------------------------------------------------
// Complete
// ---------------------------------------------------------------------------

/**
 * Publish a fully-received upload over its target. Requires offset === size,
 * fsyncs the temp file, recomputes its streaming SHA-256, and compares it with
 * the expected checksum. On a mismatch the session and temp file are retained
 * (resumable); on success the temp file is atomically renamed over the target
 * using AgentFS lease semantics and the session is deleted. Holds the temp and
 * target write locks for the whole operation so no observer sees intermediate
 * state and completes cannot race one another.
 */
export async function completeUpload(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  rawPath: string
): Promise<UploadCompleteResult> {
  const path = validateUploadPath(rawPath);
  const preview = selectRow(sql, path);
  if (!preview) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);

  const release = await access.acquireWrite([preview.temp_path, path]);
  try {
    const row = selectRow(sql, path);
    if (!row) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);
    if (row.offset !== row.size) {
      throw new HttpError(
        409,
        'UPLOAD_INCOMPLETE',
        `Upload is at ${row.offset} of ${row.size} bytes`,
        { 'Upload-Offset': String(row.offset) }
      );
    }

    const handle = await fs.open(row.temp_path);
    await handle.fsync();

    // Hash under the held lock; sha256Path must not re-acquire the read lock.
    const actual = await sha256Path(fs, row.temp_path);
    if (actual.checksum !== row.checksum) {
      throw new HttpError(
        409,
        'UPLOAD_CHECKSUM_MISMATCH',
        `Uploaded content SHA-256 ${actual.checksum} does not match expected ${row.checksum}`
      );
    }

    // Lease-aware rename-over: retains any inode a live FUSE handle still holds.
    await fs.rename(row.temp_path, path);
    sql.exec('DELETE FROM fs_upload WHERE path = ?', path);

    const stats = await fs.stat(path);
    return { ...toStatsDto(stats), path, checksum: row.checksum };
  } finally {
    release();
  }
}

// ---------------------------------------------------------------------------
// Abort
// ---------------------------------------------------------------------------

/** Abort an upload, removing its temp file and session row. Idempotent. */
export async function abortUpload(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  rawPath: string
): Promise<void> {
  const path = validateUploadPath(rawPath);
  const row = selectRow(sql, path);
  if (!row) throw new HttpError(404, 'UPLOAD_NOT_FOUND', `No active upload for ${path}`);

  const release = await access.acquireWrite([row.temp_path, path]);
  try {
    await fs.rm(row.temp_path, { force: true }).catch(() => undefined);
    sql.exec('DELETE FROM fs_upload WHERE path = ?', path);
  } finally {
    release();
  }
}

// ---------------------------------------------------------------------------
// Reaping
// ---------------------------------------------------------------------------

/**
 * Best-effort removal of sessions untouched for longer than the stale window.
 * Runs opportunistically on begin; failures never block a new upload.
 */
export async function reapStaleUploads(
  fs: FileSystem,
  sql: SqlExec,
  maxAgeSeconds = STALE_UPLOAD_SECONDS
): Promise<void> {
  let stale: UploadRow[];
  try {
    stale = (sql
      .exec('SELECT * FROM fs_upload WHERE updated_at < unixepoch() - ?', maxAgeSeconds)
      .toArray() as unknown as UploadRow[]).map(normalizeRow);
  } catch {
    return;
  }
  for (const row of stale) {
    try {
      await fs.rm(row.temp_path, { force: true }).catch(() => undefined);
      sql.exec('DELETE FROM fs_upload WHERE path = ?', row.path);
    } catch {
      // Leave anything we could not clean for the next opportunistic pass.
    }
  }
}

// ---------------------------------------------------------------------------
// HTTP body helper
// ---------------------------------------------------------------------------

/**
 * Read at most `limit` bytes from a request body, rejecting anything larger so a
 * PATCH chunk is bounded and never buffers a whole file. Returns the exact bytes
 * received.
 */
export async function readBoundedChunk(
  stream: ReadableStream<Uint8Array> | null,
  limit: number
): Promise<Uint8Array> {
  if (!stream) return new Uint8Array(0);
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength === 0) continue;
      total += value.byteLength;
      if (total > limit) {
        await reader.cancel().catch(() => undefined);
        throw new HttpError(413, 'CHUNK_TOO_LARGE', `Chunk exceeds ${limit} bytes`);
      }
      parts.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  if (parts.length === 1) return parts[0];
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function asErrno(error: unknown, path: string): unknown {
  if (error instanceof Error && 'code' in error) return error;
  const wrapped = new Error(`ENOENT: no such file or directory, stat '${path}'`) as ErrnoLike;
  wrapped.code = 'ENOENT';
  wrapped.path = path;
  return wrapped;
}
