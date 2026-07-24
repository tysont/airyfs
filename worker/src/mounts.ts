// ABOUTME: Persists the per-volume mount table and resolves mounted subtrees to their target volumes.
// ABOUTME: AiryFS-owned table (like fs_job); AgentFS never sees a mounted path — routing happens on the path plane.

import { normalizePath } from './auth';
import { HttpError } from './files-api';
import type { SqlExec } from './schema';

export const MOUNT_TABLES = ['fs_mount'] as const;

/**
 * Maximum forwarding hops for a single direct-path operation. Chains longer than
 * this (A→B→C→…) are treated as a cycle and rejected at runtime, complementing
 * the creation-time cycle walk.
 */
export const MAX_MOUNT_HOPS = 8;

/** A persisted mount: a subtree of this volume served by another volume's DO. */
export interface MountRecord {
  /** Normalized absolute path in the host volume where the target is grafted. */
  mountpoint: string;
  /** Volume id whose DO serves reads/writes under the mountpoint. */
  targetVolume: string;
  /** Normalized absolute path inside the target volume exposed at the mountpoint. */
  targetSubpath: string;
  /** Capability id minted on the target volume for the FUSE/guest bridge (nullable when auth is disabled). */
  credentialId: string | null;
  /** Signed capability token authorizing access to the target subpath (nullable when auth is disabled). */
  token: string | null;
  /** Free-form mount options (reserved for read-only, cache-ttl, etc.). */
  options: Record<string, unknown>;
  createdAt: number;
}

/** Mount capabilities are long-lived and revocable rather than short-TTL. */
export const MOUNT_CAPABILITY_TTL_SECONDS = 100 * 365 * 24 * 60 * 60;

/** Public projection of a mount record with the bearer token withheld. */
export function publicMount(mount: MountRecord): Omit<MountRecord, 'token'> {
  const { token: _token, ...rest } = mount;
  return rest;
}

export function initMountSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_mount (
    mountpoint TEXT PRIMARY KEY,
    target_volume TEXT NOT NULL,
    target_subpath TEXT NOT NULL DEFAULT '/',
    credential_id TEXT,
    token TEXT,
    options TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
  )`);
}

function mapMount(row: Record<string, unknown>): MountRecord {
  return {
    mountpoint: String(row.mountpoint),
    targetVolume: String(row.target_volume),
    targetSubpath: String(row.target_subpath),
    credentialId: row.credential_id === null || row.credential_id === undefined ? null : String(row.credential_id),
    token: row.token === null || row.token === undefined ? null : String(row.token),
    options: JSON.parse(String(row.options ?? '{}')) as Record<string, unknown>,
    createdAt: Number(row.created_at),
  };
}

export function listMounts(sql: SqlExec): MountRecord[] {
  return (sql.exec('SELECT * FROM fs_mount ORDER BY mountpoint').toArray() as Record<string, unknown>[]).map(mapMount);
}

export function getMount(sql: SqlExec, mountpoint: string): MountRecord | null {
  const row = sql.exec('SELECT * FROM fs_mount WHERE mountpoint = ?', normalizePath(mountpoint)).toArray()[0];
  return row ? mapMount(row) : null;
}

export interface CreateMountInput {
  mountpoint: string;
  targetVolume: string;
  targetSubpath?: string;
  hostVolume: string;
  credentialId?: string | null;
  token?: string | null;
  options?: Record<string, unknown>;
}

/**
 * Validate and persist a mount. Rejects self-mounts, the volume root as a
 * mountpoint, and any mountpoint that nests inside or contains an existing mount
 * in this volume (which would make longest-prefix resolution ambiguous).
 */
export function createMountRow(sql: SqlExec, input: CreateMountInput): MountRecord {
  const mountpoint = normalizePath(input.mountpoint);
  if (mountpoint === '/') {
    throw new HttpError(400, 'INVALID_MOUNT', 'Cannot mount over the volume root');
  }
  if (typeof input.targetVolume !== 'string' || input.targetVolume.trim() === '') {
    throw new HttpError(400, 'INVALID_MOUNT', 'A target volume is required');
  }
  if (input.targetVolume === input.hostVolume) {
    throw new HttpError(409, 'MOUNT_SELF', 'A volume cannot mount itself');
  }
  const targetSubpath = normalizePath(input.targetSubpath ?? '/');

  for (const existing of listMounts(sql)) {
    if (existing.mountpoint === mountpoint) {
      throw new HttpError(409, 'MOUNT_EXISTS', `A mount already exists at ${mountpoint}`);
    }
    if (
      mountpoint.startsWith(`${existing.mountpoint}/`) ||
      existing.mountpoint.startsWith(`${mountpoint}/`)
    ) {
      throw new HttpError(
        409,
        'MOUNT_NESTED',
        `Mount ${mountpoint} overlaps existing mount ${existing.mountpoint}`,
      );
    }
  }

  const createdAt = Math.floor(Date.now() / 1000);
  sql.exec(
    `INSERT INTO fs_mount (mountpoint, target_volume, target_subpath, credential_id, token, options, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    mountpoint,
    input.targetVolume,
    targetSubpath,
    input.credentialId ?? null,
    input.token ?? null,
    JSON.stringify(input.options ?? {}),
    createdAt,
  );
  return getMount(sql, mountpoint)!;
}

export function deleteMountRow(sql: SqlExec, mountpoint: string): MountRecord {
  const record = getMount(sql, mountpoint);
  if (!record) throw new HttpError(404, 'MOUNT_NOT_FOUND', `No mount at ${normalizePath(mountpoint)}`);
  sql.exec('DELETE FROM fs_mount WHERE mountpoint = ?', record.mountpoint);
  return record;
}

/** A path resolved through the mount table onto a target volume. */
export interface ResolvedMount {
  mount: MountRecord;
  /** The path within the target volume that `path` maps to. */
  targetPath: string;
}

/**
 * Longest-prefix match of `path` against the mount table. Returns the matching
 * mount and the translated target path, or null when `path` is local.
 */
export function resolveMount(mounts: MountRecord[], path: string): ResolvedMount | null {
  const p = normalizePath(path);
  let best: MountRecord | null = null;
  for (const mount of mounts) {
    if (p === mount.mountpoint || p.startsWith(`${mount.mountpoint}/`)) {
      if (!best || mount.mountpoint.length > best.mountpoint.length) best = mount;
    }
  }
  if (!best) return null;
  const remainder = p.slice(best.mountpoint.length);
  return { mount: best, targetPath: normalizePath(`${best.targetSubpath}${remainder}`) };
}
