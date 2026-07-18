// ABOUTME: Implements recoverable deletes by renaming entries into a reserved volume directory.
// ABOUTME: Stores durable original-path metadata for listing, restoration, purge, and undo.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { HttpError, VolumeAccessCoordinator } from './files-api';
import type { SqlExec } from './schema';

export const TRASH_ROOT = '/.airyfs-trash';
export const TRASH_TABLES = ['fs_trash'] as const;

export interface TrashEntry {
  id: string;
  originalPath: string;
  trashPath: string;
  type: 'file' | 'directory' | 'symlink';
  size: number;
  deletedAt: number;
}

export function initTrashSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_trash (
    id TEXT PRIMARY KEY,
    original_path TEXT NOT NULL,
    trash_path TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    size INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL
  )`);
  sql.exec('CREATE INDEX IF NOT EXISTS idx_fs_trash_deleted ON fs_trash(deleted_at DESC, id DESC)');
}

export async function moveToTrash(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  path: string,
): Promise<TrashEntry> {
  if (path === '/' || path === TRASH_ROOT || path.startsWith(`${TRASH_ROOT}/`)) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'The volume root and trash namespace cannot be trashed');
  }
  const id = crypto.randomUUID();
  const trashPath = `${TRASH_ROOT}/${id}`;
  const release = await access.acquireWrite([path, trashPath]);
  try {
    const stats = await fs.lstat(path);
    const type: TrashEntry['type'] = stats.isDirectory() ? 'directory' : stats.isSymbolicLink() ? 'symlink' : 'file';
    try { await fs.mkdir(TRASH_ROOT); } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'EEXIST')) throw error;
    }
    await fs.rename(path, trashPath);
    const entry = { id, originalPath: path, trashPath, type, size: stats.size, deletedAt: Math.floor(Date.now() / 1000) };
    sql.exec(
      'INSERT INTO fs_trash (id, original_path, trash_path, type, size, deleted_at) VALUES (?, ?, ?, ?, ?, ?)',
      id, path, trashPath, type, stats.size, entry.deletedAt,
    );
    return entry;
  } finally {
    release();
  }
}

export function listTrash(sql: SqlExec): TrashEntry[] {
  return sql.exec(`SELECT id, original_path AS originalPath, trash_path AS trashPath,
    type, size, deleted_at AS deletedAt FROM fs_trash ORDER BY deleted_at DESC, id DESC`).toArray() as unknown as TrashEntry[];
}

function requireTrash(sql: SqlExec, id: string): TrashEntry {
  const entry = listTrash(sql).find((candidate) => candidate.id === id);
  if (!entry) throw new HttpError(404, 'TRASH_NOT_FOUND', `Trash entry not found: ${id}`);
  return entry;
}

export async function restoreTrash(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  id: string,
  destination?: string,
): Promise<TrashEntry & { restoredPath: string }> {
  const entry = requireTrash(sql, id);
  const restoredPath = destination ?? entry.originalPath;
  if (restoredPath === TRASH_ROOT || restoredPath.startsWith(`${TRASH_ROOT}/`)) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Cannot restore into the trash namespace');
  }
  const release = await access.acquireWrite([entry.trashPath, restoredPath]);
  try {
    await fs.rename(entry.trashPath, restoredPath);
    sql.exec('DELETE FROM fs_trash WHERE id = ?', id);
    return { ...entry, restoredPath };
  } finally {
    release();
  }
}

export async function purgeTrash(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
  id: string,
): Promise<TrashEntry> {
  const entry = requireTrash(sql, id);
  const release = await access.acquireWrite(entry.trashPath);
  try {
    await fs.rm(entry.trashPath, { recursive: true });
    sql.exec('DELETE FROM fs_trash WHERE id = ?', id);
    return entry;
  } finally {
    release();
  }
}

export async function undoTrash(
  fs: FileSystem,
  sql: SqlExec,
  access: VolumeAccessCoordinator,
): Promise<TrashEntry & { restoredPath: string }> {
  const entry = listTrash(sql)[0];
  if (!entry) throw new HttpError(404, 'TRASH_EMPTY', 'Trash is empty');
  return restoreTrash(fs, sql, access, entry.id);
}
