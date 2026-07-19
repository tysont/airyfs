// ABOUTME: Defines and queries the deployment-wide volume registry schema.
// ABOUTME: Keeps persistence logic independently testable outside the Workers runtime.

import type { SqlExec } from './schema';

export interface VolumeRecord {
  name: string;
  chunkSize: number;
  createdAt: number;
}
export interface VolumePage {
  volumes: VolumeRecord[];
  nextCursor: string | null;
}

export function initVolumeRegistry(sql: SqlExec): void {
  sql.exec(`
    CREATE TABLE IF NOT EXISTS volumes (
      name TEXT PRIMARY KEY,
      chunk_size INTEGER NOT NULL,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    )
  `);
}

export function registerVolume(sql: SqlExec, name: string, chunkSize: number): VolumeRecord {
  validateRegistration(name, chunkSize);
  sql.exec(
    `INSERT INTO volumes(name, chunk_size) VALUES (?, ?)
     ON CONFLICT(name) DO UPDATE SET chunk_size = excluded.chunk_size`,
    name,
    chunkSize,
  );
  const row = sql.exec(
    'SELECT name, chunk_size, created_at FROM volumes WHERE name = ?',
    name,
  ).toArray()[0];
  return toVolumeRecord(row);
}

export function listVolumes(sql: SqlExec, after: string, limit: number): VolumePage {
  const rows = sql.exec(
    'SELECT name, chunk_size, created_at FROM volumes WHERE name > ? ORDER BY name LIMIT ?',
    after,
    limit + 1,
  ).toArray();
  const hasMore = rows.length > limit;
  const volumes = rows.slice(0, limit).map(toVolumeRecord);
  return { volumes, nextCursor: hasMore ? volumes.at(-1)?.name ?? null : null };
}

function validateRegistration(name: string, chunkSize: number): void {
  if (typeof name !== 'string' || name.trim() === '') throw new TypeError('Volume name must be non-empty');
  if (!Number.isSafeInteger(chunkSize) || chunkSize <= 0) throw new TypeError('Chunk size must be a positive integer');
}

function toVolumeRecord(row: Record<string, unknown> | undefined): VolumeRecord {
  if (!row) throw new Error('Volume registry write did not return a row');
  return {
    name: String(row.name),
    chunkSize: Number(row.chunk_size),
    createdAt: Number(row.created_at),
  };
}
