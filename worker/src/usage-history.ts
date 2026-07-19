// ABOUTME: Stores bounded per-volume usage observations in Durable Object SQLite.
// ABOUTME: Samples are captured by diagnostic reads, never filesystem mutations or alarms.

import type { SqlExec } from './schema';

export const USAGE_HISTORY_TABLES = ['fs_usage_sample'] as const;
export const USAGE_SAMPLE_INTERVAL_SECONDS = 5 * 60;
export const USAGE_SAMPLE_RETENTION = 7 * 24 * 60 / 5;
export const DEFAULT_USAGE_HISTORY_LIMIT = 288;
export const MAX_USAGE_HISTORY_LIMIT = 1_000;

export interface UsageSampleInput {
  filesystem: {
    bytesUsed: number;
    inodes: number;
    quotaBytes: number | null;
    quotaInodes: number | null;
  };
  sqliteBytes: number;
}

export interface UsageSample {
  sampledAt: number;
  bytesUsed: number;
  inodes: number;
  sqliteBytes: number;
  quotaBytes: number | null;
  quotaInodes: number | null;
}

export interface UsageHistoryPage {
  samples: UsageSample[];
  next: number | null;
}

export function initUsageHistorySchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_usage_sample (
    sampled_at INTEGER PRIMARY KEY,
    bytes_used INTEGER NOT NULL,
    inodes INTEGER NOT NULL,
    sqlite_bytes INTEGER NOT NULL,
    quota_bytes INTEGER,
    quota_inodes INTEGER
  )`);
  sql.exec(`CREATE TRIGGER IF NOT EXISTS trg_fs_usage_sample_retention
    AFTER INSERT ON fs_usage_sample BEGIN
      DELETE FROM fs_usage_sample WHERE sampled_at IN (
        SELECT sampled_at FROM fs_usage_sample
        ORDER BY sampled_at DESC
        LIMIT -1 OFFSET ${USAGE_SAMPLE_RETENTION}
      );
    END`);
}

export function recordUsageSample(sql: SqlExec, input: UsageSampleInput, now = Date.now()): UsageSample {
  const sampledAt = Math.floor(now / 1000 / USAGE_SAMPLE_INTERVAL_SECONDS) * USAGE_SAMPLE_INTERVAL_SECONDS;
  const sample: UsageSample = {
    sampledAt,
    bytesUsed: input.filesystem.bytesUsed,
    inodes: input.filesystem.inodes,
    sqliteBytes: input.sqliteBytes,
    quotaBytes: input.filesystem.quotaBytes,
    quotaInodes: input.filesystem.quotaInodes,
  };
  sql.exec(`INSERT INTO fs_usage_sample
    (sampled_at, bytes_used, inodes, sqlite_bytes, quota_bytes, quota_inodes)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(sampled_at) DO UPDATE SET
      bytes_used = excluded.bytes_used,
      inodes = excluded.inodes,
      sqlite_bytes = excluded.sqlite_bytes,
      quota_bytes = excluded.quota_bytes,
      quota_inodes = excluded.quota_inodes
    WHERE fs_usage_sample.bytes_used IS NOT excluded.bytes_used
      OR fs_usage_sample.inodes IS NOT excluded.inodes
      OR fs_usage_sample.sqlite_bytes IS NOT excluded.sqlite_bytes
      OR fs_usage_sample.quota_bytes IS NOT excluded.quota_bytes
      OR fs_usage_sample.quota_inodes IS NOT excluded.quota_inodes`,
  sample.sampledAt, sample.bytesUsed, sample.inodes, sample.sqliteBytes, sample.quotaBytes, sample.quotaInodes);
  return sample;
}

export function listUsageHistory(
  sql: SqlExec,
  options: { before?: number; limit?: number } = {},
): UsageHistoryPage {
  const limit = options.limit ?? DEFAULT_USAGE_HISTORY_LIMIT;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_USAGE_HISTORY_LIMIT) {
    throw new Error(`limit must be between 1 and ${MAX_USAGE_HISTORY_LIMIT}`);
  }
  if (options.before !== undefined && (!Number.isSafeInteger(options.before) || options.before < 0)) {
    throw new Error('before must be a non-negative integer');
  }
  const rows = sql.exec(`SELECT sampled_at, bytes_used, inodes, sqlite_bytes, quota_bytes, quota_inodes
    FROM fs_usage_sample
    WHERE (? IS NULL OR sampled_at < ?)
    ORDER BY sampled_at DESC
    LIMIT ?`, options.before ?? null, options.before ?? null, limit + 1).toArray();
  const hasMore = rows.length > limit;
  const samples = rows.slice(0, limit).map(toUsageSample);
  return { samples, next: hasMore ? samples.at(-1)!.sampledAt : null };
}

function toUsageSample(row: Record<string, unknown>): UsageSample {
  return {
    sampledAt: Number(row.sampled_at),
    bytesUsed: Number(row.bytes_used),
    inodes: Number(row.inodes),
    sqliteBytes: Number(row.sqlite_bytes),
    quotaBytes: row.quota_bytes === null ? null : Number(row.quota_bytes),
    quotaInodes: row.quota_inodes === null ? null : Number(row.quota_inodes),
  };
}
