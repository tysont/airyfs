// ABOUTME: Verifies bounded, bucketed per-volume usage history storage.
// ABOUTME: Covers replacement within a bucket, pagination, validation, and retention.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { initSchema, type SqlExec } from '../src/schema';
import {
  listUsageHistory,
  recordUsageSample,
  USAGE_SAMPLE_INTERVAL_SECONDS,
  USAGE_SAMPLE_RETENTION,
} from '../src/usage-history';

describe('usage history', () => {
  let sql: SqlExec;

  beforeEach(() => {
    const db = new Database(':memory:');
    db.function('unixepoch', () => Math.floor(Date.now() / 1000));
    sql = {
      exec(query: string, ...bindings: unknown[]) {
        const statement = db.prepare(query);
        if (statement.reader) {
          const rows = statement.all(...bindings) as Record<string, unknown>[];
          return { toArray: () => rows };
        }
        statement.run(...bindings);
        return { toArray: () => [] };
      },
    };
    initSchema(sql);
  });

  it('updates one sample per five-minute bucket', () => {
    recordUsageSample(sql, snapshot(10), 60_000);
    recordUsageSample(sql, snapshot(20), 120_000);
    recordUsageSample(sql, snapshot(20), 180_000);

    expect(listUsageHistory(sql).samples).toEqual([{
      sampledAt: 0,
      bytesUsed: 20,
      inodes: 2,
      sqliteBytes: 40,
      quotaBytes: 100,
      quotaInodes: null,
    }]);
    expect(sql.exec('SELECT changes() AS count').toArray()[0].count).toBe(0);
  });

  it('returns newest-first keyset pages', () => {
    for (let index = 1; index <= 3; index++) {
      recordUsageSample(sql, snapshot(index), index * USAGE_SAMPLE_INTERVAL_SECONDS * 1_000);
    }

    const first = listUsageHistory(sql, { limit: 2 });
    expect(first.samples.map((sample) => sample.bytesUsed)).toEqual([3, 2]);
    expect(first.next).toBe(2 * USAGE_SAMPLE_INTERVAL_SECONDS);
    expect(listUsageHistory(sql, { before: first.next!, limit: 2 }).samples.map((sample) => sample.bytesUsed)).toEqual([1]);
  });

  it('retains only the bounded sample window and validates queries', () => {
    for (let index = 0; index <= USAGE_SAMPLE_RETENTION; index++) {
      recordUsageSample(sql, snapshot(index), index * USAGE_SAMPLE_INTERVAL_SECONDS * 1_000);
    }

    expect(listUsageHistory(sql, { limit: 1_000 }).samples).toHaveLength(1_000);
    expect(sql.exec('SELECT count(*) AS count FROM fs_usage_sample').toArray()[0].count).toBe(USAGE_SAMPLE_RETENTION);
    expect(() => listUsageHistory(sql, { limit: 0 })).toThrow('limit must be between');
    expect(() => listUsageHistory(sql, { before: -1 })).toThrow('before must be');
  });
});

function snapshot(bytesUsed: number) {
  return {
    filesystem: { bytesUsed, inodes: 2, quotaBytes: 100, quotaInodes: null },
    sqliteBytes: bytesUsed * 2,
  };
}
