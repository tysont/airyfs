// ABOUTME: In-memory SQLite adapter for testing the AgentFS Cloudflare backend.

import Database from 'better-sqlite3';
import type { CloudflareStorage } from 'agentfs-sdk/cloudflare';

export function createTestStorage(db: Database.Database): CloudflareStorage {
  db.function('unixepoch', () => Math.floor(Date.now() / 1000));

  const sql = {
    exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]) {
      const statement = db.prepare(query);
      const rows = statement.reader ? statement.all(...bindings) as T[] : [];
      const runResult = statement.reader ? null : statement.run(...bindings);
      return {
        columnNames: statement.reader ? statement.columns().map((column) => column.name) : [],
        rowsRead: rows.length,
        rowsWritten: runResult?.changes ?? 0,
        toArray: () => rows,
        one: () => {
          if (rows.length !== 1) throw new Error(`Expected one row, got ${rows.length}`);
          return rows[0];
        },
        raw: function* () {
          for (const row of rows) yield Object.values(row as Record<string, unknown>);
        },
        next: () => ({ done: true as const }),
        [Symbol.iterator]: function* () { yield* rows; },
      };
    },
    get databaseSize() {
      return db.prepare('PRAGMA page_count').pluck().get() as number
        * (db.prepare('PRAGMA page_size').pluck().get() as number);
    },
  };

  return {
    sql,
    transactionSync<T>(callback: () => T): T {
      return db.transaction(callback)();
    },
  };
}
