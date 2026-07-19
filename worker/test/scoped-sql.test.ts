// ABOUTME: Verifies scoped application SQL execution and internal-table isolation.
// ABOUTME: Uses real SQLite for DDL, bindings, blobs, row limits, and attack regressions.

import Database from 'better-sqlite3';
import { describe, expect, it } from 'vitest';
import { executeScopedSql, type ScopedSqlStorage } from '../src/scoped-sql';
import { initSchema } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('scoped SQL', () => {
  it('creates, mutates, and queries app tables with typed bindings', () => {
    const { storage } = setup();
    expect(executeScopedSql(storage, 'CREATE TABLE app_notes (id INTEGER PRIMARY KEY, body TEXT, data BLOB)', []).rowsWritten).toBe(0);
    executeScopedSql(storage, 'INSERT INTO app_notes(body, data) VALUES (?, ?)', ['hello', { base64: 'AQI=' }]);
    const result = executeScopedSql(storage, 'SELECT id, body, data FROM app_notes', []);
    expect(result).toMatchObject({
      columns: ['id', 'body', 'data'],
      rows: [[1, 'hello', { base64: 'AQI=' }]],
      truncated: false,
    });
  });

  it('rejects internal, system, multi-statement, CTE, view, and trigger access', () => {
    const { storage } = setup();
    for (const sql of [
      'SELECT * FROM fs_inode',
      'SELECT * FROM "kv_store"',
      'SELECT * FROM sqlite_schema',
      "SELECT * FROM pragma_table_info('fs_inode')",
      'CREATE TABLE app_ok(id); SELECT * FROM app_ok',
      'WITH x AS (SELECT 1) SELECT * FROM x',
      'CREATE VIEW app_view AS SELECT 1',
      'CREATE TRIGGER app_trigger AFTER INSERT ON app_x BEGIN DELETE FROM fs_inode; END',
      'ATTACH DATABASE ":memory:" AS other',
    ]) {
      expect(() => executeScopedSql(storage, sql, [])).toThrow();
    }
  });

  it('requires app-prefixed schema objects and permits joins between them', () => {
    const { storage } = setup();
    expect(() => executeScopedSql(storage, 'CREATE TABLE notes(id)', [])).toThrow();
    executeScopedSql(storage, 'CREATE TABLE app_a(id INTEGER)', []);
    expect(() => executeScopedSql(storage, 'ALTER TABLE app_a RENAME TO hidden', [])).toThrow();
    executeScopedSql(storage, 'ALTER TABLE app_a RENAME TO app_renamed', []);
    executeScopedSql(storage, 'CREATE TABLE app_b(id INTEGER REFERENCES app_renamed(id))', []);
    executeScopedSql(storage, 'CREATE INDEX app_b_id ON app_b(id)', []);
    const result = executeScopedSql(storage, 'SELECT a.id FROM app_renamed a JOIN app_b b ON b.id = a.id', []);
    expect(result.rows).toEqual([]);
  });

  it('caps returned rows', () => {
    const { db, storage } = setup();
    db.exec('CREATE TABLE app_many(value INTEGER)');
    const insert = db.prepare('INSERT INTO app_many(value) VALUES (?)');
    const transaction = db.transaction(() => { for (let index = 0; index < 1002; index++) insert.run(index); });
    transaction();
    const result = executeScopedSql(storage, 'SELECT value FROM app_many ORDER BY value', []);
    expect(result.rows).toHaveLength(1000);
    expect(result.truncated).toBe(true);
  });
});

function setup(): { db: Database.Database; storage: ScopedSqlStorage } {
  const db = new Database(':memory:');
  initSchema(createTestStorage(db).sql);
  return {
    db,
    storage: {
      exec(query, ...bindings) {
        const statement = db.prepare(query);
        const rows = statement.reader ? statement.all(...bindings) as Record<string, unknown>[] : [];
        const result = statement.reader ? null : statement.run(...bindings);
        return {
          columnNames: statement.reader ? statement.columns().map((column) => column.name) : [],
          rowsRead: rows.length,
          rowsWritten: Number(result?.changes ?? 0),
          *[Symbol.iterator]() { yield* rows; },
        };
      },
    },
  };
}
