// ABOUTME: Tests for AgentFS schema initialization against a real SQLite database.
// ABOUTME: Verifies table creation, column structure, seed data, and idempotency.

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  ChunkSizeConflictError,
  configureChunkSize,
  DEFAULT_CHUNK_SIZE,
  initSchema,
  InvalidChunkSizeError,
  SCHEMA_TABLES,
  type SqlExec,
} from '../src/schema';

/**
 * Wraps better-sqlite3 to satisfy the SqlExec interface that initSchema expects.
 *
 * better-sqlite3 uses synchronous APIs:
 *   db.prepare(sql).all(...bindings)  -> rows for SELECT
 *   db.prepare(sql).run(...bindings)  -> run for DDL/DML
 *
 * SqlExec.exec() needs to return { toArray(): Record<string, unknown>[] }.
 * We detect SELECTs by trying .all() first — if the statement returns columns,
 * it's a query. Otherwise it's DDL/DML.
 *
 * Note: better-sqlite3 doesn't support SQLite's unixepoch() function natively,
 * so we register it as a custom function for testing.
 */
function createTestSql(db: Database.Database): SqlExec {
  // Register unixepoch() — returns current Unix timestamp in seconds.
  // In production DO SQLite this is a built-in function.
  db.function('unixepoch', () => Math.floor(Date.now() / 1000));

  return {
    exec(query: string, ...bindings: unknown[]) {
      const stmt = db.prepare(query);
      // If the statement returns columns, it's a SELECT-like query
      if (stmt.reader) {
        const rows = stmt.all(...bindings) as Record<string, unknown>[];
        return { toArray: () => rows };
      }
      // DDL/DML: run it and return empty results
      stmt.run(...bindings);
      return { toArray: () => [] };
    },
  };
}

describe('initSchema', () => {
  let db: Database.Database;
  let sql: SqlExec;

  beforeEach(() => {
    db = new Database(':memory:');
    sql = createTestSql(db);
  });

  it('creates all expected tables', () => {
    initSchema(sql);

    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
      .all() as { name: string }[];
    const tableNames = tables.map((t) => t.name);

    for (const expected of SCHEMA_TABLES) {
      expect(tableNames).toContain(expected);
    }
  });

  it('creates all expected indexes', () => {
    initSchema(sql);

    const indexes = db
      .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%' ORDER BY name")
      .all() as { name: string }[];
    const indexNames = indexes.map((i) => i.name);

    expect(indexNames).toContain('idx_fs_dentry_parent');
    expect(indexNames).toContain('idx_kv_store_created_at');
    expect(indexNames).toContain('idx_tool_calls_name');
    expect(indexNames).toContain('idx_tool_calls_started_at');
    expect(indexNames).toContain('idx_fs_open_inode_expires');
  });

  it('creates the open-inode lease table with a composite primary key', () => {
    initSchema(sql);

    const columns = db.prepare("PRAGMA table_info('fs_open_inode')").all() as { name: string; pk: number }[];
    const columnNames = columns.map((c) => c.name);
    expect(columnNames).toEqual(expect.arrayContaining(['session_id', 'ino', 'open_count', 'expires_at']));
    // session_id and ino form the primary key
    expect(columns.filter((c) => c.pk > 0).map((c) => c.name).sort()).toEqual(['ino', 'session_id']);

    db.prepare('INSERT INTO fs_open_inode (session_id, ino, open_count, expires_at) VALUES (?, ?, 1, ?)').run('s', 2, 100);
    expect(() => {
      db.prepare('INSERT INTO fs_open_inode (session_id, ino, open_count, expires_at) VALUES (?, ?, 1, ?)').run('s', 2, 200);
    }).toThrow();
  });

  it('cascades chunk, symlink, and lease cleanup when an inode is deleted', () => {
    initSchema(sql);

    db.prepare('INSERT INTO fs_inode (ino, mode, nlink, size, atime, mtime, ctime) VALUES (2, 33188, 0, 5, 0, 0, 0)').run();
    db.prepare('INSERT INTO fs_data (ino, chunk_index, data) VALUES (2, 0, ?)').run(Buffer.from('hello'));
    db.prepare("INSERT INTO fs_symlink (ino, target) VALUES (2, '/target')").run();
    db.prepare('INSERT INTO fs_open_inode (session_id, ino, open_count, expires_at) VALUES (?, 2, 1, ?)').run('s', 100);

    db.prepare('DELETE FROM fs_inode WHERE ino = 2').run();

    expect((db.prepare('SELECT COUNT(*) AS c FROM fs_data WHERE ino = 2').get() as { c: number }).c).toBe(0);
    expect((db.prepare('SELECT COUNT(*) AS c FROM fs_symlink WHERE ino = 2').get() as { c: number }).c).toBe(0);
    expect((db.prepare('SELECT COUNT(*) AS c FROM fs_open_inode WHERE ino = 2').get() as { c: number }).c).toBe(0);
  });

  it('configures new volumes with the 256 KiB default', () => {
    initSchema(sql);
    configureChunkSize(sql);

    const row = db
      .prepare("SELECT value FROM fs_config WHERE key='chunk_size'")
      .get() as { value: string } | undefined;

    expect(row).toBeDefined();
    expect(row!.value).toBe(String(DEFAULT_CHUNK_SIZE));
  });

  it('seeds root inode (ino=1, directory, mode=16877)', () => {
    initSchema(sql);

    const row = db
      .prepare('SELECT ino, mode, nlink, uid, gid, size FROM fs_inode WHERE ino=1')
      .get() as { ino: number; mode: number; nlink: number; uid: number; gid: number; size: number } | undefined;

    expect(row).toBeDefined();
    expect(row!.ino).toBe(1);
    expect(row!.mode).toBe(16877);  // 0o040755: directory + rwxr-xr-x
    expect(row!.nlink).toBe(1);
    expect(row!.uid).toBe(0);
    expect(row!.gid).toBe(0);
    expect(row!.size).toBe(0);
  });

  it('sets timestamps on root inode', () => {
    initSchema(sql);

    const row = db
      .prepare('SELECT atime, mtime, ctime FROM fs_inode WHERE ino=1')
      .get() as { atime: number; mtime: number; ctime: number } | undefined;

    expect(row).toBeDefined();
    // Timestamps should be recent (within last 10 seconds)
    const now = Math.floor(Date.now() / 1000);
    expect(row!.atime).toBeGreaterThan(now - 10);
    expect(row!.atime).toBeLessThanOrEqual(now);
    expect(row!.mtime).toBeGreaterThan(now - 10);
    expect(row!.ctime).toBeGreaterThan(now - 10);
  });

  it('creates exactly 1 root inode and defers chunk configuration', () => {
    initSchema(sql);

    const inodeCount = db.prepare('SELECT count(*) as c FROM fs_inode').get() as { c: number };
    const configCount = db.prepare('SELECT count(*) as c FROM fs_config').get() as { c: number };

    expect(inodeCount.c).toBe(1);
    expect(configCount.c).toBe(1);
  });

  it('creates empty data tables', () => {
    initSchema(sql);

    for (const table of ['fs_dentry', 'fs_data', 'fs_symlink', 'fs_whiteout', 'fs_origin', 'kv_store', 'tool_calls', 'fs_open_inode']) {
      const row = db.prepare(`SELECT count(*) as c FROM ${table}`).get() as { c: number };
      expect(row.c).toBe(0);
    }
  });

  it('verifies fs_inode has all expected columns', () => {
    initSchema(sql);

    const columns = db.prepare("PRAGMA table_info('fs_inode')").all() as { name: string }[];
    const columnNames = columns.map((c) => c.name);

    const expected = [
      'ino', 'mode', 'nlink', 'uid', 'gid', 'size',
      'atime', 'mtime', 'ctime', 'rdev',
      'atime_nsec', 'mtime_nsec', 'ctime_nsec',
    ];
    for (const col of expected) {
      expect(columnNames).toContain(col);
    }
  });

  it('verifies fs_dentry has UNIQUE constraint on (parent_ino, name)', () => {
    initSchema(sql);

    // Insert two entries with different (parent_ino, name) — should succeed
    db.prepare('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)').run('a', 1, 2);
    db.prepare('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)').run('b', 1, 3);

    // Insert duplicate (parent_ino, name) — should fail
    expect(() => {
      db.prepare('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)').run('a', 1, 4);
    }).toThrow();
  });

  it('verifies fs_data has composite primary key (ino, chunk_index)', () => {
    initSchema(sql);

    // Insert two chunks for same inode — should succeed
    db.prepare('INSERT INTO fs_data (ino, chunk_index, data) VALUES (?, ?, ?)').run(2, 0, Buffer.from('hello'));
    db.prepare('INSERT INTO fs_data (ino, chunk_index, data) VALUES (?, ?, ?)').run(2, 1, Buffer.from('world'));

    // Duplicate (ino, chunk_index) — should fail
    expect(() => {
      db.prepare('INSERT INTO fs_data (ino, chunk_index, data) VALUES (?, ?, ?)').run(2, 0, Buffer.from('dup'));
    }).toThrow();
  });

  it('is idempotent — calling twice does not error or duplicate data', () => {
    initSchema(sql);
    initSchema(sql);

    const inodeCount = db.prepare('SELECT count(*) as c FROM fs_inode').get() as { c: number };
    const configCount = db.prepare('SELECT count(*) as c FROM fs_config').get() as { c: number };

    expect(inodeCount.c).toBe(1);
    expect(configCount.c).toBe(1);
  });

  it('preserves existing chunk sizes and rejects changes after data exists', () => {
    initSchema(sql);
    expect(configureChunkSize(sql, 4096)).toEqual({ chunkSize: 4096, created: true });
    expect(configureChunkSize(sql, 4096)).toEqual({ chunkSize: 4096, created: false });

    db.prepare('INSERT INTO fs_inode (ino, mode, nlink, size, atime, mtime, ctime) VALUES (2, 33188, 1, 1, 0, 0, 0)').run();
    db.prepare("INSERT INTO fs_dentry (name, parent_ino, ino) VALUES ('file', 1, 2)").run();
    expect(configureChunkSize(sql)).toEqual({ chunkSize: 4096, created: false });
    expect(() => configureChunkSize(sql, DEFAULT_CHUNK_SIZE)).toThrow(ChunkSizeConflictError);
    expect(db.prepare("SELECT value FROM fs_config WHERE key='chunk_size'").pluck().get()).toBe('4096');
  });

  it('allows an empty configured volume to select a different valid size', () => {
    initSchema(sql);
    configureChunkSize(sql, 4096);
    expect(configureChunkSize(sql, 64 * 1024)).toEqual({ chunkSize: 64 * 1024, created: false });
  });

  it('rejects invalid chunk sizes', () => {
    initSchema(sql);
    for (const value of [0, 4095, 5000, 2 * 1024 * 1024, 4096.5]) {
      expect(() => configureChunkSize(sql, value)).toThrow(InvalidChunkSizeError);
    }
  });

  it('repairs a partially initialized schema', () => {
    db.exec('CREATE TABLE fs_config (key TEXT PRIMARY KEY, value TEXT NOT NULL)');

    initSchema(sql);

    for (const table of SCHEMA_TABLES) {
      const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table);
      expect(row).toBeDefined();
    }
  });

  it('migrates old inode and tool call schemas without losing records', () => {
    db.exec(`
      CREATE TABLE fs_inode (
        ino INTEGER PRIMARY KEY AUTOINCREMENT,
        mode INTEGER NOT NULL,
        uid INTEGER NOT NULL DEFAULT 0,
        gid INTEGER NOT NULL DEFAULT 0,
        size INTEGER NOT NULL DEFAULT 0,
        atime INTEGER NOT NULL,
        mtime INTEGER NOT NULL,
        ctime INTEGER NOT NULL
      );
      CREATE TABLE tool_calls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        parameters TEXT,
        result TEXT,
        error TEXT,
        status TEXT,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        duration_ms INTEGER
      );
      INSERT INTO tool_calls (name, status, started_at, completed_at, duration_ms)
      VALUES ('existing', NULL, 1, 2, 1000);
    `);

    initSchema(sql);

    const inodeColumns = db.prepare('PRAGMA table_info(fs_inode)').all() as { name: string }[];
    expect(inodeColumns.map((column) => column.name)).toEqual(expect.arrayContaining([
      'nlink', 'rdev', 'atime_nsec', 'mtime_nsec', 'ctime_nsec',
    ]));
    const toolCall = db.prepare('SELECT name, status FROM tool_calls').get() as { name: string; status: string };
    expect(toolCall).toEqual({ name: 'existing', status: 'success' });

    const toolColumns = db.prepare('PRAGMA table_info(tool_calls)').all() as { name: string; notnull: number }[];
    expect(toolColumns.find((column) => column.name === 'status')?.notnull).toBe(1);
    expect(toolColumns.find((column) => column.name === 'completed_at')?.notnull).toBe(0);
    expect(toolColumns.find((column) => column.name === 'duration_ms')?.notnull).toBe(0);
  });
});
