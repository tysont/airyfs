// ABOUTME: Tests for the trigger-driven filesystem change feed against real SQLite.
// ABOUTME: Covers event capture, path resolution, prefix filtering, cursor/gap, and last_insert_rowid safety.

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import { initSchema, SCHEMA_TABLES, type SqlExec } from '../src/schema';
import {
  CHANGE_FEED_TABLES,
  ChangeFeedError,
  getChanges,
  initChangeFeedSchema,
  latestChangeSeq,
  type ChangeEvent,
} from '../src/change-feed';

/** Wraps better-sqlite3 to satisfy SqlExec (mirrors schema.test.ts). */
function createTestSql(db: Database.Database): SqlExec {
  db.function('unixepoch', () => Math.floor(Date.now() / 1000));
  return {
    exec(query: string, ...bindings: unknown[]) {
      const stmt = db.prepare(query);
      if (stmt.reader) {
        const rows = stmt.all(...bindings) as Record<string, unknown>[];
        return { toArray: () => rows };
      }
      stmt.run(...bindings);
      return { toArray: () => [] };
    },
  };
}

// POSIX mode encodings.
const DIR = 0o040755; // 16877
const FILE = 0o100644; // 33188

describe('change-feed', () => {
  let db: Database.Database;
  let sql: SqlExec;

  beforeEach(() => {
    db = new Database(':memory:');
    sql = createTestSql(db);
    initSchema(sql);
  });

  // ---- filesystem mutation helpers (simulate any writer touching SQLite) ----

  function addInode(ino: number, mode: number, size = 0, mtime = 0): void {
    db.prepare(
      'INSERT INTO fs_inode (ino, mode, nlink, size, atime, mtime, ctime) VALUES (?, ?, 1, ?, 0, ?, 0)'
    ).run(ino, mode, size, mtime);
  }
  function link(name: string, parent: number, ino: number): void {
    db.prepare('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)').run(name, parent, ino);
  }
  function mkdir(ino: number, name: string, parent: number): void {
    addInode(ino, DIR);
    link(name, parent, ino);
  }
  function mkfile(ino: number, name: string, parent: number, size = 0, mtime = 0): void {
    addInode(ino, FILE, size, mtime);
    link(name, parent, ino);
  }

  function allEvents(): ChangeEvent[] {
    return getChanges(sql, { since: 0, limit: 1000 }).events;
  }

  // ------------------------------------------------------------------ create

  it('emits create events with exact absolute paths for nested entries', () => {
    mkdir(2, 'a', 1);
    mkdir(3, 'b', 2);
    mkfile(4, 'c.txt', 3);

    const events = allEvents();
    expect(events.map((e) => [e.type, e.path, e.oldPath, e.ino])).toEqual([
      ['create', '/a', null, 2],
      ['create', '/a/b', null, 3],
      ['create', '/a/b/c.txt', null, 4],
    ]);
    // Sequences are strictly increasing from 1.
    expect(events.map((e) => e.seq)).toEqual([1, 2, 3]);
    // Timestamps are populated.
    expect(events.every((e) => Number.isFinite(e.timestamp))).toBe(true);
  });

  // ------------------------------------------------------------------ modify

  it('emits modify events for regular-file writes, including same-value rewrites', () => {
    mkdir(2, 'a', 1);
    mkfile(3, 'f.txt', 2, 0, 0);
    const before = latestChangeSeq(sql);

    // size change -> modify
    db.prepare('UPDATE fs_inode SET size = 10 WHERE ino = 3').run();
    // mtime change -> modify
    db.prepare('UPDATE fs_inode SET mtime = 99 WHERE ino = 3').run();
    // AgentFS can rewrite same-size content within one mtime tick.
    db.prepare('UPDATE fs_inode SET size = 10, mtime = 99 WHERE ino = 3').run();
    // unrelated column change -> no event
    db.prepare('UPDATE fs_inode SET atime = 5 WHERE ino = 3').run();
    // directory inode change -> no event
    db.prepare('UPDATE fs_inode SET size = 4096, mtime = 7 WHERE ino = 2').run();

    const modifies = allEvents().filter((e) => e.seq > before);
    expect(modifies.map((e) => [e.type, e.path, e.ino])).toEqual([
      ['modify', '/a/f.txt', 3],
      ['modify', '/a/f.txt', 3],
      ['modify', '/a/f.txt', 3],
    ]);
  });

  it('skips modify for an inode with no linked dentry', () => {
    addInode(9, FILE, 0, 0);
    db.prepare('UPDATE fs_inode SET size = 5 WHERE ino = 9').run();
    expect(allEvents()).toEqual([]);
  });

  it('emits a modify for every path of a hardlinked file', () => {
    mkfile(2, 'one', 1, 0, 0);
    link('two', 1, 2); // second hardlink to ino 2
    const before = latestChangeSeq(sql);

    db.prepare('UPDATE fs_inode SET size = 3 WHERE ino = 2').run();

    const modifies = allEvents().filter((e) => e.seq > before);
    expect(modifies.map((event) => [event.type, event.path, event.ino])).toEqual([
      ['modify', '/one', 2],
      ['modify', '/two', 2],
    ]);
  });

  // ------------------------------------------------------------------ rename

  it('emits a single rename event with both paths for a directory rename', () => {
    mkdir(2, 'dir', 1);
    mkdir(3, 'sub', 2);
    mkfile(4, 'f.txt', 3);
    const before = latestChangeSeq(sql);

    db.prepare("UPDATE fs_dentry SET name = 'renamed' WHERE ino = 2").run();

    const renames = allEvents().filter((e) => e.seq > before);
    expect(renames).toHaveLength(1);
    expect(renames[0]).toMatchObject({
      type: 'rename',
      path: '/renamed',
      oldPath: '/dir',
      ino: 2,
    });
  });

  it('captures a move across parents as a rename', () => {
    mkdir(2, 'src', 1);
    mkdir(3, 'dst', 1);
    mkfile(4, 'f.txt', 2);
    const before = latestChangeSeq(sql);

    db.prepare('UPDATE fs_dentry SET parent_ino = 3 WHERE ino = 4').run();

    const renames = allEvents().filter((e) => e.seq > before);
    expect(renames).toEqual([
      expect.objectContaining({ type: 'rename', path: '/dst/f.txt', oldPath: '/src/f.txt', ino: 4 }),
    ]);
  });

  // ------------------------------------------------------------------ remove

  it('resolves the full path on remove while ancestors still exist', () => {
    mkdir(2, 'a', 1);
    mkdir(3, 'b', 2);
    mkfile(4, 'c.txt', 3);
    const before = latestChangeSeq(sql);

    db.prepare('DELETE FROM fs_dentry WHERE ino = 4').run();

    const removes = allEvents().filter((e) => e.seq > before);
    expect(removes).toEqual([
      expect.objectContaining({ type: 'remove', path: '/a/b/c.txt', oldPath: null, ino: 4 }),
    ]);
  });

  // -------------------------------------------------------- prefix filtering

  it('filters by exact path and descendants against path and oldPath', () => {
    mkdir(2, 'keep', 1);
    mkfile(3, 'inside.txt', 2); // /keep/inside.txt
    mkdir(4, 'other', 1);
    mkfile(5, 'x.txt', 4); // /other/x.txt
    // rename a file OUT of /keep: oldPath is under the prefix, new path is not.
    mkfile(6, 'leaving.txt', 2); // /keep/leaving.txt
    db.prepare('UPDATE fs_dentry SET parent_ino = 4 WHERE ino = 6').run(); // -> /other/leaving.txt

    const paths = getChanges(sql, { since: 0, pathPrefix: '/keep' }).events.map((e) => [e.type, e.path, e.oldPath]);
    expect(paths).toEqual([
      ['create', '/keep', null], // exact match
      ['create', '/keep/inside.txt', null], // descendant
      ['create', '/keep/leaving.txt', null], // descendant
      ['rename', '/other/leaving.txt', '/keep/leaving.txt'], // matched via oldPath
    ]);
    // A sibling prefix that shares a textual root must not leak.
    expect(getChanges(sql, { since: 0, pathPrefix: '/kee' }).events).toEqual([]);
  });

  it('normalizes a non-canonical path prefix to absolute dot-free form', () => {
    mkdir(2, 'a', 1);
    mkfile(3, 'f.txt', 2);
    const events = getChanges(sql, { since: 0, pathPrefix: 'a/./b/../' }).events;
    expect(events.map((e) => e.path)).toEqual(['/a', '/a/f.txt']);
  });

  // -------------------------------------------------------- cursor / latest

  it('tails from latest with no history when since is missing or "latest"', () => {
    mkdir(2, 'a', 1);
    mkfile(3, 'f.txt', 2);
    const latest = latestChangeSeq(sql);

    for (const opts of [{}, { since: 'latest' as const }]) {
      const page = getChanges(sql, opts);
      expect(page.events).toEqual([]);
      expect(page.cursor).toBe(latest);
      expect(page.latest).toBe(latest);
      expect(page.gap).toBe(false);
    }
  });

  it('treats numeric since as exclusive and advances the cursor to the last seq', () => {
    mkdir(2, 'a', 1); // seq 1
    mkdir(3, 'b', 1); // seq 2
    mkdir(4, 'c', 1); // seq 3

    const page = getChanges(sql, { since: 1, limit: 10 });
    expect(page.events.map((e) => e.seq)).toEqual([2, 3]);
    expect(page.cursor).toBe(3);
    expect(page.latest).toBe(3);

    // Paging with the cursor drains the feed.
    const next = getChanges(sql, { since: page.cursor, limit: 10 });
    expect(next.events).toEqual([]);
    expect(next.cursor).toBe(3);
  });

  it('advances the cursor to latest when no events match, avoiding re-scans', () => {
    mkdir(2, 'a', 1);
    mkfile(3, 'f.txt', 2);
    const latest = latestChangeSeq(sql);

    // since is before the head but the prefix matches nothing.
    const page = getChanges(sql, { since: 0, pathPrefix: '/nonexistent' });
    expect(page.events).toEqual([]);
    expect(page.cursor).toBe(latest);
  });

  it('respects the limit and pages through in order', () => {
    for (let i = 0; i < 5; i++) mkdir(i + 2, `d${i}`, 1);

    const first = getChanges(sql, { since: 0, limit: 2 });
    expect(first.events.map((e) => e.seq)).toEqual([1, 2]);
    expect(first.cursor).toBe(2);

    const second = getChanges(sql, { since: first.cursor, limit: 2 });
    expect(second.events.map((e) => e.seq)).toEqual([3, 4]);
    expect(second.cursor).toBe(4);
  });

  it('clamps limit to 1..1000 and defaults to 100', () => {
    for (let i = 0; i < 3; i++) mkdir(i + 2, `d${i}`, 1);
    expect(getChanges(sql, { since: 0, limit: 0 }).events).toHaveLength(1); // clamped up to 1
    expect(getChanges(sql, { since: 0, limit: 9999 }).events).toHaveLength(3); // clamped down, all returned
    expect(getChanges(sql, { since: 0 }).events).toHaveLength(3); // default 100
  });

  // --------------------------------------------------------------- validation

  it('rejects invalid since values with ChangeFeedError', () => {
    for (const bad of [-1, 1.5, NaN, Number.MAX_SAFE_INTEGER + 1, 'nope' as unknown as number]) {
      expect(() => getChanges(sql, { since: bad })).toThrow(ChangeFeedError);
    }
  });

  it('rejects an invalid path prefix with ChangeFeedError', () => {
    expect(() => getChanges(sql, { since: 0, pathPrefix: 'a\0b' })).toThrow(ChangeFeedError);
    expect(() => getChanges(sql, { since: 0, pathPrefix: 123 as unknown as string })).toThrow(ChangeFeedError);
  });

  // --------------------------------------------------------------------- gap

  it('reports a gap once retention has pruned the caller since point', () => {
    const total = 10_005;
    const insert = db.prepare('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, 1, ?)');
    db.transaction(() => {
      for (let i = 0; i < total; i++) insert.run(`f${i}`, i + 2);
    })();

    const latest = latestChangeSeq(sql);
    expect(latest).toBe(total);

    // Retention keeps the most recent 10,000 seq values.
    const oldest = getChanges(sql, { since: latest }).oldest;
    expect(oldest).toBe(total - 10_000 + 1); // 6

    // since below oldest-1 predates the window -> gap.
    expect(getChanges(sql, { since: oldest - 2 }).gap).toBe(true);
    // since at the boundary is still fully covered -> no gap.
    expect(getChanges(sql, { since: oldest - 1 }).gap).toBe(false);
    expect(getChanges(sql, { since: latest }).gap).toBe(false);
  });

  it('reports oldest = latest + 1 and no gap for an empty feed', () => {
    const page = getChanges(sql, { since: 0 });
    expect(page.latest).toBe(0);
    expect(page.oldest).toBe(1);
    expect(page.events).toEqual([]);
    expect(page.gap).toBe(false);
  });

  // ------------------------------------------------------------- schema wiring

  it('registers the change-feed tables in SCHEMA_TABLES and creates them', () => {
    for (const table of CHANGE_FEED_TABLES) {
      expect(SCHEMA_TABLES).toContain(table);
      const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table);
      expect(row).toBeDefined();
    }
    // Both are WITHOUT ROWID (no implicit rowid column).
    for (const table of CHANGE_FEED_TABLES) {
      const hasRowid = db.prepare(`SELECT COUNT(*) c FROM pragma_index_list(?) WHERE origin='pk'`).get(table) as { c: number };
      expect(hasRowid.c).toBeGreaterThan(0);
    }
  });

  it('is idempotent and preserves the sequence counter across re-init', () => {
    mkdir(2, 'a', 1);
    mkdir(3, 'b', 1);
    const latest = latestChangeSeq(sql);
    expect(latest).toBe(2);

    initChangeFeedSchema(sql);
    initSchema(sql);

    // The singleton row is untouched (INSERT OR IGNORE), so seq keeps counting.
    expect(latestChangeSeq(sql)).toBe(2);
    const seqRows = db.prepare('SELECT COUNT(*) c FROM fs_change_sequence').get() as { c: number };
    expect(seqRows.c).toBe(1);

    mkdir(4, 'c', 1);
    expect(latestChangeSeq(sql)).toBe(3);
  });

  // ------------------------------------------- last_insert_rowid regression

  it('does not perturb last_insert_rowid() when feed triggers fire', () => {
    // Inserting an inode returns its own rowid even with triggers installed.
    const r = db.prepare('INSERT INTO fs_inode (ino, mode, nlink, size, atime, mtime, ctime) VALUES (?, ?, 1, 0, 0, 0, 0)').run(2, FILE);
    expect(r.lastInsertRowid).toBe(2);
    expect((db.prepare('SELECT last_insert_rowid() AS l').get() as { l: number }).l).toBe(2);

    // Linking a dentry fires the create trigger (feed INSERT + seq UPDATE). The
    // caller-visible last_insert_rowid must be the dentry rowid, not a feed seq.
    const d = db.prepare('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, 1, ?)').run('f.txt', 2);
    expect((db.prepare('SELECT last_insert_rowid() AS l').get() as { l: number }).l).toBe(Number(d.lastInsertRowid));

    // A modify (fs_inode UPDATE) writes to the feed too; a subsequent inode
    // insert still reports its own rowid.
    db.prepare('UPDATE fs_inode SET size = 10, mtime = 5 WHERE ino = 2').run();
    const r2 = db.prepare('INSERT INTO fs_inode (ino, mode, nlink, size, atime, mtime, ctime) VALUES (?, ?, 1, 0, 0, 0, 0)').run(3, FILE);
    expect(r2.lastInsertRowid).toBe(3);
    expect((db.prepare('SELECT last_insert_rowid() AS l').get() as { l: number }).l).toBe(3);

    // Sanity: the feed actually recorded the create and modify.
    const types = allEvents().map((e) => e.type);
    expect(types).toEqual(['create', 'modify']);
  });
});
