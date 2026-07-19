// ABOUTME: Trigger-driven filesystem change feed capturing create/modify/remove/rename mutations.
// ABOUTME: Additive tables + triggers observe fs_dentry/fs_inode from any writer (direct API or FUSE/Hrana).

import type { SqlExec } from './schema';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** A single filesystem mutation observed by the change-feed triggers. */
export interface ChangeEvent {
  /** Monotonic sequence number, unique and strictly increasing per volume. */
  seq: number;
  /** Kind of mutation. */
  type: 'create' | 'modify' | 'remove' | 'rename';
  /** Exact full absolute path captured at mutation time. For rename, the new path. */
  path: string;
  /** Prior absolute path for a rename; null for every other event type. */
  oldPath: string | null;
  /** Inode number the event refers to. */
  ino: number;
  /** unixepoch() seconds at the moment the trigger fired. */
  timestamp: number;
}

/** A page of change events plus the cursor and window metadata to resume polling. */
export interface ChangePage {
  events: ChangeEvent[];
  /**
   * Sequence to pass as `since` on the next poll. Advances to the last returned
   * event, or to the current latest when the page is empty, so a caller never
   * re-scans the same tail repeatedly.
   */
  cursor: number;
  /** Highest sequence ever allocated (0 before the first mutation). */
  latest: number;
  /**
   * Lowest sequence still retained. When the feed is empty this is `latest + 1`
   * (a valid "nothing older than the next seq" sentinel).
   */
  oldest: number;
  /** True when a numeric `since` predates the retained window (history was lost). */
  gap: boolean;
}

/** Options for {@link getChanges}. */
export interface GetChangesOptions {
  /**
   * Exclusive lower bound. A number returns events with seq strictly greater.
   * Missing or the literal 'latest' tails from the current latest with no
   * history. A negative, non-safe, or otherwise invalid number is rejected.
   */
  since?: number | 'latest';
  /** Page size, clamped to 1..1000. Defaults to 100. */
  limit?: number;
  /** Absolute path prefix; matches the exact path or any descendant. */
  pathPrefix?: string;
}

/** Raised for invalid caller input to {@link getChanges}. */
export class ChangeFeedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ChangeFeedError';
  }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Additive table names owned by this module, for schema verification/introspection. */
export const CHANGE_FEED_TABLES = ['fs_change_sequence', 'fs_change_feed'] as const;

/** Maximum number of most-recent seq values retained; older rows are pruned. */
export const CHANGE_FEED_RETENTION = 10_000;

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

// POSIX mode bits used to isolate regular files for the modify trigger.
const S_IFMT = 0o170000; // 61440
const S_IFREG = 0o100000; // 32768

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/**
 * Build a scalar subquery that resolves the exact absolute path for a dentry by
 * walking parent_ino up to the root inode (1). `baseSelect` must yield two
 * columns in order: (parent_ino, leading-slash name). The recursion prepends
 * each ancestor directory name, so a directory rename resolves in one pass and
 * a removal resolves while ancestor dentries still exist.
 */
function pathExpr(baseSelect: string): string {
  return `(WITH RECURSIVE _anc(parent_ino, path) AS (
      ${baseSelect}
      UNION ALL
      SELECT d.parent_ino, '/' || d.name || _anc.path
      FROM fs_dentry d JOIN _anc ON d.ino = _anc.parent_ino
      WHERE _anc.parent_ino != 1
    ) SELECT path FROM _anc WHERE parent_ino = 1)`;
}

// Sequence allocation reads the current next_seq for the event's seq, then bumps
// it. Both tables are WITHOUT ROWID so neither the INSERT into fs_change_feed nor
// the seed insert perturbs last_insert_rowid(), which AgentFS relies on to read a
// freshly created inode number after inserting into fs_inode.
const NEXT_SEQ = '(SELECT next_seq FROM fs_change_sequence WHERE id = 1)';
const BUMP_SEQ = 'UPDATE fs_change_sequence SET next_seq = next_seq + 1 WHERE id = 1;';

const CHANGE_FEED_DDL = [
  `CREATE TABLE IF NOT EXISTS fs_change_sequence (
    id INTEGER PRIMARY KEY,
    next_seq INTEGER NOT NULL
  ) WITHOUT ROWID`,

  `CREATE TABLE IF NOT EXISTS fs_change_feed (
    seq INTEGER PRIMARY KEY,
    type TEXT NOT NULL,
    path TEXT NOT NULL,
    oldPath TEXT,
    ino INTEGER NOT NULL,
    timestamp INTEGER NOT NULL
  ) WITHOUT ROWID`,

  // create: a new directory entry appears. The path guard skips entries whose
  // ancestors are not (yet) present — e.g. a bulk snapshot restore that inserts
  // rows out of parent-before-child order — so the feed never records a NULL path.
  `CREATE TRIGGER IF NOT EXISTS trg_fs_change_create
    AFTER INSERT ON fs_dentry
    WHEN ${pathExpr("SELECT NEW.parent_ino, '/' || NEW.name")} IS NOT NULL
    BEGIN
      INSERT INTO fs_change_feed (seq, type, path, oldPath, ino, timestamp)
      VALUES (
        ${NEXT_SEQ},
        'create',
        ${pathExpr("SELECT NEW.parent_ino, '/' || NEW.name")},
        NULL,
        NEW.ino,
        unixepoch()
      );
      ${BUMP_SEQ}
    END`,

  // remove: a directory entry is deleted. For a single live removal the ancestor
  // dentries still exist, so the full path resolves from OLD; the guard skips
  // rows torn down after their ancestors (bulk restore/clear).
  `CREATE TRIGGER IF NOT EXISTS trg_fs_change_remove
    AFTER DELETE ON fs_dentry
    WHEN ${pathExpr("SELECT OLD.parent_ino, '/' || OLD.name")} IS NOT NULL
    BEGIN
      INSERT INTO fs_change_feed (seq, type, path, oldPath, ino, timestamp)
      VALUES (
        ${NEXT_SEQ},
        'remove',
        ${pathExpr("SELECT OLD.parent_ino, '/' || OLD.name")},
        NULL,
        OLD.ino,
        unixepoch()
      );
      ${BUMP_SEQ}
    END`,

  // rename: an entry's name or parent changes. Emits one event carrying both the
  // new and the old absolute path. A directory rename touches only its own
  // dentry, so a single event covers the whole subtree.
  `CREATE TRIGGER IF NOT EXISTS trg_fs_change_rename
    AFTER UPDATE ON fs_dentry
    WHEN (OLD.name != NEW.name OR OLD.parent_ino != NEW.parent_ino)
      AND ${pathExpr("SELECT NEW.parent_ino, '/' || NEW.name")} IS NOT NULL
    BEGIN
      INSERT INTO fs_change_feed (seq, type, path, oldPath, ino, timestamp)
      VALUES (
        ${NEXT_SEQ},
        'rename',
        ${pathExpr("SELECT NEW.parent_ino, '/' || NEW.name")},
        ${pathExpr("SELECT OLD.parent_ino, '/' || OLD.name")},
        NEW.ino,
        unixepoch()
      );
      ${BUMP_SEQ}
    END`,

  // Recreate this trigger during additive schema initialization so older volumes
  // gain metadata-change events without a schema-version migration.
  `DROP TRIGGER IF EXISTS trg_fs_change_modify`,

  // modify: AgentFS writes size/mtime together, including same-size rewrites that
  // happen within one timestamp tick. Permission changes also count as modifies,
  // including chmod on directories and symlinks. Parent-directory timestamp
  // maintenance remains silent. Every hard-link path receives an event so a
  // path-scoped watcher cannot miss writes made through another alias.
  `CREATE TRIGGER trg_fs_change_modify
    AFTER UPDATE OF size, mtime, mode ON fs_inode
    WHEN ((NEW.mode & ${S_IFMT}) = ${S_IFREG} OR OLD.mode != NEW.mode)
    BEGIN
      INSERT INTO fs_change_feed (seq, type, path, oldPath, ino, timestamp)
      SELECT
        ${NEXT_SEQ} + row_number() OVER (ORDER BY link.id) - 1,
        'modify',
        ${pathExpr("SELECT link.parent_ino, '/' || link.name")},
        NULL,
        NEW.ino,
        unixepoch()
      FROM fs_dentry link
      WHERE link.ino = NEW.ino
        AND ${pathExpr("SELECT link.parent_ino, '/' || link.name")} IS NOT NULL;
      UPDATE fs_change_sequence
      SET next_seq = next_seq + (
        SELECT count(*) FROM fs_dentry link
        WHERE link.ino = NEW.ino
          AND ${pathExpr("SELECT link.parent_ino, '/' || link.name")} IS NOT NULL
      )
      WHERE id = 1;
    END`,

  // Bounded retention: keep only the most recent CHANGE_FEED_RETENTION seq values.
  `CREATE TRIGGER IF NOT EXISTS trg_fs_change_feed_retention
    AFTER INSERT ON fs_change_feed
    BEGIN
      DELETE FROM fs_change_feed WHERE seq <= NEW.seq - ${CHANGE_FEED_RETENTION};
    END`,
];

const CHANGE_FEED_SEED = `INSERT OR IGNORE INTO fs_change_sequence (id, next_seq) VALUES (1, 1)`;

/**
 * Create the change-feed tables, triggers, and singleton sequence row. Idempotent
 * and safe to call on every schema init.
 */
export function initChangeFeedSchema(sql: SqlExec): void {
  for (const stmt of CHANGE_FEED_DDL) sql.exec(stmt);
  sql.exec(CHANGE_FEED_SEED);
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/** Highest sequence ever allocated. Returns 0 before the first mutation. */
export function latestChangeSeq(sql: SqlExec): number {
  const rows = sql.exec('SELECT next_seq FROM fs_change_sequence WHERE id = 1').toArray() as Array<{ next_seq: number }>;
  const next = rows.length > 0 ? Number(rows[0].next_seq) : 1;
  return next - 1;
}

function oldestChangeSeq(sql: SqlExec, latest: number): number {
  const rows = sql.exec('SELECT MIN(seq) AS oldest FROM fs_change_feed').toArray() as Array<{ oldest: number | null }>;
  const oldest = rows.length > 0 ? rows[0].oldest : null;
  // Empty feed: report latest + 1, a sentinel meaning "nothing older than next".
  return oldest === null || oldest === undefined ? latest + 1 : Number(oldest);
}

/**
 * Normalize a path prefix to absolute, dot-segment-free form, mirroring the
 * volume's canonical path semantics. '' or '/' both denote the root ("match
 * everything"). Rejects non-strings and embedded null bytes.
 */
function normalizePrefix(prefix: string): string {
  if (typeof prefix !== 'string') {
    throw new ChangeFeedError('pathPrefix must be a string');
  }
  if (prefix.includes('\0')) {
    throw new ChangeFeedError('pathPrefix contains a null byte');
  }
  const segments: string[] = [];
  for (const segment of prefix.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join('/')}`;
}

function clampLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (typeof limit !== 'number' || !Number.isFinite(limit)) return DEFAULT_LIMIT;
  const floored = Math.floor(limit);
  if (floored < 1) return 1;
  if (floored > MAX_LIMIT) return MAX_LIMIT;
  return floored;
}

function resolveSince(since: number | 'latest' | undefined, latest: number): { seq: number; tail: boolean } {
  if (since === undefined || since === 'latest') {
    // Default/tail: start from the current head with no backfilled history.
    return { seq: latest, tail: true };
  }
  if (typeof since !== 'number' || !Number.isSafeInteger(since) || since < 0) {
    throw new ChangeFeedError('since must be a non-negative safe integer or "latest"');
  }
  return { seq: since, tail: false };
}

interface ChangeRow {
  seq: number;
  type: string;
  path: string;
  oldPath: string | null;
  ino: number;
  timestamp: number;
}

/**
 * Read a page of change events after an exclusive `since` cursor.
 *
 * - Missing/'latest' since tails the current head (empty page, cursor = latest).
 * - Numeric since is exclusive; invalid values raise {@link ChangeFeedError}.
 * - `pathPrefix` matches the exact path or any descendant, against both `path`
 *   and `oldPath` (so a rename out of the prefix still surfaces).
 * - `cursor` advances to the last returned seq, or the current latest when the
 *   page is empty, preventing repeated tail scans.
 * - `gap` is true when a numeric since predates the retained window.
 */
export function getChanges(sql: SqlExec, options: GetChangesOptions = {}): ChangePage {
  const latest = latestChangeSeq(sql);
  const oldest = oldestChangeSeq(sql, latest);
  const limit = clampLimit(options.limit);
  const { seq: sinceSeq, tail } = resolveSince(options.since, latest);

  const prefix = options.pathPrefix === undefined ? '/' : normalizePrefix(options.pathPrefix);

  const bindings: unknown[] = [sinceSeq];
  let where = `seq > ?
    AND path != '/.airyfs-trash' AND substr(path, 1, 15) != '/.airyfs-trash/'
    AND (oldPath IS NULL OR (oldPath != '/.airyfs-trash' AND substr(oldPath, 1, 15) != '/.airyfs-trash/'))`;
  if (prefix !== '/') {
    const withSlash = `${prefix}/`;
    const slashLen = withSlash.length;
    where += ' AND ('
      + 'path = ? OR substr(path, 1, ?) = ? '
      + 'OR oldPath = ? OR substr(oldPath, 1, ?) = ?'
      + ')';
    bindings.push(prefix, slashLen, withSlash, prefix, slashLen, withSlash);
  }
  bindings.push(limit);

  const rows = sql.exec(
    `SELECT seq, type, path, oldPath, ino, timestamp
     FROM fs_change_feed
     WHERE ${where}
     ORDER BY seq ASC
     LIMIT ?`,
    ...bindings
  ).toArray() as unknown as ChangeRow[];

  const events: ChangeEvent[] = rows.map((row) => ({
    seq: Number(row.seq),
    type: row.type as ChangeEvent['type'],
    path: String(row.path),
    oldPath: row.oldPath === null || row.oldPath === undefined ? null : String(row.oldPath),
    ino: Number(row.ino),
    timestamp: Number(row.timestamp),
  }));

  // Cursor advances to the last returned seq, or to the current latest when
  // nothing matched, so subsequent polls never re-scan the same tail.
  const cursor = events.length > 0 ? events[events.length - 1].seq : latest;

  // A numeric since strictly below (oldest - 1) means the caller's next expected
  // seq was already pruned: history was lost. Tailing never reports a gap.
  const gap = !tail && sinceSeq < oldest - 1;

  return { events, cursor, latest, oldest, gap };
}
