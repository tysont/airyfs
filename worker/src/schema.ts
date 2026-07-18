// ABOUTME: AgentFS schema initialization for DO SQLite.
// ABOUTME: Creates all tables from AgentFS SPEC v0.4 and seeds root inode + config.

import { initSnapshotSchema, SNAPSHOT_TABLES } from './snapshots';
import { initUploadSchema, UPLOAD_TABLES } from './uploads';
import { initJobSchema, JOB_TABLES } from './jobs';
import { initChangeFeedSchema, CHANGE_FEED_TABLES } from './change-feed';
import { initWebhookSchema, WEBHOOK_TABLES } from './webhooks';
import { initScheduleSchema, SCHEDULE_TABLES } from './schedules';
import { initTrashSchema, TRASH_TABLES } from './trash';

// Minimal interface matching the subset of SqlStorage that initSchema needs.
// DO SqlStorage satisfies this; tests can provide a lightweight adapter.
export interface SqlExec {
  exec(query: string, ...bindings: unknown[]): { toArray(): Record<string, unknown>[] };
}

export type TransactionSync = <T>(callback: () => T) => T;

export const DEFAULT_CHUNK_SIZE = 256 * 1024;
export const MIN_CHUNK_SIZE = 4 * 1024;
export const MAX_CHUNK_SIZE = 1024 * 1024;

export class InvalidChunkSizeError extends Error {}
export class ChunkSizeConflictError extends Error {}

const DDL_STATEMENTS = [
  // Filesystem configuration
  `CREATE TABLE IF NOT EXISTS fs_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )`,

  // Inode metadata (POSIX-style)
  `CREATE TABLE IF NOT EXISTS fs_inode (
    ino INTEGER PRIMARY KEY AUTOINCREMENT,
    mode INTEGER NOT NULL,
    nlink INTEGER NOT NULL DEFAULT 0,
    uid INTEGER NOT NULL DEFAULT 0,
    gid INTEGER NOT NULL DEFAULT 0,
    size INTEGER NOT NULL DEFAULT 0,
    atime INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    ctime INTEGER NOT NULL,
    rdev INTEGER NOT NULL DEFAULT 0,
    atime_nsec INTEGER NOT NULL DEFAULT 0,
    mtime_nsec INTEGER NOT NULL DEFAULT 0,
    ctime_nsec INTEGER NOT NULL DEFAULT 0
  )`,

  // Directory entries: (parent_ino, name) -> ino
  `CREATE TABLE IF NOT EXISTS fs_dentry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_ino INTEGER NOT NULL,
    ino INTEGER NOT NULL,
    UNIQUE(parent_ino, name)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_fs_dentry_parent ON fs_dentry(parent_ino, name)`,

  // File content in immutable, per-volume chunks
  `CREATE TABLE IF NOT EXISTS fs_data (
    ino INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (ino, chunk_index)
  )`,

  // Symbolic link targets
  `CREATE TABLE IF NOT EXISTS fs_symlink (
    ino INTEGER PRIMARY KEY,
    target TEXT NOT NULL
  )`,

  // Overlay filesystem whiteouts (copy-on-write support)
  `CREATE TABLE IF NOT EXISTS fs_whiteout (
    path TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS fs_overlay_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )`,

  // Origin inode tracking (overlay filesystem)
  `CREATE TABLE IF NOT EXISTS fs_origin (
    delta_ino INTEGER PRIMARY KEY,
    base_ino INTEGER NOT NULL
  )`,

  // Key-value store for volume state
  `CREATE TABLE IF NOT EXISTS kv_store (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at INTEGER DEFAULT (unixepoch()),
    updated_at INTEGER DEFAULT (unixepoch())
  )`,
  `CREATE INDEX IF NOT EXISTS idx_kv_store_created_at ON kv_store(created_at)`,

  // Tool call audit trail (append-only)
  `CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parameters TEXT,
    result TEXT,
    error TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    duration_ms INTEGER
  )`,
  `CREATE INDEX IF NOT EXISTS idx_tool_calls_name ON tool_calls(name)`,
  `CREATE INDEX IF NOT EXISTS idx_tool_calls_started_at ON tool_calls(started_at)`,

  // Direct-runtime mutations consumed by remote FUSE invalidation pollers
  `CREATE TABLE IF NOT EXISTS fs_mutation_journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_ino INTEGER NOT NULL,
    name TEXT NOT NULL,
    ino INTEGER,
    created_at REAL NOT NULL
  )`,

  // Persistent, expiring open-handle leases. A remote FUSE mount pins the inode
  // behind a live handle so a direct unlink or streaming rename-over retains the
  // nlink=0 inode and its data until the handle closes or the lease expires.
  `CREATE TABLE IF NOT EXISTS fs_open_inode (
    session_id TEXT NOT NULL,
    ino INTEGER NOT NULL,
    open_count INTEGER NOT NULL DEFAULT 0,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (session_id, ino)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_fs_open_inode_expires ON fs_open_inode(expires_at)`,

  // Revoked capability tokens, checked on every capability-authenticated request.
  // Additive and per-volume; the root credential is never represented here.
  `CREATE TABLE IF NOT EXISTS capability_revocations (
    id TEXT PRIMARY KEY,
    revoked_at INTEGER NOT NULL DEFAULT (unixepoch())
  )`,

  // Optional per-volume password verifier (PBKDF2). A single row (id=1) holds the
  // salt/hash/iterations; the plaintext password is never stored. Presence lets a
  // caller self-service a volume-scoped token via /auth/login without the root secret.
  `CREATE TABLE IF NOT EXISTS volume_auth (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    salt TEXT NOT NULL,
    hash TEXT NOT NULL,
    iterations INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
  )`,

  // Public web hosting config. A single kind='site' row (id='web') defines the
  // published web root; kind='share' rows are random-id links to individual files
  // with optional expiry. Both are served unauthenticated through /s and /d.
  `CREATE TABLE IF NOT EXISTS site_config (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    index_document TEXT NOT NULL DEFAULT 'index.html',
    spa INTEGER NOT NULL DEFAULT 0,
    directory_listing INTEGER NOT NULL DEFAULT 0,
    cache_control TEXT,
    expires_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )`,
  `CREATE INDEX IF NOT EXISTS idx_site_config_kind ON site_config(kind)`,

  // Deleting an inode is the single authoritative point at which its chunks,
  // symlink target, and any residual leases are removed together.
  `CREATE TRIGGER IF NOT EXISTS trg_fs_inode_delete_cleanup
    AFTER DELETE ON fs_inode
    BEGIN
      DELETE FROM fs_data WHERE ino = OLD.ino;
      DELETE FROM fs_symlink WHERE ino = OLD.ino;
      DELETE FROM fs_open_inode WHERE ino = OLD.ino;
    END`,

  `CREATE TRIGGER IF NOT EXISTS trg_fs_inode_quota_insert
    BEFORE INSERT ON fs_inode
    BEGIN
      SELECT CASE WHEN
        (SELECT CAST(value AS INTEGER) FROM fs_config WHERE key = 'quota_inodes') IS NOT NULL
        AND (SELECT count(*) + 1 FROM fs_inode) >
          (SELECT CAST(value AS INTEGER) FROM fs_config WHERE key = 'quota_inodes')
      THEN RAISE(ABORT, 'AIRYFS_ENOSPC_INODES') END;
      SELECT CASE WHEN
        NEW.size > 0
        AND (SELECT CAST(value AS INTEGER) FROM fs_config WHERE key = 'quota_bytes') IS NOT NULL
        AND (SELECT COALESCE(sum(size), 0) + NEW.size FROM fs_inode) >
          (SELECT CAST(value AS INTEGER) FROM fs_config WHERE key = 'quota_bytes')
      THEN RAISE(ABORT, 'AIRYFS_ENOSPC_BYTES') END;
    END`,
  `CREATE TRIGGER IF NOT EXISTS trg_fs_inode_quota_update
    BEFORE UPDATE OF size ON fs_inode
    WHEN NEW.size > OLD.size
    BEGIN
      SELECT CASE WHEN
        (SELECT CAST(value AS INTEGER) FROM fs_config WHERE key = 'quota_bytes') IS NOT NULL
        AND (SELECT COALESCE(sum(size), 0) - OLD.size + NEW.size FROM fs_inode) >
          (SELECT CAST(value AS INTEGER) FROM fs_config WHERE key = 'quota_bytes')
      THEN RAISE(ABORT, 'AIRYFS_ENOSPC_BYTES') END;
    END`,
];

const SEED_STATEMENTS = [
  `INSERT OR REPLACE INTO fs_config (key, value) VALUES ('schema_version', '0.4')`,

  // Root directory: inode 1, mode 0o040755 = 16877 (directory, rwxr-xr-x)
  `INSERT OR IGNORE INTO fs_inode (ino, mode, nlink, uid, gid, size, atime, mtime, ctime)
    VALUES (1, 16877, 1, 0, 0, 0, unixepoch(), unixepoch(), unixepoch())`,
];

// All table names that initSchema creates, for verification and db diagnostics.
// The additive snapshot tables are owned by snapshots.ts and appended here so a
// single list drives row-count introspection.
export const SCHEMA_TABLES = [
  'fs_config',
  'fs_inode',
  'fs_dentry',
  'fs_data',
  'fs_symlink',
  'fs_whiteout',
  'fs_overlay_config',
  'fs_origin',
  'kv_store',
  'tool_calls',
  'fs_mutation_journal',
  'fs_open_inode',
  'capability_revocations',
  'volume_auth',
  'site_config',
  ...UPLOAD_TABLES,
  ...SNAPSHOT_TABLES,
  ...JOB_TABLES,
  ...CHANGE_FEED_TABLES,
  ...WEBHOOK_TABLES,
  ...SCHEDULE_TABLES,
  ...TRASH_TABLES,
] as const;

/**
 * Initialize or migrate the AgentFS schema in DO SQLite. Every operation is
 * idempotent so interrupted initialization is repaired on the next startup.
 */
export function initSchema(sql: SqlExec, transactionSync?: TransactionSync): void {
  const initialize = (): void => {
    for (const stmt of DDL_STATEMENTS) {
      sql.exec(stmt);
    }

    migrateInodeColumns(sql);
    migrateWhiteouts(sql);
    migrateToolCalls(sql);
    migrateSiteConfig(sql);

    // Additive snapshot metadata + payload tables. Raw snapshot SQL is isolated
    // in snapshots.ts; initialization stays idempotent alongside the core DDL.
    initSnapshotSchema(sql);

    // Additive resumable-upload session table, owned by uploads.ts.
    initUploadSchema(sql);

    // Additive durable job queue + log tables, owned by jobs.ts.
    initJobSchema(sql);

    // Additive filesystem change-feed tables + capture triggers, owned by
    // change-feed.ts. Installed after the core DDL so the fs_dentry/fs_inode
    // tables the triggers reference already exist.
    initChangeFeedSchema(sql);

    // Webhook outbox triggers depend on the change-feed table and triggers.
    initWebhookSchema(sql);

    initScheduleSchema(sql);
    initTrashSchema(sql);

    for (const stmt of SEED_STATEMENTS) {
      sql.exec(stmt);
    }
  };

  if (transactionSync) transactionSync(initialize);
  else initialize();
}

export interface ChunkSizeConfiguration {
  chunkSize: number;
  created: boolean;
}

export interface QuotaConfiguration {
  bytes: number | null;
  inodes: number | null;
}

function quotaValue(sql: SqlExec, key: string): number | null {
  const rows = sql.exec('SELECT value FROM fs_config WHERE key = ?', key).toArray() as Array<{ value: string }>;
  if (rows.length === 0) return null;
  const value = Number(rows[0].value);
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`Stored ${key} is invalid: ${rows[0].value}`);
  return value;
}

export function readQuota(sql: SqlExec): QuotaConfiguration {
  return { bytes: quotaValue(sql, 'quota_bytes'), inodes: quotaValue(sql, 'quota_inodes') };
}

function validateQuotaValue(value: unknown, name: string): number | null | undefined {
  if (value === undefined || value === null) return value;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer or null`);
  }
  return value;
}

export function configureQuota(
  sql: SqlExec,
  input: { bytes?: unknown; inodes?: unknown },
): QuotaConfiguration {
  const bytes = validateQuotaValue(input.bytes, 'bytes');
  const inodes = validateQuotaValue(input.inodes, 'inodes');
  const usage = sql.exec('SELECT COALESCE(sum(size), 0) AS bytes, count(*) AS inodes FROM fs_inode').toArray()[0] as {
    bytes: number; inodes: number;
  };
  if (typeof bytes === 'number' && bytes < usage.bytes) throw new Error(`bytes quota cannot be below current usage (${usage.bytes})`);
  if (typeof inodes === 'number' && inodes < usage.inodes) throw new Error(`inodes quota cannot be below current usage (${usage.inodes})`);
  for (const [key, value] of [['quota_bytes', bytes], ['quota_inodes', inodes]] as const) {
    if (value === undefined) continue;
    if (value === null) sql.exec('DELETE FROM fs_config WHERE key = ?', key);
    else sql.exec('INSERT OR REPLACE INTO fs_config (key, value) VALUES (?, ?)', key, String(value));
  }
  return readQuota(sql);
}

export function validateChunkSize(value: unknown): number {
  if (
    typeof value !== 'number'
    || !Number.isSafeInteger(value)
    || value < MIN_CHUNK_SIZE
    || value > MAX_CHUNK_SIZE
    || (value & (value - 1)) !== 0
  ) {
    throw new InvalidChunkSizeError(
      `chunkSize must be a power of two between ${MIN_CHUNK_SIZE} and ${MAX_CHUNK_SIZE} bytes`
    );
  }
  return value;
}

/** Set a volume's chunk size before AgentFS caches it. Existing data makes it immutable. */
export function configureChunkSize(
  sql: SqlExec,
  requestedChunkSize?: unknown
): ChunkSizeConfiguration {
  const requested = requestedChunkSize === undefined ? undefined : validateChunkSize(requestedChunkSize);
  const rows = sql.exec("SELECT value FROM fs_config WHERE key = 'chunk_size'").toArray() as Array<{ value: string }>;

  if (rows.length === 0) {
    const chunkSize = requested ?? DEFAULT_CHUNK_SIZE;
    sql.exec("INSERT INTO fs_config (key, value) VALUES ('chunk_size', ?)", String(chunkSize));
    return { chunkSize, created: true };
  }

  const current = Number(rows[0].value);
  if (!Number.isSafeInteger(current) || current <= 0) {
    throw new Error(`Stored volume chunk size is invalid: ${rows[0].value}`);
  }
  if (requested === undefined || current === requested) {
    return { chunkSize: current, created: false };
  }

  const data = sql.exec(`SELECT EXISTS(
    SELECT 1 FROM fs_dentry
    UNION ALL
    SELECT 1 FROM fs_data
  ) AS has_data`).toArray() as Array<{ has_data: number }>;
  if (data[0]?.has_data) {
    throw new ChunkSizeConflictError(
      `Volume chunk size is ${current} bytes and cannot change after filesystem data exists`
    );
  }

  sql.exec("UPDATE fs_config SET value = ? WHERE key = 'chunk_size'", String(requested));
  return { chunkSize: requested, created: false };
}

function tableColumns(sql: SqlExec, table: string): Array<{ name: string; notnull: number }> {
  return sql.exec(`PRAGMA table_info(${table})`).toArray() as Array<{ name: string; notnull: number }>;
}

function migrateInodeColumns(sql: SqlExec): void {
  const columns = new Set(tableColumns(sql, 'fs_inode').map((column) => column.name));
  const additions = [
    ['nlink', 'INTEGER NOT NULL DEFAULT 0'],
    ['rdev', 'INTEGER NOT NULL DEFAULT 0'],
    ['atime_nsec', 'INTEGER NOT NULL DEFAULT 0'],
    ['mtime_nsec', 'INTEGER NOT NULL DEFAULT 0'],
    ['ctime_nsec', 'INTEGER NOT NULL DEFAULT 0'],
  ] as const;

  for (const [name, definition] of additions) {
    if (!columns.has(name)) sql.exec(`ALTER TABLE fs_inode ADD COLUMN ${name} ${definition}`);
  }
}

function migrateWhiteouts(sql: SqlExec): void {
  const columns = tableColumns(sql, 'fs_whiteout');
  if (!columns.some((column) => column.name === 'parent_path')) return;

  sql.exec('ALTER TABLE fs_whiteout RENAME TO fs_whiteout_legacy');
  sql.exec(`CREATE TABLE fs_whiteout (
    path TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
  )`);
  sql.exec('INSERT INTO fs_whiteout (path, created_at) SELECT path, created_at FROM fs_whiteout_legacy');
  sql.exec('DROP TABLE fs_whiteout_legacy');
}

function migrateToolCalls(sql: SqlExec): void {
  const columns = tableColumns(sql, 'tool_calls');
  const status = columns.find((column) => column.name === 'status');
  const completedAt = columns.find((column) => column.name === 'completed_at');
  const durationMs = columns.find((column) => column.name === 'duration_ms');
  if (status?.notnull === 1 && completedAt?.notnull === 0 && durationMs?.notnull === 0) return;

  sql.exec('DROP INDEX IF EXISTS idx_tool_calls_name');
  sql.exec('DROP INDEX IF EXISTS idx_tool_calls_started_at');
  sql.exec('ALTER TABLE tool_calls RENAME TO tool_calls_legacy');
  sql.exec(`CREATE TABLE tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parameters TEXT,
    result TEXT,
    error TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    duration_ms INTEGER
  )`);
  const statusExpression = status
    ? "COALESCE(status, CASE WHEN error IS NOT NULL THEN 'error' WHEN completed_at IS NOT NULL THEN 'success' ELSE 'pending' END)"
    : "CASE WHEN error IS NOT NULL THEN 'error' WHEN completed_at IS NOT NULL THEN 'success' ELSE 'pending' END";
  sql.exec(`INSERT INTO tool_calls
    (id, name, parameters, result, error, status, started_at, completed_at, duration_ms)
    SELECT id, name, parameters, result, error, ${statusExpression}, started_at, completed_at, duration_ms
    FROM tool_calls_legacy`);
  sql.exec('DROP TABLE tool_calls_legacy');
  sql.exec('CREATE INDEX idx_tool_calls_name ON tool_calls(name)');
  sql.exec('CREATE INDEX idx_tool_calls_started_at ON tool_calls(started_at)');
}

function migrateSiteConfig(sql: SqlExec): void {
  const columns = new Set(tableColumns(sql, 'site_config').map((column) => column.name));
  if (!columns.has('directory_listing')) {
    sql.exec('ALTER TABLE site_config ADD COLUMN directory_listing INTEGER NOT NULL DEFAULT 0');
  }
}
