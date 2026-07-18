// ABOUTME: Full-volume AiryFS snapshots: transactional create/list/delete, diff, restore, and AIRYFS export.
// ABOUTME: Owns all snapshot raw SQL; payload tables mirror the seven AgentFS filesystem tables additively.

import {
  archiveEndMarker,
  archiveMagic,
  ArchiveError,
  encodeDirectoryEntry,
  encodeFileEntry,
  encodeRootEntry,
  encodeSymlinkEntry,
  validateRelativePath,
} from './archive';

// ---------------------------------------------------------------------------
// Storage interface
// ---------------------------------------------------------------------------
//
// A structural subset of the DO SqlStorage + transactionSync surface. Both the
// production `ctx.storage` and the in-memory test storage satisfy it. Keeping
// this local avoids a runtime dependency on the DO types and keeps the module
// testable against better-sqlite3.

/** Cursor returned by exec: iterable (streaming) plus a materializing helper. */
export interface SnapshotCursor<T> extends Iterable<T> {
  toArray(): T[];
}

export interface SnapshotSql {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SnapshotCursor<T>;
}

export interface SnapshotStorage {
  readonly sql: SnapshotSql;
  transactionSync<T>(callback: () => T): T;
}

// ---------------------------------------------------------------------------
// Filesystem type constants (mirrors AgentFS mode encoding)
// ---------------------------------------------------------------------------

const S_IFMT = 0o170000;
const S_IFDIR = 0o040000;
const S_IFREG = 0o100000;
const S_IFLNK = 0o120000;
const ROOT_INO = 1;

export type EntryKind = 'file' | 'directory' | 'symlink' | 'other';

function kindOf(mode: number): EntryKind {
  const type = mode & S_IFMT;
  if (type === S_IFDIR) return 'directory';
  if (type === S_IFREG) return 'file';
  if (type === S_IFLNK) return 'symlink';
  return 'other';
}

// ---------------------------------------------------------------------------
// Table specifications: the seven filesystem tables captured per snapshot
// ---------------------------------------------------------------------------

interface TableSpec {
  /** Live filesystem table. */
  live: string;
  /** Per-snapshot payload table. */
  snap: string;
  /** Column list copied verbatim (excludes the injected snapshot_id). */
  columns: readonly string[];
  /** Original primary key columns, prefixed by snapshot_id in the payload table. */
  pk: readonly string[];
}

const TABLE_SPECS: readonly TableSpec[] = [
  {
    live: 'fs_inode',
    snap: 'fs_snapshot_inode',
    columns: [
      'ino', 'mode', 'nlink', 'uid', 'gid', 'size',
      'atime', 'mtime', 'ctime', 'rdev',
      'atime_nsec', 'mtime_nsec', 'ctime_nsec',
    ],
    pk: ['ino'],
  },
  {
    live: 'fs_dentry',
    snap: 'fs_snapshot_dentry',
    columns: ['id', 'name', 'parent_ino', 'ino'],
    pk: ['id'],
  },
  {
    live: 'fs_data',
    snap: 'fs_snapshot_data',
    columns: ['ino', 'chunk_index', 'data'],
    pk: ['ino', 'chunk_index'],
  },
  {
    live: 'fs_symlink',
    snap: 'fs_snapshot_symlink',
    columns: ['ino', 'target'],
    pk: ['ino'],
  },
  {
    live: 'fs_whiteout',
    snap: 'fs_snapshot_whiteout',
    columns: ['path', 'created_at'],
    pk: ['path'],
  },
  {
    live: 'fs_overlay_config',
    snap: 'fs_snapshot_overlay_config',
    columns: ['key', 'value'],
    pk: ['key'],
  },
  {
    live: 'fs_origin',
    snap: 'fs_snapshot_origin',
    columns: ['delta_ino', 'base_ino'],
    pk: ['delta_ino'],
  },
] as const;

/** All additive tables this module creates, for verification/introspection. */
export const SNAPSHOT_TABLES = [
  'fs_snapshot',
  ...TABLE_SPECS.map((spec) => spec.snap),
] as const;

// Column type definitions for the payload tables. snapshot_id is always first
// and part of the composite primary key; the remaining columns mirror the live
// table exactly. data is the only BLOB.
const COLUMN_DDL: Record<string, string> = {
  ino: 'INTEGER NOT NULL',
  mode: 'INTEGER NOT NULL',
  nlink: 'INTEGER NOT NULL',
  uid: 'INTEGER NOT NULL',
  gid: 'INTEGER NOT NULL',
  size: 'INTEGER NOT NULL',
  atime: 'INTEGER NOT NULL',
  mtime: 'INTEGER NOT NULL',
  ctime: 'INTEGER NOT NULL',
  rdev: 'INTEGER NOT NULL',
  atime_nsec: 'INTEGER NOT NULL',
  mtime_nsec: 'INTEGER NOT NULL',
  ctime_nsec: 'INTEGER NOT NULL',
  id: 'INTEGER NOT NULL',
  name: 'TEXT NOT NULL',
  parent_ino: 'INTEGER NOT NULL',
  chunk_index: 'INTEGER NOT NULL',
  data: 'BLOB NOT NULL',
  target: 'TEXT NOT NULL',
  path: 'TEXT NOT NULL',
  created_at: 'INTEGER NOT NULL',
  key: 'TEXT NOT NULL',
  value: 'TEXT NOT NULL',
  delta_ino: 'INTEGER NOT NULL',
  base_ino: 'INTEGER NOT NULL',
};

function payloadTableDdl(spec: TableSpec): string {
  const cols = spec.columns
    .map((column) => `${column} ${COLUMN_DDL[column]}`)
    .join(',\n    ');
  const pk = ['snapshot_id', ...spec.pk].join(', ');
  return `CREATE TABLE IF NOT EXISTS ${spec.snap} (
    snapshot_id TEXT NOT NULL,
    ${cols},
    PRIMARY KEY (${pk})
  )`;
}

/**
 * Minimal SQL surface needed for DDL and idempotent column migration. Any
 * exec-shaped sql with a materializing cursor (production or test) fits.
 */
export interface SnapshotSchemaSql {
  exec(query: string, ...bindings: unknown[]): { toArray(): Record<string, unknown>[] };
}

/**
 * Create the snapshot metadata and payload tables. Additive and idempotent so
 * it is safe to call on every schema initialization. Called by initSchema.
 * Also migrates a pre-existing fs_snapshot table that predates the note,
 * directory_count, and symlink_count columns.
 */
export function initSnapshotSchema(sql: SnapshotSchemaSql): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_snapshot (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    note TEXT,
    created_at INTEGER NOT NULL,
    chunk_size INTEGER,
    inode_count INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    directory_count INTEGER NOT NULL DEFAULT 0,
    symlink_count INTEGER NOT NULL DEFAULT 0,
    byte_count INTEGER NOT NULL DEFAULT 0
  )`);
  migrateSnapshotColumns(sql);
  for (const spec of TABLE_SPECS) {
    sql.exec(payloadTableDdl(spec));
  }
}

/**
 * Add columns introduced after the initial fs_snapshot schema. Safe to run on
 * every startup: a fresh CREATE already has every column, and ALTER TABLE ADD
 * COLUMN is only issued for a column that is actually missing.
 */
function migrateSnapshotColumns(sql: SnapshotSchemaSql): void {
  const columns = new Set(
    (sql.exec('PRAGMA table_info(fs_snapshot)').toArray() as Array<{ name: string }>).map((c) => c.name)
  );
  const additions: ReadonlyArray<readonly [string, string]> = [
    ['note', 'TEXT'],
    ['directory_count', 'INTEGER NOT NULL DEFAULT 0'],
    ['symlink_count', 'INTEGER NOT NULL DEFAULT 0'],
  ];
  for (const [name, definition] of additions) {
    if (!columns.has(name)) sql.exec(`ALTER TABLE fs_snapshot ADD COLUMN ${name} ${definition}`);
  }
}

// ---------------------------------------------------------------------------
// Errors and public metadata types
// ---------------------------------------------------------------------------

export class SnapshotError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = 'SnapshotError';
  }
}

export class SnapshotNotFoundError extends SnapshotError {
  constructor(nameOrId: string) {
    super('SNAPSHOT_NOT_FOUND', `Snapshot not found: ${nameOrId}`);
  }
}

export class SnapshotExistsError extends SnapshotError {
  constructor(name: string) {
    super('SNAPSHOT_EXISTS', `A snapshot named "${name}" already exists`);
  }
}

export interface SnapshotInfo {
  id: string;
  name: string;
  note: string | null;
  createdAt: number;
  chunkSize: number | null;
  inodeCount: number;
  fileCount: number;
  directoryCount: number;
  symlinkCount: number;
  byteCount: number;
}

interface SnapshotRow {
  id: string;
  name: string;
  note: string | null;
  created_at: number;
  chunk_size: number | null;
  inode_count: number;
  file_count: number;
  directory_count: number;
  symlink_count: number;
  byte_count: number;
}

function toInfo(row: SnapshotRow): SnapshotInfo {
  return {
    id: row.id,
    name: row.name,
    note: row.note === null || row.note === undefined ? null : String(row.note),
    createdAt: Number(row.created_at),
    chunkSize: row.chunk_size === null || row.chunk_size === undefined ? null : Number(row.chunk_size),
    inodeCount: Number(row.inode_count),
    fileCount: Number(row.file_count),
    directoryCount: Number(row.directory_count ?? 0),
    symlinkCount: Number(row.symlink_count ?? 0),
    byteCount: Number(row.byte_count),
  };
}

const MAX_NAME_LENGTH = 255;
const MAX_NOTE_LENGTH = 4096;

function validateName(name: unknown): string {
  if (typeof name !== 'string') throw new SnapshotError('INVALID_NAME', 'Snapshot name must be a string');
  const trimmed = name.trim();
  if (trimmed.length === 0) throw new SnapshotError('INVALID_NAME', 'Snapshot name must not be empty');
  if (trimmed.length > MAX_NAME_LENGTH) {
    throw new SnapshotError('INVALID_NAME', `Snapshot name must be at most ${MAX_NAME_LENGTH} characters`);
  }
  if (trimmed.includes('\0') || /[\u0000-\u001f]/.test(trimmed)) {
    throw new SnapshotError('INVALID_NAME', 'Snapshot name must not contain control characters');
  }
  // A snapshot is addressable by name-or-id in a single route segment, so a
  // name must never contain '/' (which would fork the segment) and must not
  // masquerade as a relative-path token.
  if (trimmed.includes('/')) {
    throw new SnapshotError('INVALID_NAME', 'Snapshot name must not contain "/"');
  }
  if (trimmed === '.' || trimmed === '..') {
    throw new SnapshotError('INVALID_NAME', 'Snapshot name must not be "." or ".."');
  }
  return trimmed;
}

/** Validate an optional free-form note. Undefined/null is allowed and stored as null. */
function validateNote(note: unknown): string | null {
  if (note === undefined || note === null) return null;
  if (typeof note !== 'string') throw new SnapshotError('INVALID_NOTE', 'Snapshot note must be a string');
  if (note.length > MAX_NOTE_LENGTH) {
    throw new SnapshotError('INVALID_NOTE', `Snapshot note must be at most ${MAX_NOTE_LENGTH} characters`);
  }
  if (note.includes('\0')) throw new SnapshotError('INVALID_NOTE', 'Snapshot note must not contain null bytes');
  return note;
}

/**
 * Generate a concise, sortable default name when the caller omits one:
 * `snap-YYYYMMDD-HHMMSS-<short>` in UTC, with a short random suffix to avoid
 * collisions within the same second.
 */
function generateDefaultName(): string {
  const now = new Date();
  const pad = (value: number): string => String(value).padStart(2, '0');
  const stamp =
    `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}` +
    `-${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`;
  const short = crypto.randomUUID().slice(0, 6);
  return `snap-${stamp}-${short}`;
}

// ---------------------------------------------------------------------------
// Create / list / resolve / delete
// ---------------------------------------------------------------------------

/**
 * Capture a full-volume snapshot in a single synchronous transaction.
 *
 * Uses INSERT ... SELECT for every payload table so BLOB chunk data is copied
 * inside SQLite and never crosses the JS boundary — no file bytes are buffered.
 * The metadata row and all seven payload copies commit atomically.
 */
export function createSnapshot(storage: SnapshotStorage, name?: string, note?: string): SnapshotInfo {
  const validated = name === undefined ? generateDefaultName() : validateName(name);
  const validatedNote = validateNote(note);
  const id = crypto.randomUUID();
  return storage.transactionSync(() => {
    const existing = storage.sql
      .exec<{ id: string }>('SELECT id FROM fs_snapshot WHERE name = ?', validated)
      .toArray();
    if (existing.length > 0) throw new SnapshotExistsError(validated);

    const chunkSizeRows = storage.sql
      .exec<{ value: string }>("SELECT value FROM fs_config WHERE key = 'chunk_size'")
      .toArray();
    const chunkSize = chunkSizeRows.length > 0 ? Number(chunkSizeRows[0].value) : null;

    for (const spec of TABLE_SPECS) {
      const columns = spec.columns.join(', ');
      storage.sql.exec(
        `INSERT INTO ${spec.snap} (snapshot_id, ${columns})
         SELECT ?, ${columns} FROM ${spec.live}`,
        id
      );
    }

    const inodeCount = countRows(storage.sql, 'fs_snapshot_inode', id);
    const fileCount = countByMode(storage.sql, id, S_IFREG);
    const directoryCount = countByMode(storage.sql, id, S_IFDIR);
    const symlinkCount = countByMode(storage.sql, id, S_IFLNK);
    const byteCount = storage.sql
      .exec<{ c: number }>(
        'SELECT COALESCE(SUM(LENGTH(data)), 0) AS c FROM fs_snapshot_data WHERE snapshot_id = ?',
        id
      )
      .toArray()[0].c;

    storage.sql.exec(
      `INSERT INTO fs_snapshot
        (id, name, note, created_at, chunk_size, inode_count, file_count, directory_count, symlink_count, byte_count)
       VALUES (?, ?, ?, unixepoch(), ?, ?, ?, ?, ?, ?)`,
      id, validated, validatedNote, chunkSize,
      inodeCount, fileCount, directoryCount, symlinkCount, Number(byteCount)
    );

    return getById(storage.sql, id)!;
  });
}

/** Count captured inodes of a given file type (S_IFREG/S_IFDIR/S_IFLNK) in a snapshot. */
function countByMode(sql: SnapshotSql, id: string, type: number): number {
  return Number(
    sql.exec<{ c: number }>(
      `SELECT COUNT(*) AS c FROM fs_snapshot_inode WHERE snapshot_id = ? AND (mode & ?) = ?`,
      id, S_IFMT, type
    ).toArray()[0].c
  );
}

function countRows(sql: SnapshotSql, table: string, id: string): number {
  return Number(
    sql.exec<{ c: number }>(`SELECT COUNT(*) AS c FROM ${table} WHERE snapshot_id = ?`, id).toArray()[0].c
  );
}

function getById(sql: SnapshotSql, id: string): SnapshotInfo | null {
  const rows = sql.exec<SnapshotRow>('SELECT * FROM fs_snapshot WHERE id = ?', id).toArray();
  return rows.length > 0 ? toInfo(rows[0]) : null;
}

/** List every snapshot, oldest first (rowid breaks ties within a second). */
export function listSnapshots(storage: SnapshotStorage): SnapshotInfo[] {
  return storage.sql
    .exec<SnapshotRow>('SELECT * FROM fs_snapshot ORDER BY created_at ASC, rowid ASC')
    .toArray()
    .map(toInfo);
}

/** Resolve a snapshot by exact id first, then by unique name. Null if neither matches. */
export function resolveSnapshot(storage: SnapshotStorage, nameOrId: string): SnapshotInfo | null {
  if (typeof nameOrId !== 'string' || nameOrId.length === 0) return null;
  const byId = getById(storage.sql, nameOrId);
  if (byId) return byId;
  const byName = storage.sql
    .exec<SnapshotRow>('SELECT * FROM fs_snapshot WHERE name = ?', nameOrId)
    .toArray();
  return byName.length > 0 ? toInfo(byName[0]) : null;
}

function requireSnapshot(storage: SnapshotStorage, nameOrId: string): SnapshotInfo {
  const info = resolveSnapshot(storage, nameOrId);
  if (!info) throw new SnapshotNotFoundError(nameOrId);
  return info;
}

/**
 * Delete a snapshot and all its payload rows in a single transaction. Resolves
 * by name-or-id. Throws {@link SnapshotNotFoundError} when nothing matches.
 */
export function deleteSnapshot(storage: SnapshotStorage, nameOrId: string): SnapshotInfo {
  return storage.transactionSync(() => {
    const info = requireSnapshot(storage, nameOrId);
    for (const spec of TABLE_SPECS) {
      storage.sql.exec(`DELETE FROM ${spec.snap} WHERE snapshot_id = ?`, info.id);
    }
    storage.sql.exec('DELETE FROM fs_snapshot WHERE id = ?', info.id);
    return info;
  });
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

/**
 * Restore a snapshot over the live volume in a single transaction.
 *
 * Replaces the seven filesystem tables wholesale via DELETE + INSERT ... SELECT
 * (BLOBs copied inside SQLite, never buffered), preserves fs_config (schema
 * version and chunk size), and clears all open-handle leases so no stale lease
 * survives the swap. Callers own lifecycle and locking around this call.
 */
export function restoreSnapshot(storage: SnapshotStorage, nameOrId: string): SnapshotInfo {
  return storage.transactionSync(() => {
    const info = requireSnapshot(storage, nameOrId);
    for (const spec of TABLE_SPECS) {
      storage.sql.exec(`DELETE FROM ${spec.live}`);
    }
    for (const spec of TABLE_SPECS) {
      const columns = spec.columns.join(', ');
      storage.sql.exec(
        `INSERT INTO ${spec.live} (${columns})
         SELECT ${columns} FROM ${spec.snap} WHERE snapshot_id = ?`,
        info.id
      );
    }
    // Any lease is meaningless once the inode table it referenced is replaced.
    storage.sql.exec('DELETE FROM fs_open_inode');
    return info;
  });
}

// ---------------------------------------------------------------------------
// Filesystem state view (live or a snapshot) for diff and export
// ---------------------------------------------------------------------------

export type StateSelector = { live: true } | { snapshotId: string };

interface InodeMeta {
  ino: number;
  mode: number;
  nlink: number;
  uid: number;
  gid: number;
  size: number;
  rdev: number;
}

/** Inode metadata fields compared by diff. Deliberately excludes atime/mtime/ctime noise. */
const META_FIELDS: ReadonlyArray<keyof InodeMeta> = ['mode', 'nlink', 'uid', 'gid', 'size', 'rdev'];

function metadataEqual(a: InodeMeta, b: InodeMeta): boolean {
  return META_FIELDS.every((field) => a[field] === b[field]);
}

interface DentryRow {
  name: string;
  parent_ino: number;
  ino: number;
}

/**
 * A read-only view over one filesystem state — the live tables or a snapshot's
 * payload. Metadata (inodes + directory tree) is loaded up front for path
 * reconstruction; chunk BLOBs are fetched lazily so no file bytes are buffered
 * beyond the file currently under comparison/export.
 */
class FsStateView {
  readonly inodes = new Map<number, InodeMeta>();
  readonly childrenByParent = new Map<number, DentryRow[]>();
  readonly symlinks = new Map<number, string>();

  private constructor(
    private readonly sql: SnapshotSql,
    private readonly selector: StateSelector,
  ) {}

  static load(sql: SnapshotSql, selector: StateSelector): FsStateView {
    const view = new FsStateView(sql, selector);
    const scope = view.scope();

    for (const row of sql.exec<{
      ino: number; mode: number; nlink: number; uid: number; gid: number; size: number; rdev: number;
    }>(
      `SELECT ino, mode, nlink, uid, gid, size, rdev FROM ${scope.inode.table}${scope.inode.where}`,
      ...scope.inode.args
    )) {
      view.inodes.set(Number(row.ino), {
        ino: Number(row.ino),
        mode: Number(row.mode),
        nlink: Number(row.nlink),
        uid: Number(row.uid),
        gid: Number(row.gid),
        size: Number(row.size),
        rdev: Number(row.rdev),
      });
    }

    for (const row of sql.exec<DentryRow>(
      `SELECT name, parent_ino, ino FROM ${scope.dentry.table}${scope.dentry.where}`,
      ...scope.dentry.args
    )) {
      const parent = Number(row.parent_ino);
      const entry: DentryRow = { name: String(row.name), parent_ino: parent, ino: Number(row.ino) };
      const list = view.childrenByParent.get(parent);
      if (list) list.push(entry);
      else view.childrenByParent.set(parent, [entry]);
    }

    for (const row of sql.exec<{ ino: number; target: string }>(
      `SELECT ino, target FROM ${scope.symlink.table}${scope.symlink.where}`,
      ...scope.symlink.args
    )) {
      view.symlinks.set(Number(row.ino), String(row.target));
    }

    return view;
  }

  private scope() {
    if ('live' in this.selector) {
      return {
        inode: { table: 'fs_inode', where: '', args: [] as unknown[] },
        dentry: { table: 'fs_dentry', where: '', args: [] as unknown[] },
        symlink: { table: 'fs_symlink', where: '', args: [] as unknown[] },
      };
    }
    const id = this.selector.snapshotId;
    return {
      inode: { table: 'fs_snapshot_inode', where: ' WHERE snapshot_id = ?', args: [id] },
      dentry: { table: 'fs_snapshot_dentry', where: ' WHERE snapshot_id = ?', args: [id] },
      symlink: { table: 'fs_snapshot_symlink', where: ' WHERE snapshot_id = ?', args: [id] },
    };
  }

  /**
   * Reconstruct every canonical path in this state, keyed by absolute POSIX
   * path (leading '/'), walking from the root inode. Cycles and orphaned
   * dentries are ignored defensively. Names are sorted for deterministic order.
   */
  paths(): Map<string, { ino: number; kind: EntryKind }> {
    const out = new Map<string, { ino: number; kind: EntryKind }>();
    const root = this.inodes.get(ROOT_INO);
    if (!root) return out;
    out.set('/', { ino: ROOT_INO, kind: 'directory' });

    const visited = new Set<number>([ROOT_INO]);
    const walk = (parentIno: number, prefix: string): void => {
      const children = (this.childrenByParent.get(parentIno) ?? [])
        .slice()
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
      for (const child of children) {
        const inode = this.inodes.get(child.ino);
        if (!inode) continue;
        const path = prefix === '/' ? `/${child.name}` : `${prefix}/${child.name}`;
        const kind = kindOf(inode.mode);
        if (out.has(path)) continue;
        out.set(path, { ino: child.ino, kind });
        if (kind === 'directory' && !visited.has(child.ino)) {
          visited.add(child.ino);
          walk(child.ino, path);
        }
      }
    };
    walk(ROOT_INO, '/');
    return out;
  }

  inodeMeta(ino: number): InodeMeta | undefined {
    return this.inodes.get(ino);
  }

  symlinkTarget(ino: number): string | undefined {
    return this.symlinks.get(ino);
  }

  /** Yield the file's chunks in order without buffering the whole file. */
  *chunks(ino: number): Generator<Uint8Array> {
    if ('live' in this.selector) {
      for (const row of this.sql.exec<{ data: unknown }>(
        'SELECT data FROM fs_data WHERE ino = ? ORDER BY chunk_index ASC',
        ino
      )) {
        yield toUint8Array(row.data);
      }
    } else {
      for (const row of this.sql.exec<{ data: unknown }>(
        'SELECT data FROM fs_snapshot_data WHERE snapshot_id = ? AND ino = ? ORDER BY chunk_index ASC',
        this.selector.snapshotId, ino
      )) {
        yield toUint8Array(row.data);
      }
    }
  }

  /** Total content byte length for a file inode, computed inside SQLite. */
  fileBytes(ino: number): number {
    if ('live' in this.selector) {
      return Number(
        this.sql.exec<{ c: number }>(
          'SELECT COALESCE(SUM(LENGTH(data)), 0) AS c FROM fs_data WHERE ino = ?', ino
        ).toArray()[0].c
      );
    }
    return Number(
      this.sql.exec<{ c: number }>(
        'SELECT COALESCE(SUM(LENGTH(data)), 0) AS c FROM fs_snapshot_data WHERE snapshot_id = ? AND ino = ?',
        this.selector.snapshotId, ino
      ).toArray()[0].c
    );
  }
}

function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value === null || value === undefined) return new Uint8Array(0);
  throw new SnapshotError('INVALID_CHUNK', 'Unexpected chunk data type');
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

export type DiffChange = 'added' | 'removed' | 'modified';

export interface SnapshotDiffEntry {
  path: string;
  change: DiffChange;
  /** Entry kind: target kind for added/modified, source kind for removed. */
  kind: EntryKind;
}

/** Resolve a diff endpoint spec to a concrete state selector. */
function resolveEndpoint(
  storage: SnapshotStorage,
  endpoint: 'live' | { snapshot: string }
): StateSelector {
  if (endpoint === 'live') return { live: true };
  const info = requireSnapshot(storage, endpoint.snapshot);
  return { snapshotId: info.id };
}

/**
 * Compute an exact, path-sorted diff between two filesystem states. Each
 * endpoint is either 'live' or a snapshot referenced by name-or-id. Reports
 * added / removed / modified entries. "modified" covers a kind change, a
 * differing symlink target, or differing file content (compared chunk bytes,
 * short-circuiting on the first mismatch).
 */
export function diffSnapshot(
  storage: SnapshotStorage,
  base: 'live' | { snapshot: string },
  target: 'live' | { snapshot: string },
): SnapshotDiffEntry[] {
  const baseState = FsStateView.load(storage.sql, resolveEndpoint(storage, base));
  const targetState = FsStateView.load(storage.sql, resolveEndpoint(storage, target));
  const basePaths = baseState.paths();
  const targetPaths = targetState.paths();

  const allPaths = new Set<string>([...basePaths.keys(), ...targetPaths.keys()]);
  allPaths.delete('/'); // The root always exists on both sides; never a diff entry.

  const entries: SnapshotDiffEntry[] = [];
  for (const path of allPaths) {
    const inBase = basePaths.get(path);
    const inTarget = targetPaths.get(path);

    if (inBase && !inTarget) {
      entries.push({ path, change: 'removed', kind: inBase.kind });
      continue;
    }
    if (!inBase && inTarget) {
      entries.push({ path, change: 'added', kind: inTarget.kind });
      continue;
    }
    if (!inBase || !inTarget) continue;

    if (inBase.kind !== inTarget.kind) {
      entries.push({ path, change: 'modified', kind: inTarget.kind });
      continue;
    }

    // Same path, same kind: report modified when relevant inode metadata,
    // a symlink target, or file content differs. Metadata is compared for
    // every kind (permissions/ownership/rdev/nlink/size) but excludes
    // atime/mtime/ctime so timestamp churn is not reported as a change.
    const baseMeta = baseState.inodeMeta(inBase.ino);
    const targetMeta = targetState.inodeMeta(inTarget.ino);
    if (baseMeta && targetMeta && !metadataEqual(baseMeta, targetMeta)) {
      entries.push({ path, change: 'modified', kind: inTarget.kind });
      continue;
    }
    if (inTarget.kind === 'symlink') {
      const a = baseState.symlinkTarget(inBase.ino) ?? '';
      const b = targetState.symlinkTarget(inTarget.ino) ?? '';
      if (a !== b) entries.push({ path, change: 'modified', kind: 'symlink' });
      continue;
    }
    if (inTarget.kind === 'file') {
      if (!fileContentsEqual(baseState, inBase.ino, targetState, inTarget.ino)) {
        entries.push({ path, change: 'modified', kind: 'file' });
      }
    }
  }

  entries.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return entries;
}

/** Compare file content by streaming chunk bytes; short-circuits on first mismatch. */
function fileContentsEqual(a: FsStateView, aIno: number, b: FsStateView, bIno: number): boolean {
  if (a.fileBytes(aIno) !== b.fileBytes(bIno)) return false;
  const left = a.chunks(aIno);
  const right = b.chunks(bIno);
  let leftBuf: Uint8Array = new Uint8Array(0);
  let rightBuf: Uint8Array = new Uint8Array(0);
  let leftDone = false;
  let rightDone = false;

  const pull = (side: 'l' | 'r'): void => {
    if (side === 'l') {
      const next = left.next();
      if (next.done) leftDone = true;
      else leftBuf = concat(leftBuf, next.value);
    } else {
      const next = right.next();
      if (next.done) rightDone = true;
      else rightBuf = concat(rightBuf, next.value);
    }
  };

  while (true) {
    while (leftBuf.byteLength === 0 && !leftDone) pull('l');
    while (rightBuf.byteLength === 0 && !rightDone) pull('r');
    if (leftBuf.byteLength === 0 && rightBuf.byteLength === 0) {
      return leftDone && rightDone;
    }
    const n = Math.min(leftBuf.byteLength, rightBuf.byteLength);
    if (n === 0) return false; // one side exhausted before the other
    for (let i = 0; i < n; i++) {
      if (leftBuf[i] !== rightBuf[i]) return false;
    }
    leftBuf = leftBuf.subarray(n);
    rightBuf = rightBuf.subarray(n);
  }
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.byteLength === 0) return b;
  if (b.byteLength === 0) return a;
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a, 0);
  out.set(b, a.byteLength);
  return out;
}

// ---------------------------------------------------------------------------
// AIRYFS export from a snapshot
// ---------------------------------------------------------------------------

/**
 * Produce a AIRYFS archive byte stream for a snapshot's full tree. Reconstructs
 * safe canonical relative paths from the snapshot's directory entries, emits the
 * root first, then directories before their children (names sorted), streaming
 * fs_data chunk bytes for each file. The output is byte-compatible with
 * {@link extractTree}.
 */
export function* encodeSnapshotArchive(
  storage: SnapshotStorage,
  nameOrId: string,
): Generator<Uint8Array> {
  const info = requireSnapshot(storage, nameOrId);
  const state = FsStateView.load(storage.sql, { snapshotId: info.id });

  yield archiveMagic();
  yield encodeRootEntry();
  yield* walkSnapshot(state, ROOT_INO, '');
  yield archiveEndMarker();
}

function* walkSnapshot(state: FsStateView, parentIno: number, prefix: string): Generator<Uint8Array> {
  const children = (state.childrenByParent.get(parentIno) ?? [])
    .slice()
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));

  for (const child of children) {
    const inode = state.inodeMeta(child.ino);
    if (!inode) continue;
    const rel = prefix ? `${prefix}/${child.name}` : child.name;
    // Validate defensively; a corrupt name that escapes the tree is rejected.
    validateRelativePath(rel);
    const kind = kindOf(inode.mode);

    if (kind === 'directory') {
      yield encodeDirectoryEntry(rel);
      yield* walkSnapshot(state, child.ino, rel);
    } else if (kind === 'symlink') {
      const target = state.symlinkTarget(child.ino);
      if (target === undefined) throw new ArchiveError(`Snapshot symlink missing target: ${rel}`);
      yield encodeSymlinkEntry(rel, target);
    } else if (kind === 'file') {
      const size = state.fileBytes(child.ino);
      yield encodeFileEntry(rel, size);
      let emitted = 0;
      for (const chunk of state.chunks(child.ino)) {
        if (chunk.byteLength === 0) continue;
        emitted += chunk.byteLength;
        yield chunk;
      }
      if (emitted !== size) {
        throw new ArchiveError(`Snapshot file size drifted during export: ${rel}`);
      }
    }
    // Other node types are skipped, matching the live tree exporter.
  }
}

/**
 * Wrap {@link encodeSnapshotArchive} as a ReadableStream, invoking `release`
 * (if provided) on completion, error, or cancel. Callers own the surrounding
 * read lock / lifecycle; this only guarantees the release hook fires once.
 */
export function encodeSnapshotArchiveStream(
  storage: SnapshotStorage,
  nameOrId: string,
  release?: () => void,
): ReadableStream<Uint8Array> {
  const iterator = encodeSnapshotArchive(storage, nameOrId);
  let released = false;
  const releaseOnce = (): void => {
    if (released) return;
    released = true;
    release?.();
  };
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      try {
        const { done, value } = iterator.next();
        if (done) {
          controller.close();
          releaseOnce();
          return;
        }
        controller.enqueue(value);
      } catch (error) {
        controller.error(error);
        releaseOnce();
      }
    },
    cancel() {
      iterator.return?.(undefined);
      releaseOnce();
    },
  });
}
