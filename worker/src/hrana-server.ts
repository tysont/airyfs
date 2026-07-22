// ABOUTME: Hrana pipeline server that executes SQL against DO SQLite.
// ABOUTME: Reads pipeline requests from a TCP stream, executes SQL, writes pipeline responses.

import {
  FrameBuffer,
  serializeFrame,
  type PipelineRequest,
  type PipelineResponse,
  type StreamRequest,
  type StreamResult,
  type StreamResponse,
  type HranaValue,
  type StmtResult,
  type Stmt,
  type BatchResult,
  type Row,
} from './hrana-protocol';

// ---------------------------------------------------------------------------
// SQL backend interface
// ---------------------------------------------------------------------------

export interface SqlCursorResult {
  columnNames: string[];
  rows: Record<string, unknown>[];
  rowsRead: number;
  rowsWritten: number;
}

export interface SqlBackend {
  exec(query: string, ...bindings: unknown[]): SqlCursorResult;
}

type TransactionSync = <T>(callback: () => T) => T;

/** Wraps DO SqlStorage into the SqlBackend interface. */
export function wrapSqlStorage(sql: {
  exec(query: string, ...bindings: unknown[]): {
    columnNames: string[];
    toArray(): Record<string, unknown>[];
    rowsRead: number;
    rowsWritten: number;
  };
}): SqlBackend {
  return {
    exec(query: string, ...bindings: unknown[]): SqlCursorResult {
      const cursor = sql.exec(query, ...bindings);
      return {
        columnNames: cursor.columnNames,
        rows: cursor.toArray(),
        rowsRead: cursor.rowsRead,
        rowsWritten: cursor.rowsWritten,
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Value conversion: Hrana ↔ JS
// ---------------------------------------------------------------------------

function hranaToJs(val: HranaValue): unknown {
  switch (val.type) {
    case 'null':
      return null;
    case 'integer':
      {
        const integer = BigInt(val.value);
        if (integer < BigInt(Number.MIN_SAFE_INTEGER) || integer > BigInt(Number.MAX_SAFE_INTEGER)) {
          throw new Error(`Integer is outside the supported safe range: ${val.value}`);
        }
        return Number(integer);
      }
    case 'float':
      return val.value;
    case 'text':
      return val.value;
    case 'blob': {
      const binary = atob(val.base64);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
      }
      return bytes;
    }
  }
}

function jsToHrana(val: unknown): HranaValue {
  if (val === null || val === undefined) return { type: 'null' };
  if (typeof val === 'number') {
    return Number.isInteger(val)
      ? { type: 'integer', value: String(val) }
      : { type: 'float', value: val };
  }
  if (typeof val === 'bigint') return { type: 'integer', value: String(val) };
  if (typeof val === 'string') return { type: 'text', value: val };
  if (val instanceof ArrayBuffer || val instanceof Uint8Array) {
    const bytes = val instanceof Uint8Array ? val : new Uint8Array(val);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return { type: 'blob', base64: btoa(binary) };
  }
  return { type: 'text', value: String(val) };
}

// ---------------------------------------------------------------------------
// DO SQLite statement filter
//
// DO SQLite wraps every sql.exec() call in an implicit transaction and does
// not support explicit transaction control or most PRAGMA statements.
// Statements that hit these restrictions are filtered here rather than
// letting them fail at the runtime level.
// ---------------------------------------------------------------------------

const EMPTY_RESULT: StmtResult = {
  cols: [], rows: [], affected_row_count: 0, last_insert_rowid: null,
  replication_index: null, rows_read: 0, rows_written: 0, query_duration_ms: 0,
};

/** Statements that DO SQLite does not support. Return empty results as no-ops. */
const BLOCKED_PREFIXES = ['BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE'];

/** Build a strict regex for `SELECT <fn>(?, ?, ...)` that tolerates the
 *  libSQL-normalized optional whitespace before the parenthesis and optional
 *  trailing semicolons, analogous to airyfs_pwrite_v1. */
function remoteCompoundRegex(fn: string, argCount: number): RegExp {
  const placeholders = Array.from({ length: argCount }, () => '\\?\\s*').join(',\\s*');
  return new RegExp(`^SELECT\\s+${fn}\\s*\\(\\s*${placeholders}\\)\\s*;*\\s*$`, 'i');
}

const REMOTE_PWRITE_V1 = /^SELECT\s+AIRYFS_PWRITE_V1\s*\(\s*\?\s*,\s*\?\s*,\s*\?\s*,\s*\?\s*,\s*\?\s*,\s*\?\s*\)\s*;*\s*$/i;
const REMOTE_CREATE_NODE_V1 = remoteCompoundRegex('AIRYFS_CREATE_NODE_V1', 10);
const REMOTE_LINK_V1 = remoteCompoundRegex('AIRYFS_LINK_V1', 5);
const REMOTE_TRUNCATE_V1 = remoteCompoundRegex('AIRYFS_TRUNCATE_V1', 5);
const REMOTE_RENAME_V1 = remoteCompoundRegex('AIRYFS_RENAME_V1', 6);

function isRemotePwriteV1(sql: string): boolean {
  return REMOTE_PWRITE_V1.test(sql.trim());
}

function isRemoteCreateNodeV1(sql: string): boolean {
  return REMOTE_CREATE_NODE_V1.test(sql.trim());
}

function isRemoteLinkV1(sql: string): boolean {
  return REMOTE_LINK_V1.test(sql.trim());
}

function isRemoteTruncateV1(sql: string): boolean {
  return REMOTE_TRUNCATE_V1.test(sql.trim());
}

function isRemoteRenameV1(sql: string): boolean {
  return REMOTE_RENAME_V1.test(sql.trim());
}

// Unix file-type mode bits, mirrored from the SDK's filesystem module.
const S_IFMT = 0o170000;
const S_IFDIR = 0o040000;
const S_IFLNK = 0o120000;

function isBlockedStatement(sql: string): boolean {
  const upper = sql.trimStart().toUpperCase();
  return BLOCKED_PREFIXES.some(p => upper === p || upper.startsWith(p + ' '));
}

function isPragma(sql: string): boolean {
  return sql.trimStart().toUpperCase().startsWith('PRAGMA');
}

/**
 * Simulate PRAGMA table_info(table) through SQLite's table-valued function,
 * which DO SQLite permits in a regular SELECT.
 */
function simulateTableInfo(backend: SqlBackend, tableName: string): StmtResult {
  const result = backend.exec(
    'SELECT cid, name, type, "notnull", dflt_value, pk FROM pragma_table_info(?)',
    tableName
  );
  const rows: Row[] = result.rows.map((row) => ({
    values: ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'].map((column) => jsToHrana(row[column])),
  }));

  return {
    cols: [
      { name: 'cid', decltype: null }, { name: 'name', decltype: null },
      { name: 'type', decltype: null }, { name: 'notnull', decltype: null },
      { name: 'dflt_value', decltype: null }, { name: 'pk', decltype: null },
    ],
    rows,
    affected_row_count: 0, last_insert_rowid: null,
    replication_index: null, rows_read: result.rowsRead, rows_written: 0, query_duration_ms: 0,
  };
}

// ---------------------------------------------------------------------------
// Statement execution
// ---------------------------------------------------------------------------

function isReadOnlyStatement(sql: string): boolean {
  const trimmed = sql.trim();
  const withoutTrailingSemicolons = trimmed.replace(/;+\s*$/, '');
  if (withoutTrailingSemicolons.includes(';')) return false;
  const upper = withoutTrailingSemicolons.toUpperCase();
  if (isRemotePwriteV1(trimmed) || isRemoteCreateNodeV1(trimmed) || isRemoteLinkV1(trimmed) || isRemoteTruncateV1(trimmed) || isRemoteRenameV1(trimmed)) return false;
  return upper.startsWith('SELECT') || upper.startsWith('EXPLAIN');
}

function isMutatingStatement(sql: string): boolean {
  return !isBlockedStatement(sql) && !isPragma(sql) && !isReadOnlyStatement(sql);
}

function requireSafeInteger(value: unknown, name: string): number {
  if (!Number.isSafeInteger(value)) throw new Error(`${name} must be a safe integer`);
  return value as number;
}

function requireBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  throw new Error('data must be a blob');
}

function executeRemotePwrite(backend: SqlBackend, transactionSync: TransactionSync, bindings: unknown[]): StmtResult {
  if (bindings.length !== 6) throw new Error('airyfs_pwrite_v1 requires six arguments');
  const ino = requireSafeInteger(bindings[0], 'ino');
  const offset = requireSafeInteger(bindings[1], 'offset');
  const data = requireBytes(bindings[2]);
  const chunkSize = requireSafeInteger(bindings[3], 'chunk_size');
  const nowSecs = requireSafeInteger(bindings[4], 'now_secs');
  const nowNsecs = requireSafeInteger(bindings[5], 'now_nsecs');
  if (ino <= 0 || offset < 0 || chunkSize <= 0 || data.byteLength === 0) {
    throw new Error('invalid airyfs_pwrite_v1 arguments');
  }
  const writeEnd = offset + data.byteLength;
  if (!Number.isSafeInteger(writeEnd)) throw new Error('write end is outside the supported range');

  transactionSync(() => {
    const inode = backend.exec('SELECT 1 AS found FROM fs_inode WHERE ino = ?', ino).rows[0];
    if (!inode) throw new Error(`inode not found: ${ino}`);

    let written = 0;
    while (written < data.byteLength) {
      const currentOffset = offset + written;
      const chunkIndex = Math.floor(currentOffset / chunkSize);
      const offsetInChunk = currentOffset % chunkSize;
      const toWrite = Math.min(chunkSize - offsetInChunk, data.byteLength - written);
      let chunkData: Uint8Array;
      if (toWrite === chunkSize) {
        chunkData = data.slice(written, written + toWrite);
      } else {
        const row = backend.exec(
          'SELECT data FROM fs_data WHERE ino = ? AND chunk_index = ?',
          ino, chunkIndex,
        ).rows[0];
        const existing = row?.data === undefined ? new Uint8Array() : requireBytes(row.data);
        const size = Math.max(existing.byteLength, offsetInChunk + toWrite);
        chunkData = new Uint8Array(size);
        chunkData.set(existing);
        chunkData.set(data.subarray(written, written + toWrite), offsetInChunk);
      }
      backend.exec(
        'INSERT OR REPLACE INTO fs_data (ino, chunk_index, data) VALUES (?, ?, ?)',
        ino, chunkIndex, chunkData,
      );
      written += toWrite;
    }
    backend.exec(
      'UPDATE fs_inode SET size = MAX(size, ?), mtime = ?, mtime_nsec = ? WHERE ino = ?',
      writeEnd, nowSecs, nowNsecs, ino,
    );
  });

  return {
    ...EMPTY_RESULT,
    cols: [{ name: 'bytes_written', decltype: 'INTEGER' }],
    rows: [{ values: [{ type: 'integer', value: String(data.byteLength) }] }],
    affected_row_count: 1,
    rows_written: 1,
  };
}

/** One-row integer result carrying a compound operation's status, mirroring
 *  the shape used by pwrite's result. */
function statusResult(status: number): StmtResult {
  return {
    ...EMPTY_RESULT,
    cols: [{ name: 'status', decltype: 'INTEGER' }],
    rows: [{ values: [{ type: 'integer', value: String(status) }] }],
    affected_row_count: 1,
    rows_written: 1,
  };
}

/** Atomically truncate an inode. Extensions materialize zeros so both the Rust
 *  and TypeScript AgentFS readers observe the same byte layout. */
function executeRemoteTruncate(backend: SqlBackend, transactionSync: TransactionSync, bindings: unknown[]): StmtResult {
  if (bindings.length !== 5) throw new Error('airyfs_truncate_v1 requires five arguments');
  const ino = requireSafeInteger(bindings[0], 'ino');
  const newSize = requireSafeInteger(bindings[1], 'new_size');
  const chunkSize = requireSafeInteger(bindings[2], 'chunk_size');
  const nowSecs = requireSafeInteger(bindings[3], 'now_secs');
  const nowNsecs = requireSafeInteger(bindings[4], 'now_nsecs');
  if (ino <= 0 || newSize < 0 || chunkSize <= 0) {
    throw new Error('invalid airyfs_truncate_v1 arguments');
  }

  let status = -1;
  transactionSync(() => {
    const inode = backend.exec('SELECT size FROM fs_inode WHERE ino = ?', ino).rows[0];
    if (inode === undefined) return;
    const currentSize = Number(inode.size);
    if (!Number.isSafeInteger(currentSize) || currentSize < 0) {
      throw new Error(`invalid size for inode: ${ino}`);
    }

    if (newSize === 0) {
      backend.exec('DELETE FROM fs_data WHERE ino = ?', ino);
    } else if (newSize < currentSize) {
      const lastChunkIndex = Math.floor((newSize - 1) / chunkSize);
      backend.exec(
        'DELETE FROM fs_data WHERE ino = ? AND chunk_index > ?',
        ino, lastChunkIndex,
      );
      const finalLength = newSize % chunkSize;
      if (finalLength > 0) {
        const row = backend.exec(
          'SELECT data FROM fs_data WHERE ino = ? AND chunk_index = ?',
          ino, lastChunkIndex,
        ).rows[0];
        if (row?.data !== undefined) {
          const data = requireBytes(row.data);
          if (data.byteLength > finalLength) {
            backend.exec(
              'UPDATE fs_data SET data = ? WHERE ino = ? AND chunk_index = ?',
              data.slice(0, finalLength), ino, lastChunkIndex,
            );
          }
        }
      }
    } else if (newSize > currentSize) {
      const lastExistingChunk = currentSize === 0 ? null : Math.floor((currentSize - 1) / chunkSize);
      const lastNewChunk = Math.floor((newSize - 1) / chunkSize);
      if (lastExistingChunk !== null) {
        const row = backend.exec(
          'SELECT data FROM fs_data WHERE ino = ? AND chunk_index = ?',
          ino, lastExistingChunk,
        ).rows[0];
        if (row?.data !== undefined) {
          const data = requireBytes(row.data);
          const neededLength = lastExistingChunk === lastNewChunk
            ? ((newSize - 1) % chunkSize) + 1
            : chunkSize;
          if (data.byteLength < neededLength) {
            const padded = new Uint8Array(neededLength);
            padded.set(data);
            backend.exec(
              'UPDATE fs_data SET data = ? WHERE ino = ? AND chunk_index = ?',
              padded, ino, lastExistingChunk,
            );
          }
        }
      }
      const firstNewChunk = lastExistingChunk === null ? 0 : lastExistingChunk + 1;
      for (let chunkIndex = firstNewChunk; chunkIndex <= lastNewChunk; chunkIndex++) {
        const length = chunkIndex === lastNewChunk
          ? ((newSize - 1) % chunkSize) + 1
          : chunkSize;
        backend.exec(
          'INSERT OR REPLACE INTO fs_data (ino, chunk_index, data) VALUES (?, ?, ?)',
          ino, chunkIndex, new Uint8Array(length),
        );
      }
    }

    backend.exec(
      `UPDATE fs_inode
       SET size = ?, mtime = ?, ctime = ?, mtime_nsec = ?, ctime_nsec = ?
       WHERE ino = ?`,
      newSize, nowSecs, nowSecs, nowNsecs, nowNsecs, ino,
    );
    status = ino;
  });

  return statusResult(status);
}

function requireText(value: unknown, name: string): string {
  if (typeof value !== 'string') throw new Error(`${name} must be text`);
  return value;
}

function dentryExists(backend: SqlBackend, parentIno: number, name: string): boolean {
  return backend.exec(
    'SELECT 1 AS found FROM fs_dentry WHERE parent_ino = ? AND name = ?',
    parentIno, name,
  ).rows[0] !== undefined;
}

function inodeMode(backend: SqlBackend, ino: number): number | null {
  const row = backend.exec('SELECT mode FROM fs_inode WHERE ino = ?', ino).rows[0];
  if (row === undefined || row.mode === null || row.mode === undefined) return null;
  return Number(row.mode);
}

/**
 * Compound create for mkdir / mknod / symlink. Runs entirely inside one
 * transaction and leaves no partial state:
 *  - destination dentry already present -> status 0 (EEXIST), no mutations
 *  - parent missing or not a directory   -> status -1, no mutations
 *  - otherwise the new inode number is returned as a positive status.
 */
function executeRemoteCreateNode(backend: SqlBackend, transactionSync: TransactionSync, bindings: unknown[]): StmtResult {
  if (bindings.length !== 10) throw new Error('airyfs_create_node_v1 requires ten arguments');
  const kind = requireText(bindings[0], 'kind');
  const parentIno = requireSafeInteger(bindings[1], 'parent_ino');
  const name = requireText(bindings[2], 'name');
  const mode = requireSafeInteger(bindings[3], 'mode');
  const uid = requireSafeInteger(bindings[4], 'uid');
  const gid = requireSafeInteger(bindings[5], 'gid');
  const rdev = requireSafeInteger(bindings[6], 'rdev');
  const nowSecs = requireSafeInteger(bindings[8], 'now_secs');
  const nowNsecs = requireSafeInteger(bindings[9], 'now_nsecs');
  if (kind !== 'mkdir' && kind !== 'mknod' && kind !== 'symlink') {
    throw new Error(`invalid airyfs_create_node_v1 kind: ${kind}`);
  }
  const target = kind === 'symlink' ? requireText(bindings[7], 'target') : null;

  let status = 0;
  transactionSync(() => {
    if (dentryExists(backend, parentIno, name)) {
      status = 0;
      return;
    }
    const parentMode = inodeMode(backend, parentIno);
    if (parentMode === null || (parentMode & S_IFMT) !== S_IFDIR) {
      status = -1;
      return;
    }

    let nodeMode: number;
    let size = 0;
    let nodeRdev = 0;
    let nlink = 1;
    if (kind === 'mkdir') {
      nodeMode = S_IFDIR | (mode & 0o7777);
      nlink = 2;
    } else if (kind === 'symlink') {
      nodeMode = S_IFLNK | 0o777;
      size = new TextEncoder().encode(target as string).byteLength;
    } else {
      nodeMode = mode;
      nodeRdev = rdev;
    }

    backend.exec(
      `INSERT INTO fs_inode (mode, nlink, uid, gid, size, atime, mtime, ctime, rdev, atime_nsec, mtime_nsec, ctime_nsec)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      nodeMode, nlink, uid, gid, size, nowSecs, nowSecs, nowSecs, nodeRdev, nowNsecs, nowNsecs, nowNsecs,
    );
    const ino = Number(backend.exec('SELECT last_insert_rowid() AS rid').rows[0]?.rid);
    if (!Number.isSafeInteger(ino) || ino <= 0) throw new Error('failed to allocate inode');

    backend.exec('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)', name, parentIno, ino);
    if (kind === 'symlink') {
      backend.exec('INSERT INTO fs_symlink (ino, target) VALUES (?, ?)', ino, target);
    }

    if (kind === 'mkdir') {
      backend.exec(
        'UPDATE fs_inode SET nlink = nlink + 1, ctime = ?, mtime = ?, ctime_nsec = ?, mtime_nsec = ? WHERE ino = ?',
        nowSecs, nowSecs, nowNsecs, nowNsecs, parentIno,
      );
    } else {
      backend.exec(
        'UPDATE fs_inode SET ctime = ?, mtime = ?, ctime_nsec = ?, mtime_nsec = ? WHERE ino = ?',
        nowSecs, nowSecs, nowNsecs, nowNsecs, parentIno,
      );
    }
    status = ino;
  });

  return statusResult(status);
}

/**
 * Compound hardlink. Runs entirely inside one transaction and leaves no
 * partial state:
 *  - destination dentry already present -> status 0 (EEXIST)
 *  - source inode missing               -> status -1
 *  - source inode is a directory        -> status -2
 *  - new parent missing or not a dir    -> status -1
 *  - otherwise the source inode number is returned as a positive status.
 */
function executeRemoteLink(backend: SqlBackend, transactionSync: TransactionSync, bindings: unknown[]): StmtResult {
  if (bindings.length !== 5) throw new Error('airyfs_link_v1 requires five arguments');
  const sourceIno = requireSafeInteger(bindings[0], 'source_ino');
  const parentIno = requireSafeInteger(bindings[1], 'new_parent_ino');
  const name = requireText(bindings[2], 'new_name');
  const nowSecs = requireSafeInteger(bindings[3], 'now_secs');
  const nowNsecs = requireSafeInteger(bindings[4], 'now_nsecs');

  let status = 0;
  transactionSync(() => {
    if (dentryExists(backend, parentIno, name)) {
      status = 0;
      return;
    }
    const sourceMode = inodeMode(backend, sourceIno);
    if (sourceMode === null) {
      status = -1;
      return;
    }
    if ((sourceMode & S_IFMT) === S_IFDIR) {
      status = -2;
      return;
    }
    const parentMode = inodeMode(backend, parentIno);
    if (parentMode === null || (parentMode & S_IFMT) !== S_IFDIR) {
      status = -1;
      return;
    }

    backend.exec('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)', name, parentIno, sourceIno);
    backend.exec(
      'UPDATE fs_inode SET nlink = nlink + 1, ctime = ?, ctime_nsec = ? WHERE ino = ?',
      nowSecs, nowNsecs, sourceIno,
    );
    backend.exec(
      'UPDATE fs_inode SET ctime = ?, mtime = ?, ctime_nsec = ?, mtime_nsec = ? WHERE ino = ?',
      nowSecs, nowSecs, nowNsecs, nowNsecs, parentIno,
    );
    status = sourceIno;
  });

  return statusResult(status);
}

/**
 * Compound rename. Status values are the source inode on success, or:
 * -1 ENOENT, -2 EISDIR, -3 ENOTDIR, -4 ENOTEMPTY, -5 EINVAL, -6 EPERM.
 */
function executeRemoteRename(backend: SqlBackend, transactionSync: TransactionSync, bindings: unknown[]): StmtResult {
  if (bindings.length !== 6) throw new Error('airyfs_rename_v1 requires six arguments');
  const oldParentIno = requireSafeInteger(bindings[0], 'old_parent_ino');
  const oldName = requireText(bindings[1], 'old_name');
  const newParentIno = requireSafeInteger(bindings[2], 'new_parent_ino');
  const newName = requireText(bindings[3], 'new_name');
  const nowSecs = requireSafeInteger(bindings[4], 'now_secs');
  const nowNsecs = requireSafeInteger(bindings[5], 'now_nsecs');
  if (oldParentIno <= 0 || newParentIno <= 0 || oldName.length === 0 || newName.length === 0) {
    throw new Error('invalid airyfs_rename_v1 arguments');
  }

  let status = -1;
  transactionSync(() => {
    const oldParentMode = inodeMode(backend, oldParentIno);
    const newParentMode = inodeMode(backend, newParentIno);
    if (oldParentMode === null || newParentMode === null
      || (oldParentMode & S_IFMT) !== S_IFDIR || (newParentMode & S_IFMT) !== S_IFDIR) return;

    const source = backend.exec(
      `SELECT d.ino, i.mode
       FROM fs_dentry d JOIN fs_inode i ON i.ino = d.ino
       WHERE d.parent_ino = ? AND d.name = ?`,
      oldParentIno, oldName,
    ).rows[0];
    if (source === undefined) return;
    const sourceIno = requireSafeInteger(source.ino, 'source ino');
    const sourceMode = requireSafeInteger(source.mode, 'source mode');
    if (sourceIno === 1) {
      status = -6;
      return;
    }
    if (oldParentIno === newParentIno && oldName === newName) {
      status = sourceIno;
      return;
    }

    const sourceIsDirectory = (sourceMode & S_IFMT) === S_IFDIR;
    if (sourceIsDirectory) {
      const cycle = backend.exec(
        `WITH RECURSIVE ancestors(ino) AS (
           SELECT ?
           UNION
           SELECT d.parent_ino FROM fs_dentry d JOIN ancestors a ON d.ino = a.ino
         )
         SELECT 1 AS found FROM ancestors WHERE ino = ? LIMIT 1`,
        newParentIno, sourceIno,
      ).rows[0];
      if (cycle !== undefined) {
        status = -5;
        return;
      }
    }

    const destination = backend.exec(
      `SELECT d.ino, i.mode
       FROM fs_dentry d JOIN fs_inode i ON i.ino = d.ino
       WHERE d.parent_ino = ? AND d.name = ?`,
      newParentIno, newName,
    ).rows[0];
    if (destination !== undefined) {
      const destinationIno = requireSafeInteger(destination.ino, 'destination ino');
      const destinationMode = requireSafeInteger(destination.mode, 'destination mode');
      if (destinationIno === sourceIno) {
        status = sourceIno;
        return;
      }
      const destinationIsDirectory = (destinationMode & S_IFMT) === S_IFDIR;
      if (destinationIsDirectory && !sourceIsDirectory) {
        status = -2;
        return;
      }
      if (!destinationIsDirectory && sourceIsDirectory) {
        status = -3;
        return;
      }
      if (destinationIsDirectory && backend.exec(
        'SELECT 1 AS found FROM fs_dentry WHERE parent_ino = ? LIMIT 1', destinationIno,
      ).rows[0] !== undefined) {
        status = -4;
        return;
      }

      backend.exec('DELETE FROM fs_dentry WHERE parent_ino = ? AND name = ?', newParentIno, newName);
      if (destinationIsDirectory) {
        backend.exec(
          'UPDATE fs_inode SET nlink = 0, ctime = ?, ctime_nsec = ? WHERE ino = ?',
          nowSecs, nowNsecs, destinationIno,
        );
        backend.exec('UPDATE fs_inode SET nlink = nlink - 1 WHERE ino = ?', newParentIno);
      } else {
        backend.exec(
          'UPDATE fs_inode SET nlink = nlink - 1, ctime = ?, ctime_nsec = ? WHERE ino = ?',
          nowSecs, nowNsecs, destinationIno,
        );
      }
      const destinationLinks = backend.exec('SELECT nlink FROM fs_inode WHERE ino = ?', destinationIno).rows[0];
      if (Number(destinationLinks?.nlink) === 0 && backend.exec(
        `SELECT 1 AS found FROM fs_open_inode
         WHERE ino = ? AND open_count > 0 AND expires_at > ? LIMIT 1`,
        destinationIno, nowSecs,
      ).rows[0] === undefined) {
        backend.exec('DELETE FROM fs_inode WHERE ino = ?', destinationIno);
      }
    }

    backend.exec(
      'UPDATE fs_dentry SET parent_ino = ?, name = ? WHERE parent_ino = ? AND name = ?',
      newParentIno, newName, oldParentIno, oldName,
    );
    if (sourceIsDirectory && oldParentIno !== newParentIno) {
      backend.exec('UPDATE fs_inode SET nlink = nlink - 1 WHERE ino = ?', oldParentIno);
      backend.exec('UPDATE fs_inode SET nlink = nlink + 1 WHERE ino = ?', newParentIno);
    }
    backend.exec(
      'UPDATE fs_inode SET ctime = ?, ctime_nsec = ? WHERE ino = ?',
      nowSecs, nowNsecs, sourceIno,
    );
    backend.exec(
      'UPDATE fs_inode SET mtime = ?, ctime = ?, mtime_nsec = ?, ctime_nsec = ? WHERE ino = ?',
      nowSecs, nowSecs, nowNsecs, nowNsecs, oldParentIno,
    );
    if (oldParentIno !== newParentIno) {
      backend.exec(
        'UPDATE fs_inode SET mtime = ?, ctime = ?, mtime_nsec = ?, ctime_nsec = ? WHERE ino = ?',
        nowSecs, nowSecs, nowNsecs, nowNsecs, newParentIno,
      );
    }
    status = sourceIno;
  });

  return statusResult(status);
}

function executeStmtSync(backend: SqlBackend, stmt: Stmt, transactionSync?: TransactionSync): StmtResult {
  const query = stmt.sql ?? '';

  if (isBlockedStatement(query)) return EMPTY_RESULT;

  if (isPragma(query)) {
    const tableInfoMatch = query.match(/PRAGMA\s+table_info\s*\(\s*(\w+)\s*\)/i);
    if (tableInfoMatch) return simulateTableInfo(backend, tableInfoMatch[1]);
    return EMPTY_RESULT;
  }

  const bindings: unknown[] = [];
  if (stmt.args) {
    for (const arg of stmt.args) bindings.push(hranaToJs(arg));
  }
  if (stmt.named_args) {
    for (const na of stmt.named_args) bindings.push(hranaToJs(na.value));
  }

  if (isRemotePwriteV1(query)) {
    if (!transactionSync) throw new Error('airyfs_pwrite_v1 requires transaction support');
    return executeRemotePwrite(backend, transactionSync, bindings);
  }

  if (isRemoteCreateNodeV1(query)) {
    if (!transactionSync) throw new Error('airyfs_create_node_v1 requires transaction support');
    return executeRemoteCreateNode(backend, transactionSync, bindings);
  }

  if (isRemoteLinkV1(query)) {
    if (!transactionSync) throw new Error('airyfs_link_v1 requires transaction support');
    return executeRemoteLink(backend, transactionSync, bindings);
  }

  if (isRemoteTruncateV1(query)) {
    if (!transactionSync) throw new Error('airyfs_truncate_v1 requires transaction support');
    return executeRemoteTruncate(backend, transactionSync, bindings);
  }

  if (isRemoteRenameV1(query)) {
    if (!transactionSync) throw new Error('airyfs_rename_v1 requires transaction support');
    return executeRemoteRename(backend, transactionSync, bindings);
  }

  const cursor = backend.exec(query, ...bindings);

  const cols = cursor.columnNames.map((name) => ({
    name,
    decltype: null as string | null,
  }));

  const rows: Row[] = cursor.rows.map((row) => ({
    values: cursor.columnNames.map((col) => jsToHrana(row[col])),
  }));

  let lastInsertRowid: string | null = null;
  if (cursor.rowsWritten > 0) {
    try {
      const ridCursor = backend.exec('SELECT last_insert_rowid() as rid');
      const rid = ridCursor.rows[0]?.['rid'];
      if (rid !== null && rid !== undefined) lastInsertRowid = String(rid);
    } catch {
      // last_insert_rowid() may not be available in all contexts
    }
  }

  return {
    cols, rows,
    affected_row_count: cursor.rowsWritten,
    last_insert_rowid: lastInsertRowid,
    replication_index: null,
    rows_read: cursor.rowsRead,
    rows_written: cursor.rowsWritten,
    query_duration_ms: 0,
  };
}

async function executeStmt(
  backend: SqlBackend,
  stmt: Stmt,
  writeLock?: () => Promise<() => void>,
  transactionSync?: TransactionSync,
): Promise<StmtResult> {
  const query = stmt.sql ?? '';
  const release = writeLock && !isReadOnlyStatement(query)
    ? await writeLock()
    : () => undefined;
  try {
    return executeStmtSync(backend, stmt, transactionSync);
  } finally {
    release();
  }
}

function isCondition(condition: unknown, type: string, step: number): boolean {
  const value = condition as { type?: string; step?: number } | null | undefined;
  return value?.type === type && value.step === step;
}

function isNotOkCondition(condition: unknown, step: number): boolean {
  const value = condition as { type?: string; cond?: unknown } | null | undefined;
  return value?.type === 'not' && isCondition(value.cond, 'ok', step);
}

function canonicalTransactionalBody(steps: { condition?: unknown; stmt: Stmt }[]): { start: number; end: number } | null {
  if (steps.length < 3) return null;
  const last = steps.length - 1;
  const commit = last - 1;
  if (steps[0].condition || (steps[0].stmt.sql ?? '').trim().toUpperCase() !== 'BEGIN TRANSACTION') return null;
  if ((steps[commit].stmt.sql ?? '').trim().toUpperCase() !== 'COMMIT') return null;
  if ((steps[last].stmt.sql ?? '').trim().toUpperCase() !== 'ROLLBACK') return null;
  for (let index = 1; index < commit; index++) {
    if (!isCondition(steps[index].condition, 'ok', index - 1)) return null;
  }
  if (!isCondition(steps[commit].condition, 'ok', commit - 1)) return null;
  if (!isNotOkCondition(steps[last].condition, commit)) return null;
  return { start: 1, end: commit };
}

class TransactionalBatchFailure extends Error {
  constructor(
    readonly step: number,
    readonly cause: unknown,
    readonly results: (StmtResult | null)[],
  ) {
    super(cause instanceof Error ? cause.message : String(cause));
  }
}

// ---------------------------------------------------------------------------
// HranaServer
// ---------------------------------------------------------------------------

export class HranaServer {
  readonly sessionId = crypto.randomUUID();
  private sql: SqlBackend;
  private readable: ReadableStream<Uint8Array>;
  private writable: WritableStream<Uint8Array>;
  private writeLock?: () => Promise<() => void>;
  private onWrite?: () => void | Promise<void>;
  private transactionSync?: TransactionSync;
  activeOperation: { kind: string; startedAt: number } | null = null;
  private baton: string | null = null;
  private storedSql = new Map<number, string>();

  /** Count of pipeline requests processed (for performance measurement). */
  pipelineCount = 0;
  /** Count of individual SQL statements executed (for performance measurement). */
  statementCount = 0;

  constructor(opts: {
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
    sql: SqlBackend;
    writeLock?: () => Promise<() => void>;
    onWrite?: () => void | Promise<void>;
    transactionSync?: TransactionSync;
  }) {
    this.readable = opts.readable;
    this.writable = opts.writable;
    this.sql = opts.sql;
    this.writeLock = opts.writeLock;
    this.onWrite = opts.onWrite;
    this.transactionSync = opts.transactionSync;
  }

  async serve(): Promise<void> {
    const reader = this.readable.getReader();
    const writer = this.writable.getWriter();
    const buffer = new FrameBuffer();

    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer.push(value);
        for (const msg of buffer.drain()) {
          const response = await this.handlePipeline(msg as PipelineRequest);
          await writer.write(serializeFrame(response));
        }
      }
    } finally {
      try { reader.releaseLock(); } catch { /* already released */ }
      try { await writer.close(); } catch { /* already closed */ }
    }
  }

  private async handlePipeline(req: PipelineRequest): Promise<PipelineResponse> {
    this.pipelineCount++;
    const results: StreamResult[] = [];
    for (const streamReq of req.requests) {
      try {
        results.push({ type: 'ok' as const, response: await this.handleStreamRequest(streamReq) });
      } catch (err) {
        results.push({
          type: 'error' as const,
          error: { message: err instanceof Error ? err.message : String(err) },
        });
      }
    }

    const hasClose = req.requests.some(r => r.type === 'close');
    if (!hasClose && !this.baton) this.baton = 'airyfs-1';

    return { baton: this.baton, base_url: null, results };
  }

  private async handleStreamRequest(req: StreamRequest): Promise<StreamResponse> {
    switch (req.type) {
      case 'close':
        this.baton = null;
        this.storedSql.clear();
        return { type: 'close' };

      case 'execute':
        this.statementCount++;
        {
          const statement = this.resolveStmt(req.stmt);
          return this.trackOperation('execute', async () => {
            const result = await executeStmt(this.sql, statement, this.writeLock, this.transactionSync);
            if (!isReadOnlyStatement(statement.sql ?? '')) await this.onWrite?.();
            return { type: 'execute', result };
          });
        }

      case 'batch':
        {
          return this.trackOperation('batch', async () => {
            const { result, wrote } = await this.executeBatch(req.batch.steps);
            if (wrote) await this.onWrite?.();
            return { type: 'batch', result };
          });
        }

      case 'get_autocommit':
        return { type: 'get_autocommit', is_autocommit: true };

      case 'sequence': {
        return this.trackOperation('sequence', async () => {
          const seqSql = this.resolveSql(req.sql, req.sql_id);
          for (const part of seqSql.split(';')) {
            const t = part.trim();
            if (!t || isBlockedStatement(t) || isPragma(t)) continue;
            const release = this.writeLock && !isReadOnlyStatement(t)
              ? await this.writeLock()
              : () => undefined;
            try { this.sql.exec(t); } finally { release(); }
          }
          await this.onWrite?.();
          return { type: 'sequence' };
        });
      }

      case 'store_sql':
        this.storedSql.set(req.sql_id, req.sql);
        return { type: 'store_sql' };

      case 'close_sql':
        this.storedSql.delete(req.sql_id);
        return { type: 'close_sql' };

      case 'describe':
        this.resolveSql(req.sql, req.sql_id);
        return { type: 'describe', result: { params: [], cols: [], is_explain: false, is_readonly: false } };

      default:
        throw new Error(`Unsupported stream request: ${(req as { type: string }).type}`);
    }
  }

  private async trackOperation<T>(kind: string, operation: () => Promise<T>): Promise<T> {
    const active = {
      kind,
      startedAt: Date.now(),
    };
    this.activeOperation = active;
    try {
      return await operation();
    } finally {
      if (this.activeOperation === active) this.activeOperation = null;
    }
  }

  private async executeBatch(steps: { condition?: unknown; stmt: Stmt }[]): Promise<{ result: BatchResult; wrote: boolean }> {
    const stepResults: (StmtResult | null)[] = [];
    const stepErrors: ({ message: string } | null)[] = [];
    const resolvedSteps = steps.map((step) => ({ ...step, stmt: this.resolveStmt(step.stmt) }));
    const needsWriteLock = resolvedSteps.some((step) => isMutatingStatement(step.stmt.sql ?? ''));
    const release = this.writeLock && needsWriteLock
      ? await this.writeLock()
      : () => undefined;

    try {
      const body = this.transactionSync ? canonicalTransactionalBody(resolvedSteps) : null;
      if (body) return this.executeTransactionalBatch(resolvedSteps, body);

      let wrote = false;
      for (const step of resolvedSteps) {
        if (step.condition && !this.evaluateCondition(step.condition, stepResults, stepErrors)) {
          stepResults.push(null);
          stepErrors.push(null);
          continue;
        }

        this.statementCount++;
        try {
          stepResults.push(await executeStmt(this.sql, step.stmt, undefined, this.transactionSync));
          stepErrors.push(null);
          wrote ||= isMutatingStatement(step.stmt.sql ?? '');
        } catch (err) {
          stepResults.push(null);
          stepErrors.push({ message: err instanceof Error ? err.message : String(err) });
        }
      }
      return { result: { step_results: stepResults, step_errors: stepErrors }, wrote };
    } finally {
      release();
    }
  }

  private executeTransactionalBatch(
    steps: { condition?: unknown; stmt: Stmt }[],
    body: { start: number; end: number },
  ): { result: BatchResult; wrote: boolean } {
    const results: (StmtResult | null)[] = [EMPTY_RESULT];
    this.statementCount++;
    try {
      this.transactionSync!(() => {
        for (let index = body.start; index < body.end; index++) {
          this.statementCount++;
          try {
            results.push(executeStmtSync(this.sql, steps[index].stmt));
          } catch (error) {
            throw new TransactionalBatchFailure(index, error, [...results]);
          }
        }
      });
      this.statementCount++;
      results.push(EMPTY_RESULT, null);
      return {
        result: { step_results: results, step_errors: results.map(() => null) },
        wrote: steps.slice(body.start, body.end).some((step) => isMutatingStatement(step.stmt.sql ?? '')),
      };
    } catch (error) {
      if (!(error instanceof TransactionalBatchFailure)) throw error;
      const stepResults = error.results;
      const stepErrors: ({ message: string } | null)[] = stepResults.map(() => null);
      stepResults.push(null);
      stepErrors.push({ message: error.message });
      while (stepResults.length < body.end) {
        stepResults.push(null);
        stepErrors.push(null);
      }
      stepResults.push(null, EMPTY_RESULT);
      stepErrors.push(null, null);
      this.statementCount++;
      return { result: { step_results: stepResults, step_errors: stepErrors }, wrote: false };
    }
  }

  private resolveStmt(stmt: Stmt): Stmt {
    return { ...stmt, sql: this.resolveSql(stmt.sql, stmt.sql_id) };
  }

  private resolveSql(sql?: string | null, sqlId?: number | null): string {
    if (sql !== null && sql !== undefined) return sql;
    if (sqlId !== null && sqlId !== undefined) {
      const stored = this.storedSql.get(sqlId);
      if (stored !== undefined) return stored;
      throw new Error(`Unknown stored SQL id: ${sqlId}`);
    }
    throw new Error('SQL text or sql_id is required');
  }

  private evaluateCondition(
    cond: unknown,
    results: (StmtResult | null)[],
    errors: ({ message: string } | null)[]
  ): boolean {
    const c = cond as { type: string; step?: number; cond?: unknown; conds?: unknown[] };
    switch (c.type) {
      case 'ok':
        return c.step !== undefined && c.step < results.length && results[c.step] !== null;
      case 'error':
        return c.step !== undefined && c.step < errors.length && errors[c.step] !== null;
      case 'not':
        return !this.evaluateCondition(c.cond, results, errors);
      case 'and':
        return (c.conds ?? []).every((sub) => this.evaluateCondition(sub, results, errors));
      case 'or':
        return (c.conds ?? []).some((sub) => this.evaluateCondition(sub, results, errors));
      case 'is_autocommit':
        return true;
      default:
        return true;
    }
  }
}
