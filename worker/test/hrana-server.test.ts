// ABOUTME: Tests for HranaServer pipeline format against a real SQLite database.
// ABOUTME: Verifies execute, batch, get_autocommit, close, and error handling.

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import { HranaServer, type SqlBackend } from '../src/hrana-server';
import {
  serializeFrame,
  FrameBuffer,
  type PipelineRequest,
  type PipelineResponse,
} from '../src/hrana-protocol';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function createTestBackend(db: Database.Database): SqlBackend {
  db.function('unixepoch', () => Math.floor(Date.now() / 1000));
  return {
    exec(query: string, ...bindings: unknown[]) {
      const stmt = db.prepare(query);
      if (stmt.reader) {
        const columns = stmt.columns();
        const rows = stmt.all(...bindings) as Record<string, unknown>[];
        return { columnNames: columns.map((c) => c.name), rows, rowsRead: rows.length, rowsWritten: 0 };
      }
      const info = stmt.run(...bindings);
      return { columnNames: [], rows: [], rowsRead: 0, rowsWritten: info.changes };
    },
  };
}

function transactionalSteps(sql: string[]) {
  return [
    { stmt: { sql: 'BEGIN TRANSACTION' } },
    ...sql.map((statement, index) => ({
      condition: { type: 'ok' as const, step: index },
      stmt: { sql: statement },
    })),
    {
      condition: { type: 'ok' as const, step: sql.length },
      stmt: { sql: 'COMMIT' },
    },
    {
      condition: { type: 'not' as const, cond: { type: 'ok' as const, step: sql.length + 1 } },
      stmt: { sql: 'ROLLBACK' },
    },
  ];
}

function pwriteRequest(dataBase64: string): PipelineRequest {
  return {
    baton: null,
    requests: [{
      type: 'batch',
      batch: {
        steps: [{ stmt: {
          sql: 'SELECT airyfs_pwrite_v1 (?, ?, ?, ?, ?, ?);',
          args: [
            { type: 'integer', value: '2' },
            { type: 'integer', value: '2' },
            { type: 'blob', base64: dataBase64 },
            { type: 'integer', value: '4' },
            { type: 'integer', value: '100' },
            { type: 'integer', value: '200' },
          ],
        } }],
      },
    }],
  };
}

// Minimal filesystem schema mirroring the SDK's v0.4 layout, with a root
// directory inode (ino 1) already present.
function createFsSchema(db: Database.Database) {
  db.exec(`
    CREATE TABLE fs_inode (
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
    );
    CREATE TABLE fs_dentry (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      parent_ino INTEGER NOT NULL,
      ino INTEGER NOT NULL,
      UNIQUE(parent_ino, name)
    );
    CREATE TABLE fs_symlink (
      ino INTEGER PRIMARY KEY,
      target TEXT NOT NULL
    );
    -- Root directory: mode S_IFDIR|0755 = 0o40755 (16877), nlink 2.
    INSERT INTO fs_inode (ino, mode, nlink, uid, gid, size, atime, mtime, ctime)
    VALUES (1, 16877, 2, 0, 0, 0, 0, 0, 0);
  `);
}

function arg(v: number | string | null): { type: 'integer'; value: string } | { type: 'text'; value: string } | { type: 'null' } {
  if (v === null) return { type: 'null' };
  if (typeof v === 'number') return { type: 'integer', value: String(v) };
  return { type: 'text', value: v };
}

function createNodeRequest(
  kind: string,
  parent: number,
  name: string,
  mode: number,
  uid: number,
  gid: number,
  rdev: number,
  target: string | null,
  nowSecs: number,
  nowNsecs: number,
  sqlOverride?: string,
): PipelineRequest {
  return {
    baton: null,
    requests: [{
      type: 'execute',
      stmt: {
        sql: sqlOverride ?? 'SELECT airyfs_create_node_v1 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
        args: [kind, parent, name, mode, uid, gid, rdev, target, nowSecs, nowNsecs].map(arg),
      },
    }],
  };
}

function linkRequest(
  sourceIno: number,
  parent: number,
  name: string,
  nowSecs: number,
  nowNsecs: number,
  sqlOverride?: string,
): PipelineRequest {
  return {
    baton: null,
    requests: [{
      type: 'execute',
      stmt: {
        sql: sqlOverride ?? 'SELECT airyfs_link_v1 (?, ?, ?, ?, ?);',
        args: [sourceIno, parent, name, nowSecs, nowNsecs].map(arg),
      },
    }],
  };
}

function truncateRequest(
  ino: number,
  newSize: number,
  chunkSize = 4,
  nowSecs = 1000,
  nowNsecs = 500,
  sqlOverride?: string,
): PipelineRequest {
  return {
    baton: null,
    requests: [{
      type: 'execute',
      stmt: {
        sql: sqlOverride ?? 'SELECT airyfs_truncate_v1 (?, ?, ?, ?, ?);',
        args: [ino, newSize, chunkSize, nowSecs, nowNsecs].map(arg),
      },
    }],
  };
}

function renameRequest(
  oldParent: number,
  oldName: string,
  newParent: number,
  newName: string,
  nowSecs = 1000,
  nowNsecs = 500,
  sqlOverride?: string,
): PipelineRequest {
  return {
    baton: null,
    requests: [{
      type: 'execute',
      stmt: {
        sql: sqlOverride ?? 'SELECT airyfs_rename_v1 (?, ?, ?, ?, ?, ?);',
        args: [oldParent, oldName, newParent, newName, nowSecs, nowNsecs].map(arg),
      },
    }],
  };
}

function statusOf(resp: PipelineResponse): number {
  const r = resp.results[0];
  if (r.type === 'ok' && r.response.type === 'execute') {
    const value = r.response.result.rows[0].values[0];
    expect(r.response.result.cols).toEqual([{ name: 'status', decltype: 'INTEGER' }]);
    if (value.type !== 'integer') throw new Error(`expected integer status, got ${value.type}`);
    return Number(value.value);
  }
  throw new Error(`expected ok/execute result, got ${JSON.stringify(r)}`);
}

class TestClient {
  private toServerWriter: WritableStreamDefaultWriter<Uint8Array>;
  private fromServerReader: ReadableStreamDefaultReader<Uint8Array>;
  private frameBuffer = new FrameBuffer();
  public serverDone: Promise<void>;
  public server: HranaServer;

  constructor(
    sql: SqlBackend,
    writeLock?: () => Promise<() => void>,
    transactionSync?: <T>(callback: () => T) => T,
    onWrite?: () => void,
  ) {
    const toServer = new TransformStream<Uint8Array, Uint8Array>();
    const fromServer = new TransformStream<Uint8Array, Uint8Array>();
    this.toServerWriter = toServer.writable.getWriter();
    this.fromServerReader = fromServer.readable.getReader();

    const server = new HranaServer({
      readable: toServer.readable,
      writable: fromServer.writable,
      sql,
      writeLock,
      transactionSync,
      onWrite,
    });
    this.server = server;
    this.serverDone = server.serve();
  }

  async send(req: PipelineRequest): Promise<PipelineResponse> {
    await this.toServerWriter.write(serializeFrame(req));
    while (true) {
      const messages = this.frameBuffer.drain();
      if (messages.length > 0) return messages[0] as PipelineResponse;
      const { value, done } = await this.fromServerReader.read();
      if (done) throw new Error('Server closed');
      this.frameBuffer.push(value);
    }
  }

  async close(): Promise<void> {
    await this.toServerWriter.close();
    await this.serverDone;
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('HranaServer pipeline', () => {
  let db: Database.Database;
  let sql: SqlBackend;

  beforeEach(() => {
    db = new Database(':memory:');
    sql = createTestBackend(db);
  });

  it('executes SELECT 1', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT 1 as v' } }],
    });

    expect(resp.baton).not.toBeNull();
    expect(resp.results).toHaveLength(1);
    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'execute') {
      const result = resp.results[0].response.result;
      expect(result.cols).toEqual([{ name: 'v', decltype: null }]);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].values[0]).toEqual({ type: 'integer', value: '1' });
    }
    await client.close();
  });

  it('takes the write lock for mutations but not reads', async () => {
    let lockRequests = 0;
    let allowWrite!: () => void;
    const writeGate = new Promise<void>((resolve) => { allowWrite = resolve; });
    const client = new TestClient(sql, async () => {
      lockRequests++;
      await writeGate;
      return () => undefined;
    });

    await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT 1' } }],
    });
    expect(lockRequests).toBe(0);

    const mutation = client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE locked (id INTEGER)' } }],
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(lockRequests).toBe(1);
    expect(db.prepare("SELECT name FROM sqlite_master WHERE name='locked'").get()).toBeUndefined();

    allowWrite();
    await mutation;
    expect(db.prepare("SELECT name FROM sqlite_master WHERE name='locked'").get()).toBeDefined();

    await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT 1; DELETE FROM locked' } }],
    });
    expect(lockRequests).toBe(2);
    await client.close();
  });

  it('creates table and inserts data', async () => {
    const client = new TestClient(sql);

    // Create table
    let resp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE test (id INTEGER, name TEXT)' } }],
    });
    expect(resp.results[0].type).toBe('ok');

    // Insert with args
    resp = await client.send({
      baton: resp.baton,
      requests: [{
        type: 'execute',
        stmt: {
          sql: 'INSERT INTO test VALUES (?, ?)',
          args: [{ type: 'integer', value: '1' }, { type: 'text', value: 'alice' }],
        },
      }],
    });
    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'execute') {
      expect(resp.results[0].response.result.affected_row_count).toBe(1);
    }

    // Select back
    resp = await client.send({
      baton: resp.baton,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT * FROM test' } }],
    });
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'execute') {
      expect(resp.results[0].response.result.rows).toHaveLength(1);
      expect(resp.results[0].response.result.rows[0].values).toEqual([
        { type: 'integer', value: '1' },
        { type: 'text', value: 'alice' },
      ]);
    }

    await client.close();
  });

  it('handles multiple requests in one pipeline', async () => {
    const client = new TestClient(sql);

    const resp = await client.send({
      baton: null,
      requests: [
        { type: 'execute', stmt: { sql: 'CREATE TABLE t (x INTEGER)' } },
        { type: 'execute', stmt: { sql: 'INSERT INTO t VALUES (1)' } },
        { type: 'execute', stmt: { sql: 'INSERT INTO t VALUES (2)' } },
        { type: 'execute', stmt: { sql: 'SELECT count(*) as c FROM t' } },
        { type: 'get_autocommit' },
      ],
    });

    expect(resp.results).toHaveLength(5);
    // All should succeed
    for (let i = 0; i < 4; i++) {
      expect(resp.results[i].type).toBe('ok');
    }
    // get_autocommit
    if (resp.results[4].type === 'ok' && resp.results[4].response.type === 'get_autocommit') {
      expect(resp.results[4].response.is_autocommit).toBe(true);
    }
    // Count should be 2
    if (resp.results[3].type === 'ok' && resp.results[3].response.type === 'execute') {
      expect(resp.results[3].response.result.rows[0].values[0]).toEqual({ type: 'integer', value: '2' });
    }

    await client.close();
  });

  it('handles batch with conditions', async () => {
    const client = new TestClient(sql);
    await client.send({ baton: null, requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE t (x INTEGER PRIMARY KEY)' } }] });

    const resp = await client.send({
      baton: null,
      requests: [{
        type: 'batch',
        batch: {
          steps: [
            { stmt: { sql: 'INSERT INTO t VALUES (1)' } },
            { condition: { type: 'ok', step: 0 }, stmt: { sql: 'INSERT INTO t VALUES (2)' } },
            { condition: { type: 'error', step: 0 }, stmt: { sql: 'INSERT INTO t VALUES (99)' } },
          ],
        },
      }],
    });

    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'batch') {
      const batch = resp.results[0].response.result;
      expect(batch.step_results[0]).not.toBeNull(); // step 0 succeeded
      expect(batch.step_results[1]).not.toBeNull(); // step 1 ran (ok condition met)
      expect(batch.step_results[2]).toBeNull();      // step 2 skipped (error condition not met)
    }

    const rows = db.prepare('SELECT x FROM t ORDER BY x').all() as { x: number }[];
    expect(rows.map((r) => r.x)).toEqual([1, 2]);

    await client.close();
  });

  it('holds one write lock for an entire mutating batch', async () => {
    let lockRequests = 0;
    let releases = 0;
    const client = new TestClient(sql, async () => {
      lockRequests++;
      return () => { releases++; };
    });
    await client.send({
      baton: null,
      requests: [{
        type: 'batch',
        batch: {
          steps: [
            { stmt: { sql: 'CREATE TABLE batch_locked (x INTEGER)' } },
            { stmt: { sql: 'INSERT INTO batch_locked VALUES (1)' } },
          ],
        },
      }],
    });

    expect(lockRequests).toBe(1);
    expect(releases).toBe(1);
    await client.close();
  });

  it('commits a canonical transactional batch atomically', async () => {
    const writes: string[] = [];
    let locks = 0;
    let releases = 0;
    const client = new TestClient(
      sql,
      async () => {
        locks++;
        return () => { releases++; };
      },
      (callback) => db.transaction(callback)(),
      () => { writes.push('committed'); },
    );
    await client.send({ baton: null, requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE atomic (x INTEGER PRIMARY KEY)' } }] });
    locks = 0;
    releases = 0;
    client.server.statementCount = 0;

    const resp = await client.send({
      baton: null,
      requests: [{ type: 'batch', batch: { steps: transactionalSteps([
        'INSERT INTO atomic VALUES (1)',
        'INSERT INTO atomic VALUES (2)',
      ]) } }],
    });

    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'batch') {
      expect(resp.results[0].response.result.step_results).toHaveLength(5);
      expect(resp.results[0].response.result.step_errors).toEqual([null, null, null, null, null]);
      expect(resp.results[0].response.result.step_results.at(-1)).toBeNull();
    }
    expect(db.prepare('SELECT x FROM atomic ORDER BY x').pluck().all()).toEqual([1, 2]);
    expect(writes).toEqual(['committed', 'committed']);
    expect(locks).toBe(1);
    expect(releases).toBe(1);
    expect(client.server.statementCount).toBe(4);
    await client.close();
  });

  it('rolls back a failed canonical transactional batch', async () => {
    let writes = 0;
    const client = new TestClient(
      sql,
      undefined,
      (callback) => db.transaction(callback)(),
      () => { writes++; },
    );
    await client.send({ baton: null, requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE atomic (x INTEGER PRIMARY KEY)' } }] });
    writes = 0;
    client.server.statementCount = 0;

    const resp = await client.send({
      baton: null,
      requests: [{ type: 'batch', batch: { steps: transactionalSteps([
        'INSERT INTO atomic VALUES (1)',
        'INSERT INTO atomic VALUES (1)',
        'INSERT INTO atomic VALUES (2)',
      ]) } }],
    });

    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'batch') {
      expect(resp.results[0].response.result.step_errors[2]?.message).toContain('UNIQUE');
      expect(resp.results[0].response.result.step_results).toHaveLength(6);
      expect(resp.results[0].response.result.step_errors).toHaveLength(6);
      expect(resp.results[0].response.result.step_results.at(-1)).not.toBeNull();
    }
    expect(db.prepare('SELECT x FROM atomic').all()).toEqual([]);
    expect(writes).toBe(0);
    expect(client.server.statementCount).toBe(4);
    await client.close();
  });

  it('executes compound pwrite in one transaction', async () => {
    db.exec(`
      CREATE TABLE fs_inode (ino INTEGER PRIMARY KEY, size INTEGER, mtime INTEGER, mtime_nsec INTEGER);
      CREATE TABLE fs_data (ino INTEGER, chunk_index INTEGER, data BLOB, PRIMARY KEY (ino, chunk_index));
      INSERT INTO fs_inode VALUES (2, 4, 0, 0);
      INSERT INTO fs_data VALUES (2, 0, X'41424344');
    `);
    const client = new TestClient(sql, undefined, (callback) => db.transaction(callback)());

    const resp = await client.send(pwriteRequest('eHl6MTI='));

    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'batch') {
      expect(resp.results[0].response.result.step_results[0]?.rows).toEqual([
        { values: [{ type: 'integer', value: '5' }] },
      ]);
    }
    const chunks = db.prepare('SELECT chunk_index, hex(data) AS data FROM fs_data ORDER BY chunk_index').all();
    expect(chunks).toEqual([
      { chunk_index: 0, data: '41427879' },
      { chunk_index: 1, data: '7A3132' },
    ]);
    expect(db.prepare('SELECT size, mtime, mtime_nsec FROM fs_inode WHERE ino = 2').get()).toEqual({
      size: 7, mtime: 100, mtime_nsec: 200,
    });
    await client.close();
  });

  it('rolls back compound pwrite when metadata update fails', async () => {
    db.exec(`
      CREATE TABLE fs_inode (ino INTEGER PRIMARY KEY, size INTEGER, mtime INTEGER, mtime_nsec INTEGER);
      CREATE TABLE fs_data (ino INTEGER, chunk_index INTEGER, data BLOB, PRIMARY KEY (ino, chunk_index));
      INSERT INTO fs_inode VALUES (2, 4, 0, 0);
      INSERT INTO fs_data VALUES (2, 0, X'41424344');
      CREATE TRIGGER fail_pwrite BEFORE UPDATE ON fs_inode BEGIN SELECT RAISE(ABORT, 'injected failure'); END;
    `);
    let writes = 0;
    const client = new TestClient(
      sql,
      undefined,
      (callback) => db.transaction(callback)(),
      () => { writes++; },
    );

    const resp = await client.send(pwriteRequest('eHl6MTI='));

    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'batch') {
      expect(resp.results[0].response.result.step_errors[0]?.message).toContain('injected failure');
    }
    expect(db.prepare('SELECT chunk_index, hex(data) AS data FROM fs_data').all()).toEqual([
      { chunk_index: 0, data: '41424344' },
    ]);
    expect(db.prepare('SELECT size, mtime, mtime_nsec FROM fs_inode WHERE ino = 2').get()).toEqual({
      size: 4, mtime: 0, mtime_nsec: 0,
    });
    expect(writes).toBe(0);
    await client.close();
  });

  it('returns error for bad SQL', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT * FROM nonexistent' } }],
    });

    expect(resp.results[0].type).toBe('error');
    if (resp.results[0].type === 'error') {
      expect(resp.results[0].error.message).toContain('nonexistent');
    }
    await client.close();
  });

  it('handles close', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'close' }],
    });
    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok') {
      expect(resp.results[0].response.type).toBe('close');
    }
    // After close, baton should be null
    expect(resp.baton).toBeNull();
    await client.close();
  });

  it('handles BLOB values', async () => {
    const client = new TestClient(sql);
    await client.send({ baton: null, requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE blobs (data BLOB)' } }] });

    await client.send({
      baton: null,
      requests: [{
        type: 'execute',
        stmt: { sql: 'INSERT INTO blobs VALUES (?)', args: [{ type: 'blob', base64: 'aGVsbG8=' }] },
      }],
    });

    const resp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT data FROM blobs' } }],
    });

    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'execute') {
      const val = resp.results[0].response.result.rows[0].values[0];
      expect(val.type).toBe('blob');
      if (val.type === 'blob') {
        expect(atob(val.base64)).toBe('hello');
      }
    }
    await client.close();
  });

  it('rejects integer bindings outside the DO SQLite safe range', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{
        type: 'execute',
        stmt: { sql: 'SELECT ?', args: [{ type: 'integer', value: '9007199254740992' }] },
      }],
    });

    expect(resp.results[0].type).toBe('error');
    if (resp.results[0].type === 'error') {
      expect(resp.results[0].error.message).toContain('outside the supported safe range');
    }
    await client.close();
  });

  it('serve exits cleanly on stream close', async () => {
    const client = new TestClient(sql);
    await client.send({ baton: null, requests: [{ type: 'execute', stmt: { sql: 'SELECT 1' } }] });
    await client.close();
  });

  // ---------------------------------------------------------------------------
  // Statement filtering
  // ---------------------------------------------------------------------------

  it('skips PRAGMA statements (returns empty result)', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'PRAGMA synchronous = OFF' } }],
    });
    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'execute') {
      expect(resp.results[0].response.result.rows).toEqual([]);
      expect(resp.results[0].response.result.cols).toEqual([]);
    }
    await client.close();
  });

  it('skips BEGIN/COMMIT/ROLLBACK (returns empty result)', async () => {
    const client = new TestClient(sql);
    for (const stmt of ['BEGIN', 'COMMIT', 'ROLLBACK', 'BEGIN DEFERRED', 'SAVEPOINT test', 'RELEASE test']) {
      const resp = await client.send({
        baton: null,
        requests: [{ type: 'execute', stmt: { sql: stmt } }],
      });
      expect(resp.results[0].type).toBe('ok');
    }
    await client.close();
  });

  it('simulates PRAGMA table_info', async () => {
    const client = new TestClient(sql);
    await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE info_test (id INTEGER, name TEXT, data BLOB)' } }],
    });

    const resp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'PRAGMA table_info(info_test)' } }],
    });

    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok' && resp.results[0].response.type === 'execute') {
      const result = resp.results[0].response.result;
      expect(result.cols.map(c => c.name)).toEqual(['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']);
      expect(result.rows.length).toBe(3);
      // Check column names and metadata from SQLite (indexes 1, 3, 4, and 5).
      const colNames = result.rows.map(r => r.values[1]);
      expect(colNames).toEqual([
        { type: 'text', value: 'id' },
        { type: 'text', value: 'name' },
        { type: 'text', value: 'data' },
      ]);
      expect(result.rows[0].values[5]).toEqual({ type: 'integer', value: '0' });
    }
    await client.close();
  });

  it('handles sequence with mixed PRAGMAs and SQL', async () => {
    const client = new TestClient(sql);
    await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'CREATE TABLE seq_test (x INTEGER)' } }],
    });

    // Sequence with PRAGMAs and real SQL mixed together
    const resp = await client.send({
      baton: null,
      requests: [{
        type: 'sequence',
        sql: 'PRAGMA synchronous = OFF; INSERT INTO seq_test VALUES (1); BEGIN; INSERT INTO seq_test VALUES (2); COMMIT',
      }],
    });
    expect(resp.results[0].type).toBe('ok');

    // Verify only the INSERT statements ran
    const countResp = await client.send({
      baton: null,
      requests: [{ type: 'execute', stmt: { sql: 'SELECT count(*) as c FROM seq_test' } }],
    });
    if (countResp.results[0].type === 'ok' && countResp.results[0].response.type === 'execute') {
      expect(countResp.results[0].response.result.rows[0].values[0]).toEqual({ type: 'integer', value: '2' });
    }
    await client.close();
  });

  it('handles get_autocommit', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'get_autocommit' }],
    });
    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok') {
      expect(resp.results[0].response).toEqual({ type: 'get_autocommit', is_autocommit: true });
    }
    await client.close();
  });

  it('handles describe', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'describe', sql: 'SELECT 1' }],
    });
    expect(resp.results[0].type).toBe('ok');
    if (resp.results[0].type === 'ok') {
      expect(resp.results[0].response.type).toBe('describe');
    }
    await client.close();
  });

  it('executes stored SQL and rejects it after close_sql', async () => {
    const client = new TestClient(sql);
    let resp = await client.send({
      baton: null,
      requests: [
        { type: 'store_sql', sql: 'SELECT 1', sql_id: 1 },
        { type: 'execute', stmt: { sql_id: 1 } },
      ],
    });
    expect(resp.results[0].type).toBe('ok');
    expect(resp.results[1].type).toBe('ok');

    resp = await client.send({
      baton: resp.baton,
      requests: [
        { type: 'close_sql', sql_id: 1 },
        { type: 'execute', stmt: { sql_id: 1 } },
      ],
    });
    expect(resp.results[0].type).toBe('ok');
    expect(resp.results[1].type).toBe('error');
    await client.close();
  });

  it('returns error for unsupported stream request type', async () => {
    const client = new TestClient(sql);
    const resp = await client.send({
      baton: null,
      requests: [{ type: 'unknown_type' } as never],
    });
    expect(resp.results[0].type).toBe('error');
    if (resp.results[0].type === 'error') {
      expect(resp.results[0].error.message).toContain('Unsupported');
    }
    await client.close();
  });

  // ---------------------------------------------------------------------------
  // Compound create_node (mkdir / mknod / symlink)
  // ---------------------------------------------------------------------------

  describe('airyfs_create_node_v1', () => {
    let client: TestClient;

    beforeEach(() => {
      createFsSchema(db);
      client = new TestClient(sql, undefined, (callback) => db.transaction(callback)());
    });

    it('creates a directory (mkdir): nlink 2 and bumps parent nlink/timestamps', async () => {
      const resp = await client.send(
        createNodeRequest('mkdir', 1, 'sub', 0o755, 7, 8, 0, null, 1000, 500),
      );
      const ino = statusOf(resp);
      expect(ino).toBeGreaterThan(0);

      expect(db.prepare('SELECT mode, nlink, uid, gid, size, rdev FROM fs_inode WHERE ino = ?').get(ino)).toEqual({
        mode: 0o040755, nlink: 2, uid: 7, gid: 8, size: 0, rdev: 0,
      });
      expect(db.prepare('SELECT parent_ino, ino FROM fs_dentry WHERE name = ?').get('sub')).toEqual({
        parent_ino: 1, ino,
      });
      // Parent nlink incremented (2 -> 3) and timestamps updated.
      expect(db.prepare('SELECT nlink, mtime, ctime, mtime_nsec, ctime_nsec FROM fs_inode WHERE ino = 1').get()).toEqual({
        nlink: 3, mtime: 1000, ctime: 1000, mtime_nsec: 500, ctime_nsec: 500,
      });
      await client.close();
    });

    it('creates a device node (mknod): nlink 1, rdev, parent nlink unchanged', async () => {
      const mode = 0o020644; // S_IFCHR | 0644
      const resp = await client.send(
        createNodeRequest('mknod', 1, 'dev', mode, 0, 0, 259, null, 1000, 500),
      );
      const ino = statusOf(resp);
      expect(ino).toBeGreaterThan(0);

      expect(db.prepare('SELECT mode, nlink, rdev, size FROM fs_inode WHERE ino = ?').get(ino)).toEqual({
        mode, nlink: 1, rdev: 259, size: 0,
      });
      // Parent nlink unchanged (still 2), timestamps updated.
      expect(db.prepare('SELECT nlink, mtime, ctime FROM fs_inode WHERE ino = 1').get()).toEqual({
        nlink: 2, mtime: 1000, ctime: 1000,
      });
      await client.close();
    });

    it('creates a symlink: mode S_IFLNK|0777, size = UTF-8 byte length, fs_symlink row', async () => {
      // "café" is 4 code points but 5 UTF-8 bytes.
      const resp = await client.send(
        createNodeRequest('symlink', 1, 'lnk', 0, 0, 0, 0, 'café', 1000, 500),
      );
      const ino = statusOf(resp);
      expect(ino).toBeGreaterThan(0);

      expect(db.prepare('SELECT mode, nlink, size FROM fs_inode WHERE ino = ?').get(ino)).toEqual({
        mode: 0o120777, nlink: 1, size: 5,
      });
      expect(db.prepare('SELECT target FROM fs_symlink WHERE ino = ?').get(ino)).toEqual({ target: 'café' });
      expect(db.prepare('SELECT nlink, mtime FROM fs_inode WHERE ino = 1').get()).toEqual({ nlink: 2, mtime: 1000 });
      await client.close();
    });

    it('returns 0 with no mutations when the destination already exists', async () => {
      // Pre-create the entry.
      expect(statusOf(await client.send(createNodeRequest('mkdir', 1, 'dup', 0o755, 0, 0, 0, null, 1000, 500)))).toBeGreaterThan(0);
      const inodeCount = () => (db.prepare('SELECT COUNT(*) AS c FROM fs_inode').get() as { c: number }).c;
      const before = inodeCount();
      const parentBefore = db.prepare('SELECT nlink FROM fs_inode WHERE ino = 1').get();

      const resp = await client.send(createNodeRequest('mknod', 1, 'dup', 0o020644, 0, 0, 5, null, 2000, 600));
      expect(statusOf(resp)).toBe(0);
      // No new inode created, parent untouched.
      expect(inodeCount()).toBe(before);
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = 1').get()).toEqual(parentBefore);
      await client.close();
    });

    it('returns -1 with no mutations when the parent is missing or not a directory', async () => {
      const inodeCount = () => (db.prepare('SELECT COUNT(*) AS c FROM fs_inode').get() as { c: number }).c;

      // Missing parent.
      expect(statusOf(await client.send(createNodeRequest('mkdir', 999, 'x', 0o755, 0, 0, 0, null, 1000, 500)))).toBe(-1);

      // Parent that is a regular file, not a directory.
      const fileIno = statusOf(await client.send(createNodeRequest('mknod', 1, 'file', 0o100644, 0, 0, 0, null, 1000, 500)));
      const before = inodeCount();
      const resp = await client.send(createNodeRequest('mkdir', fileIno, 'x', 0o755, 0, 0, 0, null, 1000, 500));
      expect(statusOf(resp)).toBe(-1);
      expect(inodeCount()).toBe(before);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_dentry WHERE name = ?').get('x')).toEqual({ c: 0 });
      await client.close();
    });

    it('rolls back leaving no partial state when a mutation fails', async () => {
      db.exec("CREATE TRIGGER fail_dentry BEFORE INSERT ON fs_dentry BEGIN SELECT RAISE(ABORT, 'injected failure'); END;");
      const before = (db.prepare('SELECT COUNT(*) AS c FROM fs_inode').get() as { c: number }).c;

      const resp = await client.send(createNodeRequest('symlink', 1, 'lnk', 0, 0, 0, 0, 'target', 1000, 500));
      expect(resp.results[0].type).toBe('error');
      if (resp.results[0].type === 'error') {
        expect(resp.results[0].error.message).toContain('injected failure');
      }
      // Inode insert rolled back; no symlink row; parent untouched.
      expect((db.prepare('SELECT COUNT(*) AS c FROM fs_inode').get() as { c: number }).c).toBe(before);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_symlink').get()).toEqual({ c: 0 });
      expect(db.prepare('SELECT mtime FROM fs_inode WHERE ino = 1').get()).toEqual({ mtime: 0 });
      await client.close();
    });

    it('recognizes libSQL-normalized whitespace and trailing semicolons', async () => {
      const resp = await client.send(createNodeRequest(
        'mkdir', 1, 'norm', 0o755, 0, 0, 0, null, 1000, 500,
        'SELECT airyfs_create_node_v1(?,?,?,?,?,?,?,?,?,?)  ;;',
      ));
      expect(statusOf(resp)).toBeGreaterThan(0);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_dentry WHERE name = ?').get('norm')).toEqual({ c: 1 });
      await client.close();
    });
  });

  // ---------------------------------------------------------------------------
  // Compound truncate
  // ---------------------------------------------------------------------------

  describe('airyfs_truncate_v1', () => {
    let client: TestClient;

    beforeEach(() => {
      createFsSchema(db);
      db.exec(`
        CREATE TABLE fs_data (
          ino INTEGER NOT NULL,
          chunk_index INTEGER NOT NULL,
          data BLOB NOT NULL,
          PRIMARY KEY (ino, chunk_index)
        );
        INSERT INTO fs_inode (ino, mode, nlink, size, atime, mtime, ctime)
        VALUES (2, 33188, 1, 10, 0, 0, 0);
        INSERT INTO fs_data VALUES (2, 0, X'41424344');
        INSERT INTO fs_data VALUES (2, 1, X'45464748');
        INSERT INTO fs_data VALUES (2, 2, X'494A');
      `);
      client = new TestClient(sql, undefined, (callback) => db.transaction(callback)());
    });

    it('atomically deletes excess chunks and trims the retained chunk', async () => {
      expect(statusOf(await client.send(truncateRequest(2, 6)))).toBe(2);
      expect(db.prepare('SELECT chunk_index, hex(data) AS data FROM fs_data ORDER BY chunk_index').all()).toEqual([
        { chunk_index: 0, data: '41424344' },
        { chunk_index: 1, data: '4546' },
      ]);
      expect(db.prepare('SELECT size, mtime, ctime, mtime_nsec, ctime_nsec FROM fs_inode WHERE ino = 2').get()).toEqual({
        size: 6, mtime: 1000, ctime: 1000, mtime_nsec: 500, ctime_nsec: 500,
      });
      await client.close();
    });

    it('deletes all chunks when truncating to zero', async () => {
      expect(statusOf(await client.send(truncateRequest(2, 0)))).toBe(2);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_data WHERE ino = 2').get()).toEqual({ c: 0 });
      expect(db.prepare('SELECT size FROM fs_inode WHERE ino = 2').get()).toEqual({ size: 0 });
      await client.close();
    });

    it('extends with zero-filled chunks visible to all readers', async () => {
      expect(statusOf(await client.send(truncateRequest(2, 14)))).toBe(2);
      expect(db.prepare('SELECT chunk_index, hex(data) AS data FROM fs_data ORDER BY chunk_index').all()).toEqual([
        { chunk_index: 0, data: '41424344' },
        { chunk_index: 1, data: '45464748' },
        { chunk_index: 2, data: '494A0000' },
        { chunk_index: 3, data: '0000' },
      ]);
      expect(db.prepare('SELECT size FROM fs_inode WHERE ino = 2').get()).toEqual({ size: 14 });
      await client.close();
    });

    it('returns -1 without mutating chunks when the inode is missing', async () => {
      db.prepare('INSERT INTO fs_data VALUES (?, ?, ?)').run(999, 0, Buffer.from('orphan'));
      expect(statusOf(await client.send(truncateRequest(999, 0)))).toBe(-1);
      expect(db.prepare('SELECT hex(data) AS data FROM fs_data WHERE ino = 999').get()).toEqual({
        data: Buffer.from('orphan').toString('hex').toUpperCase(),
      });
      await client.close();
    });

    it('rolls back destructive chunk changes when the inode update fails', async () => {
      db.exec("CREATE TRIGGER fail_truncate BEFORE UPDATE ON fs_inode BEGIN SELECT RAISE(ABORT, 'injected failure'); END;");
      let writes = 0;
      await client.close();
      client = new TestClient(sql, undefined, (callback) => db.transaction(callback)(), () => { writes++; });

      const resp = await client.send(truncateRequest(2, 6));
      expect(resp.results[0].type).toBe('error');
      if (resp.results[0].type === 'error') expect(resp.results[0].error.message).toContain('injected failure');
      expect(db.prepare('SELECT chunk_index, hex(data) AS data FROM fs_data ORDER BY chunk_index').all()).toEqual([
        { chunk_index: 0, data: '41424344' },
        { chunk_index: 1, data: '45464748' },
        { chunk_index: 2, data: '494A' },
      ]);
      expect(db.prepare('SELECT size, mtime, ctime FROM fs_inode WHERE ino = 2').get()).toEqual({ size: 10, mtime: 0, ctime: 0 });
      expect(writes).toBe(0);
      await client.close();
    });

    it('recognizes normalized SQL whitespace and trailing semicolons', async () => {
      const resp = await client.send(truncateRequest(2, 4, 4, 1000, 500, 'SELECT airyfs_truncate_v1(?,?,?,?,?) ;;'));
      expect(statusOf(resp)).toBe(2);
      expect(db.prepare('SELECT chunk_index FROM fs_data ORDER BY chunk_index').all()).toEqual([{ chunk_index: 0 }]);
      await client.close();
    });

    it('requires transaction support', async () => {
      await client.close();
      client = new TestClient(sql);
      const resp = await client.send(truncateRequest(2, 0));
      expect(resp.results[0].type).toBe('error');
      if (resp.results[0].type === 'error') {
        expect(resp.results[0].error.message).toContain('airyfs_truncate_v1 requires transaction support');
      }
      expect(db.prepare('SELECT size FROM fs_inode WHERE ino = 2').get()).toEqual({ size: 10 });
      await client.close();
    });
  });

  // ---------------------------------------------------------------------------
  // Compound hardlink (link)
  // ---------------------------------------------------------------------------

  describe('airyfs_link_v1', () => {
    let client: TestClient;
    let fileIno: number;

    beforeEach(async () => {
      createFsSchema(db);
      client = new TestClient(sql, undefined, (callback) => db.transaction(callback)());
      // Seed a regular file to link against.
      fileIno = statusOf(await client.send(createNodeRequest('mknod', 1, 'file', 0o100644, 0, 0, 0, null, 10, 20)));
    });

    it('creates a hardlink: returns source ino, bumps nlink/ctime and parent timestamps', async () => {
      const nlinkBefore = (db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(fileIno) as { nlink: number }).nlink;
      const resp = await client.send(linkRequest(fileIno, 1, 'hardlink', 1000, 500));
      expect(statusOf(resp)).toBe(fileIno);

      expect(db.prepare('SELECT parent_ino, ino FROM fs_dentry WHERE name = ?').get('hardlink')).toEqual({
        parent_ino: 1, ino: fileIno,
      });
      expect(db.prepare('SELECT nlink, ctime, ctime_nsec FROM fs_inode WHERE ino = ?').get(fileIno)).toEqual({
        nlink: nlinkBefore + 1, ctime: 1000, ctime_nsec: 500,
      });
      expect(db.prepare('SELECT mtime, ctime FROM fs_inode WHERE ino = 1').get()).toEqual({ mtime: 1000, ctime: 1000 });
      await client.close();
    });

    it('returns 0 with no mutations when the destination already exists', async () => {
      const nlinkBefore = (db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(fileIno) as { nlink: number }).nlink;
      const resp = await client.send(linkRequest(fileIno, 1, 'file', 1000, 500));
      expect(statusOf(resp)).toBe(0);
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(fileIno)).toEqual({ nlink: nlinkBefore });
      await client.close();
    });

    it('returns -1 when the source inode is missing', async () => {
      const resp = await client.send(linkRequest(9999, 1, 'ghost', 1000, 500));
      expect(statusOf(resp)).toBe(-1);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_dentry WHERE name = ?').get('ghost')).toEqual({ c: 0 });
      await client.close();
    });

    it('returns -2 when the source inode is a directory', async () => {
      const dirIno = statusOf(await client.send(createNodeRequest('mkdir', 1, 'dir', 0o755, 0, 0, 0, null, 10, 20)));
      const resp = await client.send(linkRequest(dirIno, 1, 'dirlink', 1000, 500));
      expect(statusOf(resp)).toBe(-2);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_dentry WHERE name = ?').get('dirlink')).toEqual({ c: 0 });
      await client.close();
    });

    it('returns -1 when the new parent is missing or not a directory', async () => {
      // Missing parent.
      expect(statusOf(await client.send(linkRequest(fileIno, 999, 'x', 1000, 500)))).toBe(-1);
      // Parent that is a regular file.
      const resp = await client.send(linkRequest(fileIno, fileIno, 'x', 1000, 500));
      expect(statusOf(resp)).toBe(-1);
      await client.close();
    });

    it('rolls back leaving no partial state when a mutation fails', async () => {
      db.exec("CREATE TRIGGER fail_link_dentry BEFORE INSERT ON fs_dentry BEGIN SELECT RAISE(ABORT, 'injected failure'); END;");
      const nlinkBefore = (db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(fileIno) as { nlink: number }).nlink;

      const resp = await client.send(linkRequest(fileIno, 1, 'hardlink', 1000, 500));
      expect(resp.results[0].type).toBe('error');
      if (resp.results[0].type === 'error') {
        expect(resp.results[0].error.message).toContain('injected failure');
      }
      // Source nlink and parent timestamps unchanged (parent mtime stays at the
      // seed value of 10, not bumped to the attempted link time of 1000).
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(fileIno)).toEqual({ nlink: nlinkBefore });
      expect(db.prepare('SELECT mtime FROM fs_inode WHERE ino = 1').get()).toEqual({ mtime: 10 });
      await client.close();
    });
  });

  describe('airyfs_rename_v1', () => {
    let client: TestClient;

    beforeEach(() => {
      createFsSchema(db);
      db.exec(`
        CREATE TABLE fs_data (ino INTEGER NOT NULL, chunk_index INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (ino, chunk_index));
        CREATE TABLE fs_open_inode (session_id TEXT NOT NULL, ino INTEGER NOT NULL, open_count INTEGER NOT NULL DEFAULT 0, expires_at INTEGER NOT NULL, PRIMARY KEY (session_id, ino));
        CREATE TRIGGER trg_fs_inode_delete_cleanup AFTER DELETE ON fs_inode BEGIN
          DELETE FROM fs_data WHERE ino = OLD.ino;
          DELETE FROM fs_symlink WHERE ino = OLD.ino;
          DELETE FROM fs_open_inode WHERE ino = OLD.ino;
        END;
      `);
      client = new TestClient(sql, undefined, (callback) => db.transaction(callback)());
    });

    it('renames a file and updates inode and parent timestamps', async () => {
      const source = statusOf(await client.send(createNodeRequest('mknod', 1, 'old', 0o100644, 0, 0, 0, null, 10, 20)));
      expect(statusOf(await client.send(renameRequest(1, 'old', 1, 'new')))).toBe(source);
      expect(db.prepare('SELECT name, parent_ino, ino FROM fs_dentry').all()).toEqual([{ name: 'new', parent_ino: 1, ino: source }]);
      expect(db.prepare('SELECT ctime, ctime_nsec FROM fs_inode WHERE ino = ?').get(source)).toEqual({ ctime: 1000, ctime_nsec: 500 });
      expect(db.prepare('SELECT mtime, ctime, mtime_nsec, ctime_nsec FROM fs_inode WHERE ino = 1').get()).toEqual({
        mtime: 1000, ctime: 1000, mtime_nsec: 500, ctime_nsec: 500,
      });
      await client.close();
    });

    it('moves a directory across parents and adjusts parent link counts', async () => {
      const oldParent = statusOf(await client.send(createNodeRequest('mkdir', 1, 'from', 0o755, 0, 0, 0, null, 10, 20)));
      const newParent = statusOf(await client.send(createNodeRequest('mkdir', 1, 'to', 0o755, 0, 0, 0, null, 10, 20)));
      const source = statusOf(await client.send(createNodeRequest('mkdir', oldParent, 'child', 0o755, 0, 0, 0, null, 10, 20)));
      const oldLinks = (db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(oldParent) as { nlink: number }).nlink;
      const newLinks = (db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(newParent) as { nlink: number }).nlink;
      expect(statusOf(await client.send(renameRequest(oldParent, 'child', newParent, 'moved')))).toBe(source);
      expect(db.prepare('SELECT parent_ino, name FROM fs_dentry WHERE ino = ?').get(source)).toEqual({ parent_ino: newParent, name: 'moved' });
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(oldParent)).toEqual({ nlink: oldLinks - 1 });
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(newParent)).toEqual({ nlink: newLinks + 1 });
      await client.close();
    });

    it('retains an overwritten inode while a lease is live', async () => {
      const source = statusOf(await client.send(createNodeRequest('mknod', 1, 'source', 0o100644, 0, 0, 0, null, 10, 20)));
      const destination = statusOf(await client.send(createNodeRequest('mknod', 1, 'destination', 0o100644, 0, 0, 0, null, 10, 20)));
      db.prepare('INSERT INTO fs_data VALUES (?, 0, ?)').run(destination, Buffer.from('old'));
      db.prepare('INSERT INTO fs_open_inode VALUES (?, ?, 1, ?)').run('session', destination, 2000);
      expect(statusOf(await client.send(renameRequest(1, 'source', 1, 'destination')))).toBe(source);
      expect(db.prepare('SELECT ino FROM fs_dentry WHERE name = ?').get('destination')).toEqual({ ino: source });
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(destination)).toEqual({ nlink: 0 });
      expect(db.prepare('SELECT hex(data) AS data FROM fs_data WHERE ino = ?').get(destination)).toEqual({ data: '6F6C64' });
      await client.close();
    });

    it('deletes an unleased overwritten inode and dependent rows', async () => {
      const source = statusOf(await client.send(createNodeRequest('mknod', 1, 'source', 0o100644, 0, 0, 0, null, 10, 20)));
      const destination = statusOf(await client.send(createNodeRequest('symlink', 1, 'destination', 0, 0, 0, 0, 'target', 10, 20)));
      expect(statusOf(await client.send(renameRequest(1, 'source', 1, 'destination')))).toBe(source);
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_inode WHERE ino = ?').get(destination)).toEqual({ c: 0 });
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_symlink WHERE ino = ?').get(destination)).toEqual({ c: 0 });
      await client.close();
    });

    it('rejects incompatible and nonempty directory replacements', async () => {
      const file = statusOf(await client.send(createNodeRequest('mknod', 1, 'file', 0o100644, 0, 0, 0, null, 10, 20)));
      const directory = statusOf(await client.send(createNodeRequest('mkdir', 1, 'directory', 0o755, 0, 0, 0, null, 10, 20)));
      expect(statusOf(await client.send(renameRequest(1, 'file', 1, 'directory')))).toBe(-2);
      expect(statusOf(await client.send(renameRequest(1, 'directory', 1, 'file')))).toBe(-3);
      const other = statusOf(await client.send(createNodeRequest('mkdir', 1, 'other', 0o755, 0, 0, 0, null, 10, 20)));
      await client.send(createNodeRequest('mknod', other, 'child', 0o100644, 0, 0, 0, null, 10, 20));
      expect(statusOf(await client.send(renameRequest(1, 'directory', 1, 'other')))).toBe(-4);
      expect(db.prepare('SELECT ino FROM fs_dentry WHERE name = ?').get('file')).toEqual({ ino: file });
      expect(db.prepare('SELECT ino FROM fs_dentry WHERE name = ?').get('directory')).toEqual({ ino: directory });
      await client.close();
    });

    it('replaces an empty directory without leaking its inode or parent links', async () => {
      const source = statusOf(await client.send(createNodeRequest('mkdir', 1, 'source', 0o755, 0, 0, 0, null, 10, 20)));
      const destination = statusOf(await client.send(createNodeRequest('mkdir', 1, 'destination', 0o755, 0, 0, 0, null, 10, 20)));
      const parentLinks = (db.prepare('SELECT nlink FROM fs_inode WHERE ino = 1').get() as { nlink: number }).nlink;
      expect(statusOf(await client.send(renameRequest(1, 'source', 1, 'destination')))).toBe(source);
      expect(db.prepare('SELECT ino FROM fs_dentry WHERE name = ?').get('destination')).toEqual({ ino: source });
      expect(db.prepare('SELECT COUNT(*) AS c FROM fs_inode WHERE ino = ?').get(destination)).toEqual({ c: 0 });
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = 1').get()).toEqual({ nlink: parentLinks - 1 });
      await client.close();
    });

    it('rejects moving a directory into its own subtree', async () => {
      const parent = statusOf(await client.send(createNodeRequest('mkdir', 1, 'parent', 0o755, 0, 0, 0, null, 10, 20)));
      const child = statusOf(await client.send(createNodeRequest('mkdir', parent, 'child', 0o755, 0, 0, 0, null, 10, 20)));
      expect(statusOf(await client.send(renameRequest(1, 'parent', child, 'cycle')))).toBe(-5);
      expect(db.prepare('SELECT parent_ino, name FROM fs_dentry WHERE ino = ?').get(parent)).toEqual({ parent_ino: 1, name: 'parent' });
      await client.close();
    });

    it('does nothing for identical paths or two links to the same inode', async () => {
      const source = statusOf(await client.send(createNodeRequest('mknod', 1, 'first', 0o100644, 0, 0, 0, null, 10, 20)));
      expect(statusOf(await client.send(renameRequest(1, 'first', 1, 'first')))).toBe(source);
      expect(statusOf(await client.send(linkRequest(source, 1, 'second', 10, 20)))).toBe(source);
      expect(statusOf(await client.send(renameRequest(1, 'first', 1, 'second')))).toBe(source);
      expect(db.prepare('SELECT name FROM fs_dentry WHERE ino = ? ORDER BY name').all(source)).toEqual([{ name: 'first' }, { name: 'second' }]);
      await client.close();
    });

    it('rolls back destination removal when moving the source fails', async () => {
      const source = statusOf(await client.send(createNodeRequest('mknod', 1, 'source', 0o100644, 0, 0, 0, null, 10, 20)));
      const destination = statusOf(await client.send(createNodeRequest('mknod', 1, 'destination', 0o100644, 0, 0, 0, null, 10, 20)));
      db.exec("CREATE TRIGGER fail_move BEFORE UPDATE ON fs_dentry BEGIN SELECT RAISE(ABORT, 'injected failure'); END;");
      const resp = await client.send(renameRequest(1, 'source', 1, 'destination'));
      expect(resp.results[0].type).toBe('error');
      expect(db.prepare('SELECT name, ino FROM fs_dentry ORDER BY name').all()).toEqual([
        { name: 'destination', ino: destination }, { name: 'source', ino: source },
      ]);
      expect(db.prepare('SELECT nlink FROM fs_inode WHERE ino = ?').get(destination)).toEqual({ nlink: 1 });
      await client.close();
    });

    it('recognizes normalized SQL and requires transaction support', async () => {
      const source = statusOf(await client.send(createNodeRequest('mknod', 1, 'old', 0o100644, 0, 0, 0, null, 10, 20)));
      expect(statusOf(await client.send(renameRequest(1, 'old', 1, 'new', 1000, 500, 'SELECT airyfs_rename_v1(?,?,?,?,?,?) ;;')))).toBe(source);
      await client.close();
      client = new TestClient(sql);
      const resp = await client.send(renameRequest(1, 'new', 1, 'again'));
      expect(resp.results[0].type).toBe('error');
      if (resp.results[0].type === 'error') expect(resp.results[0].error.message).toContain('requires transaction support');
      await client.close();
    });
  });
});
