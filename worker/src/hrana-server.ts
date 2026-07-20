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
  return upper.startsWith('SELECT') || upper.startsWith('EXPLAIN');
}

async function executeStmt(
  backend: SqlBackend,
  stmt: Stmt,
  writeLock?: () => Promise<() => void>
): Promise<StmtResult> {
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

  const release = writeLock && !isReadOnlyStatement(query)
    ? await writeLock()
    : () => undefined;
  let cursor: SqlCursorResult;
  try {
    cursor = backend.exec(query, ...bindings);
  } finally {
    release();
  }

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
  }) {
    this.readable = opts.readable;
    this.writable = opts.writable;
    this.sql = opts.sql;
    this.writeLock = opts.writeLock;
    this.onWrite = opts.onWrite;
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
          const result = await executeStmt(this.sql, statement, this.writeLock);
          if (!isReadOnlyStatement(statement.sql ?? '')) await this.onWrite?.();
          return { type: 'execute', result };
        }

      case 'batch':
        {
          const result = await this.executeBatch(req.batch.steps);
          if (req.batch.steps.some((step) => !isReadOnlyStatement(step.stmt.sql ?? ''))) await this.onWrite?.();
          return { type: 'batch', result };
        }

      case 'get_autocommit':
        return { type: 'get_autocommit', is_autocommit: true };

      case 'sequence': {
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

  private async executeBatch(steps: { condition?: unknown; stmt: Stmt }[]): Promise<BatchResult> {
    const stepResults: (StmtResult | null)[] = [];
    const stepErrors: ({ message: string } | null)[] = [];
    const resolvedSteps = steps.map((step) => ({ ...step, stmt: this.resolveStmt(step.stmt) }));
    const needsWriteLock = resolvedSteps.some((step) => {
      const query = step.stmt.sql ?? '';
      return !isBlockedStatement(query) && !isPragma(query) && !isReadOnlyStatement(query);
    });
    const release = this.writeLock && needsWriteLock
      ? await this.writeLock()
      : () => undefined;

    try {
      for (const step of resolvedSteps) {
        if (step.condition && !this.evaluateCondition(step.condition, stepResults, stepErrors)) {
          stepResults.push(null);
          stepErrors.push(null);
          continue;
        }

        this.statementCount++;
        try {
          stepResults.push(await executeStmt(this.sql, step.stmt));
          stepErrors.push(null);
        } catch (err) {
          stepResults.push(null);
          stepErrors.push({ message: err instanceof Error ? err.message : String(err) });
        }
      }
    } finally {
      release();
    }

    return { step_results: stepResults, step_errors: stepErrors };
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
