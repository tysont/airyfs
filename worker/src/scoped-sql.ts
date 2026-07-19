// ABOUTME: Validates and executes user-owned SQLite statements inside a volume DO.
// ABOUTME: Restricts SQL to app_* objects so AiryFS filesystem and control tables stay hidden.

import { HttpError } from './files-api';

const MAX_SQL_BYTES = 64 * 1024;
const MAX_RESULT_ROWS = 1000;
const FORBIDDEN_PREFIXES = ['FS_', 'SQLITE_', 'PRAGMA_'];
const FORBIDDEN_NAMES = new Set([
  '_CF_KV', '__CF_KV', 'KV_STORE', 'TOOL_CALLS', 'CAPABILITY_REVOCATIONS', 'VOLUME_AUTH', 'SITE_CONFIG',
]);

export type SqlJsonValue = string | number | null | { base64: string };
export interface ScopedSqlResult {
  columns: string[];
  rows: SqlJsonValue[][];
  rowsRead: number;
  rowsWritten: number;
  truncated: boolean;
}

interface SqlCursor extends Iterable<Record<string, unknown>> {
  columnNames: string[];
  rowsRead: number;
  rowsWritten: number;
}

export interface ScopedSqlStorage {
  exec(query: string, ...bindings: unknown[]): SqlCursor;
}

interface Token {
  kind: 'identifier' | 'string' | 'symbol';
  value: string;
  upper: string;
}

export function executeScopedSql(
  storage: ScopedSqlStorage,
  statement: unknown,
  rawArgs: unknown,
): ScopedSqlResult {
  if (typeof statement !== 'string' || statement.trim() === '') {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'sql must be a non-empty string');
  }
  if (new TextEncoder().encode(statement).byteLength > MAX_SQL_BYTES) {
    throw new HttpError(413, 'SQL_TOO_LARGE', `sql cannot exceed ${MAX_SQL_BYTES} bytes`);
  }
  const tokens = tokenize(statement);
  validateStatement(tokens);
  const args = parseArgs(rawArgs);
  let cursor: SqlCursor;
  try {
    cursor = storage.exec(statement, ...args);
  } catch (error) {
    throw new HttpError(400, 'SQL_ERROR', error instanceof Error ? error.message : String(error));
  }

  const rows: SqlJsonValue[][] = [];
  let truncated = false;
  const readOnly = tokens[0]?.upper === 'SELECT' || tokens[0]?.upper === 'EXPLAIN';
  for (const row of cursor) {
    if (rows.length < MAX_RESULT_ROWS) {
      rows.push(cursor.columnNames.map((column) => toJsonValue(row[column])));
    } else {
      truncated = true;
      if (readOnly) break;
    }
  }
  return {
    columns: [...cursor.columnNames],
    rows,
    rowsRead: cursor.rowsRead,
    rowsWritten: cursor.rowsWritten,
    truncated,
  };
}

function parseArgs(value: unknown): unknown[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new HttpError(400, 'INVALID_ARGUMENT', 'args must be an array');
  return value.map((entry) => {
    if (entry === null || typeof entry === 'string' || typeof entry === 'number') {
      if (typeof entry === 'number' && (!Number.isFinite(entry) || !Number.isSafeInteger(entry) && Number.isInteger(entry))) {
        throw new HttpError(400, 'INVALID_ARGUMENT', 'integer args must be within the JavaScript safe range');
      }
      return entry;
    }
    if (typeof entry === 'object' && entry !== null && Object.keys(entry).length === 1 && typeof (entry as { base64?: unknown }).base64 === 'string') {
      return decodeBase64((entry as { base64: string }).base64);
    }
    throw new HttpError(400, 'INVALID_ARGUMENT', 'args may contain strings, numbers, null, or {base64} blobs');
  });
}

function validateStatement(tokens: Token[]): void {
  const meaningful = tokens.filter((token) => token.kind !== 'symbol' || token.value !== ';');
  if (meaningful.length === 0) throw new HttpError(400, 'INVALID_SQL', 'sql must contain a statement');
  const semicolons = tokens.filter((token) => token.kind === 'symbol' && token.value === ';');
  if (semicolons.length > 1 || (semicolons.length === 1 && tokens[tokens.length - 1] !== semicolons[0])) {
    throw new HttpError(400, 'INVALID_SQL', 'exactly one SQL statement is allowed');
  }
  for (const token of tokens) {
    if (token.kind === 'identifier' && isInternalName(token.upper)) {
      throw new HttpError(403, 'SQL_SCOPE_VIOLATION', `AiryFS internal object is not accessible: ${token.value}`);
    }
  }

  const first = meaningful[0].upper;
  if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER'].includes(first)) {
    throw new HttpError(400, 'UNSUPPORTED_SQL', `Unsupported SQL statement: ${meaningful[0].value}`);
  }
  if (meaningful.some((token) => token.kind === 'identifier' && ['ATTACH', 'DETACH', 'VACUUM', 'REINDEX', 'ANALYZE', 'WITH'].includes(token.upper))) {
    throw new HttpError(400, 'UNSUPPORTED_SQL', 'Statement uses an unsupported SQLite feature');
  }

  if (first === 'CREATE' || first === 'DROP') {
    const objectType = meaningful[1]?.upper;
    if (objectType !== 'TABLE' && objectType !== 'INDEX') {
      throw new HttpError(400, 'UNSUPPORTED_SQL', `${first} only supports TABLE or INDEX`);
    }
  }
  if (first === 'ALTER' && meaningful[1]?.upper !== 'TABLE') {
    throw new HttpError(400, 'UNSUPPORTED_SQL', 'ALTER only supports TABLE');
  }

  for (let index = 0; index < meaningful.length; index++) {
    const token = meaningful[index];
    if (token.kind !== 'identifier') continue;
    if (['FROM', 'JOIN', 'INTO', 'UPDATE', 'REFERENCES'].includes(token.upper)) {
      requireAppObject(nextIdentifier(meaningful, index + 1), token.value);
    }
    if (['TABLE', 'INDEX'].includes(token.upper) && ['CREATE', 'DROP', 'ALTER'].includes(first)) {
      let next = index + 1;
      if (meaningful[next]?.upper === 'IF') next += meaningful[next + 1]?.upper === 'NOT' ? 3 : 2;
      requireAppObject(nextIdentifier(meaningful, next), token.value);
    }
    if (first === 'CREATE' && meaningful[1]?.upper === 'INDEX' && token.upper === 'ON') {
      requireAppObject(nextIdentifier(meaningful, index + 1), token.value);
    }
    if (first === 'ALTER' && token.upper === 'RENAME' && meaningful[index + 1]?.upper === 'TO') {
      requireAppObject(nextIdentifier(meaningful, index + 2), token.value);
    }
  }
}

function nextIdentifier(tokens: Token[], start: number): Token | undefined {
  for (let index = start; index < tokens.length; index++) {
    if (tokens[index].kind === 'identifier') return tokens[index];
    if (tokens[index].value === '(') return undefined;
  }
  return undefined;
}

function requireAppObject(token: Token | undefined, context: string): void {
  if (!token || !token.upper.startsWith('APP_')) {
    throw new HttpError(403, 'SQL_SCOPE_VIOLATION', `${context} may reference only app_* tables and indexes`);
  }
}

function isInternalName(name: string): boolean {
  return FORBIDDEN_PREFIXES.some((prefix) => name.startsWith(prefix)) || FORBIDDEN_NAMES.has(name);
}

function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  for (let index = 0; index < sql.length;) {
    const character = sql[index];
    if (/\s/.test(character)) { index++; continue; }
    if (character === '-' && sql[index + 1] === '-') {
      index = sql.indexOf('\n', index + 2);
      if (index < 0) break;
      continue;
    }
    if (character === '/' && sql[index + 1] === '*') {
      const end = sql.indexOf('*/', index + 2);
      if (end < 0) throw new HttpError(400, 'INVALID_SQL', 'Unterminated SQL comment');
      index = end + 2;
      continue;
    }
    if (character === "'") {
      const { value, next } = quoted(sql, index, "'", "'");
      tokens.push({ kind: 'string', value, upper: value.toUpperCase() });
      index = next;
      continue;
    }
    if (character === '"' || character === '`' || character === '[') {
      const closing = character === '[' ? ']' : character;
      const { value, next } = quoted(sql, index, closing, character === '[' ? null : character);
      tokens.push({ kind: 'identifier', value, upper: value.toUpperCase() });
      index = next;
      continue;
    }
    const word = /^[A-Za-z_][A-Za-z0-9_$]*/.exec(sql.slice(index));
    if (word) {
      tokens.push({ kind: 'identifier', value: word[0], upper: word[0].toUpperCase() });
      index += word[0].length;
      continue;
    }
    tokens.push({ kind: 'symbol', value: character, upper: character });
    index++;
  }
  return tokens;
}

function quoted(sql: string, start: number, closing: string, escape: string | null): { value: string; next: number } {
  let value = '';
  for (let index = start + 1; index < sql.length; index++) {
    if (sql[index] === closing) {
      if (escape && sql[index + 1] === escape) { value += closing; index++; continue; }
      return { value, next: index + 1 };
    }
    value += sql[index];
  }
  throw new HttpError(400, 'INVALID_SQL', 'Unterminated quoted SQL token');
}

function toJsonValue(value: unknown): SqlJsonValue {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' || typeof value === 'number') return value;
  if (value instanceof ArrayBuffer || value instanceof Uint8Array) {
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
    let binary = '';
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return { base64: btoa(binary) };
  }
  return String(value);
}

function decodeBase64(value: string): Uint8Array {
  try {
    const binary = atob(value);
    return Uint8Array.from(binary, (character) => character.charCodeAt(0));
  } catch {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Invalid base64 blob argument');
  }
}
