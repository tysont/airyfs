// ABOUTME: Typed, server-side text operations that run in the volume Durable
// ABOUTME: Object with no Container — line reads today, more to follow.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import type { SqlExec } from './schema';
import { normalizePath } from './auth';
import { HttpError, VolumeAccessCoordinator, writeFileStream } from './files-api';

/** Largest file a text operation will read, matching grep's scan bound. */
export const MAX_TEXT_FILE_BYTES = 10 * 1024 * 1024;
/** Default number of lines returned when a caller does not specify one. */
export const DEFAULT_LINES = 1000;
/** Hard ceiling on lines returned in a single readLines call. */
export const MAX_LINES = 10_000;
/** Hard ceiling on matches a single replaceText call may rewrite. */
export const MAX_MATCHES = 100_000;
/**
 * Largest JSON file jsonQuery will bind to SQLite. Empirically, Durable Object
 * SQLite rejects a bound parameter somewhere between 2 and 3 MiB (an opaque
 * failure), so this sits well below that with margin. readLines/replaceText/
 * lineStats keep the 10 MiB bound because they never bind content to SQL.
 */
export const MAX_JSON_QUERY_BYTES = 1024 * 1024;

function escapeRegExp(value: string): string {
  return value.replace(/[\\^$.*+?()[\]{}|]/g, '\\$&');
}

function reqString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `Missing "${name}" string`);
  }
  return value;
}

function optCount(value: unknown): number {
  if (value === undefined) return DEFAULT_LINES;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1 || value > MAX_LINES) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `count must be between 1 and ${MAX_LINES}`);
  }
  return value;
}

function reqPositive(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `"${name}" must be a positive integer`);
  }
  return value;
}

/**
 * Split UTF-8 text into lines. A single trailing newline does not create an
 * empty final line (POSIX text-file convention); content after the last
 * newline is kept as the final (unterminated) line.
 */
function splitLines(text: string): string[] {
  const parts = text.split('\n');
  if (parts.length > 0 && parts[parts.length - 1] === '') parts.pop();
  return parts;
}

export interface ReadLinesInput {
  path?: unknown;
  /** `head` (first N), `tail` (last N), or `range` (lines start..end). Default `head`. */
  mode?: unknown;
  /** Line count for `head`/`tail`. Default {@link DEFAULT_LINES}, max {@link MAX_LINES}. */
  count?: unknown;
  /** 1-based inclusive start line for `range`. */
  start?: unknown;
  /** 1-based inclusive end line for `range`. */
  end?: unknown;
}

export interface ReadLinesResult {
  lines: string[];
  /** 1-based line number of the first returned line, or 0 when none. */
  startLine: number;
  /** 1-based line number of the last returned line, or 0 when none. */
  endLine: number;
  /** Total lines in the file. */
  totalLines: number;
  /** True when the file has more lines beyond what was returned (in the read direction). */
  truncated: boolean;
}

/**
 * Read a bounded, line-addressed slice of a text file directly from Durable
 * Object SQLite (no Container). Line numbers are 1-based and inclusive; `tail`
 * is an explicit mode rather than a negative index.
 */
export async function readLines(
  fs: FileSystem,
  access: VolumeAccessCoordinator | undefined,
  input: ReadLinesInput,
): Promise<ReadLinesResult> {
  const original = reqString(input.path, 'path');
  const path = normalizePath(original);
  const mode = input.mode === undefined ? 'head' : input.mode;
  if (mode !== 'head' && mode !== 'tail' && mode !== 'range') {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'mode must be head, tail, or range');
  }

  const release = access ? await access.acquireRead(path) : () => undefined;
  try {
    const stats = await fs.lstat(path);
    if (stats.isDirectory()) {
      throw new HttpError(409, 'EISDIR', `EISDIR: illegal operation on a directory, readLines '${original}'`);
    }
    if (stats.size > MAX_TEXT_FILE_BYTES) {
      throw new HttpError(413, 'FILE_TOO_LARGE', `file exceeds ${MAX_TEXT_FILE_BYTES} bytes; use exec for larger files`);
    }

    const text = (await fs.readFile(path, 'utf8')) as unknown as string;
    const all = splitLines(text);
    const total = all.length;

    let startIdx: number;
    let endExclusive: number;
    if (mode === 'range') {
      const start = reqPositive(input.start, 'start');
      const end = reqPositive(input.end, 'end');
      if (end < start) throw new HttpError(400, 'INVALID_ARGUMENT', '"end" must be >= "start"');
      if (end - start + 1 > MAX_LINES) {
        throw new HttpError(400, 'INVALID_ARGUMENT', `range exceeds ${MAX_LINES} lines`);
      }
      startIdx = start - 1;
      endExclusive = Math.min(end, total);
    } else if (mode === 'tail') {
      const count = optCount(input.count);
      startIdx = Math.max(0, total - count);
      endExclusive = total;
    } else {
      const count = optCount(input.count);
      startIdx = 0;
      endExclusive = Math.min(count, total);
    }

    if (startIdx >= total || endExclusive <= startIdx) {
      return { lines: [], startLine: 0, endLine: 0, totalLines: total, truncated: false };
    }
    const lines = all.slice(startIdx, endExclusive);
    return {
      lines,
      startLine: startIdx + 1,
      endLine: startIdx + lines.length,
      totalLines: total,
      truncated: mode === 'tail' ? startIdx > 0 : endExclusive < total,
    };
  } finally {
    release();
  }
}

export interface ReplaceTextInput {
  path?: unknown;
  /** Search pattern — a JS regular expression source, or a literal when `literal` is set. */
  pattern?: unknown;
  /** Replacement string. Supports `$1`..`$n`, `$&`, `$\`` and `$'` (JS String.replace semantics). */
  replacement?: unknown;
  /** Case-insensitive matching. Default false. */
  ignoreCase?: unknown;
  /** Treat `pattern` as a literal string instead of a regular expression. Default false. */
  literal?: unknown;
  /** Report the match count without writing. Default false. */
  dryRun?: unknown;
}

export interface ReplaceTextResult {
  /** Number of matches found (and replaced, unless dryRun). */
  matches: number;
  /** True when the file was rewritten (matches > 0 and not dryRun). */
  changed: boolean;
  dryRun: boolean;
}

/**
 * Find/replace over one text file, written back atomically (temp + rename) so a
 * reader never sees a partial file. Bounded to a {@link MAX_TEXT_FILE_BYTES}
 * file and {@link MAX_MATCHES} matches; rejects a too-broad match before
 * writing. `dryRun` reports the count without mutating. Global by default
 * (all occurrences).
 */
export async function replaceText(
  fs: FileSystem,
  access: VolumeAccessCoordinator | undefined,
  input: ReplaceTextInput,
): Promise<ReplaceTextResult> {
  const original = reqString(input.path, 'path');
  const path = normalizePath(original);
  if (typeof input.pattern !== 'string' || input.pattern.length === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "pattern" string');
  }
  if (typeof input.replacement !== 'string') {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing "replacement" string');
  }
  const dryRun = input.dryRun === true;
  const source = input.literal === true ? escapeRegExp(input.pattern) : input.pattern;
  let regex: RegExp;
  try {
    regex = new RegExp(source, input.ignoreCase === true ? 'gi' : 'g');
  } catch (error) {
    throw new HttpError(400, 'INVALID_PATTERN', error instanceof Error ? error.message : String(error));
  }

  const release = access ? await access.acquireWrite(path) : () => undefined;
  try {
    const stats = await fs.lstat(path);
    if (stats.isDirectory()) {
      throw new HttpError(409, 'EISDIR', `EISDIR: illegal operation on a directory, replaceText '${original}'`);
    }
    if (stats.size > MAX_TEXT_FILE_BYTES) {
      throw new HttpError(413, 'FILE_TOO_LARGE', `file exceeds ${MAX_TEXT_FILE_BYTES} bytes; use exec for larger files`);
    }
    const content = (await fs.readFile(path, 'utf8')) as unknown as string;
    const matches = content.match(regex)?.length ?? 0;
    if (matches > MAX_MATCHES) {
      throw new HttpError(400, 'INVALID_ARGUMENT', `pattern matches more than ${MAX_MATCHES} times; narrow it`);
    }
    if (dryRun || matches === 0) {
      return { matches, changed: false, dryRun };
    }
    const next = content.replace(regex, input.replacement);
    // Atomic write-back: we already hold the path write lock, so hand
    // writeFileStream a null coordinator to avoid re-locking.
    const stream = new Response(next).body as ReadableStream<Uint8Array>;
    await writeFileStream(fs, path, stream, undefined);
    return { matches, changed: true, dryRun: false };
  } finally {
    release();
  }
}

export interface LineStatsInput {
  path?: unknown;
}

export interface LineStatsResult {
  path: string;
  lines: number;
  words: number;
  bytes: number;
}

/**
 * Count lines, whitespace-delimited words, and bytes of one text file,
 * server-side (no Container). Bytes are the exact file size; a single trailing
 * newline does not add an empty line. Match counting is intentionally excluded
 * — that lives in `search` (grep) where match semantics are defined once.
 * Bounded to a {@link MAX_TEXT_FILE_BYTES} file. Single-file only; a directory
 * is rejected (aggregate scope is not offered here to avoid silently crossing
 * mount boundaries).
 */
export async function lineStats(
  fs: FileSystem,
  access: VolumeAccessCoordinator | undefined,
  input: LineStatsInput,
): Promise<LineStatsResult> {
  const original = reqString(input.path, 'path');
  const path = normalizePath(original);
  const release = access ? await access.acquireRead(path) : () => undefined;
  try {
    const stats = await fs.lstat(path);
    if (stats.isDirectory()) {
      throw new HttpError(409, 'EISDIR', `EISDIR: illegal operation on a directory, lineStats '${original}'`);
    }
    if (stats.size > MAX_TEXT_FILE_BYTES) {
      throw new HttpError(413, 'FILE_TOO_LARGE', `file exceeds ${MAX_TEXT_FILE_BYTES} bytes; use exec for larger files`);
    }
    const text = (await fs.readFile(path, 'utf8')) as unknown as string;
    const lines = splitLines(text).length;
    const words = text.split(/\s+/).filter((w) => w.length > 0).length;
    return { path, lines, words, bytes: stats.size };
  } finally {
    release();
  }
}

export interface JsonQueryInput {
  path?: unknown;
  /** A JSONPath expression starting with `$`, e.g. `$.items[0].name`. */
  query?: unknown;
}

export interface JsonQueryResult {
  /** The extracted value (objects/arrays parsed; booleans/null typed). */
  value: unknown;
  /** SQLite json_type of the match, or null when the path did not match. */
  type: string | null;
  /** True when the JSONPath matched something in the document. */
  found: boolean;
}

/**
 * Evaluate a JSONPath against a JSON file using SQLite's built-in json_extract
 * — a defined JSONPath subset, not a jq/query DSL — server-side (no Container).
 * Trusted internal SQL with the content bound as a parameter (not the scoped-SQL
 * path). Bounded to a {@link MAX_TEXT_FILE_BYTES} file; verify this stays within
 * the DO SQLite bind limit.
 */
export async function jsonQuery(
  fs: FileSystem,
  access: VolumeAccessCoordinator | undefined,
  sql: SqlExec | undefined,
  input: JsonQueryInput,
): Promise<JsonQueryResult> {
  const original = reqString(input.path, 'path');
  const path = normalizePath(original);
  const query = reqString(input.query, 'query');
  if (!query.startsWith('$')) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'query must be a JSONPath starting with "$"');
  }
  if (!sql) {
    throw new HttpError(500, 'INTERNAL_ERROR', 'SQL executor unavailable for jsonQuery');
  }

  const release = access ? await access.acquireRead(path) : () => undefined;
  try {
    const stats = await fs.lstat(path);
    if (stats.isDirectory()) {
      throw new HttpError(409, 'EISDIR', `EISDIR: illegal operation on a directory, jsonQuery '${original}'`);
    }
    if (stats.size > MAX_JSON_QUERY_BYTES) {
      throw new HttpError(413, 'FILE_TOO_LARGE', `JSON file exceeds ${MAX_JSON_QUERY_BYTES} bytes; use exec (jq) for larger files`);
    }
    const content = (await fs.readFile(path, 'utf8')) as unknown as string;

    const valid = sql.exec('SELECT json_valid(?) AS ok', content).toArray();
    if (Number(valid[0]?.ok ?? 0) !== 1) {
      throw new HttpError(400, 'INVALID_JSON', 'file content is not valid JSON');
    }

    let type: string | null;
    try {
      const typeRow = sql.exec('SELECT json_type(?, ?) AS t', content, query).toArray();
      type = typeRow[0]?.t == null ? null : String(typeRow[0].t);
    } catch (error) {
      // Malformed JSONPath surfaces as a SQLite error; map to a clean 400.
      throw new HttpError(400, 'INVALID_ARGUMENT', `invalid JSONPath: ${error instanceof Error ? error.message : String(error)}`);
    }
    if (type === null) {
      return { value: null, type: null, found: false };
    }

    const raw = sql.exec('SELECT json_extract(?, ?) AS v', content, query).toArray()[0]?.v;
    let value: unknown;
    switch (type) {
      case 'object':
      case 'array':
        value = JSON.parse(String(raw));
        break;
      case 'true':
        value = true;
        break;
      case 'false':
        value = false;
        break;
      case 'null':
        value = null;
        break;
      default:
        value = raw; // integer | real | text
    }
    return { value, type, found: true };
  } finally {
    release();
  }
}
