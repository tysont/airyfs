// ABOUTME: Bounded server-side path discovery, glob matching, and streaming text search.
// ABOUTME: Walks AgentFS directly so search does not require a Container cold start.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { normalizePath } from './auth';
import { HttpError, VolumeAccessCoordinator } from './files-api';

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const MAX_ENTRIES = 100_000;
const MAX_FILE_BYTES = 10 * 1024 * 1024;
const MAX_SCAN_BYTES = 100 * 1024 * 1024;
const READ_SIZE = 256 * 1024;

export interface SearchResult {
  path: string;
  type: 'file' | 'directory' | 'symlink';
  line?: number;
  column?: number;
  text?: string;
}

export interface SearchResponse {
  results: SearchResult[];
  truncated: boolean;
  scannedEntries: number;
  scannedBytes: number;
}

export interface SearchInput {
  mode: unknown;
  path?: unknown;
  pattern: unknown;
  regex?: unknown;
  ignoreCase?: unknown;
  limit?: unknown;
}

export interface SearchSql {
  exec(query: string, ...bindings: unknown[]): { toArray(): Record<string, unknown>[] };
}

interface FindRow {
  path: string;
  mode: number;
}

const S_IFMT = 0o170000;
const S_IFDIR = 0o040000;
const S_IFLNK = 0o120000;

function resultType(mode: number): SearchResult['type'] {
  if ((mode & S_IFMT) === S_IFDIR) return 'directory';
  if ((mode & S_IFMT) === S_IFLNK) return 'symlink';
  return 'file';
}

function ftsPhrase(pattern: string): string {
  return `"${pattern.replace(/"/g, '""')}"`;
}

async function findNames(
  fs: FileSystem,
  sql: SearchSql,
  access: VolumeAccessCoordinator,
  root: string,
  pattern: string,
  ignoreCase: boolean,
  resultLimit: number,
): Promise<SearchResponse> {
  const release = await access.acquireRead(root);
  try {
    const rootIno = (await fs.stat(root)).ino;
    const useFts = [...pattern].length >= 3;
    const match = useFts
      ? `fs_dentry_fts MATCH ?${ignoreCase ? '' : ' AND instr(subtree.name, ?) > 0'}`
      : `${ignoreCase ? 'instr(lower(subtree.name), lower(?))' : 'instr(subtree.name, ?)'} > 0`;
    const bindings: unknown[] = [];
    if (useFts) {
      bindings.push(ftsPhrase(pattern));
      if (!ignoreCase) bindings.push(pattern);
    } else bindings.push(pattern);
    bindings.push(rootIno, root, root, rootIno);
    if (root === '/') bindings.push('/.airyfs-trash', '/.airyfs-trash/%');
    bindings.push(resultLimit + 1);

    const rows = sql.exec(`WITH RECURSIVE
    candidates(id, name, parent_ino, ino) AS (
      SELECT d.id, d.name, d.parent_ino, d.ino
      FROM fs_dentry d
      ${useFts ? 'JOIN fs_dentry_fts ON fs_dentry_fts.rowid = d.id' : ''}
      WHERE ${match.replaceAll('subtree.name', 'd.name')}
    ),
    paths(id, ino, parent_ino, path, depth) AS (
      SELECT id, ino, parent_ino, '/' || name, 1 FROM candidates
      UNION ALL
      SELECT paths.id, paths.ino, parent.parent_ino, '/' || parent.name || paths.path, paths.depth + 1
      FROM paths JOIN fs_dentry parent ON parent.ino = paths.parent_ino
      WHERE paths.parent_ino != ?
    )
    SELECT CASE WHEN ? = '/' THEN paths.path ELSE ? || paths.path END AS path, inode.mode
    FROM paths JOIN fs_inode inode ON inode.ino = paths.ino
    WHERE paths.parent_ino = ?
      ${root === '/' ? 'AND paths.path != ? AND paths.path NOT LIKE ?' : ''}
    ORDER BY paths.depth, paths.path
    LIMIT ?`, ...bindings).toArray() as unknown as FindRow[];
    const truncated = rows.length > resultLimit;
    const results = rows.slice(0, resultLimit).map((row) => ({
      path: row.path,
      type: resultType(Number(row.mode)),
    }));
    return { results, truncated, scannedEntries: rows.length, scannedBytes: 0 };
  } finally {
    release();
  }
}

function limit(value: unknown): number {
  if (value === undefined) return DEFAULT_LIMIT;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1 || value > MAX_LIMIT) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `limit must be between 1 and ${MAX_LIMIT}`);
  }
  return value;
}

function globRegex(pattern: string): RegExp {
  let source = '^';
  for (let index = 0; index < pattern.length; index++) {
    const character = pattern[index];
    if (character === '*') {
      if (pattern[index + 1] === '*') {
        source += '.*';
        index++;
      } else {
        source += '[^/]*';
      }
    } else if (character === '?') source += '[^/]';
    else source += character.replace(/[\\^$+?.()|{}\[\]]/g, '\\$&');
  }
  return new RegExp(`${source}$`);
}

function pathType(entry: { stats: { isDirectory(): boolean; isSymbolicLink(): boolean } }): SearchResult['type'] {
  if (entry.stats.isDirectory()) return 'directory';
  if (entry.stats.isSymbolicLink()) return 'symlink';
  return 'file';
}

async function grepFile(
  fs: FileSystem,
  path: string,
  matcher: RegExp,
  results: SearchResult[],
  resultLimit: number,
): Promise<number> {
  const stats = await fs.stat(path);
  if (stats.size > MAX_FILE_BYTES) return 0;
  const handle = await fs.open(path);
  const decoder = new TextDecoder();
  let offset = 0;
  let pending = '';
  let lineNumber = 1;
  while (offset < stats.size && results.length < resultLimit) {
    const chunk = new Uint8Array(await handle.pread(offset, Math.min(READ_SIZE, stats.size - offset)));
    if (chunk.includes(0)) return stats.size;
    offset += chunk.byteLength;
    pending += decoder.decode(chunk, { stream: offset < stats.size });
    const lines = pending.split('\n');
    pending = lines.pop() ?? '';
    for (const line of lines) {
      matcher.lastIndex = 0;
      const match = matcher.exec(line.replace(/\r$/, ''));
      if (match) results.push({ path, type: 'file', line: lineNumber, column: match.index + 1, text: line.replace(/\r$/, '') });
      lineNumber++;
      if (results.length >= resultLimit) break;
    }
  }
  if (pending && results.length < resultLimit) {
    matcher.lastIndex = 0;
    const match = matcher.exec(pending);
    if (match) results.push({ path, type: 'file', line: lineNumber, column: match.index + 1, text: pending });
  }
  return stats.size;
}

export async function search(
  fs: FileSystem,
  sql: SearchSql,
  access: VolumeAccessCoordinator,
  input: SearchInput,
): Promise<SearchResponse> {
  if (input.mode !== 'find' && input.mode !== 'glob' && input.mode !== 'grep') {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'mode must be find, glob, or grep');
  }
  if (typeof input.pattern !== 'string' || input.pattern.length === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'pattern must be a non-empty string');
  }
  const root = normalizePath(typeof input.path === 'string' ? input.path : '/');
  const resultLimit = limit(input.limit);
  if (input.mode === 'find') {
    return findNames(fs, sql, access, root, input.pattern, input.ignoreCase === true, resultLimit);
  }
  let matcher: RegExp;
  try {
    if (input.mode === 'glob') matcher = globRegex(input.pattern.replace(/^\/+/, ''));
    else if (input.regex === true) matcher = new RegExp(input.pattern, input.ignoreCase === true ? 'i' : '');
    else matcher = new RegExp(input.pattern.replace(/[\\^$.*+?()[\]{}|]/g, '\\$&'), input.ignoreCase === true ? 'i' : '');
  } catch (error) {
    throw new HttpError(400, 'INVALID_PATTERN', error instanceof Error ? error.message : String(error));
  }

  const release = await access.acquireRead(root);
  try {
    const results: SearchResult[] = [];
    const queue = [root];
    let scannedEntries = 0;
    let scannedBytes = 0;
    let truncated = false;
    while (queue.length > 0 && results.length < resultLimit && scannedEntries < MAX_ENTRIES) {
      const directory = queue.shift()!;
      const entries = await fs.readdirPlus(directory);
      if (directory === '/') {
        const internal = entries.findIndex((entry) => entry.name === '.airyfs-trash');
        if (internal >= 0) entries.splice(internal, 1);
      }
      entries.sort((left, right) => left.name.localeCompare(right.name));
      for (const entry of entries) {
        scannedEntries++;
        const path = directory === '/' ? `/${entry.name}` : `${directory}/${entry.name}`;
        const type = pathType(entry);
        if (type === 'directory') queue.push(path);
        const relative = path.slice(root === '/' ? 1 : root.length + 1);
        if (input.mode === 'glob') {
          if (matcher.test(relative)) results.push({ path, type });
        } else if (type === 'file' && scannedBytes < MAX_SCAN_BYTES) {
          scannedBytes += await grepFile(fs, path, matcher, results, resultLimit);
        }
        if (results.length >= resultLimit || scannedEntries >= MAX_ENTRIES || scannedBytes >= MAX_SCAN_BYTES) {
          truncated = queue.length > 0 || entries.indexOf(entry) < entries.length - 1;
          break;
        }
      }
    }
    return { results, truncated, scannedEntries, scannedBytes };
  } finally {
    release();
  }
}
