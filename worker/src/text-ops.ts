// ABOUTME: Typed, server-side text operations that run in the volume Durable
// ABOUTME: Object with no Container — line reads today, more to follow.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { normalizePath } from './auth';
import { HttpError, VolumeAccessCoordinator } from './files-api';

/** Largest file a text operation will read, matching grep's scan bound. */
export const MAX_TEXT_FILE_BYTES = 10 * 1024 * 1024;
/** Default number of lines returned when a caller does not specify one. */
export const DEFAULT_LINES = 1000;
/** Hard ceiling on lines returned in a single readLines call. */
export const MAX_LINES = 10_000;

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
