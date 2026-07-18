// ABOUTME: Transactional directory import: stage into a hidden sibling, then swap under the write lock.
// ABOUTME: Keeps the archive codec focused; this orchestrates atomic, observer-safe publication.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { ArchiveError, extractTree, type TreeSummary } from './archive';
import { HttpError } from './files-api';

export interface TreeImportDeps {
  /** Acquire a write lock covering `path`; the returned function releases it. */
  acquireWrite: (path: string) => Promise<() => void>;
  /** Record the published target for remote FUSE invalidation. */
  record?: (paths: string[]) => Promise<void>;
  /** Injectable unique-name source for staging and backup directories. */
  uuid?: () => string;
}

/** Normalize a tree path to a canonical absolute POSIX path. */
export function normalizeTreePath(path: string): string {
  const normalized = `/${path.split('/').filter(Boolean).join('/')}`;
  return normalized === '' ? '/' : normalized;
}

function siblingPath(parent: string, name: string): string {
  return `${parent === '/' ? '' : parent}/${name}`;
}

/**
 * Import an archive stream into a directory, transactionally.
 *
 * Staging is extracted into a hidden sibling directory WITHOUT the target lock,
 * so a long upload does not block other operations. Only the atomic swap holds
 * the write lock on the target — which conflicts with the FUSE whole-volume
 * ('*') lock — so no outside observer ever sees intermediate state. When the
 * target already exists, `replace` must be set: target -> backup, staging ->
 * target, then the backup is cleaned up; a failed publish rolls the backup
 * back. Staging and backup are always cleaned up best-effort. Only the
 * published target is journaled, and only after success.
 *
 * Importing at the volume root ('/') is refused unless `allowRoot` is set. That
 * option is for trusted internal/RPC callers only (e.g. cloning a snapshot into
 * a fresh volume); ordinary HTTP tree PUTs must never enable it. See
 * {@link importTreeRoot} for the root-specific swap.
 */
export async function importTree(
  fs: FileSystem,
  path: string,
  stream: ReadableStream<Uint8Array> | null,
  options: { replace?: boolean; allowRoot?: boolean },
  deps: TreeImportDeps,
): Promise<TreeSummary> {
  if (!stream) throw new HttpError(400, 'INVALID_ARGUMENT', 'Missing archive body');
  const target = normalizeTreePath(path);
  if (target === '/') {
    if (!options.allowRoot) {
      throw new HttpError(400, 'INVALID_PATH', 'Import target must be a non-root directory');
    }
    return importTreeRoot(fs, stream, options, deps);
  }
  const uuid = deps.uuid ?? (() => crypto.randomUUID());
  const parent = target.slice(0, target.lastIndexOf('/')) || '/';
  const staging = siblingPath(parent, `.airyfs-import-${uuid()}`);

  let summary: TreeSummary;
  try {
    await fs.mkdir(staging);
  } catch (error) {
    throw mapArchiveError(error);
  }
  try {
    summary = await extractTree(fs, staging, stream);
  } catch (error) {
    await fs.rm(staging, { recursive: true, force: true }).catch(() => undefined);
    throw mapArchiveError(error);
  }

  const backup = siblingPath(parent, `.airyfs-backup-${uuid()}`);
  const release = await deps.acquireWrite(target);
  let targetExisted = false;
  try {
    try {
      await fs.stat(target);
      targetExisted = true;
    } catch {
      targetExisted = false;
    }
    if (targetExisted && !options.replace) {
      throw new HttpError(409, 'EEXIST', `Import target already exists: ${target} (use ?replace=true)`);
    }
    if (targetExisted) await fs.rename(target, backup);
    try {
      await fs.rename(staging, target);
    } catch (error) {
      if (targetExisted) await fs.rename(backup, target).catch(() => undefined);
      throw error;
    }
    await deps.record?.([target]);
  } catch (error) {
    release();
    await fs.rm(staging, { recursive: true, force: true }).catch(() => undefined);
    throw mapArchiveError(error);
  }
  release();
  if (targetExisted) await fs.rm(backup, { recursive: true, force: true }).catch(() => undefined);
  return summary;
}

/**
 * Import an archive into the volume root, transactionally, for trusted callers.
 *
 * The root cannot be renamed like a subdirectory, so the swap operates on the
 * root's children instead. Extraction stages into a hidden directory under root
 * WITHOUT the whole-volume lock. The swap then holds the write lock on '/' —
 * which conflicts with the FUSE '*' lock and every path — so no observer sees
 * intermediate state:
 *   1. move each pre-existing top-level entry into a hidden backup directory,
 *   2. move each staged child up to the root,
 *   3. remove the now-empty staging directory.
 * Any failure during the swap rolls the backed-up entries back to the root. On
 * success the backup is removed and the new top-level children are journaled so
 * a remote FUSE mount re-reads the fresh root. Because clone targets a
 * fresh/replaced root and the lock excludes FUSE, this is observer-safe.
 */
async function importTreeRoot(
  fs: FileSystem,
  stream: ReadableStream<Uint8Array>,
  options: { replace?: boolean },
  deps: TreeImportDeps,
): Promise<TreeSummary> {
  const uuid = deps.uuid ?? (() => crypto.randomUUID());
  const stagingName = `.airyfs-import-${uuid()}`;
  const backupName = `.airyfs-backup-${uuid()}`;
  const staging = siblingPath('/', stagingName);
  const backup = siblingPath('/', backupName);

  let summary: TreeSummary;
  try {
    await fs.mkdir(staging);
  } catch (error) {
    throw mapArchiveError(error);
  }
  try {
    summary = await extractTree(fs, staging, stream);
  } catch (error) {
    await fs.rm(staging, { recursive: true, force: true }).catch(() => undefined);
    throw mapArchiveError(error);
  }

  const control = new Set([stagingName, backupName]);
  const release = await deps.acquireWrite('/');
  const movedToBackup: string[] = [];
  const published: string[] = [];
  let backupCreated = false;
  let existing: string[] = [];
  try {
    existing = (await fs.readdir('/')).filter((name) => !control.has(name));
    if (existing.length > 0 && !options.replace) {
      throw new HttpError(409, 'EEXIST', 'Import target already exists: / (use replace)');
    }

    await fs.mkdir(backup);
    backupCreated = true;
    try {
      for (const name of existing) {
        await fs.rename(siblingPath('/', name), siblingPath(backup, name));
        movedToBackup.push(name);
      }

      const staged = await fs.readdir(staging);
      for (const name of staged) {
        await fs.rename(siblingPath(staging, name), siblingPath('/', name));
        published.push(name);
      }
    } catch (error) {
      // Roll back every phase, including a partial move into the backup.
      for (const name of published) {
        await fs.rm(siblingPath('/', name), { recursive: true, force: true }).catch(() => undefined);
      }
      for (const name of movedToBackup.slice().reverse()) {
        await fs.rename(siblingPath(backup, name), siblingPath('/', name)).catch(() => undefined);
      }
      throw error;
    }

    await fs.rm(staging, { recursive: true, force: true }).catch(() => undefined);
  } catch (error) {
    release();
    await fs.rm(staging, { recursive: true, force: true }).catch(() => undefined);
    // Never delete a non-empty backup after a failed rollback; preserving old
    // data under the hidden backup is safer than turning recovery failure into loss.
    if (backupCreated) {
      const remaining = await fs.readdir(backup).catch(() => [] as string[]);
      if (remaining.length === 0) await fs.rm(backup, { recursive: true, force: true }).catch(() => undefined);
    }
    throw mapArchiveError(error);
  }
  release();
  if (backupCreated) await fs.rm(backup, { recursive: true, force: true }).catch(() => undefined);
  await deps.record?.([...new Set([...existing, ...published])].map((name) => siblingPath('/', name)));
  return summary;
}

/** Translate archive codec errors into stable HTTP errors; pass others through. */
function mapArchiveError(error: unknown): unknown {
  if (error instanceof ArchiveError) {
    return new HttpError(400, 'INVALID_ARCHIVE', error.message);
  }
  return error;
}
