// ABOUTME: Builds bounded, structured directory trees directly from AgentFS metadata.
// ABOUTME: Avoids Container startup while preserving stable sorting and truncation details.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { normalizePath } from './auth';
import { HttpError, VolumeAccessCoordinator } from './files-api';

const DEFAULT_DEPTH = 20;
const MAX_DEPTH = 100;
const DEFAULT_LIMIT = 1000;
const MAX_LIMIT = 100_000;

export interface TreeEntry {
  path: string;
  name: string;
  depth: number;
  type: 'file' | 'directory' | 'symlink';
  size: number;
}

export interface TreeResponse {
  root: string;
  entries: TreeEntry[];
  truncated: boolean;
}

function boundedInteger(value: unknown, fallback: number, maximum: number, name: string): number {
  if (value === undefined) return fallback;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new HttpError(400, 'INVALID_ARGUMENT', `${name} must be between 0 and ${maximum}`);
  }
  return value;
}

export async function readTree(
  fs: FileSystem,
  access: VolumeAccessCoordinator,
  input: { path?: unknown; depth?: unknown; limit?: unknown },
): Promise<TreeResponse> {
  const root = normalizePath(typeof input.path === 'string' ? input.path : '/');
  const maxDepth = boundedInteger(input.depth, DEFAULT_DEPTH, MAX_DEPTH, 'depth');
  const limit = boundedInteger(input.limit, DEFAULT_LIMIT, MAX_LIMIT, 'limit');
  if (limit === 0) throw new HttpError(400, 'INVALID_ARGUMENT', 'limit must be at least 1');

  const release = await access.acquireRead(root);
  try {
    const stats = await fs.stat(root);
    if (!stats.isDirectory()) throw new HttpError(400, 'ENOTDIR', `${root} is not a directory`);
    const result: TreeEntry[] = [];
    let truncated = false;
    const walk = async (directory: string, parentDepth: number): Promise<void> => {
      if (parentDepth >= maxDepth || truncated) return;
      const entries = await fs.readdirPlus(directory);
      if (directory === '/') {
        const internal = entries.findIndex((entry) => entry.name === '.airyfs-trash');
        if (internal >= 0) entries.splice(internal, 1);
      }
      entries.sort((left, right) => left.name.localeCompare(right.name));
      for (const entry of entries) {
        if (result.length >= limit) {
          truncated = true;
          return;
        }
        const path = directory === '/' ? `/${entry.name}` : `${directory}/${entry.name}`;
        const type: TreeEntry['type'] = entry.stats.isDirectory()
          ? 'directory'
          : entry.stats.isSymbolicLink() ? 'symlink' : 'file';
        const depth = parentDepth + 1;
        result.push({ path, name: entry.name, depth, type, size: entry.stats.size });
        if (type === 'directory') await walk(path, depth);
      }
    };
    await walk(root, 0);
    return { root, entries: result, truncated };
  } finally {
    release();
  }
}
