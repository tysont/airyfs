// ABOUTME: Records direct filesystem mutations for remote FUSE cache invalidation.
// ABOUTME: Journal rows identify the parent entry and current inode when one exists.

import type { AgentFS } from 'agentfs-sdk/cloudflare';
import type { SqlExec } from './schema';

function entryParts(path: string): { parent: string; name: string } | null {
  const normalized = `/${path.split('/').filter(Boolean).join('/')}`;
  if (normalized === '/') return null;
  const separator = normalized.lastIndexOf('/');
  return {
    parent: separator === 0 ? '/' : normalized.slice(0, separator),
    name: normalized.slice(separator + 1),
  };
}

export class MutationJournal {
  constructor(private readonly sql: SqlExec) {}

  async record(fs: AgentFS, paths: string[]): Promise<void> {
    for (const path of new Set(paths)) {
      const entry = entryParts(path);
      if (!entry) continue;

      let parentIno: number;
      try {
        parentIno = (await fs.stat(entry.parent)).ino;
      } catch {
        continue;
      }

      let ino: number | null = null;
      try {
        ino = (await fs.lstat(path)).ino;
      } catch {
        // Deleted and renamed source entries intentionally have no current inode.
      }

      this.sql.exec(
        `INSERT INTO fs_mutation_journal (parent_ino, name, ino, created_at)
         VALUES (?, ?, ?, unixepoch('subsec'))`,
        parentIno,
        entry.name,
        ino
      );
    }
  }
}
