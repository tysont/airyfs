// ABOUTME: Tests bounded structured directory tree traversal over a real AgentFS volume.
// ABOUTME: Covers stable metadata, depth limits, result limits, and invalid roots.

import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { VolumeAccessCoordinator } from '../src/files-api';
import { initSchema } from '../src/schema';
import { readTree } from '../src/tree';
import { createTestStorage } from './support/storage';

describe('server-side tree', () => {
  let fs: AgentFS;
  const access = new VolumeAccessCoordinator();

  beforeEach(async () => {
    const storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    fs = AgentFS.create(storage);
    await fs.mkdir('/src');
    await fs.mkdir('/src/lib');
    await fs.writeFile('/src/a.ts', 'abc');
    await fs.writeFile('/src/lib/b.ts', '12345');
  });

  it('returns sorted structured entries beneath a root', async () => {
    expect(await readTree(fs, access, { path: '/src' })).toEqual({
      root: '/src',
      entries: [
        { path: '/src/a.ts', name: 'a.ts', depth: 1, type: 'file', size: 3 },
        { path: '/src/lib', name: 'lib', depth: 1, type: 'directory', size: 0 },
        { path: '/src/lib/b.ts', name: 'b.ts', depth: 2, type: 'file', size: 5 },
      ],
      truncated: false,
    });
  });

  it('honors depth and result limits', async () => {
    expect((await readTree(fs, access, { path: '/src', depth: 1 })).entries).toHaveLength(2);
    expect(await readTree(fs, access, { path: '/src', limit: 1 })).toMatchObject({ truncated: true });
  });

  it('rejects files as roots and invalid limits', async () => {
    await expect(readTree(fs, access, { path: '/src/a.ts' })).rejects.toMatchObject({ code: 'ENOTDIR' });
    await expect(readTree(fs, access, { limit: 0 })).rejects.toMatchObject({ code: 'INVALID_ARGUMENT' });
  });
});
