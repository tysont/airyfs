// ABOUTME: Tests persistent byte and inode quotas at the shared SQLite filesystem boundary.
// ABOUTME: Verifies configuration, clearing, current-usage checks, and AgentFS write enforcement.

import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { configureQuota, initSchema, readQuota } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('volume quota', () => {
  let storage: ReturnType<typeof createTestStorage>;
  let fs: AgentFS;

  beforeEach(() => {
    storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    fs = AgentFS.create(storage);
  });

  it('stores, updates, and clears limits', () => {
    expect(configureQuota(storage.sql, { bytes: 100, inodes: 3 })).toEqual({ bytes: 100, inodes: 3 });
    expect(configureQuota(storage.sql, { bytes: null })).toEqual({ bytes: null, inodes: 3 });
    expect(readQuota(storage.sql)).toEqual({ bytes: null, inodes: 3 });
  });

  it('enforces logical byte limits for AgentFS writes', async () => {
    configureQuota(storage.sql, { bytes: 4 });
    await fs.writeFile('/fits', '1234');
    await expect(fs.writeFile('/too-large', '1')).rejects.toThrow(/AIRYFS_ENOSPC_BYTES/);
  });

  it('enforces inode limits and rejects limits below current usage', async () => {
    configureQuota(storage.sql, { inodes: 2 });
    await fs.writeFile('/one', '');
    await expect(fs.writeFile('/two', '')).rejects.toThrow(/AIRYFS_ENOSPC_INODES/);
    expect(() => configureQuota(storage.sql, { inodes: 1 })).toThrow(/current usage/);
  });
});
