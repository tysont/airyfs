// ABOUTME: Verifies deployment-wide volume catalog persistence and ordering.
// ABOUTME: Covers idempotent registration, metadata updates, and input validation.

import Database from 'better-sqlite3';
import { describe, expect, it } from 'vitest';
import { createTestStorage } from './support/storage';
import { initVolumeRegistry, listVolumes, registerVolume } from '../src/volume-registry-storage';

describe('volume registry', () => {
  it('registers volumes idempotently and lists them by name', () => {
    const db = new Database(':memory:');
    const sql = createTestStorage(db).sql;
    initVolumeRegistry(sql);

    registerVolume(sql, 'zeta', 262144);
    registerVolume(sql, 'alpha', 65536);
    const updated = registerVolume(sql, 'zeta', 524288);

    expect(updated).toMatchObject({ name: 'zeta', chunkSize: 524288 });
    expect(listVolumes(sql, '', 10).volumes.map(({ name, chunkSize }) => ({ name, chunkSize }))).toEqual([
      { name: 'alpha', chunkSize: 65536 },
      { name: 'zeta', chunkSize: 524288 },
    ]);
  });

  it('pages by volume name with a bounded query', () => {
    const db = new Database(':memory:');
    const sql = createTestStorage(db).sql;
    initVolumeRegistry(sql);
    for (const name of ['alpha', 'beta', 'gamma']) registerVolume(sql, name, 262144);

    const first = listVolumes(sql, '', 2);
    expect(first.volumes.map((volume) => volume.name)).toEqual(['alpha', 'beta']);
    expect(first.nextCursor).toBe('beta');
    expect(listVolumes(sql, first.nextCursor!, 2).volumes.map((volume) => volume.name)).toEqual(['gamma']);
  });

  it('rejects invalid registrations', () => {
    const db = new Database(':memory:');
    const sql = createTestStorage(db).sql;
    initVolumeRegistry(sql);
    expect(() => registerVolume(sql, '', 262144)).toThrow('Volume name must be non-empty');
    expect(() => registerVolume(sql, 'valid', 0)).toThrow('Chunk size must be a positive integer');
  });
});
