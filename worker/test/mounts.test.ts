// ABOUTME: Tests the persisted mount table: validation, longest-prefix resolution, and token withholding.
// ABOUTME: The DO-level forwarding/EXDEV/hop behavior is exercised by the CLI integration suite.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { initSchema } from '../src/schema';
import {
  createMountRow,
  deleteMountRow,
  getMount,
  listMounts,
  MAX_MOUNT_HOPS,
  publicMount,
  resolveMount,
  type MountRecord,
} from '../src/mounts';
import { createTestStorage } from './support/storage';

describe('mount table', () => {
  let storage: ReturnType<typeof createTestStorage>;
  beforeEach(() => {
    storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
  });

  const base = { hostVolume: 'a', targetVolume: 'b' };

  it('persists a mount and reads it back', () => {
    const record = createMountRow(storage.sql, {
      ...base,
      mountpoint: '/data',
      targetSubpath: '/exports',
      credentialId: 'cap-1',
      token: 'tok-1',
      options: { readOnly: true },
    });
    expect(record).toMatchObject({
      mountpoint: '/data',
      targetVolume: 'b',
      targetSubpath: '/exports',
      credentialId: 'cap-1',
      token: 'tok-1',
      options: { readOnly: true },
    });
    expect(getMount(storage.sql, '/data')).toMatchObject({ targetVolume: 'b' });
    expect(listMounts(storage.sql)).toHaveLength(1);
  });

  it('normalizes the mountpoint and defaults the subpath to root', () => {
    const record = createMountRow(storage.sql, { ...base, mountpoint: '/data/../data/./sub' });
    expect(record.mountpoint).toBe('/data/sub');
    expect(record.targetSubpath).toBe('/');
    expect(getMount(storage.sql, '/data/sub')).not.toBeNull();
  });

  it('rejects mounting the root, self-mounts, duplicates, and overlaps', () => {
    expect(() => createMountRow(storage.sql, { ...base, mountpoint: '/' })).toThrow(/root/);
    expect(() => createMountRow(storage.sql, { hostVolume: 'a', targetVolume: 'a', mountpoint: '/x' }))
      .toThrow(/cannot mount itself/);

    createMountRow(storage.sql, { ...base, mountpoint: '/data' });
    expect(() => createMountRow(storage.sql, { ...base, mountpoint: '/data' })).toThrow(/already exists/);
    expect(() => createMountRow(storage.sql, { ...base, mountpoint: '/data/inner' })).toThrow(/overlaps/);
    // A mount that would contain the existing one is also rejected.
    createMountRow(storage.sql, { ...base, mountpoint: '/other/deep' });
    expect(() => createMountRow(storage.sql, { ...base, mountpoint: '/other' })).toThrow(/overlaps/);
  });

  it('deletes a mount and errors on a missing one', () => {
    createMountRow(storage.sql, { ...base, mountpoint: '/data' });
    expect(deleteMountRow(storage.sql, '/data').mountpoint).toBe('/data');
    expect(listMounts(storage.sql)).toHaveLength(0);
    expect(() => deleteMountRow(storage.sql, '/data')).toThrow(/No mount/);
  });

  it('withholds the bearer token from the public projection', () => {
    const record = createMountRow(storage.sql, { ...base, mountpoint: '/data', token: 'secret' });
    const projected = publicMount(record) as Record<string, unknown>;
    expect(projected.token).toBeUndefined();
    expect(projected.credentialId).toBeNull();
    expect(projected.mountpoint).toBe('/data');
  });
});

describe('resolveMount', () => {
  const mount = (mountpoint: string, targetSubpath = '/', targetVolume = 'b'): MountRecord => ({
    mountpoint,
    targetVolume,
    targetSubpath,
    credentialId: null,
    token: null,
    options: {},
    createdAt: 0,
  });

  it('translates a path under a mount to the target subpath', () => {
    const hit = resolveMount([mount('/data', '/exports')], '/data/reports/q1.csv');
    expect(hit).toEqual({ mount: expect.objectContaining({ mountpoint: '/data' }), targetPath: '/exports/reports/q1.csv' });
  });

  it('maps the mountpoint itself to the subpath root', () => {
    expect(resolveMount([mount('/data', '/exports')], '/data')?.targetPath).toBe('/exports');
    expect(resolveMount([mount('/data')], '/data')?.targetPath).toBe('/');
  });

  it('returns null for local paths and for a prefix that is not a path boundary', () => {
    expect(resolveMount([mount('/data')], '/other')).toBeNull();
    // /database must not match the /data mountpoint.
    expect(resolveMount([mount('/data')], '/database/x')).toBeNull();
  });

  it('picks the longest matching prefix', () => {
    const mounts = [mount('/data', '/shallow', 'b'), mount('/data/deep', '/deep', 'c')];
    const hit = resolveMount(mounts, '/data/deep/file');
    expect(hit?.mount.targetVolume).toBe('c');
    expect(hit?.targetPath).toBe('/deep/file');
  });

  it('exposes a bounded hop constant', () => {
    expect(MAX_MOUNT_HOPS).toBeGreaterThanOrEqual(4);
  });
});
