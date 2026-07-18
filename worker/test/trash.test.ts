// ABOUTME: Tests recoverable deletes, restoration, purge, and last-delete undo.
// ABOUTME: Verifies inode-backed content and directory subtrees survive trash moves.

import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { VolumeAccessCoordinator } from '../src/files-api';
import { getChanges } from '../src/change-feed';
import { initSchema } from '../src/schema';
import { listTrash, moveToTrash, purgeTrash, restoreTrash, undoTrash } from '../src/trash';
import { createTestStorage } from './support/storage';

describe('trash and undo', () => {
  let storage: ReturnType<typeof createTestStorage>;
  let fs: AgentFS;
  let access: VolumeAccessCoordinator;

  beforeEach(() => {
    storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    fs = AgentFS.create(storage);
    access = new VolumeAccessCoordinator();
  });

  it('moves and restores a file with its content intact', async () => {
    await fs.writeFile('/note', 'hello');
    const entry = await moveToTrash(fs, storage.sql, access, '/note');
    await expect(fs.stat('/note')).rejects.toMatchObject({ code: 'ENOENT' });
    expect(await fs.readFile(entry.trashPath, 'utf8')).toBe('hello');
    expect(listTrash(storage.sql)).toEqual([entry]);
    expect((await restoreTrash(fs, storage.sql, access, entry.id)).restoredPath).toBe('/note');
    expect(await fs.readFile('/note', 'utf8')).toBe('hello');
    expect(listTrash(storage.sql)).toEqual([]);
  });

  it('preserves directory subtrees and symlinks', async () => {
    await fs.mkdir('/dir');
    await fs.writeFile('/dir/file', 'data');
    const directory = await moveToTrash(fs, storage.sql, access, '/dir');
    await restoreTrash(fs, storage.sql, access, directory.id);
    expect(await fs.readFile('/dir/file', 'utf8')).toBe('data');
    await fs.symlink('/dir/file', '/link');
    const link = await moveToTrash(fs, storage.sql, access, '/link');
    await restoreTrash(fs, storage.sql, access, link.id);
    expect(await fs.readlink('/link')).toBe('/dir/file');
  });

  it('purges permanently and undoes the most recent delete', async () => {
    await fs.writeFile('/first', '1');
    await fs.writeFile('/second', '2');
    const first = await moveToTrash(fs, storage.sql, access, '/first');
    await purgeTrash(fs, storage.sql, access, first.id);
    await expect(fs.stat(first.trashPath)).rejects.toMatchObject({ code: 'ENOENT' });
    await moveToTrash(fs, storage.sql, access, '/second');
    expect((await undoTrash(fs, storage.sql, access)).restoredPath).toBe('/second');
    expect(await fs.readFile('/second', 'utf8')).toBe('2');
  });

  it('keeps internal trash moves out of the public change feed', async () => {
    await fs.writeFile('/note', 'hello');
    const cursor = getChanges(storage.sql, { since: 'latest' }).cursor;
    await moveToTrash(fs, storage.sql, access, '/note');
    expect(getChanges(storage.sql, { since: cursor }).events).toEqual([]);
  });
});
