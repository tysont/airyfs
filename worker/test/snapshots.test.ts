// ABOUTME: Tests for full-volume AiryFS snapshots: create/list/delete, resolve, diff, restore, AIRYFS export.
// ABOUTME: Runs AgentFS + raw snapshot SQL against in-memory SQLite via the shared test storage.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { AgentFS, type CloudflareStorage } from 'agentfs-sdk/cloudflare';
import { initSchema } from '../src/schema';
import { encodeTree, extractTree } from '../src/archive';
import {
  SNAPSHOT_TABLES,
  SnapshotExistsError,
  SnapshotNotFoundError,
  createSnapshot,
  deleteSnapshot,
  diffSnapshot,
  encodeSnapshotArchive,
  encodeSnapshotArchiveStream,
  listSnapshots,
  resolveSnapshot,
  restoreSnapshot,
} from '../src/snapshots';
import { createTestStorage } from './support/storage';

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

async function collect(
  source: AsyncGenerator<Uint8Array> | Generator<Uint8Array> | ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  if (source instanceof ReadableStream) {
    const reader = source.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  } else {
    for await (const chunk of source as AsyncGenerator<Uint8Array>) chunks.push(chunk);
  }
  return concat(chunks);
}

function streamOf(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

describe('full-volume snapshots', () => {
  let db: Database.Database;
  let storage: CloudflareStorage;
  let fs: AgentFS;

  beforeEach(async () => {
    db = new Database(':memory:');
    storage = createTestStorage(db);
    initSchema(storage.sql as never, (cb) => storage.transactionSync(cb));
    fs = AgentFS.create(storage);
    await seedTree();
  });

  async function seedTree(): Promise<void> {
    await fs.mkdir('/dir');
    await fs.mkdir('/dir/nested');
    await fs.mkdir('/empty');
    await fs.writeFile('/dir/a.txt', Buffer.from('hello world'));
    await fs.writeFile('/dir/nested/bin.dat', Buffer.from([0, 255, 1, 128, 42, 7]));
    await fs.symlink('../a.txt', '/dir/nested/link');
  }

  // -------------------------------------------------------------------------
  // Schema
  // -------------------------------------------------------------------------

  it('creates all additive snapshot tables', () => {
    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table'")
      .all() as { name: string }[];
    const names = tables.map((t) => t.name);
    for (const table of SNAPSHOT_TABLES) {
      expect(names).toContain(table);
    }
  });

  // -------------------------------------------------------------------------
  // Create / list / resolve / delete
  // -------------------------------------------------------------------------

  it('captures a snapshot with correct metadata counts', () => {
    const info = createSnapshot(storage, 'snap-1');
    expect(info.name).toBe('snap-1');
    expect(info.id).toMatch(/^[0-9a-f-]{36}$/);
    // Root + 3 dirs + 2 files + 1 symlink = 7 inodes.
    expect(info.inodeCount).toBe(7);
    expect(info.fileCount).toBe(2);
    // Root, /dir, /dir/nested, /empty.
    expect(info.directoryCount).toBe(4);
    expect(info.symlinkCount).toBe(1);
    expect(info.note).toBeNull();
    expect(info.byteCount).toBe('hello world'.length + 6);
    expect(info.createdAt).toBeGreaterThan(0);
  });

  it('stores an optional note and rejects an over-long one', () => {
    const info = createSnapshot(storage, 'noted', 'before the big refactor');
    expect(info.note).toBe('before the big refactor');
    expect(resolveSnapshot(storage, info.id)?.note).toBe('before the big refactor');

    expect(() => createSnapshot(storage, 'too-long', 'x'.repeat(4097))).toThrow(/note/i);
  });

  it('generates a concise timestamped default name when none is given', () => {
    const info = createSnapshot(storage);
    expect(info.name).toMatch(/^snap-\d{8}-\d{6}-[0-9a-f]{6}$/);
    expect(resolveSnapshot(storage, info.name)?.id).toBe(info.id);
  });

  it('rejects a name containing a slash so name-or-id route segments are safe', () => {
    expect(() => createSnapshot(storage, 'a/b')).toThrow(/"\/"/);
    expect(() => createSnapshot(storage, '..')).toThrow(/name/i);
  });

  it('copies chunk BLOBs into the payload table via INSERT SELECT', () => {
    createSnapshot(storage, 'snap-1');
    const live = db.prepare('SELECT ino, chunk_index, data FROM fs_data ORDER BY ino, chunk_index').all() as {
      ino: number; chunk_index: number; data: Buffer;
    }[];
    const snap = db
      .prepare('SELECT ino, chunk_index, data FROM fs_snapshot_data ORDER BY ino, chunk_index')
      .all() as { ino: number; chunk_index: number; data: Buffer }[];
    expect(snap.length).toBe(live.length);
    for (let i = 0; i < live.length; i++) {
      expect(snap[i].ino).toBe(live[i].ino);
      expect(snap[i].chunk_index).toBe(live[i].chunk_index);
      expect(Uint8Array.from(snap[i].data)).toEqual(Uint8Array.from(live[i].data));
    }
  });

  it('lists snapshots oldest first', () => {
    const a = createSnapshot(storage, 'alpha');
    const b = createSnapshot(storage, 'beta');
    const list = listSnapshots(storage);
    expect(list.map((s) => s.name)).toEqual(['alpha', 'beta']);
    expect(list.map((s) => s.id)).toEqual([a.id, b.id]);
  });

  it('rejects duplicate snapshot names', () => {
    createSnapshot(storage, 'dup');
    expect(() => createSnapshot(storage, 'dup')).toThrow(SnapshotExistsError);
  });

  it('rejects empty or invalid names', () => {
    expect(() => createSnapshot(storage, '')).toThrow(/name/i);
    expect(() => createSnapshot(storage, '   ')).toThrow(/name/i);
    expect(() => createSnapshot(storage, 'bad\u0001name')).toThrow(/control/i);
  });

  it('resolves a snapshot by id and by name, null otherwise', () => {
    const info = createSnapshot(storage, 'named');
    expect(resolveSnapshot(storage, info.id)?.name).toBe('named');
    expect(resolveSnapshot(storage, 'named')?.id).toBe(info.id);
    expect(resolveSnapshot(storage, 'missing')).toBeNull();
  });

  it('deletes a snapshot and all its payload rows', () => {
    const info = createSnapshot(storage, 'gone');
    createSnapshot(storage, 'stays');
    const deleted = deleteSnapshot(storage, 'gone');
    expect(deleted.id).toBe(info.id);
    expect(resolveSnapshot(storage, 'gone')).toBeNull();

    const remaining = db
      .prepare('SELECT COUNT(*) AS c FROM fs_snapshot_data WHERE snapshot_id = ?')
      .get(info.id) as { c: number };
    expect(remaining.c).toBe(0);
    const inodes = db
      .prepare('SELECT COUNT(*) AS c FROM fs_snapshot_inode WHERE snapshot_id = ?')
      .get(info.id) as { c: number };
    expect(inodes.c).toBe(0);
    // The other snapshot is untouched.
    expect(resolveSnapshot(storage, 'stays')).not.toBeNull();
  });

  it('throws when deleting a nonexistent snapshot', () => {
    expect(() => deleteSnapshot(storage, 'nope')).toThrow(SnapshotNotFoundError);
  });

  // -------------------------------------------------------------------------
  // Diff
  // -------------------------------------------------------------------------

  it('diffs a snapshot against a mutated live volume', async () => {
    const snap = createSnapshot(storage, 'base');

    await fs.writeFile('/dir/added.txt', Buffer.from('new'));   // added
    await fs.rm('/empty', { recursive: true });                 // removed dir
    await fs.writeFile('/dir/a.txt', Buffer.from('CHANGED!!'));  // modified content
    await fs.rm('/dir/nested/link');                            // removed symlink

    const diff = diffSnapshot(storage, { snapshot: snap.id }, 'live');
    expect(diff).toEqual([
      { path: '/dir/a.txt', change: 'modified', kind: 'file' },
      { path: '/dir/added.txt', change: 'added', kind: 'file' },
      { path: '/dir/nested/link', change: 'removed', kind: 'symlink' },
      { path: '/empty', change: 'removed', kind: 'directory' },
    ]);
  });

  it('reports no diff when live is unchanged', () => {
    const snap = createSnapshot(storage, 'base');
    expect(diffSnapshot(storage, { snapshot: snap.id }, 'live')).toEqual([]);
    expect(diffSnapshot(storage, 'live', { snapshot: snap.id })).toEqual([]);
  });

  it('detects a symlink target change and a type change', async () => {
    const snap = createSnapshot(storage, 'base');
    await fs.rm('/dir/nested/link');
    await fs.symlink('../../elsewhere', '/dir/nested/link'); // same path, new target
    await fs.rm('/dir/a.txt');
    await fs.mkdir('/dir/a.txt'); // file -> directory at same path

    const diff = diffSnapshot(storage, { snapshot: snap.id }, 'live');
    expect(diff).toContainEqual({ path: '/dir/nested/link', change: 'modified', kind: 'symlink' });
    expect(diff).toContainEqual({ path: '/dir/a.txt', change: 'modified', kind: 'directory' });
  });

  it('detects inode metadata changes (mode) without content changes', async () => {
    const snap = createSnapshot(storage, 'meta');
    // Change permission bits only; content and kind are unchanged.
    db.prepare("UPDATE fs_inode SET mode = mode & ~511 | ? WHERE ino = (SELECT ino FROM fs_dentry WHERE name = 'a.txt')")
      .run(0o600);
    const diff = diffSnapshot(storage, { snapshot: snap.id }, 'live');
    expect(diff).toContainEqual({ path: '/dir/a.txt', change: 'modified', kind: 'file' });
  });

  it('detects uid/gid/rdev/nlink metadata changes', async () => {
    const snap = createSnapshot(storage, 'owners');
    db.prepare("UPDATE fs_inode SET uid = 1000, gid = 1000 WHERE ino = (SELECT ino FROM fs_dentry WHERE name = 'a.txt')")
      .run();
    db.prepare("UPDATE fs_inode SET nlink = 3 WHERE ino = (SELECT ino FROM fs_dentry WHERE name = 'nested')")
      .run();
    const diff = diffSnapshot(storage, { snapshot: snap.id }, 'live');
    expect(diff).toContainEqual({ path: '/dir/a.txt', change: 'modified', kind: 'file' });
    expect(diff).toContainEqual({ path: '/dir/nested', change: 'modified', kind: 'directory' });
  });

  it('ignores atime/mtime/ctime churn as diff noise', async () => {
    const snap = createSnapshot(storage, 'timestamps');
    db.prepare('UPDATE fs_inode SET atime = atime + 10000, mtime = mtime + 10000, ctime = ctime + 10000').run();
    expect(diffSnapshot(storage, { snapshot: snap.id }, 'live')).toEqual([]);
  });

  it('diffs one snapshot against another', async () => {
    const before = createSnapshot(storage, 'before');
    await fs.writeFile('/dir/a.txt', Buffer.from('hello world extended'));
    await fs.writeFile('/dir/extra.txt', Buffer.from('x'));
    const after = createSnapshot(storage, 'after');

    const diff = diffSnapshot(storage, { snapshot: before.id }, { snapshot: after.id });
    expect(diff).toEqual([
      { path: '/dir/a.txt', change: 'modified', kind: 'file' },
      { path: '/dir/extra.txt', change: 'added', kind: 'file' },
    ]);
  });

  it('treats equal file content across states as unchanged even in separate chunks', async () => {
    // A file whose content spans multiple chunks compares equal to its snapshot.
    const big = Buffer.alloc(600 * 1024);
    for (let i = 0; i < big.length; i++) big[i] = i % 251;
    await fs.writeFile('/dir/big.bin', big);
    const snap = createSnapshot(storage, 'withbig');
    const diff = diffSnapshot(storage, { snapshot: snap.id }, 'live');
    expect(diff).toEqual([]);
  });

  // -------------------------------------------------------------------------
  // Restore
  // -------------------------------------------------------------------------

  it('restores the volume, preserving config and clearing leases', async () => {
    // Ensure a chunk_size config row exists to prove config survives restore.
    db.prepare("INSERT OR REPLACE INTO fs_config (key, value) VALUES ('chunk_size', ?)")
      .run(String(256 * 1024));
    const configBefore = db.prepare("SELECT value FROM fs_config WHERE key='chunk_size'").get() as { value: string };

    const snap = createSnapshot(storage, 'restore-point');

    // Mutate the volume after snapshotting.
    await fs.writeFile('/dir/a.txt', Buffer.from('totally different'));
    await fs.writeFile('/dir/new.txt', Buffer.from('brand new'));
    await fs.rm('/empty', { recursive: true });

    // Simulate an open-handle lease that must be cleared on restore.
    db.prepare('INSERT INTO fs_open_inode (session_id, ino, open_count, expires_at) VALUES (?, ?, 1, ?)')
      .run('sess', 2, Date.now() + 100000);

    const restored = restoreSnapshot(storage, 'restore-point');
    expect(restored.id).toBe(snap.id);

    // Filesystem content matches the snapshot exactly.
    expect(await fs.readFile('/dir/a.txt', 'utf8')).toBe('hello world');
    await expect(fs.stat('/dir/new.txt')).rejects.toMatchObject({ code: 'ENOENT' });
    expect((await fs.stat('/empty')).isDirectory()).toBe(true);
    expect(await fs.readlink('/dir/nested/link')).toBe('../a.txt');
    expect(new Uint8Array(await fs.readFile('/dir/nested/bin.dat'))).toEqual(
      Uint8Array.from([0, 255, 1, 128, 42, 7]),
    );

    // Config is preserved; leases are cleared.
    const configAfter = db.prepare("SELECT value FROM fs_config WHERE key='chunk_size'").get() as { value: string };
    expect(configAfter.value).toBe(configBefore.value);
    const leases = db.prepare('SELECT COUNT(*) AS c FROM fs_open_inode').get() as { c: number };
    expect(leases.c).toBe(0);
  });

  it('restore is exact — a subsequent diff against the snapshot is empty', async () => {
    const snap = createSnapshot(storage, 'point');
    await fs.writeFile('/dir/a.txt', Buffer.from('changed'));
    await fs.writeFile('/dir/z.txt', Buffer.from('added'));
    restoreSnapshot(storage, snap.id);
    expect(diffSnapshot(storage, { snapshot: snap.id }, 'live')).toEqual([]);
  });

  it('throws when restoring a nonexistent snapshot', () => {
    expect(() => restoreSnapshot(storage, 'nope')).toThrow(SnapshotNotFoundError);
  });

  // -------------------------------------------------------------------------
  // AIRYFS export
  // -------------------------------------------------------------------------

  it('exports a snapshot to a AIRYFS archive that extracts to the original tree', async () => {
    const snap = createSnapshot(storage, 'export');
    const archive = await collect(encodeSnapshotArchive(storage, snap.id));

    await fs.mkdir('/restored');
    const summary = await extractTree(fs, '/restored', streamOf(archive));

    expect(summary).toEqual({ files: 2, directories: 3, symlinks: 1, bytes: 'hello world'.length + 6 });
    expect(await fs.readFile('/restored/dir/a.txt', 'utf8')).toBe('hello world');
    expect(new Uint8Array(await fs.readFile('/restored/dir/nested/bin.dat'))).toEqual(
      Uint8Array.from([0, 255, 1, 128, 42, 7]),
    );
    expect(await fs.readlink('/restored/dir/nested/link')).toBe('../a.txt');
    expect((await fs.stat('/restored/empty')).isDirectory()).toBe(true);
  });

  it('produces byte-identical output to the live whole-volume tree export', async () => {
    const snap = createSnapshot(storage, 'identical');
    const fromLive = await collect(encodeTree(fs, '/'));
    const fromSnap = await collect(encodeSnapshotArchive(storage, snap.id));
    expect(Uint8Array.from(fromSnap)).toEqual(Uint8Array.from(fromLive));
  });

  it('emits the root record first', async () => {
    const snap = createSnapshot(storage, 'rooted');
    const iterator = encodeSnapshotArchive(storage, snap.id);
    const magic = iterator.next().value as Uint8Array;
    expect(new TextDecoder().decode(magic.subarray(0, 6))).toBe('AIRYFS');
    const rootFrame = iterator.next().value as Uint8Array;
    const len = new DataView(rootFrame.buffer, rootFrame.byteOffset, 4).getUint32(0, false);
    const header = JSON.parse(new TextDecoder().decode(rootFrame.subarray(4, 4 + len)));
    expect(header).toEqual({ t: 'd', p: '' });
  });

  it('streams the archive and reports it back through a ReadableStream wrapper', async () => {
    const snap = createSnapshot(storage, 'streamed');
    let released = false;
    const stream = encodeSnapshotArchiveStream(storage, snap.id, () => { released = true; });
    const bytes = await collect(stream);
    const direct = await collect(encodeSnapshotArchive(storage, snap.id));
    expect(Uint8Array.from(bytes)).toEqual(Uint8Array.from(direct));
    expect(released).toBe(true);
  });

  it('throws when exporting a nonexistent snapshot', () => {
    expect(() => encodeSnapshotArchive(storage, 'nope').next()).toThrow(SnapshotNotFoundError);
  });
});

describe('snapshot schema migration', () => {
  it('adds note/directory_count/symlink_count to a legacy fs_snapshot table idempotently', () => {
    const db = new Database(':memory:');
    const storage = createTestStorage(db);
    // A legacy metadata table created before the newer columns existed.
    db.exec(`CREATE TABLE fs_snapshot (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      created_at INTEGER NOT NULL,
      chunk_size INTEGER,
      inode_count INTEGER NOT NULL DEFAULT 0,
      file_count INTEGER NOT NULL DEFAULT 0,
      byte_count INTEGER NOT NULL DEFAULT 0
    )`);
    db.prepare(
      'INSERT INTO fs_snapshot (id, name, created_at, chunk_size, inode_count, file_count, byte_count) VALUES (?, ?, ?, ?, ?, ?, ?)'
    ).run('legacy-id', 'legacy', 123, null, 1, 0, 0);

    initSchema(storage.sql as never, (cb) => storage.transactionSync(cb));
    // Running again must not fail (idempotent ALTER guarded by column presence).
    initSchema(storage.sql as never, (cb) => storage.transactionSync(cb));

    const columns = (db.prepare('PRAGMA table_info(fs_snapshot)').all() as { name: string }[])
      .map((column) => column.name);
    expect(columns).toEqual(expect.arrayContaining(['note', 'directory_count', 'symlink_count']));

    // The legacy row reads back with defaults for the new columns.
    const legacy = listSnapshots(storage).find((snapshot) => snapshot.id === 'legacy-id');
    expect(legacy).toMatchObject({ note: null, directoryCount: 0, symlinkCount: 0 });

    // New snapshots created after migration populate the new columns.
    AgentFS.create(storage);
    const created = createSnapshot(storage, 'post-migration');
    expect(created.directoryCount).toBeGreaterThanOrEqual(1);
  });
});
