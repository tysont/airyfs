// ABOUTME: Tests for the transactional directory import (staging, swap, rollback, locking).
// ABOUTME: Runs AgentFS against in-memory SQLite with the real VolumeAccessCoordinator.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { encodeTree } from '../src/archive';
import { importTree } from '../src/tree-import';
import { HttpError, VolumeAccessCoordinator } from '../src/files-api';
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

function streamOf(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

describe('transactional tree import', () => {
  let fs: AgentFS;
  let access: VolumeAccessCoordinator;
  let archive: Uint8Array;

  const deps = () => ({
    acquireWrite: (path: string) => access.acquireWrite(path),
  });

  beforeEach(async () => {
    fs = AgentFS.create(createTestStorage(new Database(':memory:')));
    access = new VolumeAccessCoordinator();
    // Build a small archive from a scratch source tree.
    await fs.mkdir('/source');
    await fs.mkdir('/source/sub');
    await fs.writeFile('/source/top.txt', Buffer.from('top'));
    await fs.writeFile('/source/sub/deep.bin', Buffer.from([1, 2, 3, 4]));
    const chunks: Uint8Array[] = [];
    for await (const chunk of encodeTree(fs, '/source')) chunks.push(chunk);
    archive = concat(chunks);
    await fs.rm('/source', { recursive: true });
  });

  it('publishes into a fresh non-root directory and returns a summary', async () => {
    const summary = await importTree(fs, '/app', streamOf(archive), {}, deps());

    expect(summary).toEqual({ files: 2, directories: 1, symlinks: 0, bytes: 7 });
    expect(await fs.readFile('/app/top.txt', 'utf8')).toBe('top');
    expect(new Uint8Array(await fs.readFile('/app/sub/deep.bin'))).toEqual(Uint8Array.from([1, 2, 3, 4]));
  });

  it('leaves no hidden staging or backup directories after publish', async () => {
    await importTree(fs, '/app', streamOf(archive), {}, deps());
    const roots = await fs.readdir('/');
    expect(roots.filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('refuses to overwrite an existing target without replace', async () => {
    await fs.mkdir('/app');
    await fs.writeFile('/app/keep.txt', Buffer.from('keep'));

    await expect(importTree(fs, '/app', streamOf(archive), {}, deps())).rejects.toMatchObject({
      status: 409,
      code: 'EEXIST',
    });
    // The original target is untouched and no staging leaks remain.
    expect(await fs.readFile('/app/keep.txt', 'utf8')).toBe('keep');
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('replaces an existing target when replace is set', async () => {
    await fs.mkdir('/app');
    await fs.writeFile('/app/old.txt', Buffer.from('old'));

    await importTree(fs, '/app', streamOf(archive), { replace: true }, deps());

    expect(await fs.readFile('/app/top.txt', 'utf8')).toBe('top');
    await expect(fs.stat('/app/old.txt')).rejects.toMatchObject({ code: 'ENOENT' });
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('rejects the volume root as a target', async () => {
    await expect(importTree(fs, '/', streamOf(archive), {}, deps())).rejects.toMatchObject({
      status: 400,
      code: 'INVALID_PATH',
    });
  });

  it('imports at the root with allowRoot, replacing existing root entries', async () => {
    // Seed pre-existing root content that must be swapped out.
    await fs.mkdir('/old');
    await fs.writeFile('/stale.txt', Buffer.from('stale'));

    const summary = await importTree(fs, '/', streamOf(archive), { replace: true, allowRoot: true }, deps());
    expect(summary).toEqual({ files: 2, directories: 1, symlinks: 0, bytes: 7 });

    // New content is at the root; old content is gone.
    expect(await fs.readFile('/top.txt', 'utf8')).toBe('top');
    expect(new Uint8Array(await fs.readFile('/sub/deep.bin'))).toEqual(Uint8Array.from([1, 2, 3, 4]));
    await expect(fs.stat('/old')).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(fs.stat('/stale.txt')).rejects.toMatchObject({ code: 'ENOENT' });
    // No staging or backup control directories leak.
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('imports at the root into an empty volume without replace', async () => {
    const summary = await importTree(fs, '/', streamOf(archive), { allowRoot: true }, deps());
    expect(summary.files).toBe(2);
    expect(await fs.readFile('/top.txt', 'utf8')).toBe('top');
  });

  it('refuses a non-empty root without replace even with allowRoot', async () => {
    await fs.writeFile('/keep.txt', Buffer.from('keep'));
    await expect(importTree(fs, '/', streamOf(archive), { allowRoot: true }, deps())).rejects.toMatchObject({
      status: 409,
      code: 'EEXIST',
    });
    // The original root content is untouched and staging is cleaned up.
    expect(await fs.readFile('/keep.txt', 'utf8')).toBe('keep');
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('maps a malformed archive to 400 and leaves the root untouched', async () => {
    await fs.writeFile('/keep.txt', Buffer.from('keep'));
    const bad = new Uint8Array([9, 9, 9, 9, 9, 9, 9, 9]);
    await expect(importTree(fs, '/', streamOf(bad), { replace: true, allowRoot: true }, deps())).rejects.toMatchObject({
      status: 400,
      code: 'INVALID_ARCHIVE',
    });
    expect(await fs.readFile('/keep.txt', 'utf8')).toBe('keep');
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('holds the whole-volume lock for the root swap, conflicting with the FUSE lock', async () => {
    const releaseVolume = await access.acquireWrite('*');
    let published = false;
    const promise = importTree(fs, '/', streamOf(archive), { replace: true, allowRoot: true }, deps())
      .then((summary) => { published = true; return summary; });

    // Extraction stages without the volume lock; the swap is blocked.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(published).toBe(false);

    releaseVolume();
    await promise;
    expect(published).toBe(true);
    expect(await fs.readFile('/top.txt', 'utf8')).toBe('top');
  });

  it('journals the new root children after a successful root import', async () => {
    const record = vi.fn(async () => undefined);
    await importTree(fs, '/', streamOf(archive), { replace: true, allowRoot: true }, {
      acquireWrite: (p) => access.acquireWrite(p),
      record,
    });
    expect(record).toHaveBeenCalledTimes(1);
    const journaled = record.mock.calls[0][0] as string[];
    expect(journaled.slice().sort()).toEqual(['/sub', '/top.txt']);
  });

  it('journals removed and published root children', async () => {
    await fs.writeFile('/stale.txt', Buffer.from('stale'));
    const record = vi.fn(async () => undefined);
    await importTree(fs, '/', streamOf(archive), { replace: true, allowRoot: true }, {
      acquireWrite: (p) => access.acquireWrite(p),
      record,
    });
    expect((record.mock.calls[0][0] as string[]).slice().sort()).toEqual(['/stale.txt', '/sub', '/top.txt']);
  });

  it('restores entries already moved when the backup phase fails partway through', async () => {
    await fs.writeFile('/a.txt', Buffer.from('a'));
    await fs.writeFile('/b.txt', Buffer.from('b'));
    const rename = fs.rename.bind(fs);
    let failed = false;
    vi.spyOn(fs, 'rename').mockImplementation(async (from, to) => {
      if (!failed && from === '/b.txt' && String(to).endsWith('/b.txt')) {
        failed = true;
        throw new Error('injected backup failure');
      }
      return rename(from, to);
    });

    await expect(importTree(fs, '/', streamOf(archive), { replace: true, allowRoot: true }, {
      acquireWrite: (p) => access.acquireWrite(p),
      uuid: (() => {
        const ids = ['stage', 'backup'];
        return () => ids.shift()!;
      })(),
    })).rejects.toThrow('injected backup failure');

    expect(await fs.readFile('/a.txt', 'utf8')).toBe('a');
    expect(await fs.readFile('/b.txt', 'utf8')).toBe('b');
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('maps malformed archives to a 400 and cleans up staging', async () => {
    const bad = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    await expect(importTree(fs, '/app', streamOf(bad), {}, deps())).rejects.toMatchObject({
      status: 400,
      code: 'INVALID_ARCHIVE',
    });
    await expect(fs.stat('/app')).rejects.toMatchObject({ code: 'ENOENT' });
    expect((await fs.readdir('/')).filter((name) => name.startsWith('.airyfs-'))).toEqual([]);
  });

  it('holds the write lock only for the swap, conflicting with the whole-volume lock', async () => {
    // A held whole-volume ('*') write lock must block the swap until released,
    // proving no outside observer sees intermediate state during publication.
    const releaseVolume = await access.acquireWrite('*');
    let published = false;
    const promise = importTree(fs, '/app', streamOf(archive), {}, deps())
      .then((summary) => { published = true; return summary; });

    // Staging/extraction runs without the target lock; the swap is blocked.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(published).toBe(false);

    releaseVolume();
    await promise;
    expect(published).toBe(true);
    expect(await fs.readFile('/app/top.txt', 'utf8')).toBe('top');
  });

  it('journals only the published target after success', async () => {
    const record = vi.fn(async () => undefined);
    await importTree(fs, '/app', streamOf(archive), {}, { acquireWrite: (p) => access.acquireWrite(p), record });
    expect(record).toHaveBeenCalledTimes(1);
    expect(record).toHaveBeenCalledWith(['/app']);
  });
});
