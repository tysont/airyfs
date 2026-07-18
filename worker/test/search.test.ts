// ABOUTME: Tests bounded server-side find, glob, and grep over real AgentFS trees.
// ABOUTME: Covers path semantics, regex validation, binary skipping, line metadata, and limits.

import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { VolumeAccessCoordinator } from '../src/files-api';
import { search } from '../src/search';
import { initSchema, type SqlExec } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('server-side search', () => {
  let fs: AgentFS;
  let sql: SqlExec;
  const access = new VolumeAccessCoordinator();

  beforeEach(async () => {
    const storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    sql = storage.sql;
    fs = AgentFS.create(storage);
    await fs.mkdir('/src');
    await fs.mkdir('/src/lib');
    await fs.writeFile('/src/index.ts', 'first line\nconst Needle = 1;\n');
    await fs.writeFile('/src/lib/util.test.ts', 'needle here\nsecond\n');
    await fs.writeFile('/src/image.bin', Uint8Array.from([0, 1, 2, 3]));
    await fs.writeFile('/README.md', 'Needle outside root\n');
  });

  it('finds names beneath a scoped root', async () => {
    const response = await search(fs, sql, access, { mode: 'find', path: '/src', pattern: 'index' });
    expect(response.results).toEqual([{ path: '/src/index.ts', type: 'file' }]);
  });

  it('matches recursive globs against root-relative paths', async () => {
    const response = await search(fs, sql, access, { mode: 'glob', path: '/src', pattern: '**/*.test.ts' });
    expect(response.results).toEqual([{ path: '/src/lib/util.test.ts', type: 'file' }]);
  });

  it('greps text with line and column metadata while skipping binary files', async () => {
    const response = await search(fs, sql, access, {
      mode: 'grep', path: '/src', pattern: 'needle', ignoreCase: true,
    });
    expect(response.results).toEqual([
      { path: '/src/index.ts', type: 'file', line: 2, column: 7, text: 'const Needle = 1;' },
      { path: '/src/lib/util.test.ts', type: 'file', line: 1, column: 1, text: 'needle here' },
    ]);
  });

  it('enforces result limits and rejects invalid regex', async () => {
    const limited = await search(fs, sql, access, { mode: 'find', path: '/', pattern: '.', limit: 1 });
    expect(limited.results).toHaveLength(1);
    expect(limited.truncated).toBe(true);
    await expect(search(fs, sql, access, { mode: 'grep', pattern: '[', regex: true }))
      .rejects.toMatchObject({ status: 400, code: 'INVALID_PATTERN' });
  });

  it('keeps the FTS index synchronized across rename and delete', async () => {
    await fs.rename('/src/index.ts', '/src/INDEX.ts');
    expect((await search(fs, sql, access, { mode: 'find', path: '/src', pattern: 'index' })).results).toEqual([]);
    expect((await search(fs, sql, access, { mode: 'find', path: '/src', pattern: 'index', ignoreCase: true })).results)
      .toEqual([{ path: '/src/INDEX.ts', type: 'file' }]);
    await fs.rm('/src/INDEX.ts');
    expect((await search(fs, sql, access, { mode: 'find', path: '/src', pattern: 'index', ignoreCase: true })).results).toEqual([]);
  });
});
