// ABOUTME: Unit tests for the deploy command's pure helpers (repo detection, output parsing).

import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { ConfigError } from '../src/config/store.js';
import { findRepoRoot, parseProvisionOutput } from '../src/commands/index.js';

const temporaryPaths: string[] = [];

afterAll(async () => {
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe('findRepoRoot', () => {
  it('locates the repo root from a nested directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'airyfs-repo-'));
    temporaryPaths.push(root);
    await mkdir(join(root, 'worker'), { recursive: true });
    await mkdir(join(root, 'scripts'), { recursive: true });
    await writeFile(join(root, 'worker', 'wrangler.jsonc'), '{}');
    await writeFile(join(root, 'scripts', 'provision.mjs'), '');
    const nested = join(root, 'cli', 'src', 'deep');
    await mkdir(nested, { recursive: true });

    expect(findRepoRoot(nested)).toBe(root);
  });

  it('returns null when no repo is found', async () => {
    const bare = await mkdtemp(join(tmpdir(), 'airyfs-bare-'));
    temporaryPaths.push(bare);
    expect(findRepoRoot(bare)).toBeNull();
  });
});

describe('parseProvisionOutput', () => {
  it('extracts the airyfsProvision result from mixed output', () => {
    const text = [
      'Total Upload: ...',
      'https://airyfs-int.example.workers.dev',
      JSON.stringify({ airyfsProvision: { env: 'int', url: 'https://x.workers.dev', secret: 's', accountId: 'a' } }),
    ].join('\n');
    expect(parseProvisionOutput(text)).toEqual({
      env: 'int', url: 'https://x.workers.dev', secret: 's', accountId: 'a',
    });
  });

  it('throws when no result line is present', () => {
    expect(() => parseProvisionOutput('nothing structured here')).toThrow(ConfigError);
  });
});
