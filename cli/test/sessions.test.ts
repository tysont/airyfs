// ABOUTME: Verifies atomic config persistence and named AiryFS session behavior.
// ABOUTME: Uses isolated temporary homes so tests never touch the user's settings.

import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigError, ConfigStore } from '../src/config/store.js';

const homes: string[] = [];

async function manager(): Promise<SessionManager> {
  const home = await mkdtemp(join(tmpdir(), 'airyfs-cli-'));
  homes.push(home);
  return new SessionManager(new ConfigStore(home));
}

afterEach(async () => {
  await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
  delete process.env.AIRYFS_SESSION;
});

describe('SessionManager', () => {
  it('requires an explicitly created or selected session', async () => {
    const sessions = await manager();

    await expect(sessions.resolve()).rejects.toThrow('No active session');
    expect((await sessions.list()).sessions).toEqual([]);
  });

  it('automatically selects each newly created session', async () => {
    const sessions = await manager();

    await sessions.create('first', { endpoint: 'https://first.example.com', volume: 'one' });
    expect((await sessions.resolve()).name).toBe('first');
    await sessions.create('second', { endpoint: 'https://second.example.com', volume: 'two' });

    expect((await sessions.resolve()).name).toBe('second');
  });

  it('keeps endpoint, volume, and cwd isolated by session', async () => {
    const sessions = await manager();
    await sessions.create('int', { endpoint: 'https://int.example.com/', volume: 'alpha' });
    await sessions.setCwd('int', 'src');
    await sessions.create('prod', { endpoint: 'https://prod.example.com', volume: 'beta' });

    expect((await sessions.resolve('int')).session).toMatchObject({
      endpoint: 'https://int.example.com', volume: 'alpha', cwd: '/src',
    });
    expect((await sessions.resolve('prod')).session).toMatchObject({
      endpoint: 'https://prod.example.com', volume: 'beta', cwd: '/',
    });
  });

  it('honors AIRYFS_SESSION without changing the persisted current session', async () => {
    const sessions = await manager();
    await sessions.create('first', { endpoint: 'https://first.example.com', volume: 'one' });
    await sessions.create('second', { endpoint: 'https://second.example.com', volume: 'two' });
    await sessions.use('first');
    process.env.AIRYFS_SESSION = 'second';

    expect((await sessions.resolve()).name).toBe('second');
    expect((await sessions.list()).currentSession).toBe('first');
  });

  it('serializes concurrent updates without losing sessions', async () => {
    const sessions = await manager();

    await Promise.all(Array.from({ length: 8 }, (_, index) => sessions.create(`session-${index}`, {
      endpoint: `https://session-${index}.example.com`,
      volume: `volume-${index}`,
    })));

    expect((await sessions.list()).sessions).toHaveLength(8);
  });

  it('atomically writes valid JSON with private file mode', async () => {
    const sessions = await manager();
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'vol' });

    const contents = await readFile(sessions.store.configPath, 'utf8');
    expect(JSON.parse(contents).sessions.test.volume).toBe('vol');
  });

  it('rejects malformed endpoints and unknown requested sessions', async () => {
    const sessions = await manager();

    await expect(sessions.create('bad', { endpoint: 'ftp://example.com', volume: 'vol' })).rejects.toThrow(ConfigError);
    await expect(sessions.resolve('missing')).rejects.toThrow('Session "missing" does not exist');
  });

  it('normalizes remote cwd and resets it when changing volume', async () => {
    const sessions = await manager();
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'one' });
    await sessions.setCwd('test', '/a/b');
    await sessions.setCwd('test', '../c');
    expect((await sessions.resolve('test')).session.cwd).toBe('/a/c');

    await sessions.setVolume('test', 'two');
    expect((await sessions.resolve('test')).session.cwd).toBe('/');
  });

  it('leaves no active session after deleting the current session', async () => {
    const sessions = await manager();
    await sessions.create('first', { endpoint: 'https://first.example.com', volume: 'one' });
    await sessions.create('second', { endpoint: 'https://second.example.com', volume: 'two' });

    await sessions.remove('second');

    expect((await sessions.list()).currentSession).toBeUndefined();
    await expect(sessions.resolve()).rejects.toThrow('No active session');
    expect((await sessions.resolve('first')).name).toBe('first');
  });

  it('supports session names inherited by ordinary JavaScript objects', async () => {
    const sessions = await manager();

    await sessions.create('constructor', { endpoint: 'https://example.com', volume: 'one' });
    await sessions.create('toString', { endpoint: 'https://example.com', volume: 'two' });

    expect((await sessions.resolve('constructor')).session.volume).toBe('one');
    expect((await sessions.resolve('toString')).session.volume).toBe('two');
    expect((await sessions.list()).sessions).toHaveLength(2);
  });

  it('applies endpoint and volume edits atomically after validating both', async () => {
    const sessions = await manager();
    await sessions.create('test', { endpoint: 'https://old.example.com', volume: 'old' });

    await expect(sessions.edit('test', {
      endpoint: 'https://new.example.com',
      volume: 'invalid/volume',
    })).rejects.toThrow('Volume must be a non-empty name without slashes');

    expect((await sessions.resolve('test')).session).toMatchObject({
      endpoint: 'https://old.example.com',
      volume: 'old',
    });
  });

  it('rejects incompatible and internally inconsistent persisted config', async () => {
    const sessions = await manager();
    await writeFile(sessions.store.configPath, JSON.stringify({ version: 1, sessions: {} }));
    await expect(sessions.list()).rejects.toThrow('Unsupported AiryFS config version');

    await writeFile(sessions.store.configPath, JSON.stringify({
      version: 2,
      currentSession: 'missing',
      sessions: {},
    }));
    await expect(sessions.list()).rejects.toThrow('Current session "missing" does not exist');
  });
});
