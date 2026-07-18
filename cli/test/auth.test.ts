// ABOUTME: Verifies session token storage, version-2 config migration, and bearer auth on the client.
// ABOUTME: Uses isolated temporary homes and fetch stubs so no deployed Worker is required.

import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { AiryFSClient } from '../src/api/client.js';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigError, ConfigStore } from '../src/config/store.js';

const homes: string[] = [];

async function manager(): Promise<SessionManager> {
  const home = await mkdtemp(join(tmpdir(), 'airyfs-auth-'));
  homes.push(home);
  return new SessionManager(new ConfigStore(home));
}

afterEach(async () => {
  await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

describe('session tokens', () => {
  it('stores and clears a token on a session', async () => {
    const sessions = await manager();
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'vol' });

    await sessions.setToken('test', ' token-value ');
    expect((await sessions.resolve('test')).session.token).toBe('token-value');

    await sessions.clearToken('test');
    expect((await sessions.resolve('test')).session.token).toBeUndefined();
  });

  it('rejects an empty token', async () => {
    const sessions = await manager();
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'vol' });
    await expect(sessions.setToken('test', '   ')).rejects.toThrow('Token cannot be empty');
  });

  it('persists the config at version 3', async () => {
    const sessions = await manager();
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'vol' });
    const contents = JSON.parse(await readFile(sessions.store.configPath, 'utf8'));
    expect(contents.version).toBe(3);
  });
});

describe('version-2 config migration', () => {
  it('reads a version-2 config and rewrites it as version 3 on the next update', async () => {
    const sessions = await manager();
    const now = new Date().toISOString();
    await writeFile(sessions.store.configPath, JSON.stringify({
      version: 2,
      currentSession: 'legacy',
      sessions: {
        legacy: { endpoint: 'https://example.com', volume: 'vol', cwd: '/', createdAt: now, updatedAt: now },
      },
    }));

    // Reading a v2 config succeeds (migrated in memory).
    expect((await sessions.resolve('legacy')).session.volume).toBe('vol');

    // The next mutation persists version 3 without losing the session.
    await sessions.setToken('legacy', 'cap-token');
    const persisted = JSON.parse(await readFile(sessions.store.configPath, 'utf8'));
    expect(persisted.version).toBe(3);
    expect(persisted.sessions.legacy.token).toBe('cap-token');
  });

  it('still rejects unsupported versions', async () => {
    const sessions = await manager();
    await writeFile(sessions.store.configPath, JSON.stringify({ version: 1, sessions: {} }));
    await expect(sessions.list()).rejects.toThrow('Unsupported AiryFS config version');
  });

  it('rejects a persisted non-string token', async () => {
    const sessions = await manager();
    const now = new Date().toISOString();
    await writeFile(sessions.store.configPath, JSON.stringify({
      version: 3,
      sessions: {
        broken: { endpoint: 'https://example.com', volume: 'vol', cwd: '/', token: 42, createdAt: now, updatedAt: now },
      },
    }));
    await expect(sessions.list()).rejects.toThrow(ConfigError);
  });
});

describe('AiryFSClient bearer auth', () => {
  it('attaches the bearer token to every request when configured', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json([]));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock, 'secret-token');

    await client.listDirectory('/');

    const [, init] = fetchMock.mock.calls[0];
    expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer secret-token');
  });

  it('omits the Authorization header when no token is configured', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json([]));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.listDirectory('/');

    expect(fetchMock).toHaveBeenCalledWith(new URL('https://example.com/v1/volumes/vol/directories'), undefined);
  });

  it('mints and revokes capabilities against the v1 routes', async () => {
    const minted = { token: 'tok', id: 'abc', volume: 'vol', operations: ['read'], pathPrefixes: [], expires: 1 };
    const fetchMock = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(minted, { status: 201 }))
      .mockResolvedValueOnce(new Response(null, { status: 200 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock, 'root');

    expect(await client.createCapability({ operations: ['read'], pathPrefixes: [], expiresInSeconds: 60 }))
      .toEqual(minted);
    const [createUrl, createInit] = fetchMock.mock.calls[0];
    expect(createUrl.toString()).toBe('https://example.com/v1/volumes/vol/capabilities');
    expect(createInit).toMatchObject({ method: 'POST' });

    await client.revokeCapability('abc');
    const [revokeUrl, revokeInit] = fetchMock.mock.calls[1];
    expect(revokeUrl.toString()).toBe('https://example.com/v1/volumes/vol/capabilities/abc');
    expect(revokeInit).toMatchObject({ method: 'DELETE' });
  });

  it('reads the current auth status', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ auth: 'root', volume: 'vol' }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock, 'root');

    expect(await client.authStatus()).toEqual({ auth: 'root', volume: 'vol' });
    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/v1/volumes/vol/capabilities');
  });
});
