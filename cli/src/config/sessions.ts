// ABOUTME: Manages named AiryFS sessions and active-session resolution.
// ABOUTME: Keeps terminal-specific selection separate from the persisted active session.

import { posix } from 'node:path';
import { ConfigError, ConfigStore } from './store.js';
import type { AiryFSConfig, AiryFSSession, NamedSession } from './types.js';

const SESSION_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;
export const NO_ACTIVE_SESSION_MESSAGE =
  'No active session. Run `airyfs session create <name> --endpoint <url> --volume <volume>` or `airyfs session use <name>`.';

export interface CreateSessionOptions {
  endpoint: string;
  volume: string;
}

export class SessionManager {
  constructor(readonly store = new ConfigStore()) {}

  async resolve(requestedName?: string): Promise<NamedSession> {
    const config = await this.store.read();
    const name = requestedName || process.env.AIRYFS_SESSION || config.currentSession;
    if (!name) throw new ConfigError(NO_ACTIVE_SESSION_MESSAGE);
    const existing = ownSession(config, name);
    if (existing) return { name, session: existing };
    throw new ConfigError(`Session "${name}" does not exist`);
  }

  async list(): Promise<{ currentSession?: string; sessions: NamedSession[] }> {
    const config = await this.store.read();
    return {
      currentSession: config.currentSession,
      sessions: Object.entries(config.sessions)
        .map(([name, session]) => ({ name, session }))
        .sort((left, right) => left.name.localeCompare(right.name)),
    };
  }

  async create(name: string, options: CreateSessionOptions): Promise<NamedSession> {
    validateSessionName(name);
    let created!: AiryFSSession;
    await this.store.update((config) => {
      if (ownSession(config, name)) throw new ConfigError(`Session "${name}" already exists`);
      const now = new Date().toISOString();
      created = {
        endpoint: normalizeEndpoint(options.endpoint),
        volume: validateVolume(options.volume),
        cwd: '/',
        createdAt: now,
        updatedAt: now,
      };
      setSession(config, name, created);
      config.currentSession = name;
    });
    return { name, session: created };
  }

  async use(name: string): Promise<NamedSession> {
    validateSessionName(name);
    let selected!: AiryFSSession;
    await this.store.update((config) => {
      selected = requireSession(config, name);
      config.currentSession = name;
    });
    return { name, session: selected };
  }

  async remove(name: string): Promise<void> {
    await this.store.update((config) => {
      requireSession(config, name);
      delete config.sessions[name];
      if (config.currentSession === name) config.currentSession = undefined;
    });
  }

  async rename(from: string, to: string): Promise<NamedSession> {
    validateSessionName(to);
    let renamed!: AiryFSSession;
    await this.store.update((config) => {
      if (ownSession(config, to)) throw new ConfigError(`Session "${to}" already exists`);
      renamed = requireSession(config, from);
      setSession(config, to, renamed);
      delete config.sessions[from];
      if (config.currentSession === from) config.currentSession = to;
    });
    return { name: to, session: renamed };
  }

  async setEndpoint(name: string, endpoint: string): Promise<NamedSession> {
    return this.edit(name, { endpoint });
  }

  async setVolume(name: string, volume: string): Promise<NamedSession> {
    return this.edit(name, { volume });
  }

  async edit(name: string, changes: { endpoint?: string; volume?: string }): Promise<NamedSession> {
    const endpoint = changes.endpoint === undefined ? undefined : normalizeEndpoint(changes.endpoint);
    const volume = changes.volume === undefined ? undefined : validateVolume(changes.volume);
    return this.updateSession(name, (session) => {
      if (endpoint !== undefined) session.endpoint = endpoint;
      if (volume !== undefined) {
        session.volume = volume;
        session.cwd = '/';
      }
    });
  }

  async setCwd(name: string, cwd: string): Promise<NamedSession> {
    return this.updateSession(name, (session) => {
      session.cwd = normalizeRemotePath(session.cwd, cwd);
    });
  }

  private async updateSession(name: string, mutate: (session: AiryFSSession) => void): Promise<NamedSession> {
    let updated!: AiryFSSession;
    await this.store.update((config) => {
      const session = requireSession(config, name);
      mutate(session);
      session.updatedAt = new Date().toISOString();
      updated = session;
    });
    return { name, session: updated };
  }
}

export function normalizeEndpoint(endpoint: string): string {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new ConfigError(`Invalid endpoint URL: ${endpoint}`);
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new ConfigError('Endpoint must use http:// or https://');
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new ConfigError('Endpoint cannot contain credentials, a query string, or a fragment');
  }
  return url.toString().replace(/\/$/, '');
}

export function normalizeRemotePath(cwd: string, path: string): string {
  return posix.resolve('/', cwd, path || '.');
}

function requireSession(config: AiryFSConfig, name: string): AiryFSSession {
  const session = ownSession(config, name);
  if (!session) throw new ConfigError(`Session "${name}" does not exist`);
  return session;
}

function ownSession(config: AiryFSConfig, name: string): AiryFSSession | undefined {
  return Object.hasOwn(config.sessions, name) ? config.sessions[name] : undefined;
}

function setSession(config: AiryFSConfig, name: string, session: AiryFSSession): void {
  Object.defineProperty(config.sessions, name, {
    value: session,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

function validateSessionName(name: string): void {
  if (!SESSION_NAME.test(name)) {
    throw new ConfigError('Session names may contain letters, numbers, periods, underscores, and hyphens');
  }
}

function validateVolume(volume: string): string {
  const trimmed = volume.trim();
  if (!trimmed || trimmed.includes('/')) {
    throw new ConfigError('Volume must be a non-empty name without slashes');
  }
  return trimmed;
}
