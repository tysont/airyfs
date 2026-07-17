// ABOUTME: Persists AiryFS CLI settings under the user's home directory.
// ABOUTME: Serializes updates with a lock and atomically replaces the config file.

import { randomUUID } from 'node:crypto';
import { mkdir, readFile, rename, unlink, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, posix } from 'node:path';
import lockfile from 'proper-lockfile';
import { CONFIG_VERSION, emptyConfig, type AiryFSConfig } from './types.js';

export class ConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConfigError';
  }
}

export class ConfigStore {
  readonly home: string;
  readonly configPath: string;

  constructor(home = process.env.AIRYFS_HOME || join(homedir(), '.airyfs')) {
    this.home = home;
    this.configPath = join(home, 'config.json');
  }

  async read(): Promise<AiryFSConfig> {
    let contents: string;
    try {
      contents = await readFile(this.configPath, 'utf8');
    } catch (error) {
      if (isNodeError(error, 'ENOENT')) return emptyConfig();
      throw error;
    }

    try {
      return validateConfig(JSON.parse(contents));
    } catch (error) {
      if (error instanceof ConfigError) throw error;
      throw new ConfigError(`Could not parse ${this.configPath}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async update(mutator: (config: AiryFSConfig) => void | Promise<void>): Promise<AiryFSConfig> {
    await mkdir(this.home, { recursive: true, mode: 0o700 });
    const release = await lockfile.lock(this.configPath, {
      realpath: false,
      stale: 10_000,
      update: 2_000,
      retries: { retries: 100, minTimeout: 25, maxTimeout: 100 },
    });
    try {
      const config = await this.read();
      await mutator(config);
      await this.writeAtomic(config);
      return config;
    } finally {
      await release();
    }
  }

  private async writeAtomic(config: AiryFSConfig): Promise<void> {
    const temporaryPath = join(this.home, `.config.${process.pid}.${randomUUID()}.tmp`);
    const contents = `${JSON.stringify(config, null, 2)}\n`;
    try {
      await writeFile(temporaryPath, contents, { encoding: 'utf8', mode: 0o600, flag: 'wx' });
      await rename(temporaryPath, this.configPath);
    } finally {
      await unlink(temporaryPath).catch((error: unknown) => {
        if (!isNodeError(error, 'ENOENT')) throw error;
      });
    }
  }

}

function validateConfig(value: unknown): AiryFSConfig {
  if (!isRecord(value) || value.version !== CONFIG_VERSION) {
    throw new ConfigError(`Unsupported AiryFS config version in config.json`);
  }
  if (!isRecord(value.sessions)) {
    throw new ConfigError('Invalid AiryFS config structure in config.json');
  }
  if (value.currentSession !== undefined && typeof value.currentSession !== 'string') {
    throw new ConfigError('Invalid currentSession in config.json');
  }
  for (const [name, session] of Object.entries(value.sessions)) {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(name)
      || !isRecord(session)
      || !isValidEndpoint(session.endpoint)
      || typeof session.volume !== 'string'
      || !session.volume.trim()
      || session.volume !== session.volume.trim()
      || session.volume.includes('/')
      || typeof session.cwd !== 'string'
      || !session.cwd.startsWith('/')
      || posix.resolve('/', session.cwd) !== session.cwd
      || !isValidTimestamp(session.createdAt)
      || !isValidTimestamp(session.updatedAt)) {
      throw new ConfigError(`Invalid session "${name}" in config.json`);
    }
  }
  if (typeof value.currentSession === 'string' && !Object.hasOwn(value.sessions, value.currentSession)) {
    throw new ConfigError(`Current session "${value.currentSession}" does not exist in config.json`);
  }
  return value as unknown as AiryFSConfig;
}

function isValidEndpoint(value: unknown): boolean {
  if (typeof value !== 'string') return false;
  try {
    const url = new URL(value);
    return (url.protocol === 'http:' || url.protocol === 'https:')
      && !url.username && !url.password && !url.search && !url.hash;
  } catch {
    return false;
  }
}

function isValidTimestamp(value: unknown): boolean {
  return typeof value === 'string' && !Number.isNaN(Date.parse(value));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNodeError(error: unknown, code: string): boolean {
  return error instanceof Error && 'code' in error && error.code === code;
}
