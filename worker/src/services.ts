// ABOUTME: Persists preview service definitions and allocates stable per-volume Container ports.
// ABOUTME: Keeps desired service state durable while running processes remain disposable compute.

import { normalizePath } from './auth';
import { HttpError } from './files-api';
import type { SqlExec } from './schema';

export const SERVICE_TABLES = ['fs_service'] as const;
const MIN_PORT = 5000;
const MAX_PORT = 5015;

export interface ServiceRecord {
  name: string;
  command: string;
  cwd: string;
  env: Record<string, string>;
  port: number;
  enabled: boolean;
  public: boolean;
  createdAt: number;
}

export function initServiceSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_service (
    name TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    env TEXT NOT NULL,
    port INTEGER NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    public INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
  )`);
}

export function createService(sql: SqlExec, input: Record<string, unknown>): ServiceRecord {
  if (typeof input.name !== 'string' || !/^[a-z0-9][a-z0-9-]{0,31}$/.test(input.name)) {
    throw new HttpError(400, 'INVALID_SERVICE', 'name must contain lowercase letters, digits, and hyphens');
  }
  if (typeof input.command !== 'string' || !input.command.trim()) throw new HttpError(400, 'INVALID_SERVICE', 'command is required');
  const cwd = normalizePath(typeof input.cwd === 'string' ? input.cwd : '/');
  const rawEnv = input.env === undefined ? {} : input.env;
  if (typeof rawEnv !== 'object' || rawEnv === null || Array.isArray(rawEnv) || Object.values(rawEnv).some((value) => typeof value !== 'string')) {
    throw new HttpError(400, 'INVALID_SERVICE', 'env must contain only string values');
  }
  const used = new Set((sql.exec('SELECT port FROM fs_service').toArray() as Array<{ port: number }>).map((row) => Number(row.port)));
  let port = MIN_PORT;
  while (port <= MAX_PORT && used.has(port)) port++;
  if (port > MAX_PORT) throw new HttpError(409, 'PREVIEW_PORTS_EXHAUSTED', 'All preview service ports are allocated');
  const createdAt = Math.floor(Date.now() / 1000);
  sql.exec(`INSERT INTO fs_service (name, command, cwd, env, port, enabled, public, created_at)
    VALUES (?, ?, ?, ?, ?, 1, ?, ?)`, input.name, input.command, cwd, JSON.stringify(rawEnv), port, input.public === true ? 1 : 0, createdAt);
  return readService(sql, input.name);
}

export function listServices(sql: SqlExec): ServiceRecord[] {
  return (sql.exec('SELECT * FROM fs_service ORDER BY name').toArray() as unknown as Record<string, unknown>[]).map(mapService);
}

export function readService(sql: SqlExec, name: string): ServiceRecord {
  const row = sql.exec('SELECT * FROM fs_service WHERE name = ?', name).toArray()[0];
  if (!row) throw new HttpError(404, 'SERVICE_NOT_FOUND', `Preview service not found: ${name}`);
  return mapService(row);
}

export function setServiceEnabled(sql: SqlExec, name: string, enabled: boolean): ServiceRecord {
  readService(sql, name);
  sql.exec('UPDATE fs_service SET enabled = ? WHERE name = ?', enabled ? 1 : 0, name);
  return readService(sql, name);
}

export function deleteService(sql: SqlExec, name: string): ServiceRecord {
  const record = readService(sql, name);
  sql.exec('DELETE FROM fs_service WHERE name = ?', name);
  return record;
}

function mapService(row: Record<string, unknown>): ServiceRecord {
  return {
    name: String(row.name), command: String(row.command), cwd: String(row.cwd),
    env: JSON.parse(String(row.env)) as Record<string, string>, port: Number(row.port),
    enabled: Boolean(row.enabled), public: Boolean(row.public), createdAt: Number(row.created_at),
  };
}
