// ABOUTME: UTC cron schedules that submit idempotent work into AiryFS's durable job queue.
// ABOUTME: Pure cron parsing and SQLite state keep scheduled execution testable and restart-safe.

import { HttpError } from './files-api';
import { validateCommand, validateCwd } from './jobs';
import type { SqlExec } from './schema';

export const SCHEDULE_TABLES = ['fs_job_schedule'] as const;

interface CronField {
  values: number[];
  wildcard: boolean;
}

interface CronSpec {
  minute: CronField;
  hour: CronField;
  day: CronField;
  month: CronField;
  weekday: CronField;
}

export interface JobSchedule {
  id: string;
  name: string;
  cron: string;
  command: string;
  cwd: string;
  enabled: boolean;
  nextRun: number | null;
  lastRun: number | null;
  createdAt: number;
}

export interface DueSchedule extends JobSchedule {
  scheduledFor: number;
}

const ALIASES: Record<string, string> = {
  '@hourly': '0 * * * *',
  '@daily': '0 0 * * *',
  '@weekly': '0 0 * * 0',
  '@monthly': '0 0 1 * *',
  '@yearly': '0 0 1 1 *',
};

export function initScheduleSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_job_schedule (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    cron TEXT NOT NULL,
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run INTEGER,
    last_run INTEGER,
    created_at INTEGER NOT NULL
  )`);
  sql.exec('CREATE INDEX IF NOT EXISTS idx_fs_job_schedule_due ON fs_job_schedule(enabled, next_run)');
}

function parseField(raw: string, min: number, max: number, weekday = false): CronField {
  const wildcard = raw === '*';
  const values = new Set<number>();
  for (const item of raw.split(',')) {
    const [rangeText, stepText] = item.split('/');
    const step = stepText === undefined ? 1 : Number(stepText);
    if (!Number.isInteger(step) || step <= 0) throw new HttpError(400, 'INVALID_CRON', `Invalid cron field: ${raw}`);
    let start: number;
    let end: number;
    if (rangeText === '*') {
      start = min;
      end = max;
    } else if (rangeText.includes('-')) {
      const parts = rangeText.split('-');
      if (parts.length !== 2) throw new HttpError(400, 'INVALID_CRON', `Invalid cron field: ${raw}`);
      start = Number(parts[0]);
      end = Number(parts[1]);
    } else {
      start = Number(rangeText);
      end = start;
    }
    if (weekday && start === 7 && end === 7) {
      values.add(0);
      continue;
    }
    if (weekday && end === 7) {
      values.add(0);
      end = 6;
    }
    if (!Number.isInteger(start) || !Number.isInteger(end) || start < min || end > max || start > end) {
      throw new HttpError(400, 'INVALID_CRON', `Invalid cron field: ${raw}`);
    }
    for (let value = start; value <= end; value += step) values.add(value);
  }
  if (values.size === 0) throw new HttpError(400, 'INVALID_CRON', `Invalid cron field: ${raw}`);
  return { values: [...values].sort((a, b) => a - b), wildcard };
}

function parseCron(expression: string): { expression: string; spec: CronSpec } {
  const canonical = ALIASES[expression.trim().toLowerCase()] ?? expression.trim();
  const fields = canonical.split(/\s+/);
  if (fields.length !== 5) {
    throw new HttpError(400, 'INVALID_CRON', 'Cron must contain five UTC fields: minute hour day month weekday');
  }
  return {
    expression: canonical,
    spec: {
      minute: parseField(fields[0], 0, 59),
      hour: parseField(fields[1], 0, 23),
      day: parseField(fields[2], 1, 31),
      month: parseField(fields[3], 1, 12),
      weekday: parseField(fields[4], 0, 7, true),
    },
  };
}

function includes(field: CronField, value: number): boolean {
  return field.values.includes(value);
}

function dayMatches(spec: CronSpec, date: Date): boolean {
  const day = includes(spec.day, date.getUTCDate());
  const weekday = includes(spec.weekday, date.getUTCDay());
  if (!spec.day.wildcard && !spec.weekday.wildcard) return day || weekday;
  return day && weekday;
}

function advanceToAllowed(date: Date, field: CronField, getter: () => number, advance: (value: number) => void): boolean {
  const current = getter();
  const next = field.values.find((value) => value > current);
  if (next === undefined) return false;
  advance(next);
  return true;
}

export function nextCronTime(expression: string, afterSeconds: number): number {
  const { spec } = parseCron(expression);
  const date = new Date((Math.floor(afterSeconds / 60) * 60 + 60) * 1000);
  const deadline = date.getUTCFullYear() + 6;

  while (date.getUTCFullYear() < deadline) {
    if (!includes(spec.month, date.getUTCMonth() + 1)) {
      const advanced = advanceToAllowed(
        date, spec.month, () => date.getUTCMonth() + 1,
        (month) => date.setUTCMonth(month - 1, 1),
      );
      if (!advanced) date.setUTCFullYear(date.getUTCFullYear() + 1, spec.month.values[0] - 1, 1);
      date.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!dayMatches(spec, date)) {
      date.setUTCDate(date.getUTCDate() + 1);
      date.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!includes(spec.hour, date.getUTCHours())) {
      const advanced = advanceToAllowed(
        date, spec.hour, () => date.getUTCHours(),
        (hour) => date.setUTCHours(hour, spec.minute.values[0], 0, 0),
      );
      if (!advanced) {
        date.setUTCDate(date.getUTCDate() + 1);
        date.setUTCHours(spec.hour.values[0], spec.minute.values[0], 0, 0);
      }
      continue;
    }
    if (!includes(spec.minute, date.getUTCMinutes())) {
      const advanced = advanceToAllowed(
        date, spec.minute, () => date.getUTCMinutes(),
        (minute) => date.setUTCMinutes(minute, 0, 0),
      );
      if (!advanced) {
        date.setUTCHours(date.getUTCHours() + 1, spec.minute.values[0], 0, 0);
      }
      continue;
    }
    return Math.floor(date.getTime() / 1000);
  }
  throw new HttpError(400, 'INVALID_CRON', 'Cron has no matching time within six years');
}

function row(row: Record<string, unknown>): JobSchedule {
  return {
    id: String(row.id),
    name: String(row.name),
    cron: String(row.cron),
    command: String(row.command),
    cwd: String(row.cwd),
    enabled: Number(row.enabled) === 1,
    nextRun: row.next_run === null ? null : Number(row.next_run),
    lastRun: row.last_run === null ? null : Number(row.last_run),
    createdAt: Number(row.created_at),
  };
}

export function createSchedule(sql: SqlExec, input: Record<string, unknown>, now = Math.floor(Date.now() / 1000)): JobSchedule {
  if (typeof input.name !== 'string' || !input.name.trim()) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'name must be a non-empty string');
  }
  if (typeof input.cron !== 'string') throw new HttpError(400, 'INVALID_CRON', 'cron must be a string');
  const { expression } = parseCron(input.cron);
  const command = validateCommand(input.command);
  const cwd = validateCwd(input.cwd ?? '/');
  const id = crypto.randomUUID();
  if (sql.exec('SELECT id FROM fs_job_schedule WHERE name = ?', input.name.trim()).toArray().length > 0) {
    throw new HttpError(409, 'SCHEDULE_EXISTS', `Schedule already exists: ${input.name.trim()}`);
  }
  sql.exec(
    `INSERT INTO fs_job_schedule (id, name, cron, command, cwd, enabled, next_run, last_run, created_at)
     VALUES (?, ?, ?, ?, ?, 1, ?, NULL, ?)`,
    id, input.name.trim(), expression, command, cwd, nextCronTime(expression, now), now,
  );
  return listSchedules(sql).find((schedule) => schedule.id === id)!;
}

export function listSchedules(sql: SqlExec): JobSchedule[] {
  return sql.exec('SELECT * FROM fs_job_schedule ORDER BY name').toArray().map(row);
}

export function deleteSchedule(sql: SqlExec, id: string): boolean {
  const exists = sql.exec('SELECT id FROM fs_job_schedule WHERE id = ?', id).toArray().length > 0;
  sql.exec('DELETE FROM fs_job_schedule WHERE id = ?', id);
  return exists;
}

export function setScheduleEnabled(sql: SqlExec, id: string, enabled: boolean, now = Math.floor(Date.now() / 1000)): JobSchedule {
  const rows = sql.exec('SELECT cron FROM fs_job_schedule WHERE id = ?', id).toArray();
  if (rows.length === 0) throw new HttpError(404, 'SCHEDULE_NOT_FOUND', `Unknown schedule: ${id}`);
  const next = enabled ? nextCronTime(String(rows[0].cron), now) : null;
  sql.exec('UPDATE fs_job_schedule SET enabled = ?, next_run = ? WHERE id = ?', enabled ? 1 : 0, next, id);
  return listSchedules(sql).find((schedule) => schedule.id === id)!;
}

export function listDueSchedules(sql: SqlExec, now = Math.floor(Date.now() / 1000)): DueSchedule[] {
  return sql.exec(
    'SELECT * FROM fs_job_schedule WHERE enabled = 1 AND next_run <= ? ORDER BY next_run, id', now,
  ).toArray().map(row).map((schedule) => ({ ...schedule, scheduledFor: schedule.nextRun! }));
}

export function advanceSchedule(sql: SqlExec, schedule: DueSchedule): void {
  sql.exec(
    `UPDATE fs_job_schedule SET last_run = ?, next_run = ?
     WHERE id = ? AND enabled = 1 AND next_run = ?`,
    schedule.scheduledFor, nextCronTime(schedule.cron, schedule.scheduledFor),
    schedule.id, schedule.scheduledFor,
  );
}

export function nextScheduleDelay(sql: SqlExec): number | null {
  const rows = sql.exec(
    'SELECT min(next_run) AS next_run FROM fs_job_schedule WHERE enabled = 1 AND next_run IS NOT NULL'
  ).toArray();
  const next = rows[0]?.next_run;
  return next === null || next === undefined ? null : Math.max(0, Number(next) - Math.floor(Date.now() / 1000));
}
