// ABOUTME: Tests UTC cron parsing and durable scheduled-job state transitions.
// ABOUTME: Covers aliases, steps, day semantics, claiming, enablement, and validation.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { initSchema, type SqlExec } from '../src/schema';
import {
  advanceSchedule,
  createSchedule,
  deleteSchedule,
  listSchedules,
  listDueSchedules,
  nextCronTime,
  setScheduleEnabled,
} from '../src/schedules';

function testSql(db: Database.Database): SqlExec {
  db.function('unixepoch', () => Math.floor(Date.now() / 1000));
  return {
    exec(query: string, ...bindings: unknown[]) {
      const statement = db.prepare(query);
      if (statement.reader) {
        const rows = statement.all(...bindings) as Record<string, unknown>[];
        return { toArray: () => rows };
      }
      statement.run(...bindings);
      return { toArray: () => [] };
    },
  };
}

describe('job schedules', () => {
  let sql: SqlExec;

  beforeEach(() => {
    sql = testSql(new Database(':memory:'));
    initSchema(sql);
  });

  it('finds the next UTC time for aliases, steps, and Sunday aliases', () => {
    const monday = Date.UTC(2026, 6, 20, 10, 7) / 1000;
    expect(new Date(nextCronTime('@hourly', monday) * 1000).toISOString()).toBe('2026-07-20T11:00:00.000Z');
    expect(new Date(nextCronTime('*/15 * * * *', monday) * 1000).toISOString()).toBe('2026-07-20T10:15:00.000Z');
    expect(new Date(nextCronTime('0 9 * * 7', monday) * 1000).toISOString()).toBe('2026-07-26T09:00:00.000Z');
  });

  it('uses standard OR semantics when day-of-month and weekday are both restricted', () => {
    const after = Date.UTC(2026, 6, 20, 0, 0) / 1000;
    expect(new Date(nextCronTime('0 0 21 * 5', after) * 1000).toISOString()).toBe('2026-07-21T00:00:00.000Z');
  });

  it('creates, claims once, and advances to the next occurrence', () => {
    const now = Date.UTC(2026, 6, 20, 10, 7) / 1000;
    const schedule = createSchedule(sql, { name: 'build', cron: '*/15 * * * *', command: 'npm run build', cwd: '/site' }, now);
    expect(schedule.nextRun).toBe(Date.UTC(2026, 6, 20, 10, 15) / 1000);
    expect(listDueSchedules(sql, schedule.nextRun! - 1)).toEqual([]);
    const claimed = listDueSchedules(sql, schedule.nextRun!);
    expect(claimed).toMatchObject([{ id: schedule.id, scheduledFor: schedule.nextRun }]);
    advanceSchedule(sql, claimed[0]);
    expect(listDueSchedules(sql, schedule.nextRun!)).toEqual([]);
    expect(listSchedules(sql)[0].lastRun).toBe(schedule.nextRun);
  });

  it('disables, re-enables, deletes, and rejects invalid schedules', () => {
    const now = Date.UTC(2026, 6, 20, 10, 7) / 1000;
    const schedule = createSchedule(sql, { name: 'daily', cron: '@daily', command: 'true' }, now);
    expect(setScheduleEnabled(sql, schedule.id, false, now).nextRun).toBeNull();
    expect(setScheduleEnabled(sql, schedule.id, true, now).nextRun).not.toBeNull();
    expect(deleteSchedule(sql, schedule.id)).toBe(true);
    expect(() => createSchedule(sql, { name: 'bad', cron: '99 * * * *', command: 'true' }, now)).toThrow('Invalid cron');
  });
});
