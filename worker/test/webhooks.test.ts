// ABOUTME: Tests durable webhook subscriptions, trigger-driven outbox filtering, signatures, and retries.
// ABOUTME: Uses real SQLite triggers so FUSE-style raw mutations exercise the same enqueue path.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { initSchema, type SqlExec } from '../src/schema';
import {
  createWebhook,
  deleteWebhook,
  deliverWebhooks,
  hasPendingWebhookDeliveries,
  listWebhooks,
} from '../src/webhooks';

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

describe('change-feed webhooks', () => {
  let db: Database.Database;
  let sql: SqlExec;

  beforeEach(() => {
    db = new Database(':memory:');
    sql = testSql(db);
    initSchema(sql);
  });

  function change(seq: number, type: string, path: string, oldPath: string | null = null): void {
    db.prepare('UPDATE fs_change_sequence SET next_seq = ? WHERE id = 1').run(seq + 1);
    db.prepare(
      'INSERT INTO fs_change_feed (seq, type, path, oldPath, ino, timestamp) VALUES (?, ?, ?, ?, 2, unixepoch())'
    ).run(seq, type, path, oldPath);
  }

  it('stores subscriptions without returning their signing secret in list responses', () => {
    const created = createWebhook(sql, {
      url: 'https://hooks.example.test/airy',
      pathPrefix: '/src/./app',
      events: ['create', 'modify', 'create'],
    });

    expect(created).toMatchObject({
      url: 'https://hooks.example.test/airy', pathPrefix: '/src/app', events: ['create', 'modify'],
    });
    expect(created.secret.length).toBeGreaterThan(30);
    expect(listWebhooks(sql)).toEqual([{ ...created, secret: undefined }].map(({ secret: _, ...value }) => value));
  });

  it('enqueues only matching event types and exact path-prefix segments', () => {
    createWebhook(sql, { url: 'https://hooks.example.test/', pathPrefix: '/src', events: ['modify', 'rename'] });
    change(1, 'create', '/src/a.ts');
    change(2, 'modify', '/src/a.ts');
    change(3, 'modify', '/src-other/a.ts');
    change(4, 'rename', '/other/a.ts', '/src/a.ts');

    const queued = db.prepare('SELECT seq FROM fs_webhook_delivery ORDER BY seq').all() as Array<{ seq: number }>;
    expect(queued.map((row) => row.seq)).toEqual([2, 4]);
  });

  it('delivers signed payloads and removes successful outbox rows', async () => {
    const webhook = createWebhook(sql, { url: 'https://hooks.example.test/', events: ['create'] });
    change(1, 'create', '/hello.txt');
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const fetchMock = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init: init! });
      return new Response(null, { status: 204 });
    }) as typeof fetch;

    expect(await deliverWebhooks(sql, 'vol', fetchMock)).toBe(1);
    expect(hasPendingWebhookDeliveries(sql)).toBe(false);
    expect(calls[0].url).toBe('https://hooks.example.test/');
    expect(new Headers(calls[0].init.headers).get('X-AiryFS-Delivery')).toBe(`${webhook.id}:1`);
    expect(new Headers(calls[0].init.headers).get('X-AiryFS-Signature')).toMatch(/^sha256=[0-9a-f]{64}$/);
    expect(JSON.parse(new TextDecoder().decode(calls[0].init.body as Uint8Array))).toMatchObject({
      volume: 'vol', event: { seq: 1, type: 'create', path: '/hello.txt' },
    });
  });

  it('retains failed deliveries for retry and removes them with their subscription', async () => {
    const webhook = createWebhook(sql, { url: 'https://hooks.example.test/' });
    change(1, 'create', '/hello.txt');
    const fetchMock = vi.fn(async () => new Response(null, { status: 503 })) as typeof fetch;

    expect(await deliverWebhooks(sql, 'vol', fetchMock)).toBe(1);
    const failed = db.prepare(
      'SELECT attempts, next_attempt, last_error FROM fs_webhook_delivery WHERE webhook_id = ?'
    ).get(webhook.id) as { attempts: number; next_attempt: number; last_error: string };
    expect(failed.attempts).toBe(1);
    expect(failed.next_attempt).toBeGreaterThan(Math.floor(Date.now() / 1000));
    expect(failed.last_error).toBe('HTTP 503');
    expect(deleteWebhook(sql, webhook.id)).toBe(true);
    expect(db.prepare('SELECT count(*) FROM fs_webhook_delivery').pluck().get()).toBe(0);
  });

  it('rejects non-HTTPS endpoints and unknown events', () => {
    expect(() => createWebhook(sql, { url: 'http://hooks.example.test/' })).toThrow('HTTPS');
    expect(() => createWebhook(sql, { url: 'https://hooks.example.test/', events: ['wat'] })).toThrow('Unknown');
  });
});
