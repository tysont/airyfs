// ABOUTME: Durable change-feed webhook subscriptions, outbox persistence, and signed delivery retries.
// ABOUTME: SQLite triggers enqueue matching events from every writer, including FUSE/Hrana mutations.

import { HttpError } from './files-api';
import type { ChangeEvent } from './change-feed';
import type { SqlExec } from './schema';

export const WEBHOOK_TABLES = ['fs_webhook', 'fs_webhook_delivery'] as const;
export const WEBHOOK_EVENTS = ['create', 'modify', 'remove', 'rename'] as const;
export type WebhookEvent = (typeof WEBHOOK_EVENTS)[number];

const MAX_ATTEMPTS = 8;
const MAX_DELIVERIES_PER_RUN = 25;

export interface WebhookInfo {
  id: string;
  url: string;
  pathPrefix: string;
  events: WebhookEvent[];
  createdAt: number;
}

export interface CreatedWebhook extends WebhookInfo {
  secret: string;
}

interface DeliveryRow {
  webhook_id: string;
  seq: number;
  attempts: number;
  url: string;
  secret: string;
  type: WebhookEvent;
  path: string;
  oldPath: string | null;
  ino: number;
  timestamp: number;
}

export function initWebhookSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_webhook (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    path_prefix TEXT NOT NULL,
    events TEXT NOT NULL,
    secret TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )`);
  sql.exec(`CREATE TABLE IF NOT EXISTS fs_webhook_delivery (
    webhook_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    path TEXT NOT NULL,
    old_path TEXT,
    ino INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt INTEGER,
    last_error TEXT,
    PRIMARY KEY (webhook_id, seq)
  )`);
  sql.exec('CREATE INDEX IF NOT EXISTS idx_fs_webhook_delivery_due ON fs_webhook_delivery(next_attempt, seq)');
  sql.exec(`CREATE TRIGGER IF NOT EXISTS trg_fs_webhook_delete
    AFTER DELETE ON fs_webhook BEGIN
      DELETE FROM fs_webhook_delivery WHERE webhook_id = OLD.id;
    END`);
  sql.exec(`CREATE TRIGGER IF NOT EXISTS trg_fs_webhook_enqueue
    AFTER INSERT ON fs_change_feed BEGIN
      INSERT OR IGNORE INTO fs_webhook_delivery
        (webhook_id, seq, type, path, old_path, ino, timestamp, attempts, next_attempt, last_error)
      SELECT id, NEW.seq, NEW.type, NEW.path, NEW.oldPath, NEW.ino, NEW.timestamp, 0, unixepoch(), NULL
      FROM fs_webhook
      WHERE instr(events, ',' || NEW.type || ',') > 0
        AND (
          path_prefix = '/'
          OR NEW.path = path_prefix
          OR substr(NEW.path, 1, length(path_prefix) + 1) = path_prefix || '/'
          OR NEW.oldPath = path_prefix
          OR substr(NEW.oldPath, 1, length(path_prefix) + 1) = path_prefix || '/'
        );
    END`);
}

function normalizePrefix(value: unknown): string {
  if (typeof value !== 'string' || value.includes('\0')) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'pathPrefix must be a path string');
  }
  const segments: string[] = [];
  for (const segment of value.split('/')) {
    if (!segment || segment === '.') continue;
    if (segment === '..') segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join('/')}`;
}

function validateUrl(value: unknown): string {
  if (typeof value !== 'string') throw new HttpError(400, 'INVALID_ARGUMENT', 'url must be an HTTPS URL');
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'url must be an HTTPS URL');
  }
  if (parsed.protocol !== 'https:' || parsed.username || parsed.password) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'url must be an HTTPS URL without embedded credentials');
  }
  return parsed.toString();
}

function validateEvents(value: unknown): WebhookEvent[] {
  if (value === undefined) return [...WEBHOOK_EVENTS];
  if (!Array.isArray(value) || value.length === 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'events must be a non-empty array');
  }
  const events: WebhookEvent[] = [];
  for (const event of value) {
    if (!(WEBHOOK_EVENTS as readonly unknown[]).includes(event)) {
      throw new HttpError(400, 'INVALID_ARGUMENT', `Unknown webhook event: ${String(event)}`);
    }
    if (!events.includes(event as WebhookEvent)) events.push(event as WebhookEvent);
  }
  return events;
}

function randomSecret(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function rowToInfo(row: Record<string, unknown>): WebhookInfo {
  return {
    id: String(row.id),
    url: String(row.url),
    pathPrefix: String(row.path_prefix),
    events: String(row.events).split(',').filter(Boolean) as WebhookEvent[],
    createdAt: Number(row.created_at),
  };
}

export function createWebhook(
  sql: SqlExec,
  input: { url: unknown; pathPrefix?: unknown; events?: unknown }
): CreatedWebhook {
  const id = crypto.randomUUID();
  const url = validateUrl(input.url);
  const pathPrefix = normalizePrefix(input.pathPrefix ?? '/');
  const events = validateEvents(input.events);
  const secret = randomSecret();
  sql.exec(
    `INSERT INTO fs_webhook (id, url, path_prefix, events, secret, created_at)
     VALUES (?, ?, ?, ?, ?, unixepoch())`,
    id, url, pathPrefix, `,${events.join(',')},`, secret,
  );
  return { ...listWebhooks(sql).find((webhook) => webhook.id === id)!, secret };
}

export function listWebhooks(sql: SqlExec): WebhookInfo[] {
  return sql.exec(
    'SELECT id, url, path_prefix, events, created_at FROM fs_webhook ORDER BY created_at, id'
  ).toArray().map(rowToInfo);
}

export function deleteWebhook(sql: SqlExec, id: string): boolean {
  const exists = sql.exec('SELECT id FROM fs_webhook WHERE id = ?', id).toArray().length > 0;
  sql.exec('DELETE FROM fs_webhook WHERE id = ?', id);
  return exists;
}

export function hasPendingWebhookDeliveries(sql: SqlExec): boolean {
  return sql.exec(
    'SELECT 1 AS pending FROM fs_webhook_delivery WHERE next_attempt IS NOT NULL LIMIT 1'
  ).toArray().length > 0;
}

export function nextWebhookDelay(sql: SqlExec): number | null {
  const rows = sql.exec(
    'SELECT min(next_attempt) AS next_attempt FROM fs_webhook_delivery WHERE next_attempt IS NOT NULL'
  ).toArray();
  const next = rows[0]?.next_attempt;
  if (next === null || next === undefined) return null;
  return Math.max(0, Number(next) - Math.floor(Date.now() / 1000));
}

function dueDeliveries(sql: SqlExec): DeliveryRow[] {
  return sql.exec(
    `SELECT d.webhook_id, d.seq, d.attempts, w.url, w.secret,
            d.type, d.path, d.old_path AS oldPath, d.ino, d.timestamp
     FROM fs_webhook_delivery d
     JOIN fs_webhook w ON w.id = d.webhook_id
     WHERE d.next_attempt IS NOT NULL AND d.next_attempt <= unixepoch()
     ORDER BY d.next_attempt, d.seq
     LIMIT ?`,
    MAX_DELIVERIES_PER_RUN,
  ).toArray() as unknown as DeliveryRow[];
}

async function signature(secret: string, body: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  );
  const digest = new Uint8Array(await crypto.subtle.sign('HMAC', key, body));
  return `sha256=${Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}

export async function deliverWebhooks(
  sql: SqlExec,
  volume: string,
  fetchImpl: typeof fetch = fetch,
): Promise<number> {
  const deliveries = dueDeliveries(sql);
  for (const delivery of deliveries) {
    const event: ChangeEvent = {
      seq: Number(delivery.seq),
      type: delivery.type,
      path: String(delivery.path),
      oldPath: delivery.oldPath === null ? null : String(delivery.oldPath),
      ino: Number(delivery.ino),
      timestamp: Number(delivery.timestamp),
    };
    const body = new TextEncoder().encode(JSON.stringify({ volume, event }));
    try {
      const response = await fetchImpl(String(delivery.url), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-AiryFS-Delivery': `${delivery.webhook_id}:${delivery.seq}`,
          'X-AiryFS-Signature': await signature(String(delivery.secret), body),
        },
        body,
        signal: AbortSignal.timeout(10_000),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      sql.exec('DELETE FROM fs_webhook_delivery WHERE webhook_id = ? AND seq = ?', delivery.webhook_id, delivery.seq);
    } catch (error) {
      const attempts = Number(delivery.attempts) + 1;
      const nextAttempt = attempts >= MAX_ATTEMPTS
        ? null
        : Math.floor(Date.now() / 1000) + Math.min(3600, 2 ** attempts);
      sql.exec(
        `UPDATE fs_webhook_delivery
         SET attempts = ?, next_attempt = ?, last_error = ?
         WHERE webhook_id = ? AND seq = ?`,
        attempts, nextAttempt, error instanceof Error ? error.message : String(error),
        delivery.webhook_id, delivery.seq,
      );
    }
  }
  return deliveries.length;
}
