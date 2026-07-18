// ABOUTME: Stores short-lived single-use tickets for browser-compatible PTY WebSocket upgrades.
// ABOUTME: Keeps long-lived bearer credentials out of WebSocket URLs and rejects replay.

import type { SqlExec } from './schema';

export const PTY_TICKET_TABLES = ['pty_ticket'] as const;
const TICKET_TTL_SECONDS = 30;

export function initPtyTicketSchema(sql: SqlExec): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS pty_ticket (
    token TEXT PRIMARY KEY,
    expires_at INTEGER NOT NULL
  )`);
  sql.exec('CREATE INDEX IF NOT EXISTS idx_pty_ticket_expires ON pty_ticket(expires_at)');
}

export function createPtyTicket(sql: SqlExec): { ticket: string; expiresAt: number } {
  const ticket = crypto.randomUUID();
  const expiresAt = Math.floor(Date.now() / 1000) + TICKET_TTL_SECONDS;
  sql.exec('DELETE FROM pty_ticket WHERE expires_at <= unixepoch()');
  sql.exec('INSERT INTO pty_ticket (token, expires_at) VALUES (?, ?)', ticket, expiresAt);
  return { ticket, expiresAt };
}

export function consumePtyTicket(sql: SqlExec, ticket: string): boolean {
  const rows = sql.exec(
    'DELETE FROM pty_ticket WHERE token = ? AND expires_at > unixepoch() RETURNING token', ticket,
  ).toArray();
  return rows.length === 1;
}
