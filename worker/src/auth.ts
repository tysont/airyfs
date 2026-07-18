// ABOUTME: Scoped capability tokens and root-credential authentication for the AiryFS Worker.
// ABOUTME: Signs/verifies HMAC-SHA256 capabilities, authorizes operations, and persists revocations.

import { HttpError } from './files-api';
import type { SqlExec } from './schema';

export const OPERATIONS = ['read', 'write', 'exec', 'admin'] as const;
export type Operation = (typeof OPERATIONS)[number];

export interface Capability {
  /** Stable identifier used for revocation. */
  id: string;
  /** Exact volume this capability may access. */
  volume: string;
  /** Operations this capability grants; `admin` implies all others. */
  operations: Operation[];
  /** Allowed path prefixes; an empty list means every path. */
  pathPrefixes: string[];
  /** Absolute expiry in Unix seconds. */
  expires: number;
}

export type Identity =
  | { kind: 'disabled' }
  | { kind: 'root' }
  | { kind: 'capability'; capability: Capability };

/** What a single request needs: a required operation (null = authentication only) and the paths it touches. */
export interface AccessRequirement {
  operation: Operation | null;
  paths: string[];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function nowSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64UrlDecode(value: string): Uint8Array {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

async function hmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify']
  );
}

/** Constant-time equality via fixed-length SHA-256 digest comparison. */
async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const [ha, hb] = await Promise.all([
    crypto.subtle.digest('SHA-256', encoder.encode(a)),
    crypto.subtle.digest('SHA-256', encoder.encode(b)),
  ]);
  const va = new Uint8Array(ha);
  const vb = new Uint8Array(hb);
  let diff = 0;
  for (let i = 0; i < va.length; i++) diff |= va[i] ^ vb[i];
  return diff === 0;
}

// ---------------------------------------------------------------------------
// Path normalization and authorization
// ---------------------------------------------------------------------------

/** Resolve `.`/`..` and redundant separators to a safe absolute path that cannot escape the root. */
export function normalizePath(path: string): string {
  const segments: string[] = [];
  for (const raw of path.split('/')) {
    if (raw === '' || raw === '.') continue;
    if (raw === '..') {
      segments.pop();
      continue;
    }
    segments.push(raw);
  }
  return `/${segments.join('/')}`;
}

function prefixMatches(prefix: string, path: string): boolean {
  const p = normalizePath(prefix);
  const target = normalizePath(path);
  if (p === '/') return true;
  return target === p || target.startsWith(`${p}/`);
}

/** True when a capability grants `operation` on every path in `paths`. */
export function capabilityAllows(
  capability: Capability,
  operation: Operation,
  paths: string[]
): boolean {
  if (capability.operations.includes('admin')) return true;
  if (!capability.operations.includes(operation)) return false;
  if (capability.pathPrefixes.length === 0) return true;
  return paths.every((path) =>
    capability.pathPrefixes.some((prefix) => prefixMatches(prefix, path))
  );
}

// ---------------------------------------------------------------------------
// Signing and verification
// ---------------------------------------------------------------------------

function isOperation(value: unknown): value is Operation {
  return typeof value === 'string' && (OPERATIONS as readonly string[]).includes(value);
}

function isCapability(value: unknown): value is Capability {
  if (typeof value !== 'object' || value === null) return false;
  const record = value as Record<string, unknown>;
  return (
    typeof record.id === 'string' &&
    typeof record.volume === 'string' &&
    Array.isArray(record.operations) &&
    record.operations.length > 0 &&
    record.operations.every(isOperation) &&
    Array.isArray(record.pathPrefixes) &&
    record.pathPrefixes.every((entry) => typeof entry === 'string') &&
    typeof record.expires === 'number' &&
    Number.isFinite(record.expires)
  );
}

/** Mint a signed capability token. The returned token is the only copy of the credential. */
export async function signCapability(secret: string, capability: Capability): Promise<string> {
  const payload = encoder.encode(JSON.stringify(capability));
  const key = await hmacKey(secret);
  const signature = new Uint8Array(await crypto.subtle.sign('HMAC', key, payload));
  return `${base64UrlEncode(payload)}.${base64UrlEncode(signature)}`;
}

export class CapabilityError extends Error {}

/** Verify a token's signature and shape. Does not check expiry, volume, or revocation. */
export async function verifyCapability(secret: string, token: string): Promise<Capability> {
  const dot = token.indexOf('.');
  if (dot <= 0 || dot === token.length - 1) {
    throw new CapabilityError('Malformed capability token');
  }
  let payload: Uint8Array;
  let signature: Uint8Array;
  try {
    payload = base64UrlDecode(token.slice(0, dot));
    signature = base64UrlDecode(token.slice(dot + 1));
  } catch {
    throw new CapabilityError('Malformed capability token');
  }

  const key = await hmacKey(secret);
  const valid = await crypto.subtle.verify('HMAC', key, signature, payload);
  if (!valid) throw new CapabilityError('Invalid capability signature');

  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(payload));
  } catch {
    throw new CapabilityError('Malformed capability payload');
  }
  if (!isCapability(parsed)) throw new CapabilityError('Malformed capability payload');
  return parsed;
}

// ---------------------------------------------------------------------------
// Request authentication
// ---------------------------------------------------------------------------

function parseBearer(header: string | null): string | null {
  if (!header) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  return match ? match[1].trim() : null;
}

/**
 * Resolve the identity of a request against the configured secret. Throws an
 * HttpError with an appropriate status when the credential is missing or invalid.
 */
export async function authenticate(
  secret: string,
  authorization: string | null,
  volume: string
): Promise<Identity> {
  const token = parseBearer(authorization);
  if (!token) {
    throw new HttpError(401, 'UNAUTHENTICATED', 'Missing bearer credential', {
      'WWW-Authenticate': 'Bearer',
    });
  }

  if (await timingSafeEqual(token, secret)) return { kind: 'root' };

  let capability: Capability;
  try {
    capability = await verifyCapability(secret, token);
  } catch (error) {
    throw new HttpError(401, 'INVALID_TOKEN', error instanceof Error ? error.message : 'Invalid token');
  }

  if (capability.volume !== volume) {
    throw new HttpError(403, 'WRONG_VOLUME', `Capability is scoped to volume "${capability.volume}"`);
  }
  if (capability.expires <= nowSeconds()) {
    throw new HttpError(401, 'TOKEN_EXPIRED', 'Capability token has expired');
  }
  return { kind: 'capability', capability };
}

/** Build a capability from a validated mint request. */
export function buildCapability(
  volume: string,
  operations: Operation[],
  pathPrefixes: string[],
  expiresInSeconds: number
): Capability {
  return {
    id: crypto.randomUUID(),
    volume,
    operations,
    pathPrefixes: pathPrefixes.map(normalizePath),
    expires: nowSeconds() + expiresInSeconds,
  };
}

// ---------------------------------------------------------------------------
// Revocation (persisted per volume in the DO's SQLite)
// ---------------------------------------------------------------------------

export function isCapabilityRevoked(sql: SqlExec, id: string): boolean {
  return sql.exec('SELECT 1 FROM capability_revocations WHERE id = ?', id).toArray().length > 0;
}

export function revokeCapability(sql: SqlExec, id: string): void {
  sql.exec(
    'INSERT OR IGNORE INTO capability_revocations (id, revoked_at) VALUES (?, unixepoch())',
    id
  );
}
