// ABOUTME: Unit tests for capability signing, verification, authorization, and revocation.
// ABOUTME: Exercises WebCrypto HMAC round-trips and the per-volume revocation table.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  authenticate,
  buildCapability,
  capabilityAllows,
  hashPassword,
  isCapabilityRevoked,
  normalizePath,
  readPasswordRecord,
  revokeCapability,
  signCapability,
  verifyCapability,
  verifyPassword,
  writePasswordRecord,
  type Capability,
} from '../src/auth';
import { HttpError } from '../src/files-api';
import { initSchema, type SqlExec } from '../src/schema';

const SECRET = 'super-secret-value';

function capability(overrides: Partial<Capability> = {}): Capability {
  return {
    id: 'cap-1',
    volume: 'vol',
    operations: ['read'],
    pathPrefixes: [],
    expires: Math.floor(Date.now() / 1000) + 3600,
    ...overrides,
  };
}

describe('capability tokens', () => {
  it('round-trips a signed capability through verification', async () => {
    const cap = capability({ operations: ['read', 'write'], pathPrefixes: ['/src'] });
    const token = await signCapability(SECRET, cap);
    expect(await verifyCapability(SECRET, token)).toEqual(cap);
  });

  it('rejects a tampered payload', async () => {
    const token = await signCapability(SECRET, capability());
    const [payload, signature] = token.split('.');
    const forged = `${payload}x.${signature}`;
    await expect(verifyCapability(SECRET, forged)).rejects.toThrow();
  });

  it('rejects a token signed with a different secret', async () => {
    const token = await signCapability('other-secret', capability());
    await expect(verifyCapability(SECRET, token)).rejects.toThrow('Invalid capability signature');
  });

  it('rejects malformed tokens', async () => {
    await expect(verifyCapability(SECRET, 'not-a-token')).rejects.toThrow('Malformed capability token');
  });
});

describe('normalizePath', () => {
  it('resolves dot segments and cannot escape the root', () => {
    expect(normalizePath('/a/b/../c')).toBe('/a/c');
    expect(normalizePath('a//b/')).toBe('/a/b');
    expect(normalizePath('/../../etc')).toBe('/etc');
    expect(normalizePath('/')).toBe('/');
  });
});

describe('capabilityAllows', () => {
  it('grants everything to an admin capability', () => {
    const cap = capability({ operations: ['admin'], pathPrefixes: ['/only'] });
    expect(capabilityAllows(cap, 'write', ['/anywhere'])).toBe(true);
    expect(capabilityAllows(cap, 'exec', ['/'])).toBe(true);
  });

  it('requires the exact operation to be granted', () => {
    const cap = capability({ operations: ['read'] });
    expect(capabilityAllows(cap, 'read', ['/a'])).toBe(true);
    expect(capabilityAllows(cap, 'write', ['/a'])).toBe(false);
  });

  it('keeps application SQL separate from filesystem permissions', () => {
    expect(capabilityAllows(capability({ operations: ['sql'] }), 'sql', [])).toBe(true);
    expect(capabilityAllows(capability({ operations: ['read', 'write'] }), 'sql', [])).toBe(false);
  });

  it('treats empty prefixes as all paths', () => {
    const cap = capability({ operations: ['write'], pathPrefixes: [] });
    expect(capabilityAllows(cap, 'write', ['/anything/at/all'])).toBe(true);
  });

  it('enforces path prefixes across every operand', () => {
    const cap = capability({ operations: ['write'], pathPrefixes: ['/src'] });
    expect(capabilityAllows(cap, 'write', ['/src/a.txt'])).toBe(true);
    expect(capabilityAllows(cap, 'write', ['/src', '/src/deep/b'])).toBe(true);
    expect(capabilityAllows(cap, 'write', ['/src/a', '/other'])).toBe(false);
    // A sibling that merely shares a prefix string is not covered.
    expect(capabilityAllows(cap, 'write', ['/srcabc'])).toBe(false);
  });

  it('scopes uploads to write and checksum to read on the target path', () => {
    // Uploads (POST/PATCH/PUT/DELETE) require write on the route.path target.
    const writer = capability({ operations: ['write'], pathPrefixes: ['/data'] });
    expect(capabilityAllows(writer, 'write', ['/data/big.bin'])).toBe(true);
    expect(capabilityAllows(writer, 'write', ['/other/big.bin'])).toBe(false);
    // A write-only capability cannot run the read-scoped checksum operation.
    expect(capabilityAllows(writer, 'read', ['/data/big.bin'])).toBe(false);

    // Checksum maps to read on the operand path.
    const reader = capability({ operations: ['read'], pathPrefixes: ['/data'] });
    expect(capabilityAllows(reader, 'read', ['/data/big.bin'])).toBe(true);
    expect(capabilityAllows(reader, 'read', ['/elsewhere.bin'])).toBe(false);
    // A read-only capability cannot begin/append/complete an upload.
    expect(capabilityAllows(reader, 'write', ['/data/big.bin'])).toBe(false);
  });
});

describe('authenticate', () => {
  it('accepts the root credential', async () => {
    const identity = await authenticate(SECRET, `Bearer ${SECRET}`, 'vol');
    expect(identity).toEqual({ kind: 'root' });
  });

  it('accepts a valid capability for the matching volume', async () => {
    const token = await signCapability(SECRET, capability());
    const identity = await authenticate(SECRET, `Bearer ${token}`, 'vol');
    expect(identity.kind).toBe('capability');
  });

  it('rejects a missing credential with 401', async () => {
    await expect(authenticate(SECRET, null, 'vol')).rejects.toMatchObject({ status: 401, code: 'UNAUTHENTICATED' });
  });

  it('rejects a capability scoped to another volume with 403', async () => {
    const token = await signCapability(SECRET, capability({ volume: 'other' }));
    await expect(authenticate(SECRET, `Bearer ${token}`, 'vol')).rejects.toMatchObject({
      status: 403,
      code: 'WRONG_VOLUME',
    });
  });

  it('rejects an expired capability with 401', async () => {
    const token = await signCapability(SECRET, capability({ expires: Math.floor(Date.now() / 1000) - 5 }));
    await expect(authenticate(SECRET, `Bearer ${token}`, 'vol')).rejects.toMatchObject({
      status: 401,
      code: 'TOKEN_EXPIRED',
    });
  });

  it('surfaces authentication failures as HttpError', async () => {
    await expect(authenticate(SECRET, 'Bearer garbage', 'vol')).rejects.toBeInstanceOf(HttpError);
  });
});

describe('buildCapability', () => {
  it('assigns an id, normalizes prefixes, and sets an absolute expiry', () => {
    const before = Math.floor(Date.now() / 1000);
    const cap = buildCapability('vol', ['read', 'write'], ['/a/../b'], 600);
    expect(cap.id).toMatch(/[0-9a-f-]{36}/);
    expect(cap.pathPrefixes).toEqual(['/b']);
    expect(cap.expires).toBeGreaterThanOrEqual(before + 600);
  });
});

describe('revocation table', () => {
  let sql: SqlExec;

  beforeEach(() => {
    const db = new Database(':memory:');
    db.function('unixepoch', () => Math.floor(Date.now() / 1000));
    sql = {
      exec(query: string, ...bindings: unknown[]) {
        const stmt = db.prepare(query);
        if (stmt.reader) {
          const rows = stmt.all(...bindings) as Record<string, unknown>[];
          return { toArray: () => rows };
        }
        stmt.run(...bindings);
        return { toArray: () => [] };
      },
    };
    initSchema(sql);
  });

  it('records and detects revoked capabilities', () => {
    expect(isCapabilityRevoked(sql, 'cap-1')).toBe(false);
    revokeCapability(sql, 'cap-1');
    expect(isCapabilityRevoked(sql, 'cap-1')).toBe(true);
  });

  it('is idempotent for repeated revocations', () => {
    revokeCapability(sql, 'cap-1');
    revokeCapability(sql, 'cap-1');
    expect(isCapabilityRevoked(sql, 'cap-1')).toBe(true);
  });

  it('stores and rotates a per-volume password verifier without the plaintext', async () => {
    expect(readPasswordRecord(sql)).toBeNull();

    writePasswordRecord(sql, await hashPassword('correct horse'));
    const stored = readPasswordRecord(sql);
    expect(stored).not.toBeNull();
    expect(JSON.stringify(stored)).not.toContain('correct horse');
    expect(await verifyPassword('correct horse', stored!)).toBe(true);
    expect(await verifyPassword('wrong', stored!)).toBe(false);

    writePasswordRecord(sql, await hashPassword('new-password'));
    const rotated = readPasswordRecord(sql);
    expect(await verifyPassword('new-password', rotated!)).toBe(true);
    expect(await verifyPassword('correct horse', rotated!)).toBe(false);
  });
});

describe('per-volume capability isolation', () => {
  it('rejects a token minted for one volume when checked against another', async () => {
    const token = await signCapability(SECRET, capability({ volume: 'alpha', operations: ['read'] }));
    // Same deployment secret, different volume in the request: authentication fails.
    await expect(authenticate(SECRET, `Bearer ${token}`, 'beta')).rejects.toThrow(HttpError);
    // The correct volume still authenticates.
    const identity = await authenticate(SECRET, `Bearer ${token}`, 'alpha');
    expect(identity.kind).toBe('capability');
  });
});
