// ABOUTME: Tests immutable SHA-256 asset publication, verification, idempotency, and serving headers.
// ABOUTME: Uses real AgentFS storage to exercise streaming temporary files and atomic rename.

import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { getAsset, putAsset, validateAssetHash } from '../src/assets';
import { VolumeAccessCoordinator } from '../src/files-api';
import { initSchema } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('content-addressed assets', () => {
  let fs: AgentFS;
  const access = new VolumeAccessCoordinator();

  beforeEach(() => {
    const storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    fs = AgentFS.create(storage);
  });

  function hash(bytes: Uint8Array): string {
    return createHash('sha256').update(bytes).digest('hex');
  }

  it('verifies and atomically publishes a streamed asset', async () => {
    const bytes = Uint8Array.from([0, 255, 1, 42]);
    const checksum = hash(bytes);
    const created = await putAsset(fs, access, checksum, new Response(bytes).body);
    expect(created).toEqual({ algorithm: 'sha256', checksum, size: 4, created: true });

    const response = await getAsset(fs, access, checksum, new Request('http://localhost'));
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=31536000, immutable');
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(bytes);
  });

  it('is idempotent and never replaces an existing digest', async () => {
    const bytes = new TextEncoder().encode('same');
    const checksum = hash(bytes);
    expect((await putAsset(fs, access, checksum, new Response(bytes).body)).created).toBe(true);
    expect((await putAsset(fs, access, checksum, new Response(bytes).body)).created).toBe(false);
  });

  it('rejects a digest mismatch and cleans the temporary file', async () => {
    const expected = hash(new TextEncoder().encode('expected'));
    await expect(putAsset(fs, access, expected, new Response('different').body))
      .rejects.toMatchObject({ status: 409, code: 'ASSET_CHECKSUM_MISMATCH' });
    expect(await fs.readdir('/.airyfs/assets/sha256')).toEqual([]);
  });

  it('validates canonical SHA-256 identifiers', () => {
    expect(validateAssetHash('A'.repeat(64))).toBe('a'.repeat(64));
    expect(() => validateAssetHash('nope')).toThrow('64-character');
  });
});
