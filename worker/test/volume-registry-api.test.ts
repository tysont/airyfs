// ABOUTME: Exercises the deployment-wide volume-list endpoint and root-auth boundary.
// ABOUTME: Keeps registry routing tests independent of the Workers runtime harness.

import { describe, expect, it, vi } from 'vitest';
import { handleVolumeRegistryRequest } from '../src/volume-registry-api';

const records = [{ name: 'project', chunkSize: 262144, createdAt: 1_700_000_000 }];
const page = { volumes: records, nextCursor: null };

describe('volume registry API', () => {
  it('lists registered volumes', async () => {
    const list = vi.fn().mockResolvedValue(page);
    const response = await handleVolumeRegistryRequest(
      new Request('https://example.com/v1/volumes'),
      undefined,
      list,
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(page);
    expect(list).toHaveBeenCalledOnce();
    expect(list).toHaveBeenCalledWith('', 100);
  });

  it('requires the deployment root credential when auth is enabled', async () => {
    const list = vi.fn().mockResolvedValue(page);
    const denied = await handleVolumeRegistryRequest(
      new Request('https://example.com/v1/volumes'),
      'root-secret',
      list,
    );
    expect(denied.status).toBe(401);

    const allowed = await handleVolumeRegistryRequest(new Request('https://example.com/v1/volumes', {
      headers: { Authorization: 'Bearer root-secret' },
    }), 'root-secret', list);
    expect(allowed.status).toBe(200);
  });

  it('rejects unsupported methods', async () => {
    const response = await handleVolumeRegistryRequest(
      new Request('https://example.com/v1/volumes', { method: 'POST' }),
      undefined,
      async () => page,
    );
    expect(response.status).toBe(405);
    expect(response.headers.get('Allow')).toBe('GET');
  });

  it('validates page limits', async () => {
    const response = await handleVolumeRegistryRequest(
      new Request('https://example.com/v1/volumes?limit=1001'),
      undefined,
      async () => page,
    );
    expect(response.status).toBe(400);
  });
});
