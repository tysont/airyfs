// ABOUTME: Exercises the path-style S3 adapter over a real in-memory AgentFS volume.
// ABOUTME: Covers object CRUD, listing semantics, and independently generated SigV4 auth.

import { createHash, createHmac } from 'node:crypto';
import Database from 'better-sqlite3';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { beforeEach, describe, expect, it } from 'vitest';
import { VolumeAccessCoordinator } from '../src/files-api';
import { handleS3Request, parseS3Route, verifySigV4 } from '../src/s3';
import { initSchema } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('S3 compatibility', () => {
  let fs: AgentFS;
  const access = new VolumeAccessCoordinator();

  beforeEach(() => {
    const storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    fs = AgentFS.create(storage);
  });

  it('parses path-style bucket and object routes', () => {
    expect(parseS3Route('/s3/bucket')).toEqual({ volume: 'bucket', key: null });
    expect(parseS3Route('/s3/bucket/')).toEqual({ volume: 'bucket', key: null });
    expect(parseS3Route('/s3/my%20bucket/a%20b/c')).toEqual({ volume: 'my bucket', key: 'a b/c' });
    expect(parseS3Route('/v1/volumes/bucket')).toBeNull();
  });

  it('puts, gets, lists, heads, and deletes objects without a Container', async () => {
    const put = await s3(new Request('https://example.com/s3/bucket/dir/file.txt', { method: 'PUT', body: 'hello' }));
    expect(put.status).toBe(200);
    const fetched = await s3(new Request('https://example.com/s3/bucket/dir/file.txt'));
    expect(fetched.status).toBe(200);
    expect(fetched.headers.get('ETag')).toBe(put.headers.get('ETag'));
    expect(await fetched.text()).toBe('hello');
    expect((await s3(new Request('https://example.com/s3/bucket/dir/file.txt', { method: 'HEAD' }))).status).toBe(200);

    await s3(new Request('https://example.com/s3/bucket/dir/second.txt', { method: 'PUT', body: 'two' }));
    const listed = await s3(new Request('https://example.com/s3/bucket?list-type=2&prefix=dir/&delimiter=/'));
    expect(await listed.text()).toContain('<Key>dir/file.txt</Key>');
    const root = await s3(new Request('https://example.com/s3/bucket?list-type=2&delimiter=/'));
    expect(await root.text()).toContain('<CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes>');

    expect((await s3(new Request('https://example.com/s3/bucket/dir/file.txt', { method: 'DELETE' }))).status).toBe(204);
    const missing = await s3(new Request('https://example.com/s3/bucket/dir/file.txt'));
    expect(missing.status).toBe(404);
    expect(await missing.text()).toContain('<Code>NoSuchKey</Code>');
  });

  it('validates payload hashes, rejects ambiguous keys, and preserves overwritten data on failure', async () => {
    await s3(new Request('https://example.com/s3/bucket/existing.txt', { method: 'PUT', body: 'original' }));
    const bad = await s3(new Request('https://example.com/s3/bucket/existing.txt', {
      method: 'PUT', body: 'replacement', headers: { 'x-amz-content-sha256': '0'.repeat(64) },
    }));
    expect(bad.status).toBe(400);
    expect(await bad.text()).toContain('<Code>BadDigest</Code>');
    expect(await (await s3(new Request('https://example.com/s3/bucket/existing.txt'))).text()).toBe('original');

    const nested = await s3(new Request('https://example.com/s3/bucket/new/dir/file.txt', {
      method: 'PUT', body: 'bad', headers: { 'x-amz-content-sha256': '0'.repeat(64) },
    }));
    expect(nested.status).toBe(400);
    await expect(fs.stat('/new')).rejects.toMatchObject({ code: 'ENOENT' });
    expect((await s3(new Request('https://example.com/s3/bucket/a//b', { method: 'PUT', body: 'x' }))).status).toBe(400);
  });

  it('uses consistent UTF-8 ordering across continuation pages', async () => {
    await s3(new Request('https://example.com/s3/bucket/a', { method: 'PUT', body: 'a' }));
    await s3(new Request('https://example.com/s3/bucket/B', { method: 'PUT', body: 'b' }));
    const first = await (await s3(new Request('https://example.com/s3/bucket?list-type=2&max-keys=1'))).text();
    expect(first).toContain('<Key>B</Key>');
    const token = /<NextContinuationToken>([^<]+)<\/NextContinuationToken>/.exec(first)?.[1];
    expect(token).toBeTruthy();
    const second = await (await s3(new Request(`https://example.com/s3/bucket?list-type=2&max-keys=1&continuation-token=${encodeURIComponent(token!)}`))).text();
    expect(second).toContain('<Key>a</Key>');
  });

  it('verifies standard header-based SigV4 and rejects tampering', async () => {
    const timestamp = '20260718T120000Z';
    const request = signedRequest('https://example.com/s3/bucket?list-type=2', 'secret', timestamp);
    await expect(verifySigV4(request, 'secret', Date.UTC(2026, 6, 18, 12))).resolves.toBeUndefined();
    await expect(verifySigV4(request, 'wrong', Date.UTC(2026, 6, 18, 12))).rejects.toMatchObject({ code: 'SignatureDoesNotMatch' });
  });

  async function s3(request: Request): Promise<Response> {
    return handleS3Request({
      request,
      route: parseS3Route(new URL(request.url).pathname)!,
      fs,
      access,
      onMutation: async () => undefined,
      versionForInode: () => 0,
    });
  }
});

function signedRequest(urlText: string, secret: string, timestamp: string): Request {
  const url = new URL(urlText);
  const payloadHash = createHash('sha256').update('').digest('hex');
  const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';
  const canonicalQuery = [...url.searchParams.entries()]
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`).sort().join('&');
  const canonicalHeaders = `host:${url.host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${timestamp}\n`;
  const canonicalRequest = `GET\n${url.pathname}\n${canonicalQuery}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`;
  const date = timestamp.slice(0, 8);
  const scope = `${date}/auto/s3/aws4_request`;
  const stringToSign = `AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${createHash('sha256').update(canonicalRequest).digest('hex')}`;
  const dateKey = createHmac('sha256', `AWS4${secret}`).update(date).digest();
  const regionKey = createHmac('sha256', dateKey).update('auto').digest();
  const serviceKey = createHmac('sha256', regionKey).update('s3').digest();
  const signingKey = createHmac('sha256', serviceKey).update('aws4_request').digest();
  const signature = createHmac('sha256', signingKey).update(stringToSign).digest('hex');
  return new Request(url, { headers: {
    'x-amz-date': timestamp,
    'x-amz-content-sha256': payloadHash,
    Authorization: `AWS4-HMAC-SHA256 Credential=airyfs/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
  } });
}
