// ABOUTME: Verifies API URL construction, streaming bodies, and error normalization.
// ABOUTME: Uses fetch stubs to test the client without a deployed Worker.

import { describe, expect, it, vi } from 'vitest';
import { AiryFSClient } from '../src/api/client.js';
import { AiryFSApiError, AiryFSTransportError, responseError } from '../src/api/errors.js';
import { encodeRemotePath, resolveRemotePath } from '../src/api/paths.js';

describe('remote paths', () => {
  it('resolves relative paths without escaping the volume root', () => {
    expect(resolveRemotePath('/a/b', '../c')).toBe('/a/c');
    expect(resolveRemotePath('/', '../../../../etc')).toBe('/etc');
  });

  it('encodes each path segment independently', () => {
    expect(encodeRemotePath('/hello world/a#b')).toBe('hello%20world/a%23b');
    expect(encodeRemotePath('/')).toBe('');
  });
});

describe('responseError', () => {
  it('parses structured Worker errors', async () => {
    const error = await responseError(Response.json({
      error: { code: 'ENOENT', message: 'missing', path: '/nope' },
    }, { status: 404 }));

    expect(error).toMatchObject({ status: 404, code: 'ENOENT', message: 'missing', path: '/nope' });
  });

  it('falls back to legacy plain-text errors', async () => {
    const error = await responseError(new Response('Not found', { status: 404 }));
    expect(error).toMatchObject({ status: 404, code: 'HTTP_404', message: 'Not found' });
  });

  it('summarizes HTML gateway errors instead of printing the entire page', async () => {
    const error = await responseError(new Response(
      '<!DOCTYPE html><html><head><title>502: Bad gateway</title></head><body>large page</body></html>',
      { status: 502, headers: { 'Content-Type': 'text/html' } },
    ));

    expect(error).toMatchObject({ status: 502, code: 'HTTP_502' });
    expect(error.message).toContain('Bad gateway');
    expect(error.message).not.toContain('<!DOCTYPE');
  });

  it('recognizes HTML gateway errors without a doctype or content type', async () => {
    const error = await responseError(new Response(
      '<html><head><title>503: Service unavailable</title></head><body>large page</body></html>',
      { status: 503 },
    ));

    expect(error.message).toContain('Service unavailable');
    expect(error.message).not.toContain('<html>');
  });

  it('recognizes case-insensitive HTML content types', async () => {
    const error = await responseError(new Response(
      '<!-- proxy --><html><head><title>Bad gateway</title></head></html>',
      { status: 502, headers: { 'Content-Type': 'Text/HTML' } },
    ));

    expect(error.message).not.toContain('<!-- proxy -->');
  });

  it('bounds large gateway error bodies', async () => {
    const error = await responseError(new Response('x'.repeat(128 * 1024), { status: 502 }));

    expect(error.message).toHaveLength(64 * 1024);
  });
});

describe('AiryFSClient', () => {
  it('uses encoded v1 paths for filesystem operations', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json([]));
    const client = new AiryFSClient('https://example.com', 'my volume', fetchMock);

    await client.listDirectory('/hello world');

    expect(fetchMock).toHaveBeenCalledWith(
      new URL('https://example.com/v1/volumes/my%20volume/directories/hello%20world'),
      undefined,
    );
  });

  it('sends operation bodies as JSON', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.rename('/from', '/to');

    const [, init] = fetchMock.mock.calls[0];
    expect(init).toMatchObject({ method: 'POST', body: JSON.stringify({ from: '/from', to: '/to' }) });
  });

  it('adds the volume query parameter for diagnostic routes', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ pipelineRequests: 1, sqlStatements: 2 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.perf();

    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/perf?volume=vol');
  });

  it('throws a typed error for unsuccessful responses', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      error: { code: 'EXEC_BUSY', message: 'busy' },
    }, { status: 503 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await expect(client.exec('true')).rejects.toBeInstanceOf(AiryFSApiError);
  });

  it('throws a typed error when fetch fails before receiving a response', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error('socket closed'));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await expect(client.exec('true')).rejects.toMatchObject({
      name: 'AiryFSTransportError',
      origin: 'https://example.com',
      message: 'Could not reach https://example.com: socket closed',
    } satisfies Partial<AiryFSTransportError>);
  });
});
