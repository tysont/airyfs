// ABOUTME: Verifies API URL construction, streaming bodies, and error normalization.
// ABOUTME: Uses fetch stubs to test the client without a deployed Worker.

import { describe, expect, it, vi } from 'vitest';
import { AiryFSClient } from '../src/api/client.js';
import { AiryFSApiError, AiryFSTransportError, responseError } from '../src/api/errors.js';
import { encodeRemotePath, resolveRemotePath } from '../src/api/paths.js';

function commandJob(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'abc', idempotencyKey: 'key', command: 'echo hi', cwd: '/', status: 'succeeded',
    execId: 'abc', exitCode: 0, error: null, cancelRequested: false, outputBytes: 2,
    outputTruncated: false, createdAt: 1, updatedAt: 2, startedAt: 1, finishedAt: 2,
    ...overrides,
  };
}

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

  it('deletes a volume with a DELETE to the volume base', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ deleted: true }));
    const client = new AiryFSClient('https://example.com', 'my volume', fetchMock);

    expect(await client.deleteVolume()).toEqual({ deleted: true });
    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/v1/volumes/my%20volume');
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: 'DELETE' });
  });

  it('sends operation bodies as JSON', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.rename('/from', '/to');

    const [, init] = fetchMock.mock.calls[0];
    expect(init).toMatchObject({ method: 'POST', body: JSON.stringify({ from: '/from', to: '/to' }) });
  });

  it('patches a file range via PATCH and returns the bytes written', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(null, { status: 204, headers: { 'X-AiryFS-Bytes-Written': '4' } }),
    );
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    expect(await client.writeFileRange('/patch.bin', 3, 'BBBB')).toBe(4);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/files/patch.bin?offset=3');
    expect(init).toMatchObject({ method: 'PATCH', body: 'BBBB' });
  });

  it('base64-encodes stdin on transient exec requests', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({ commandId: 'run', exitCode: 0, stdout: 'hi', stderr: '' }),
    );
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.execTransient('cat', { stdin: 'hi' });

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(String(init?.body))).toEqual({ command: 'cat', stdin: btoa('hi') });
  });

  it('creates, lists, and deletes mounts on the mounts resource', async () => {
    const fetchMock = vi.fn<typeof fetch>(async (url, init) => {
      if ((init?.method ?? 'GET') === 'GET') return Response.json({ volume: 'vol', mounts: [] });
      return Response.json({
        mountpoint: '/data', targetVolume: 'big', targetSubpath: '/', credentialId: null, options: {}, createdAt: 1,
        removed: init?.method === 'DELETE' ? true : undefined,
      });
    });
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.listMounts();
    await client.createMount('/data', { target: 'big', subpath: '/', create: true });
    await client.deleteMount('/data');

    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/v1/volumes/vol/mounts');
    const [createUrl, createInit] = fetchMock.mock.calls[1];
    expect(createUrl.toString()).toBe('https://example.com/v1/volumes/vol/mounts/data');
    expect(createInit).toMatchObject({
      method: 'PUT',
      body: JSON.stringify({ target: 'big', subpath: '/', create: true }),
    });
    expect(fetchMock.mock.calls[2][1]).toMatchObject({ method: 'DELETE' });
  });

  it('sends direct filesystem primitive operations without exec', async () => {
    const fetchMock = vi.fn<typeof fetch>(async (url) => {
      if (url.toString().endsWith('/lstat')) return Response.json({ type: 'file' });
      if (url.toString().endsWith('/du')) return Response.json({ bytes: 3, inodes: 1 });
      return new Response(null, { status: 204 });
    });
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.lstat('/file');
    await client.touch('/file', { mtime: 42 });
    await client.chmod('/file', 0o640);
    await client.link('/file', '/linked');
    await client.appendFile('/file', Uint8Array.from([0, 255]));
    await client.diskUsage('/');

    expect(fetchMock.mock.calls.map(([url]) => url.toString())).toEqual([
      'https://example.com/v1/volumes/vol/operations/lstat',
      'https://example.com/v1/volumes/vol/operations/touch',
      'https://example.com/v1/volumes/vol/operations/chmod',
      'https://example.com/v1/volumes/vol/operations/link',
      'https://example.com/v1/volumes/vol/operations/append',
      'https://example.com/v1/volumes/vol/operations/du',
    ]);
    expect(fetchMock.mock.calls[4][1]?.body).toBe(JSON.stringify({ path: '/file', data: 'AP8=' }));
  });

  it('adds the volume query parameter for diagnostic routes', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ sessionId: 'session-1', sessionEpoch: 1, pipelineRequests: 1, sqlStatements: 2 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.perf();

    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/perf?volume=vol');
  });

  it('reads Prometheus metrics from the volume resource', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response('airyfs_container_up 1\n'));
    const client = new AiryFSClient('https://example.com', 'my volume', fetchMock);

    expect(await client.metrics()).toBe('airyfs_container_up 1\n');
    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/v1/volumes/my%20volume/metrics');
  });

  it('reads paginated usage history from the volume resource', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ samples: [], next: null }));
    const client = new AiryFSClient('https://example.com', 'my volume', fetchMock);

    expect(await client.usageHistory({ before: 42, limit: 10 })).toEqual({ samples: [], next: null });
    expect(fetchMock.mock.calls[0][0].toString()).toBe(
      'https://example.com/v1/volumes/my%20volume/usage-history?before=42&limit=10',
    );
  });

  it('builds encoded tree URLs and forwards the bearer token', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 200 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock, 'tok');

    await client.exportTree('/my dir/sub');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/trees/my%20dir/sub');
    expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer tok');
  });

  it('sets replace=true and streams the import body', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({ files: 1, directories: 0, symlinks: 0, bytes: 3 }),
    );
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const summary = await client.importTree('/app', new Uint8Array([1, 2, 3]), true);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/trees/app?replace=true');
    expect(init).toMatchObject({ method: 'PUT', duplex: 'half' });
    expect(summary).toMatchObject({ files: 1, bytes: 3 });
  });

  it('throws a typed error for unsuccessful responses', async () => {
    const fetchMock = vi.fn<typeof fetch>(async () => Response.json({
      error: { code: 'UNAVAILABLE', message: 'busy' },
    }, { status: 503 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await expect(client.exec('true')).rejects.toBeInstanceOf(AiryFSApiError);
  });

  it('streams durable exec events from persisted job state', async () => {
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') return Response.json(commandJob({ status: 'running', exitCode: null }));
      if (url.includes('/logs')) return Response.json(url.includes('after=')
        ? { entries: [], next: null }
        : { entries: [{ seq: 0, stream: 'stdout', data: 'aGk=', timestamp: 1 }], next: null });
      return Response.json(commandJob({ status: 'failed', exitCode: 3 }));
    });
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const events = [];
    for await (const event of await client.execStream('echo hi')) events.push(event);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/jobs');
    expect(init).toMatchObject({ method: 'POST', body: JSON.stringify({ command: 'echo hi', cwd: '/' }) });
    expect(events).toEqual([
      { type: 'start', id: 'abc' },
      { type: 'stdout', id: 'abc', data: 'aGk=' },
      { type: 'exit', id: 'abc', exitCode: 3 },
    ]);
  });

  it('drains every durable log page before returning the terminal result', async () => {
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') {
        return Response.json(commandJob({ status: 'running', exitCode: null }));
      }
      if (url.endsWith('/logs')) return Response.json({
        entries: [{ seq: 0, stream: 'stdout', data: 'YQ==', timestamp: 1 }], next: 0,
      });
      if (url.endsWith('/logs?after=0')) return Response.json({
        entries: [{ seq: 1, stream: 'stdout', data: 'Yg==', timestamp: 2 }], next: null,
      });
      if (url.endsWith('/logs?after=1')) return Response.json({ entries: [], next: null });
      return Response.json(commandJob({ outputTruncated: true }));
    });
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await expect(client.exec('printf ab')).resolves.toEqual({
      commandId: 'abc', exitCode: 0, stdout: 'ab', stderr: '', outputTruncated: true,
    });
  });

  it('throws before streaming when durable submission is rejected', async () => {
    const fetchMock = vi.fn<typeof fetch>(async () => Response.json({
      error: { code: 'EXEC_BUSY', message: 'busy' },
    }, { status: 409 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await expect(client.execStream('true')).rejects.toBeInstanceOf(AiryFSApiError);
  });

  it('posts a durable command id to the job cancel route', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(commandJob({ status: 'canceled' })));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.cancelExec('run-42');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/jobs/run-42/cancel');
    expect(init).toMatchObject({ method: 'POST' });
  });

  it('builds an encoded long-poll change-feed request', async () => {
    const page = { events: [], cursor: 42, latest: 42, oldest: 1, gap: false };
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(page));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);
    const signal = new AbortController().signal;

    expect(await client.getChanges({
      since: 41,
      limit: 25,
      path: '/my dir',
      wait: 1000,
      signal,
    })).toEqual(page);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe(
      'https://example.com/v1/volumes/vol/changes/my%20dir?since=41&limit=25&wait=1000',
    );
    expect(init?.signal).toBe(signal);
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

describe('AiryFSClient snapshots', () => {
  const info = {
    id: 'sid', name: 'nightly', note: null, createdAt: 1, chunkSize: 262144,
    inodeCount: 3, fileCount: 1, directoryCount: 2, symlinkCount: 0, byteCount: 5,
  };

  it('creates a snapshot with an optional name and note', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(info, { status: 201 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const created = await client.createSnapshot('nightly', 'before refactor');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/snapshots');
    expect(init).toMatchObject({ method: 'POST' });
    expect(JSON.parse(String(init?.body))).toEqual({ name: 'nightly', note: 'before refactor' });
    expect(created).toMatchObject({ id: 'sid', directoryCount: 2 });
  });

  it('omits name and note from the body when not provided', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(info, { status: 201 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.createSnapshot();

    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({});
  });

  it('lists snapshots', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json([info]));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const list = await client.listSnapshots();

    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/v1/volumes/vol/snapshots');
    expect(list).toHaveLength(1);
  });

  it('diffs against live by default and against another snapshot when asked', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async () => Response.json([]));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.diffSnapshot('sid');
    expect(fetchMock.mock.calls[0][0].toString())
      .toBe('https://example.com/v1/volumes/vol/snapshots/sid/diff?against=live');

    await client.diffSnapshot('sid', 'other');
    expect(fetchMock.mock.calls[1][0].toString())
      .toBe('https://example.com/v1/volumes/vol/snapshots/sid/diff?against=other');
  });

  it('restores a snapshot', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(info));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.restoreSnapshot('sid');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/snapshots/sid/restore');
    expect(init).toMatchObject({ method: 'POST' });
  });

  it('clones a snapshot into a target volume', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({ files: 1, directories: 2, symlinks: 0, bytes: 5 }),
    );
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const summary = await client.cloneSnapshot('sid', 'backup');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/snapshots/sid/clone');
    expect(init).toMatchObject({ method: 'POST' });
    expect(JSON.parse(String(init?.body))).toEqual({ targetVolume: 'backup' });
    expect(summary).toMatchObject({ files: 1 });
  });

  it('deletes a snapshot', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(info));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.deleteSnapshot('sid');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/snapshots/sid');
    expect(init).toMatchObject({ method: 'DELETE' });
  });

  it('encodes ids in snapshot item routes', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(info));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.deleteSnapshot('a b');
    expect(fetchMock.mock.calls[0][0].toString())
      .toBe('https://example.com/v1/volumes/vol/snapshots/a%20b');
  });
});

describe('AiryFSClient uploads and checksum', () => {
  const status = {
    id: 'uid', path: '/data/big.bin', size: 10, offset: 0,
    checksum: 'a'.repeat(64), createdAt: 1, updatedAt: 1,
  };

  it('begins an upload with size and checksum at the target path', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(status, { status: 201 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.beginUpload('/data/big.bin', 10, 'a'.repeat(64));

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/uploads/data/big.bin');
    expect(init).toMatchObject({ method: 'POST' });
    expect(JSON.parse(String(init?.body))).toEqual({ size: 10, checksum: 'a'.repeat(64) });
  });

  it('gets upload status', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(status));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await client.uploadStatus('/data/big.bin');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/uploads/data/big.bin');
    expect(init?.method ?? 'GET').toBe('GET');
  });

  it('appends a chunk with offset and per-chunk checksum headers', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ ...status, offset: 4 }));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const data = new Uint8Array([1, 2, 3, 4]);
    const result = await client.appendUpload('/data/big.bin', 0, 'b'.repeat(64), data);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/uploads/data/big.bin');
    expect(init).toMatchObject({ method: 'PATCH' });
    const headers = new Headers(init?.headers);
    expect(headers.get('Upload-Offset')).toBe('0');
    expect(headers.get('X-AiryFS-Chunk-SHA256')).toBe('b'.repeat(64));
    expect(init?.body).toBe(data);
    expect(result.offset).toBe(4);
  });

  it('surfaces a stable offset mismatch as a typed error', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(
      { error: { code: 'UPLOAD_OFFSET_MISMATCH', message: 'stale offset' } },
      { status: 409, headers: { 'Upload-Offset': '4' } },
    ));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    await expect(client.appendUpload('/data/big.bin', 0, 'b'.repeat(64), new Uint8Array([1])))
      .rejects.toMatchObject({ code: 'UPLOAD_OFFSET_MISMATCH', status: 409 });
  });

  it('completes and aborts an upload', async () => {
    const completeMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      ...status, offset: 10, type: 'file', mode: 0o100644, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0, ino: 2,
    }));
    const client = new AiryFSClient('https://example.com', 'vol', completeMock);
    await client.completeUpload('/data/big.bin');
    expect(completeMock.mock.calls[0][1]).toMatchObject({ method: 'PUT' });

    const abortMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
    const abortClient = new AiryFSClient('https://example.com', 'vol', abortMock);
    await abortClient.abortUpload('/data/big.bin');
    expect(abortMock.mock.calls[0][1]).toMatchObject({ method: 'DELETE' });
  });

  it('requests a remote checksum via the operations route', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({ algorithm: 'sha256', checksum: 'c'.repeat(64), size: 3, ino: 2 }),
    );
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    const result = await client.checksum('/data/big.bin');

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/operations/checksum');
    expect(JSON.parse(String(init?.body))).toEqual({ path: '/data/big.bin' });
    expect(result).toMatchObject({ algorithm: 'sha256', checksum: 'c'.repeat(64) });
  });

  it('lists registered volumes from the deployment endpoint', async () => {
    const first = { name: 'project', chunkSize: 262144, createdAt: 1_700_000_000 };
    const second = { name: 'scratch', chunkSize: 65536, createdAt: 1_700_000_001 };
    const fetchMock = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ volumes: [first], nextCursor: 'project' }))
      .mockResolvedValueOnce(Response.json({ volumes: [second], nextCursor: null }));
    const client = new AiryFSClient('https://example.com', 'selected', fetchMock);

    expect(await client.listVolumes()).toEqual([first, second]);
    expect(fetchMock.mock.calls[0][0].toString()).toBe('https://example.com/v1/volumes?limit=1000');
    expect(fetchMock.mock.calls[1][0].toString()).toBe('https://example.com/v1/volumes?limit=1000&cursor=project');
  });

  it('executes scoped SQL with positional arguments', async () => {
    const result = { columns: ['body'], rows: [['hello']], rowsRead: 1, rowsWritten: 0, truncated: false };
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(Response.json(result));
    const client = new AiryFSClient('https://example.com', 'vol', fetchMock);

    expect(await client.sql('SELECT body FROM app_notes WHERE id = ?', [1])).toEqual(result);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url.toString()).toBe('https://example.com/v1/volumes/vol/sql');
    expect(init?.method).toBe('POST');
    expect(JSON.parse(String(init?.body))).toEqual({ sql: 'SELECT body FROM app_notes WHERE id = ?', args: [1] });
  });
});
