// ABOUTME: Verifies the SDK client covers every AiryFS HTTP resource family.
// ABOUTME: Uses a fetch recorder so URLs, methods, auth, streaming, and errors stay stable.

import { describe, expect, it, vi } from 'vitest';
import { AiryFSApiError, AiryFSClient } from '../src/index.js';

describe('AiryFSClient', () => {
  it('covers the complete volume API with encoded paths and credentials', async () => {
    const requests: Array<{ url: string; method: string; headers: Headers; body: unknown }> = [];
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      const method = init?.method ?? 'GET';
      requests.push({ url, method, headers: new Headers(init?.headers), body: init?.body });
      if (url.includes('/jobs/command-id/logs')) return Response.json({ entries: [], next: null });
      if (url.includes('/jobs/command-id')) return Response.json({
        id: 'command-id', idempotencyKey: 'key', command: 'true', cwd: '/', status: 'succeeded',
        execId: 'run', exitCode: 0, error: null, cancelRequested: false, outputBytes: 0,
        outputTruncated: false, createdAt: 1, updatedAt: 2, startedAt: 1, finishedAt: 2,
      });
      if (url.endsWith('/jobs') && method === 'POST') return Response.json({
        id: 'command-id', idempotencyKey: new Headers(init?.headers).get('Idempotency-Key') ?? 'key',
        command: 'true', cwd: '/', status: 'succeeded', execId: 'run', exitCode: 0, error: null,
        cancelRequested: false, outputBytes: 0, outputTruncated: false,
        createdAt: 1, updatedAt: 2, startedAt: 1, finishedAt: 2,
      });
      if (url.includes('/exec?stream=true')) {
        return new Response([
          '{"type":"start","id":"run"}',
          '{"type":"exit","id":"run","exitCode":0}',
          '',
        ].join('\n'));
      }
      if (method === 'HEAD') {
        return new Response(null, { headers: { 'Content-Length': '3', 'X-AiryFS-Inode': '2' } });
      }
      if (url.includes('/files/') && method === 'DELETE') return Response.json({
        id: 'trash', originalPath: '/a b/file', trashPath: '/.airyfs-trash/trash', type: 'file', size: 3, deletedAt: 1,
      });
      if (url.includes('/files/')) return new Response('abc');
      if (url.includes('/trees/') && method === 'GET') return new Response(new Uint8Array([1, 2]));
      if (url.includes('/operations/readlink')) return Response.json({ target: '../target' });
      if (url.includes('/kv/get')) return new Response('value');
      if (url.includes('/v1/volumes?')) return Response.json({ volumes: [], nextCursor: null });
      return Response.json({
        target: '../target',
        events: [], cursor: 0, latest: 0, oldest: 1, gap: false,
        entries: [], next: null,
      });
    });
    const client = new AiryFSClient('https://example.com/', 'my volume', {
      fetch: fetchMock,
      token: 'secret',
      headers: { 'X-Client': 'sdk' },
    });

    await client.getVolume();
    await client.listVolumes();
    await client.createVolume(262144);
    await client.listDirectory('/a b');
    await client.tree('/a b', { depth: 2, limit: 10 });
    await client.readFile('/a b/file');
    await client.headFile('/a b/file');
    await client.readFileBytes('/a b/file');
    await client.readFileText('/a b/file');
    await client.writeFile('/a b/file', 'data');
    await client.deleteFile('/a b/file');
    await client.makeDirectory('/dir');
    await client.removeDirectory('/dir', true);
    await client.listTrash();
    await client.restoreTrash('trash id');
    await client.purgeTrash('trash id');
    await client.undoTrash();
    await client.rename('/from', '/to');
    await client.copy('/from', '/to');
    await client.symlink('../target', '/link');
    expect(await client.readlink('/link')).toBe('../target');
    await client.truncate('/file', 2);
    await client.lstat('/file');
    await client.touch('/file', { mtime: 42 });
    await client.chmod('/file', 0o640);
    await client.link('/file', '/hard-link');
    await client.appendFile('/file', Uint8Array.from([0, 255]));
    await client.diskUsage('/');
    await client.checksum('/file');
    await client.exportTree('/tree');
    await client.importTree('/tree', new Uint8Array([1]).buffer, true);
    await client.exec('true');
    const execEvents = [];
    for await (const event of await client.execStream('true')) execEvents.push(event);
    expect(execEvents).toHaveLength(2);
    await client.cancelExec('run');
    await client.createPtyTicket();
    await client.listServices();
    await client.getService('dev server');
    await client.getServiceLogs('dev server', { after: 4, generation: 'gen' });
    await client.createService({ name: 'dev', command: 'npm start', public: true });
    await client.startService('dev server');
    await client.stopService('dev server');
    await client.deleteService('dev server');
    await client.beginUpload('/big', 3, 'a'.repeat(64));
    await client.uploadStatus('/big');
    await client.appendUpload('/big', 0, 'b'.repeat(64), new Uint8Array([1, 2, 3]));
    await client.completeUpload('/big');
    await client.abortUpload('/big');
    await client.submitJob('true', '/work', 'key');
    await client.listJobs('running');
    await client.getJob('job id');
    await client.getJobLogs('job id', 2, 10);
    await client.cancelJob('job id');
    await client.getChanges({ since: 4, limit: 20, path: '/a b', wait: 100 });
    await client.createSnapshot('nightly', 'note');
    await client.listSnapshots();
    await client.diffSnapshot('snap id', 'other');
    await client.restoreSnapshot('snap id');
    await client.cloneSnapshot('snap id', 'copy');
    await client.forkVolume('fork copy');
    await client.sql('SELECT * FROM app_notes WHERE id = ?', [1]);
    await client.deleteSnapshot('snap id');
    await client.authStatus();
    await client.createCapability({ operations: ['read'], pathPrefixes: ['/a'], expiresInSeconds: 60 });
    await client.revokeCapability('cap id');
    await client.usage();
    await client.usageHistory({ before: 42, limit: 10 });
    await client.metrics();
    await client.quota();
    await client.setQuota({ bytes: 1024, inodes: null });
    await client.perf();
    await client.databaseInfo();
    await client.destroyContainer();
    await client.setKv('a b', 'value');
    expect(await client.getKv('a b')).toBe('value');

    expect(requests.every((request) => request.headers.get('Authorization') === 'Bearer secret')).toBe(true);
    expect(requests.every((request) => request.headers.get('X-Client') === 'sdk')).toBe(true);
    expect(requests.some((request) => request.url.includes('/directories/a%20b'))).toBe(true);
    expect(requests.some((request) => request.url.includes('/tree/a%20b?depth=2&limit=10'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/quota') && request.method === 'PUT')).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/directories/dir?recursive=true'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/trees/tree?replace=true'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/jobs?status=running'))).toBe(true);
    expect(requests.some((request) => request.url.includes('/jobs/job%20id/logs?after=2&limit=10'))).toBe(true);
    expect(requests.some((request) => request.url.includes('/changes/a%20b?since=4&limit=20&wait=100'))).toBe(true);
    expect(requests.some((request) => request.url.includes('/snapshots/snap%20id/diff?against=other'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/forks') && request.method === 'POST')).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/v1/volumes?limit=1000') && request.method === 'GET')).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/sql') && request.method === 'POST')).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/perf?volume=my+volume'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/v1/volumes/my%20volume/metrics'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/v1/volumes/my%20volume/usage-history?before=42&limit=10'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/kv/set?volume=my+volume&key=a+b'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/services/dev%20server/start') && request.method === 'POST')).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/services/dev%20server/logs?after=4&generation=gen'))).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/services/dev%20server') && request.method === 'DELETE')).toBe(true);
    expect(requests.some((request) => request.url.endsWith('/operations/append')
      && request.body === JSON.stringify({ path: '/file', data: 'AP8=' }))).toBe(true);
  });

  it('normalizes structured API errors and transport failures', async () => {
    const rejected = new AiryFSClient('https://example.com', 'v', {
      fetch: async () => Response.json({ error: { code: 'ENOENT', message: 'missing', path: '/x' } }, { status: 404 }),
    });
    await expect(rejected.getVolume()).rejects.toMatchObject({
      name: 'AiryFSApiError', status: 404, code: 'ENOENT', path: '/x',
    } satisfies Partial<AiryFSApiError>);

    const offline = new AiryFSClient('https://example.com', 'v', {
      fetch: async () => { throw new Error('offline'); },
    });
    await expect(offline.getVolume()).rejects.toMatchObject({
      name: 'AiryFSTransportError', origin: 'https://example.com',
    });
  });

  it('drains durable log pages, retries transient polling, and reports truncation', async () => {
    let jobReads = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = input.toString();
      if (url.endsWith('/jobs') && init?.method === 'POST') return Response.json({
        id: 'command-id', idempotencyKey: 'key', command: 'true', cwd: '/', status: 'running',
        execId: 'run', exitCode: null, error: null, cancelRequested: false, outputBytes: 0,
        outputTruncated: false, createdAt: 1, updatedAt: 1, startedAt: 1, finishedAt: null,
      });
      if (url.endsWith('/logs')) return Response.json({
        entries: [{ seq: 0, stream: 'stdout', data: 'YQ==', timestamp: 1 }], next: 0,
      });
      if (url.endsWith('/logs?after=0')) return Response.json({
        entries: [{ seq: 1, stream: 'stderr', data: 'Yg==', timestamp: 2 }], next: null,
      });
      if (url.endsWith('/logs?after=1')) return Response.json({ entries: [], next: null });
      if (url.endsWith('/jobs/command-id') && ++jobReads === 1) {
        return Response.json({ error: { code: 'UNAVAILABLE', message: 'retry' } }, { status: 503 });
      }
      return Response.json({
        id: 'command-id', idempotencyKey: 'key', command: 'true', cwd: '/', status: 'succeeded',
        execId: 'run', exitCode: 0, error: null, cancelRequested: false, outputBytes: 2,
        outputTruncated: true, createdAt: 1, updatedAt: 2, startedAt: 1, finishedAt: 2,
      });
    });
    const client = new AiryFSClient('https://example.com', 'v', { fetch: fetchMock });

    const result = await client.exec('true');

    expect(result).toEqual({ commandId: 'command-id', exitCode: 0, stdout: 'a', stderr: 'b', outputTruncated: true });
    expect(jobReads).toBe(2);
    expect(fetchMock.mock.calls.some(([input]) => input.toString().includes('/cancel'))).toBe(false);
  });
});
