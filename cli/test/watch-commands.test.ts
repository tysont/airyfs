// ABOUTME: Exercises the `airyfs watch` command against a local mock AiryFS change-feed server.
// ABOUTME: Covers --once paging/markers, --json ChangePage drains, follow-mode NDJSON + SIGINT, and pre-network validation.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { execute } from '../src/program.js';
import type { ChangeEvent, ChangePage } from '../src/api/types.js';

interface CapturedRequest {
  method: string;
  path: string;
}

// Static feeds keyed by request pathname, paged by the --once drain loop.
interface Feed {
  events: ChangeEvent[];
  oldest: number;
}

const requests: CapturedRequest[] = [];
const temporaryPaths: string[] = [];
let endpoint: string;
let sessions: SessionManager;

// Mutable, per-test server state.
let feeds: Map<string, Feed> = new Map();
let longPollCount = 0;
let followTailCursor = 0;
let followEvents: ChangeEvent[] = [];
const hangingResponses: ServerResponse[] = [];

const server = createServer(async (request, response) => {
  const url = new URL(request.url || '/', 'http://localhost');
  await drainRequestBody(request);
  requests.push({ method: request.method || 'GET', path: `${url.pathname}${url.search}` });
  await route(request, response, url);
});

beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Mock server did not bind a TCP port');
  endpoint = `http://127.0.0.1:${address.port}`;
  const home = await mkdtemp(join(tmpdir(), 'airyfs-watch-cmd-'));
  temporaryPaths.push(home);
  sessions = new SessionManager(new ConfigStore(home));
  // New sessions default cwd to "/", so `watch /src` resolves to the /src prefix exactly.
  await sessions.create('test', { endpoint, volume: 'vol' });
});

afterAll(async () => {
  releaseHangingResponses();
  const closeable = server as unknown as { closeAllConnections?: () => void };
  closeable.closeAllConnections?.();
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  await Promise.all(temporaryPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

beforeEach(() => {
  requests.length = 0;
  feeds = new Map();
  longPollCount = 0;
  followTailCursor = 0;
  followEvents = [];
  releaseHangingResponses();
});

describe('watch --once', () => {
  it('pages through retained changes and prints text markers including rename old -> new', async () => {
    feeds.set('/v1/volumes/vol/changes/src', {
      oldest: 1,
      events: [
        event(1, 'create', '/src/alpha.txt'),
        event(2, 'modify', '/src/beta.txt'),
        { ...event(3, 'rename', '/src/new.txt'), oldPath: '/src/old.txt' },
        event(4, 'remove', '/src/gamma.txt'),
      ],
    });

    const result = await invoke(['--no-color', 'watch', '/src', '--once', '--since', '0', '--limit', '2']);

    expect(result.code).toBe(0);
    expect(result.stdout).toBe(
      [
        'A /src/alpha.txt',
        'M /src/beta.txt',
        'R /src/old.txt -> /src/new.txt',
        'D /src/gamma.txt',
        '',
      ].join('\n'),
    );

    // Encoded route + query for both pages, with the cursor advancing to the first page's cursor.
    const changeRequests = requests.filter((r) => r.path.startsWith('/v1/volumes/vol/changes/src'));
    expect(changeRequests.map((r) => r.path)).toEqual([
      '/v1/volumes/vol/changes/src?since=0&limit=2',
      '/v1/volumes/vol/changes/src?since=2&limit=2',
    ]);
  });
});

describe('watch --json --once', () => {
  it('emits one ChangePage object with all drained events and gap metadata', async () => {
    feeds.set('/v1/volumes/vol/changes/src', {
      oldest: 3, // history before seq 3 was dropped, so a since=0 drain reports a gap.
      events: [
        event(3, 'create', '/src/one.txt'),
        event(4, 'modify', '/src/two.txt'),
        event(5, 'remove', '/src/three.txt'),
      ],
    });

    const result = await invoke(['--json', 'watch', '/src', '--once', '--since', '0', '--limit', '2']);

    expect(result.code).toBe(0);
    // Exactly one JSON document on stdout.
    const parsed = JSON.parse(result.stdout) as ChangePage;
    expect(parsed.events.map((e) => e.seq)).toEqual([3, 4, 5]);
    expect(parsed.cursor).toBe(5);
    expect(parsed.latest).toBe(5);
    expect(parsed.oldest).toBe(3);
    expect(parsed.gap).toBe(true);

    // JSON mode routes gap reporting into the object, not a stderr warning.
    expect(result.stderr).toBe('');
    // Drain paged twice (since=0 then since=4) before cursor reached latest.
    const changeRequests = requests.filter((r) => r.path.startsWith('/v1/volumes/vol/changes/src'));
    expect(changeRequests.map((r) => r.path)).toEqual([
      '/v1/volumes/vol/changes/src?since=0&limit=2',
      '/v1/volumes/vol/changes/src?since=4&limit=2',
    ]);
  });
});

describe('watch follow mode', () => {
  it('tails from latest, long-polls compact NDJSON, and exits 130 on SIGINT with listener cleanup', async () => {
    followTailCursor = 100;
    followEvents = [event(101, 'create', '/src/live.txt')];
    const baseline = process.listenerCount('SIGINT');

    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    const run = execute(['node', 'airyfs', '--session', 'test', '--json', 'watch', '/src'], {
      sessions,
      stdin: Readable.from(''),
      stdout: sink(stdout),
      stderr: sink(stderr),
      shellMode: true,
    });

    // First poll emits an NDJSON line; the second poll hangs, letting us interrupt mid-wait.
    const added = await waitForNewSigintListener(baseline);
    await waitFor(() => Buffer.concat(stdout).toString().includes('"seq":101'));
    await waitFor(() => longPollCount >= 2);
    added();

    const code = await run;

    expect(code).toBe(130);
    expect(process.listenerCount('SIGINT')).toBe(baseline);

    // Compact NDJSON: exactly one line, parseable, no pretty-print indentation.
    const out = Buffer.concat(stdout).toString();
    const lines = out.trimEnd().split('\n');
    expect(lines).toHaveLength(1);
    expect(out).not.toContain('\n  ');
    expect(JSON.parse(lines[0])).toEqual(followEvents[0]);

    // Initial tail carries no cursor/limit/wait; long-polls carry since (=tail cursor) + wait.
    expect(requests.some((r) => r.path === '/v1/volumes/vol/changes/src')).toBe(true);
    expect(
      requests.some((r) => r.path === '/v1/volumes/vol/changes/src?since=100&limit=100&wait=25000'),
    ).toBe(true);
  });
});

describe('watch validation', () => {
  it('rejects an invalid --since before any network use', async () => {
    const result = await invoke(['watch', '/src', '--once', '--since', 'nope']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Invalid --since cursor: nope');
    expect(requests).toHaveLength(0);
  });

  it('rejects an out-of-range --limit before any network use', async () => {
    const result = await invoke(['watch', '/src', '--once', '--limit', '0']);

    expect(result.code).toBe(1);
    expect(result.stderr).toContain('Invalid --limit count: 0 (expected 1 to 1000)');
    expect(requests).toHaveLength(0);
  });
});

function event(seq: number, type: ChangeEvent['type'], path: string): ChangeEvent {
  return { seq, type, path, oldPath: null, ino: seq + 1, timestamp: 1_700_000_000 + seq };
}

function releaseHangingResponses(): void {
  for (const response of hangingResponses.splice(0)) {
    try {
      if (!response.writableEnded) response.end();
    } catch {
      // The client may have already torn down the socket; ignore.
    }
  }
}

async function route(request: IncomingMessage, response: ServerResponse, url: URL): Promise<void> {
  const method = request.method || 'GET';
  const parts = url.pathname.split('/').filter(Boolean); // v1, volumes, vol, changes, <segments...>
  if (method !== 'GET' || parts[3] !== 'changes') {
    return json(response, 404, { error: { code: 'ENOENT', message: `Unhandled ${method} ${url.pathname}` } });
  }

  const sinceParam = url.searchParams.get('since');
  const waitParam = url.searchParams.get('wait');
  const limit = Number(url.searchParams.get('limit') ?? '100');

  // Follow long-poll: the first poll drains the queued event, later polls hang until interrupt.
  if (waitParam !== null) {
    longPollCount += 1;
    if (longPollCount === 1) {
      const latest = followEvents.length ? followEvents[followEvents.length - 1].seq : Number(sinceParam);
      return json(response, 200, { events: followEvents, cursor: latest, latest, oldest: 1, gap: false });
    }
    hangingResponses.push(response); // Hold the connection open; never reply.
    return;
  }

  // Follow initial tail: no since and no wait means "report the latest cursor with no backlog".
  if (sinceParam === null) {
    return json(response, 200, {
      events: [],
      cursor: followTailCursor,
      latest: followTailCursor,
      oldest: 1,
      gap: false,
    });
  }

  // --once drain: statically page through the configured feed.
  const feed = feeds.get(url.pathname) ?? { events: [], oldest: 1 };
  const since = Number(sinceParam);
  const page = feed.events.filter((e) => e.seq > since).slice(0, limit);
  const latest = feed.events.length ? feed.events[feed.events.length - 1].seq : 0;
  const cursor = page.length ? page[page.length - 1].seq : latest;
  const gap = since < feed.oldest - 1;
  return json(response, 200, { events: page, cursor, latest, oldest: feed.oldest, gap });
}

function sink(chunks: Buffer[]): Writable {
  return new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.from(chunk));
      callback();
    },
  });
}

async function invoke(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const code = await execute(['node', 'airyfs', '--session', 'test', ...args], {
    sessions,
    stdin: Readable.from(''),
    stdout: sink(stdout),
    stderr: sink(stderr),
    shellMode: true,
  });
  return { code, stdout: Buffer.concat(stdout).toString(), stderr: Buffer.concat(stderr).toString() };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for condition');
    await delay(5);
  }
}

/** Return the SIGINT listener the running watch added, so the test can invoke it like a real signal. */
async function waitForNewSigintListener(baseline: number): Promise<() => void> {
  await waitFor(() => process.listenerCount('SIGINT') > baseline);
  const listeners = process.listeners('SIGINT');
  return listeners[listeners.length - 1] as () => void;
}

function json(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' }).end(JSON.stringify(value));
}

async function drainRequestBody(request: IncomingMessage): Promise<void> {
  for await (const _chunk of request) {
    // Watch issues GET requests with no body; consume to release the socket.
  }
}
