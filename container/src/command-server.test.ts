// ABOUTME: Integration tests for the streaming/cancellable command endpoints.
// ABOUTME: Runs the real command server on an ephemeral port and spawns real /bin/sh commands.

import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { createCommandServer } from './command-server.js';

let server: Server | null = null;

afterEach(async () => {
  if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
  server = null;
});

async function start(): Promise<string> {
  server = createCommandServer();
  await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}`;
}

interface ExecEvent {
  type: 'start' | 'stdout' | 'stderr' | 'exit';
  id: string;
  data?: string;
  exitCode?: number;
  signal?: string;
  timedOut?: boolean;
}

/** Read a whole internal SSE body into parsed events. */
async function readEvents(response: Response): Promise<ExecEvent[]> {
  const text = await response.text();
  return text.split('\n').filter((line) => line.startsWith('data: '))
    .map((line) => JSON.parse(line.slice(6)) as ExecEvent);
}

/** Iterate SSE data events incrementally so tests can act mid-stream. */
async function* iterateEvents(response: Response): AsyncGenerator<ExecEvent> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let newline: number;
    while ((newline = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.startsWith('data: ')) yield JSON.parse(line.slice(6)) as ExecEvent;
    }
  }
  if (buffer.startsWith('data: ')) yield JSON.parse(buffer.slice(6)) as ExecEvent;
}

test('streams start, base64 stdout, and a zero exit event', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'printf hello', id: 'run-1' }),
  });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('content-type'), 'text/event-stream');

  const events = await readEvents(response);
  assert.deepEqual(events[0], { type: 'start', id: 'run-1' });
  const stdout = events.filter((event) => event.type === 'stdout')
    .map((event) => Buffer.from(event.data!, 'base64').toString()).join('');
  assert.equal(stdout, 'hello');
  const exit = events.at(-1)!;
  assert.equal(exit.type, 'exit');
  assert.equal(exit.id, 'run-1');
  assert.equal(exit.exitCode, 0);
});

test('generates an id when the caller omits one', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'true' }),
  });
  const events = await readEvents(response);
  assert.equal(events[0].type, 'start');
  assert.ok(events[0].id.length > 0);
  assert.equal(events.at(-1)!.id, events[0].id);
});

test('preserves arbitrary bytes through base64 encoding', async () => {
  const base = await start();
  // Emit bytes 0x00..0xff via printf octal escapes.
  const octal = Array.from({ length: 256 }, (_, i) => `\\${i.toString(8).padStart(3, '0')}`).join('');
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: `printf '${octal}'` }),
  });
  const events = await readEvents(response);
  const bytes = Buffer.concat(
    events.filter((event) => event.type === 'stdout').map((event) => Buffer.from(event.data!, 'base64')),
  );
  assert.equal(bytes.length, 256);
  assert.deepEqual(Uint8Array.from(bytes), Uint8Array.from({ length: 256 }, (_, i) => i));
});

test('reports a non-zero exit code from the command', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'exit 7' }),
  });
  const events = await readEvents(response);
  assert.equal(events.at(-1)!.exitCode, 7);
});

test('separates stderr from stdout', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'printf out; printf err 1>&2' }),
  });
  const events = await readEvents(response);
  const decode = (type: string) => Buffer.concat(
    events.filter((event) => event.type === type).map((event) => Buffer.from(event.data!, 'base64')),
  ).toString();
  assert.equal(decode('stdout'), 'out');
  assert.equal(decode('stderr'), 'err');
});

test('rejects a second command while one is running, then admits after it finishes', async () => {
  const base = await start();
  const longRun = fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'sleep 1; printf done', id: 'busy-run' }),
  });
  // Wait for the start event so the slot is definitely held.
  const first = await longRun;
  const iterator = iterateEvents(first);
  const start1 = await iterator.next();
  assert.equal(start1.value!.type, 'start');

  const busy = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'true' }),
  });
  assert.equal(busy.status, 503);
  assert.equal(busy.headers.get('retry-after'), '1');

  // Drain the first command to release the slot.
  for await (const _ of iterator) { /* consume until exit */ }

  const admitted = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'true' }),
  });
  assert.equal(admitted.status, 200);
  await admitted.text();
});

test('cancel by id terminates the process group with a signal exit', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'sleep 30', id: 'cancel-me' }),
  });
  const iterator = iterateEvents(response);
  const started = await iterator.next();
  assert.equal(started.value!.type, 'start');

  const cancel = await fetch(`${base}/exec/cancel`, {
    method: 'POST',
    body: JSON.stringify({ id: 'cancel-me' }),
  });
  assert.deepEqual(await cancel.json(), { ok: true, canceled: true });

  let exit: ExecEvent | undefined;
  for await (const event of iterator) if (event.type === 'exit') exit = event;
  assert.ok(exit);
  assert.equal(exit!.signal, 'SIGTERM');
});

test('a stale cancel id does not terminate a newer command', async () => {
  const base = await start();
  // First command finishes immediately; its id becomes stale.
  await (await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'true', id: 'old' }),
  })).text();

  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'sleep 1; printf survived', id: 'new' }),
  });
  const iterator = iterateEvents(response);
  await iterator.next(); // start

  const cancel = await fetch(`${base}/exec/cancel`, {
    method: 'POST',
    body: JSON.stringify({ id: 'old' }),
  });
  assert.deepEqual(await cancel.json(), { ok: true, canceled: false });

  let stdout = '';
  let exit: ExecEvent | undefined;
  for await (const event of iterator) {
    if (event.type === 'stdout') stdout += Buffer.from(event.data!, 'base64').toString();
    if (event.type === 'exit') exit = event;
  }
  assert.equal(stdout, 'survived');
  assert.equal(exit!.exitCode, 0);
});

test('client disconnect terminates the running command', async () => {
  const base = await start();
  const controller = new AbortController();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'sleep 30', id: 'disconnect' }),
    signal: controller.signal,
  });
  const iterator = iterateEvents(response);
  await iterator.next(); // start
  controller.abort();

  // The slot must free up so a new command is admitted shortly after.
  let admitted = false;
  for (let i = 0; i < 50 && !admitted; i++) {
    const probe = await fetch(`${base}/exec/stream`, {
      method: 'POST',
      body: JSON.stringify({ command: 'true' }),
    });
    if (probe.status === 200) {
      admitted = true;
      await probe.text();
    } else {
      await probe.text();
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  assert.ok(admitted, 'slot should free after client disconnect');
});

test('buffered /exec still returns a single JSON object', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec`, {
    method: 'POST',
    body: JSON.stringify({ command: 'printf hi; exit 3' }),
  });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { exitCode: 3, stdout: 'hi', stderr: '' });
});

test('buffered and streaming share one active slot', async () => {
  const base = await start();
  const response = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'sleep 1', id: 'holding' }),
  });
  const iterator = iterateEvents(response);
  await iterator.next(); // start

  const buffered = await fetch(`${base}/exec`, {
    method: 'POST',
    body: JSON.stringify({ command: 'true' }),
  });
  assert.equal(buffered.status, 503);
  await buffered.text();
  for await (const _ of iterator) { /* drain */ }
});

test('rejects malformed bodies before spawning', async () => {
  const base = await start();
  const badJson = await fetch(`${base}/exec/stream`, { method: 'POST', body: '{' });
  assert.equal(badJson.status, 400);
  await badJson.text();

  const noCommand = await fetch(`${base}/exec/stream`, { method: 'POST', body: '{}' });
  assert.equal(noCommand.status, 400);
  await noCommand.text();

  const badId = await fetch(`${base}/exec/stream`, {
    method: 'POST',
    body: JSON.stringify({ command: 'true', id: 42 }),
  });
  assert.equal(badId.status, 400);
  await badId.text();
});
