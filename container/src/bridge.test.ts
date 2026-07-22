// ABOUTME: Integration tests for bridge FIFO admission, cancellation, and bounds.
// ABOUTME: Uses real local HTTP and TCP sockets without changing the wire protocol.

import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';
import { connect, type Socket } from 'node:net';
import { MAX_PENDING_REQUESTS, MAX_TRANSPORT_FRAME_BYTES, startChannel, type BridgeServers } from './bridge.js';

let bridge: BridgeServers | null = null;
let socket: Socket | null = null;

afterEach(async () => {
  socket?.destroy();
  socket = null;
  await bridge?.close();
  bridge = null;
});

async function setup(): Promise<{ base: string; socket: Socket }> {
  bridge = await startChannel(0, 0);
  socket = connect({ host: '127.0.0.1', port: bridge.tcpPort });
  await new Promise<void>((resolve, reject) => {
    socket!.once('connect', resolve);
    socket!.once('error', reject);
  });
  return { base: `http://127.0.0.1:${bridge.httpPort}`, socket };
}

async function connectTcp(port: number): Promise<Socket> {
  const connection = connect({ host: '127.0.0.1', port });
  await new Promise<void>((resolve, reject) => {
    connection.once('connect', resolve);
    connection.once('error', reject);
  });
  return connection;
}

function readFrame(connection: Socket): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const onData = (chunk: Buffer): void => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length < 4) return;
      const length = buffer.readUInt32BE(0);
      if (buffer.length < 4 + length) return;
      connection.off('data', onData);
      resolve(Buffer.from(buffer.subarray(4, 4 + length)));
    };
    connection.on('data', onData);
    connection.once('error', reject);
  });
}

function writeFrame(connection: Socket, body: Buffer): void {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  connection.write(Buffer.concat([header, body]));
}

test('pipelines serialized writes and resolves responses in FIFO order', async () => {
  const { base, socket } = await setup();
  const first = fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{"request":1}' });
  const firstFrame = await readFrame(socket);
  const second = fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{"request":2}' });
  const secondFrame = await readFrame(socket);

  writeFrame(socket, Buffer.from('{"response":1}'));
  writeFrame(socket, Buffer.from('{"response":2}'));
  const firstResponse = await first;
  assert.equal(firstResponse.headers.get('x-airyfs-request-id'), '1');
  assert.equal(await firstResponse.text(), '{"response":1}');

  const secondResponse = await second;
  assert.equal(secondResponse.headers.get('x-airyfs-request-id'), '2');
  assert.equal(await secondResponse.text(), '{"response":2}');
  assert.equal(firstFrame.toString(), '{"request":1}');
  assert.equal(secondFrame.toString(), '{"request":2}');
});

test('reports connection and queue state', async () => {
  const { socket } = await setup();
  assert.deepEqual(bridge!.status(), { connected: true, pending: 0, queued: 0, admitted: 0 });

  const pending = fetch(`http://127.0.0.1:${bridge!.httpPort}/v3/pipeline`, { method: 'POST', body: '{}' });
  const frame = JSON.parse((await readFrame(socket)).toString());
  assert.deepEqual(frame, {});
  assert.deepEqual(bridge!.status(), { connected: true, pending: 1, queued: 0, admitted: 1 });
  writeFrame(socket, Buffer.from('{}'));

  assert.equal((await pending).status, 200);
  assert.deepEqual(bridge!.status(), { connected: true, pending: 0, queued: 0, admitted: 0 });
});

test('reports a disconnected channel', async () => {
  bridge = await startChannel(0, 0);
  assert.deepEqual(bridge.status(), { connected: false, pending: 0, queued: 0, admitted: 0 });
});

test('rejects admission above the bounded queue', async () => {
  const { base, socket } = await setup();
  const requests = Array.from({ length: MAX_PENDING_REQUESTS }, (_, index) =>
    fetch(`${base}/v3/pipeline`, { method: 'POST', body: JSON.stringify({ index }) })
  );
  await readFrame(socket);
  const rejected = await fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{}' });
  assert.equal(rejected.status, 503);
  assert.equal(rejected.headers.get('retry-after'), '1');
  socket.destroy();
  await Promise.allSettled(requests);
});

test('drains a canceled dispatched response without shifting later responses', async () => {
  const { base, socket } = await setup();
  const controller = new AbortController();
  const canceled = fetch(`${base}/v3/pipeline`, {
    method: 'POST',
    body: '{"request":"canceled"}',
    signal: controller.signal,
  }).catch((error: unknown) => error);
  await readFrame(socket);
  controller.abort();

  const next = fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{"request":"next"}' });
  const nextFrame = await readFrame(socket);
  writeFrame(socket, Buffer.from('{"response":"discarded"}'));
  writeFrame(socket, Buffer.from('{"response":"next"}'));

  assert.ok((await canceled) instanceof Error);
  assert.equal(nextFrame.toString(), '{"request":"next"}');
  assert.equal(await (await next).text(), '{"response":"next"}');
});

test('drains an active retired generation while new work uses the replacement', async () => {
  const { base, socket: previousSocket } = await setup();
  const interrupted = fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{"generation":1}' });
  await readFrame(previousSocket);

  const replacement = await connectTcp(bridge!.tcpPort);
  socket = replacement;
  writeFrame(previousSocket, Buffer.from('{"generation":1}'));
  const interruptedResponse = await interrupted;
  assert.equal(interruptedResponse.status, 200);
  assert.equal(await interruptedResponse.text(), '{"generation":1}');

  const current = fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{"generation":2}' });
  const currentFrame = await readFrame(replacement);
  writeFrame(replacement, Buffer.from('{"generation":2}'));

  assert.equal(currentFrame.toString(), '{"generation":2}');
  assert.equal(await (await current).text(), '{"generation":2}');
});

test('rejects oversized HTTP bodies before TCP dispatch', async () => {
  const { base } = await setup();
  const response = await fetch(`${base}/v3/pipeline`, {
    method: 'POST',
    body: Buffer.alloc(MAX_TRANSPORT_FRAME_BYTES + 1),
  });
  assert.equal(response.status, 413);
});

test('closes the connection on an oversized response header', async () => {
  const { base, socket } = await setup();
  const response = fetch(`${base}/v3/pipeline`, { method: 'POST', body: '{}' });
  await readFrame(socket);
  const header = Buffer.alloc(4);
  header.writeUInt32BE(MAX_TRANSPORT_FRAME_BYTES + 1, 0);
  socket.write(header);
  const result = await response;
  assert.equal(result.status, 502);
});
