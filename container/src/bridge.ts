// ABOUTME: HTTP-to-TCP bridges for Hrana pipeline protocol.
// ABOUTME: Keeps FUSE and invalidation traffic on independent FIFO channels.

import { createServer as createTcpServer, type Socket } from 'net';
import { createServer as createHttpServer } from 'http';

const HEADER_SIZE = 4;
const RESPONSE_TIMEOUT_MS = 30_000;

interface BridgeChannel {
  doSocket: Socket | null;
  tcpBuffer: Buffer;
  pendingQueue: Array<{
    resolve: (data: Buffer) => void;
    reject: (err: Error) => void;
  }>;
}

function rejectPending(channel: BridgeChannel, error: Error): void {
  while (channel.pendingQueue.length) channel.pendingQueue.shift()!.reject(error);
}

function failConnection(channel: BridgeChannel, socket: Socket, error: Error): void {
  if (channel.doSocket !== socket) return;
  channel.doSocket = null;
  channel.tcpBuffer = Buffer.alloc(0);
  rejectPending(channel, error);
  if (!socket.destroyed) socket.destroy();
}

function handleTcpData(channel: BridgeChannel, data: Buffer): void {
  channel.tcpBuffer = Buffer.concat([channel.tcpBuffer, data]);
  drainFrames(channel);
}

function drainFrames(channel: BridgeChannel): void {
  while (channel.pendingQueue.length > 0) {
    if (channel.tcpBuffer.length < HEADER_SIZE) return;
    const jsonLen = channel.tcpBuffer.readUInt32BE(0);
    if (channel.tcpBuffer.length < HEADER_SIZE + jsonLen) return;

    const frame = channel.tcpBuffer.subarray(HEADER_SIZE, HEADER_SIZE + jsonLen);
    channel.tcpBuffer = channel.tcpBuffer.subarray(HEADER_SIZE + jsonLen);
    const pending = channel.pendingQueue.shift()!;
    pending.resolve(Buffer.from(frame));
  }
}

/** Send a length-prefixed JSON frame and wait for the next response frame. */
function sendAndReceive(channel: BridgeChannel, payload: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const socket = channel.doSocket;
    if (!socket || socket.destroyed) {
      reject(new Error('No DO TCP connection'));
      return;
    }

    let settled = false;
    const finishResolve = (data: Buffer): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      resolve(data);
    };
    const finishReject = (error: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      reject(error);
    };
    const timeout = setTimeout(() => {
      failConnection(channel, socket, new Error('Timed out waiting for DO response'));
    }, RESPONSE_TIMEOUT_MS);

    channel.pendingQueue.push({ resolve: finishResolve, reject: finishReject });
    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt32BE(payload.length, 0);
    socket.write(Buffer.concat([header, payload]), (err) => {
      if (err) {
        failConnection(channel, socket, err);
      }
    });
    // Check if we already have a buffered response
    drainFrames(channel);
  });
}

/** Translate a cursor request to a pipeline batch, stream NDJSON response. */
async function handleCursor(channel: BridgeChannel, body: string, res: import('http').ServerResponse): Promise<void> {
  const cursorReq = JSON.parse(body);
  const pipelineReq = {
    baton: cursorReq.baton || null,
    requests: [{ type: 'batch', batch: cursorReq.batch }],
  };
  const responseFrame = await sendAndReceive(channel, Buffer.from(JSON.stringify(pipelineReq), 'utf-8'));
  const pipelineResp = JSON.parse(responseFrame.toString('utf-8'));

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.write(JSON.stringify({
    baton: pipelineResp.baton,
    base_url: pipelineResp.base_url,
  }) + '\n');
  const batchResult = pipelineResp.results?.[0];
  if (batchResult?.type === 'ok' && batchResult.response?.type === 'batch') {
    const result = batchResult.response.result;
    const stepResults = result.step_results || [];
    const stepErrors = result.step_errors || [];
    for (let i = 0; i < stepResults.length; i++) {
      if (stepErrors[i]) {
        res.write(JSON.stringify({
          type: 'step_error', step: i,
          error: { message: stepErrors[i].message, code: stepErrors[i].code || 'UNKNOWN' },
        }) + '\n');
        continue;
      }
      const stepResult = stepResults[i];
      if (!stepResult) continue;
      res.write(JSON.stringify({
        type: 'step_begin', step: i, cols: stepResult.cols || [],
      }) + '\n');
      for (const row of stepResult.rows || []) {
        res.write(JSON.stringify({
          type: 'row', row: row.values || row,
        }) + '\n');
      }
      res.write(JSON.stringify({
        type: 'step_end',
        affected_row_count: stepResult.affected_row_count || 0,
        last_inserted_rowid: stepResult.last_insert_rowid || null,
      }) + '\n');
    }
  } else if (batchResult?.type === 'error') {
    res.write(JSON.stringify({
      type: 'error', error: batchResult.error?.message || 'unknown error',
    }) + '\n');
  }
  res.end();
}

function startChannel(tcpPort: number, httpPort: number): Promise<void> {
  return new Promise((resolve) => {
    const channel: BridgeChannel = {
      doSocket: null,
      tcpBuffer: Buffer.alloc(0),
      pendingQueue: [],
    };
    let tcpReady = false;
    let httpReady = false;
    function checkReady(): void { if (tcpReady && httpReady) resolve(); }

    const tcpServer = createTcpServer((socket) => {
      const previousSocket = channel.doSocket;
      if (previousSocket && !previousSocket.destroyed) previousSocket.destroy();
      rejectPending(channel, new Error('TCP connection replaced'));
      channel.doSocket = socket;
      channel.tcpBuffer = Buffer.alloc(0);
      socket.on('data', (data) => {
        if (channel.doSocket === socket) handleTcpData(channel, data);
      });
      socket.on('end', () => failConnection(channel, socket, new Error('DO TCP connection ended')));
      socket.on('error', (error) => failConnection(channel, socket, error));
      socket.on('close', () => failConnection(channel, socket, new Error('DO TCP connection closed')));
    });

    tcpServer.listen(tcpPort, '0.0.0.0', () => {
      tcpReady = true;
      checkReady();
    });

    const httpServer = createHttpServer(async (req, res) => {
      if (req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('ok');
        return;
      }
      if (!channel.doSocket || channel.doSocket.destroyed) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No DO TCP connection' }));
        return;
      }

      let body = '';
      for await (const chunk of req) body += chunk;
      if (req.method === 'POST' && req.url?.startsWith('/v3/pipeline')) {
        try {
          const responseFrame = await sendAndReceive(channel, Buffer.from(body, 'utf-8'));
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(responseFrame);
        } catch (err) {
          res.writeHead(502, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }));
        }
        return;
      }
      if (req.method === 'POST' && req.url?.startsWith('/v3/cursor')) {
        try {
          await handleCursor(channel, body, res);
        } catch (err) {
          if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }));
        }
        return;
      }
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end(`Not found: ${req.method} ${req.url}`);
    });

    httpServer.listen(httpPort, '0.0.0.0', () => {
      httpReady = true;
      checkReady();
    });
  });
}

export async function startBridge(): Promise<void> {
  await Promise.all([
    startChannel(9000, 8080),
    startChannel(9001, 8081),
  ]);
}
