// ABOUTME: HTTP-to-TCP bridges for Hrana pipeline protocol.
// ABOUTME: Bounds each independent FIFO channel and cancels work without wire multiplexing.

import { createServer as createTcpServer, type Server as TcpServer, type Socket } from 'net';
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from 'http';

const HEADER_SIZE = 4;
const RESPONSE_TIMEOUT_MS = 30_000;
export const MAX_TRANSPORT_FRAME_BYTES = 8 * 1024 * 1024;
export const MAX_PENDING_REQUESTS = 16;

class BridgeHttpError extends Error {
  constructor(readonly status: number, message: string, readonly headers?: Record<string, string>) {
    super(message);
  }
}

interface PendingRequest {
  id: number;
  payload: Buffer;
  canceled: boolean;
  settled: boolean;
  timeout: NodeJS.Timeout | null;
  resolve: (data: Buffer) => void;
  reject: (error: Error) => void;
  detachAbort: () => void;
}

interface BridgeChannel {
  doSocket: Socket | null;
  retired: boolean;
  tcpBuffer: Buffer;
  pendingQueue: PendingRequest[];
  writeQueue: PendingRequest[];
  writing: boolean;
  nextRequestId: number;
  admittedHttpRequests: number;
}

export interface BridgeServers {
  tcpPort: number;
  httpPort: number;
  status(): { connected: boolean; pending: number; queued: number; admitted: number };
  close(): Promise<void>;
}

export interface Bridge {
  data: BridgeServers;
  invalidation: BridgeServers;
  close(): Promise<void>;
}

function settleReject(request: PendingRequest, error: Error): void {
  if (request.timeout) clearTimeout(request.timeout);
  request.timeout = null;
  request.detachAbort();
  if (request.settled) return;
  request.settled = true;
  request.reject(error);
}

function settleResolve(request: PendingRequest, data: Buffer): void {
  if (request.settled) return;
  request.settled = true;
  if (request.timeout) clearTimeout(request.timeout);
  request.timeout = null;
  request.detachAbort();
  request.resolve(data);
}

function rejectPending(channel: BridgeChannel, error: Error): void {
  while (channel.pendingQueue.length) settleReject(channel.pendingQueue.shift()!, error);
  while (channel.writeQueue.length) settleReject(channel.writeQueue.shift()!, error);
  channel.writing = false;
}

function closeRetiredChannelIfDrained(channel: BridgeChannel): void {
  // Keep old responses isolated from new work, but do not interrupt libSQL uploads.
  if (!channel.retired || channel.admittedHttpRequests > 0 || channel.pendingQueue.length > 0
    || channel.writeQueue.length > 0 || channel.writing) return;
  const socket = channel.doSocket;
  if (socket && !socket.destroyed) socket.end();
}

function failConnection(channel: BridgeChannel, socket: Socket, error: Error): void {
  if (channel.doSocket !== socket) return;
  const pending = channel.pendingQueue.length;
  const queued = channel.writeQueue.length;
  const admitted = channel.admittedHttpRequests;
  if (pending > 0 || queued > 0 || admitted > 0) {
    console.error(JSON.stringify({
      event: 'bridge_connection_failed',
      error: error.message,
      pending,
      queued,
      admitted,
    }));
  }
  channel.doSocket = null;
  channel.tcpBuffer = Buffer.alloc(0);
  rejectPending(channel, error);
  if (!socket.destroyed) socket.destroy();
}

function pumpWrites(channel: BridgeChannel): void {
  if (channel.writing || channel.writeQueue.length === 0) return;
  const socket = channel.doSocket;
  if (!socket || socket.destroyed) {
    rejectPending(channel, new Error('No DO TCP connection'));
    return;
  }

  channel.writing = true;
  while (channel.writeQueue.length > 0) {
    const request = channel.writeQueue.shift()!;
    channel.pendingQueue.push(request);
    request.timeout = setTimeout(() => {
      failConnection(channel, socket, new Error(`Request ${request.id} timed out waiting for DO response`));
    }, RESPONSE_TIMEOUT_MS);

    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt32BE(request.payload.length, 0);
    if (!socket.write(Buffer.concat([header, request.payload]))) {
      socket.once('drain', () => {
        if (channel.doSocket !== socket) return;
        channel.writing = false;
        pumpWrites(channel);
        closeRetiredChannelIfDrained(channel);
      });
      return;
    }
  }
  channel.writing = false;
  closeRetiredChannelIfDrained(channel);
}

function handleTcpData(channel: BridgeChannel, data: Buffer): void {
  if (channel.tcpBuffer.length + data.length > MAX_TRANSPORT_FRAME_BYTES + HEADER_SIZE) {
    const socket = channel.doSocket;
    if (socket) failConnection(channel, socket, new Error('Buffered DO response exceeds the frame limit'));
    return;
  }
  channel.tcpBuffer = Buffer.concat([channel.tcpBuffer, data]);
  drainFrames(channel);
}

function drainFrames(channel: BridgeChannel): void {
  while (channel.pendingQueue.length > 0) {
    if (channel.tcpBuffer.length < HEADER_SIZE) return;
    const jsonLen = channel.tcpBuffer.readUInt32BE(0);
    if (jsonLen > MAX_TRANSPORT_FRAME_BYTES) {
      const socket = channel.doSocket;
      if (socket) failConnection(channel, socket, new Error(`DO response frame exceeds ${MAX_TRANSPORT_FRAME_BYTES} bytes`));
      return;
    }
    if (channel.tcpBuffer.length < HEADER_SIZE + jsonLen) return;

    const frame = Buffer.from(channel.tcpBuffer.subarray(HEADER_SIZE, HEADER_SIZE + jsonLen));
    try {
      JSON.parse(frame.toString('utf-8'));
    } catch {
      const socket = channel.doSocket;
      if (socket) failConnection(channel, socket, new Error('DO response is not valid JSON'));
      return;
    }
    channel.tcpBuffer = channel.tcpBuffer.subarray(HEADER_SIZE + jsonLen);
    const request = channel.pendingQueue.shift()!;
    if (request.timeout) clearTimeout(request.timeout);
    request.timeout = null;
    request.detachAbort();
    if (!request.canceled) settleResolve(request, frame);
    closeRetiredChannelIfDrained(channel);
  }

  if (channel.tcpBuffer.length > 0) {
    const socket = channel.doSocket;
    if (socket) failConnection(channel, socket, new Error('Received a DO response without an active request'));
  }
}

/** Admit one FIFO request. IDs are bridge-local and never enter the TCP payload. */
function sendAndReceive(
  channel: BridgeChannel,
  payload: Buffer,
  signal: AbortSignal
): { id: number; response: Promise<Buffer> } {
  if (payload.length > MAX_TRANSPORT_FRAME_BYTES) {
    throw new BridgeHttpError(413, `Request exceeds ${MAX_TRANSPORT_FRAME_BYTES} bytes`);
  }
  if (!channel.doSocket || channel.doSocket.destroyed) {
    throw new BridgeHttpError(503, 'No DO TCP connection', { 'Retry-After': '1' });
  }
  const id = channel.nextRequestId++;
  let request!: PendingRequest;
  const response = new Promise<Buffer>((resolve, reject) => {
    const onAbort = (): void => {
      request.canceled = true;
      const queuedIndex = channel.writeQueue.indexOf(request);
      if (queuedIndex >= 0) {
        channel.writeQueue.splice(queuedIndex, 1);
        settleReject(request, new Error(`Request ${id} canceled`));
        closeRetiredChannelIfDrained(channel);
        return;
      }
      // A dispatched response must still be consumed to preserve FIFO alignment.
      request.detachAbort();
      if (!request.settled) {
        request.settled = true;
        request.reject(new Error(`Request ${id} canceled`));
      }
    };
    request = {
      id,
      payload,
      canceled: false,
      settled: false,
      timeout: null,
      resolve,
      reject,
      detachAbort: () => signal.removeEventListener('abort', onAbort),
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });

  if (signal.aborted) {
    request.canceled = true;
    settleReject(request, new Error(`Request ${id} canceled`));
    return { id, response };
  }

  channel.writeQueue.push(request);
  pumpWrites(channel);
  return { id, response };
}

async function readBody(req: IncomingMessage): Promise<Buffer> {
  const declaredLength = Number(req.headers['content-length']);
  if (Number.isFinite(declaredLength) && declaredLength > MAX_TRANSPORT_FRAME_BYTES) {
    throw new BridgeHttpError(413, `Request exceeds ${MAX_TRANSPORT_FRAME_BYTES} bytes`);
  }

  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += data.length;
    if (total > MAX_TRANSPORT_FRAME_BYTES) {
      throw new BridgeHttpError(413, `Request exceeds ${MAX_TRANSPORT_FRAME_BYTES} bytes`);
    }
    chunks.push(data);
  }
  return Buffer.concat(chunks, total);
}

function requestAbortSignal(req: IncomingMessage, res: ServerResponse): AbortSignal {
  const controller = new AbortController();
  req.once('aborted', () => controller.abort());
  res.once('close', () => {
    if (!res.writableEnded) controller.abort();
  });
  return controller.signal;
}

function reserveHttpRequest(channel: BridgeChannel, res: ServerResponse): void {
  if (!channel.doSocket || channel.doSocket.destroyed) {
    throw new BridgeHttpError(503, 'No DO TCP connection', { 'Retry-After': '1' });
  }
  if (channel.admittedHttpRequests >= MAX_PENDING_REQUESTS) {
    throw new BridgeHttpError(503, 'Bridge request queue is full', { 'Retry-After': '1' });
  }

  channel.admittedHttpRequests++;
  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    channel.admittedHttpRequests--;
    closeRetiredChannelIfDrained(channel);
  };
  res.once('finish', release);
  res.once('close', release);
}

function writeBridgeError(res: ServerResponse, error: unknown): void {
  const status = error instanceof BridgeHttpError ? error.status : 502;
  const headers = error instanceof BridgeHttpError ? error.headers : undefined;
  if (!res.headersSent) res.writeHead(status, { 'Content-Type': 'application/json', ...headers });
  res.end(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
}

/** Translate a cursor request to a pipeline batch, stream NDJSON response. */
async function handleCursor(
  channel: BridgeChannel,
  body: Buffer,
  signal: AbortSignal,
  res: ServerResponse
): Promise<void> {
  const cursorReq = JSON.parse(body.toString('utf-8'));
  const pipelineReq = {
    baton: cursorReq.baton || null,
    requests: [{ type: 'batch', batch: cursorReq.batch }],
  };
  const admitted = sendAndReceive(channel, Buffer.from(JSON.stringify(pipelineReq), 'utf-8'), signal);
  res.setHeader('X-AiryFS-Request-ID', String(admitted.id));
  const responseFrame = await admitted.response;
  const pipelineResp = JSON.parse(responseFrame.toString('utf-8'));

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.write(JSON.stringify({ baton: pipelineResp.baton, base_url: pipelineResp.base_url }) + '\n');
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
      res.write(JSON.stringify({ type: 'step_begin', step: i, cols: stepResult.cols || [] }) + '\n');
      for (const row of stepResult.rows || []) {
        res.write(JSON.stringify({ type: 'row', row: row.values || row }) + '\n');
      }
      res.write(JSON.stringify({
        type: 'step_end',
        affected_row_count: stepResult.affected_row_count || 0,
        last_inserted_rowid: stepResult.last_insert_rowid || null,
      }) + '\n');
    }
  } else if (batchResult?.type === 'error') {
    res.write(JSON.stringify({ type: 'error', error: batchResult.error?.message || 'unknown error' }) + '\n');
  }
  res.end();
}

function listen(server: TcpServer | HttpServer, port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '0.0.0.0', () => {
      server.off('error', reject);
      const address = server.address();
      if (!address || typeof address === 'string') reject(new Error('Server did not bind a TCP port'));
      else resolve(address.port);
    });
  });
}

function closeServer(server: TcpServer | HttpServer): Promise<void> {
  return new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
}

export async function startChannel(tcpPort: number, httpPort: number): Promise<BridgeServers> {
  let channel: BridgeChannel | null = null;
  const generations = new Set<BridgeChannel>();

  const tcpServer = createTcpServer((socket) => {
    const previousChannel = channel;
    if (previousChannel) {
      previousChannel.retired = true;
      closeRetiredChannelIfDrained(previousChannel);
    }
    const socketChannel: BridgeChannel = {
      doSocket: socket,
      retired: false,
      tcpBuffer: Buffer.alloc(0),
      pendingQueue: [],
      writeQueue: [],
      writing: false,
      nextRequestId: previousChannel?.nextRequestId ?? 1,
      admittedHttpRequests: 0,
    };
    channel = socketChannel;
    generations.add(socketChannel);
    socket.on('data', (data) => {
      if (socketChannel.doSocket === socket) handleTcpData(socketChannel, data);
    });
    socket.on('end', () => failConnection(socketChannel, socket, new Error('DO TCP connection ended')));
    socket.on('error', (error) => failConnection(socketChannel, socket, error));
    socket.on('close', () => {
      failConnection(socketChannel, socket, new Error('DO TCP connection closed'));
      generations.delete(socketChannel);
      if (channel === socketChannel) channel = null;
    });
  });

  const httpServer = createHttpServer(async (req, res) => {
    if (req.method === 'GET') {
      // AgentFS uses this endpoint only to confirm that the local bridge is listening.
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      return;
    }

    const isPipeline = req.method === 'POST' && req.url?.startsWith('/v3/pipeline');
    const isCursor = req.method === 'POST' && req.url?.startsWith('/v3/cursor');
    if (!isPipeline && !isCursor) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end(`Not found: ${req.method} ${req.url}`);
      return;
    }

    const signal = requestAbortSignal(req, res);
    try {
      const requestChannel = channel;
      if (!requestChannel) throw new BridgeHttpError(503, 'No DO TCP connection', { 'Retry-After': '1' });
      reserveHttpRequest(requestChannel, res);
      const body = await readBody(req);
      if (isPipeline) {
        const admitted = sendAndReceive(requestChannel, body, signal);
        res.setHeader('X-AiryFS-Request-ID', String(admitted.id));
        const responseFrame = await admitted.response;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(responseFrame);
        return;
      }
      if (isCursor) {
        await handleCursor(requestChannel, body, signal, res);
        return;
      }
    } catch (error) {
      if (!signal.aborted) writeBridgeError(res, error);
    }
  });

  const [boundTcpPort, boundHttpPort] = await Promise.all([
    listen(tcpServer, tcpPort),
    listen(httpServer, httpPort),
  ]);

  return {
    tcpPort: boundTcpPort,
    httpPort: boundHttpPort,
    status() {
      let pending = 0;
      let queued = 0;
      let admitted = 0;
      for (const generation of generations) {
        pending += generation.pendingQueue.length;
        queued += generation.writeQueue.length;
        admitted += generation.admittedHttpRequests;
      }
      return {
        connected: channel?.doSocket !== null && channel?.doSocket.destroyed === false,
        pending,
        queued,
        admitted,
      };
    },
    async close(): Promise<void> {
      for (const generation of generations) {
        if (generation.doSocket && !generation.doSocket.destroyed) generation.doSocket.destroy();
        rejectPending(generation, new Error('Bridge channel closed'));
      }
      await Promise.all([closeServer(tcpServer), closeServer(httpServer)]);
    },
  };
}

export async function startBridge(): Promise<Bridge> {
  const [data, invalidation] = await Promise.all([
    startChannel(9000, 8080),
    startChannel(9001, 8081),
  ]);
  return {
    data,
    invalidation,
    close: async () => Promise.all([data.close(), invalidation.close()]).then(() => undefined),
  };
}
