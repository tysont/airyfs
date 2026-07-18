// ABOUTME: Provides a cross-runtime interactive PTY session over AiryFS WebSockets.
// ABOUTME: Encodes binary stdin, resize, and signal frames and exposes merged terminal output.

import type { AiryFSClient } from './client.js';
import type { OpenPtyOptions, PtyExit, PtySession } from './types.js';

const STDIN = 0x00;
const RESIZE = 0x01;
const SIGNAL = 0x02;
const STDOUT = 0x10;
const EXIT = 0x11;

export async function openPty(client: AiryFSClient, options: OpenPtyOptions = {}): Promise<PtySession> {
  const { ticket } = await client.createPtyTicket();
  const url = new URL(client.endpoint);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.pathname = `/v1/volumes/${encodeURIComponent(client.volume)}/exec/pty`;
  url.search = new URLSearchParams({ ticket }).toString();
  const WebSocketImpl = options.webSocket ?? WebSocket;
  const socket = new WebSocketImpl(url);
  socket.binaryType = 'arraybuffer';
  await new Promise<void>((resolve, reject) => {
    socket.addEventListener('open', () => resolve(), { once: true });
    socket.addEventListener('error', () => reject(new Error('PTY WebSocket connection failed')), { once: true });
  });
  return new WebSocketPtySession(socket);
}

class WebSocketPtySession implements PtySession {
  private readonly listeners = new Set<(data: Uint8Array) => void>();
  readonly closed: Promise<PtyExit>;
  private resolveClosed!: (exit: PtyExit) => void;
  private settled = false;

  constructor(private readonly socket: WebSocket) {
    this.closed = new Promise((resolve) => { this.resolveClosed = resolve; });
    socket.addEventListener('message', (event) => { void this.message(event); });
    socket.addEventListener('close', () => this.finish({ exitCode: 1 }));
  }

  write(data: Uint8Array | string): void {
    const bytes = typeof data === 'string' ? new TextEncoder().encode(data) : data;
    this.send(STDIN, bytes);
  }

  resize(cols: number, rows: number): void {
    if (!Number.isInteger(cols) || !Number.isInteger(rows) || cols < 1 || rows < 1 || cols > 65535 || rows > 65535) {
      throw new Error('PTY dimensions must be integers between 1 and 65535');
    }
    const payload = new Uint8Array(4);
    const view = new DataView(payload.buffer);
    view.setUint16(0, cols, false);
    view.setUint16(2, rows, false);
    this.send(RESIZE, payload);
  }

  signal(name: string): void { this.send(SIGNAL, new TextEncoder().encode(name)); }
  onData(listener: (data: Uint8Array) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  close(): void { this.socket.close(1000, 'Client closed PTY'); }

  private send(type: number, payload: Uint8Array): void {
    const frame = new Uint8Array(1 + payload.byteLength);
    frame[0] = type;
    frame.set(payload, 1);
    this.socket.send(frame);
  }

  private async message(event: MessageEvent): Promise<void> {
    const data = event.data instanceof Blob
      ? new Uint8Array(await event.data.arrayBuffer())
      : event.data instanceof ArrayBuffer ? new Uint8Array(event.data) : null;
    if (!data || data.byteLength === 0) return;
    if (data[0] === STDOUT) {
      const output = data.slice(1);
      for (const listener of this.listeners) listener(output);
    } else if (data[0] === EXIT && data.byteLength >= 6) {
      const signal = data[5];
      this.finish({ exitCode: new DataView(data.buffer, data.byteOffset, data.byteLength).getInt32(1, false), ...(signal ? { signal } : {}) });
      this.socket.close(1000, 'PTY exited');
    }
  }

  private finish(exit: PtyExit): void {
    if (this.settled) return;
    this.settled = true;
    this.resolveClosed(exit);
  }
}
