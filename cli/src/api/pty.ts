// ABOUTME: Implements the CLI's binary WebSocket client for interactive PTY sessions.
// ABOUTME: Keeps terminal framing separate from command registration and transport authentication.

export interface PtyExit { exitCode: number; signal?: number }
export interface PtySession {
  write(data: Uint8Array): void;
  resize(cols: number, rows: number): void;
  onData(listener: (data: Uint8Array) => void): () => void;
  readonly closed: Promise<PtyExit>;
  close(): void;
}

export async function connectPty(url: URL, WebSocketImpl: typeof WebSocket = WebSocket): Promise<PtySession> {
  const socket = new WebSocketImpl(url);
  socket.binaryType = 'arraybuffer';
  await new Promise<void>((resolve, reject) => {
    socket.addEventListener('open', () => resolve(), { once: true });
    socket.addEventListener('error', () => reject(new Error('PTY WebSocket connection failed')), { once: true });
  });
  return new Session(socket);
}

class Session implements PtySession {
  private readonly listeners = new Set<(data: Uint8Array) => void>();
  readonly closed: Promise<PtyExit>;
  private resolveClosed!: (exit: PtyExit) => void;
  private settled = false;

  constructor(private readonly socket: WebSocket) {
    this.closed = new Promise((resolve) => { this.resolveClosed = resolve; });
    socket.addEventListener('message', (event) => { void this.message(event); });
    socket.addEventListener('close', () => this.finish({ exitCode: 1 }));
  }

  write(data: Uint8Array): void { this.send(0x00, data); }
  resize(cols: number, rows: number): void {
    const payload = new Uint8Array(4);
    const view = new DataView(payload.buffer);
    view.setUint16(0, cols, false);
    view.setUint16(2, rows, false);
    this.send(0x01, payload);
  }
  onData(listener: (data: Uint8Array) => void): () => void { this.listeners.add(listener); return () => this.listeners.delete(listener); }
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
    if (data[0] === 0x10) for (const listener of this.listeners) listener(data.slice(1));
    else if (data[0] === 0x11 && data.byteLength >= 6) {
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
