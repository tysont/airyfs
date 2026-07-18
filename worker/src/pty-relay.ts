// ABOUTME: Relays binary PTY frames between a client WebSocket and Container TCP port 4001.
// ABOUTME: Preserves frame boundaries, applies backpressure, and closes both sides exactly once.

const MAX_FRAME_BYTES = 1024 * 1024;

interface PtySocket {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  close(): Promise<void>;
}

export function encodePtyTransportFrame(payload: Uint8Array): Uint8Array {
  if (payload.byteLength > MAX_FRAME_BYTES) throw new Error('PTY frame too large');
  const frame = new Uint8Array(4 + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, payload.byteLength, false);
  frame.set(payload, 4);
  return frame;
}

export async function relayPty(webSocket: WebSocket, socket: PtySocket): Promise<void> {
  webSocket.accept();
  const writer = socket.writable.getWriter();
  let writes = Promise.resolve();
  let closed = false;
  const close = (): void => {
    if (closed) return;
    closed = true;
    try { webSocket.close(1000, 'PTY session ended'); } catch { /* already closed */ }
    void socket.close().catch(() => undefined);
  };
  const onMessage = (event: MessageEvent): void => {
    const data = event.data;
    const payload = data instanceof ArrayBuffer
      ? new Uint8Array(data)
      : ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : null;
    if (!payload) {
      webSocket.close(1003, 'Binary PTY frames required');
      return;
    }
    writes = writes.then(() => writer.write(encodePtyTransportFrame(payload)));
  };
  webSocket.addEventListener('message', onMessage);
  webSocket.addEventListener('close', close, { once: true });
  webSocket.addEventListener('error', close, { once: true });

  const reader = socket.readable.getReader();
  let buffered = new Uint8Array();
  try {
    while (!closed) {
      const { value, done } = await reader.read();
      if (done) break;
      const next = new Uint8Array(buffered.byteLength + value.byteLength);
      next.set(buffered);
      next.set(value, buffered.byteLength);
      buffered = next;
      while (buffered.byteLength >= 4) {
        const length = new DataView(buffered.buffer, buffered.byteOffset, buffered.byteLength).getUint32(0, false);
        if (length > MAX_FRAME_BYTES) throw new Error('PTY frame too large');
        if (buffered.byteLength < 4 + length) break;
        webSocket.send(buffered.slice(4, 4 + length));
        buffered = buffered.slice(4 + length);
      }
    }
    await writes;
  } finally {
    webSocket.removeEventListener('message', onMessage);
    reader.releaseLock();
    writer.releaseLock();
    close();
  }
}
