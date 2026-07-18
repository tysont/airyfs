// ABOUTME: Tests SDK PTY frame encoding, terminal output delivery, resize, and exit handling.
// ABOUTME: Uses an in-memory WebSocket double so the browser-compatible API stays dependency-free.

import { describe, expect, it } from 'vitest';
import { AiryFSClient, openPty } from '../src/index.js';

describe('openPty', () => {
  it('uses a ticket and exchanges binary terminal frames', async () => {
    const client = new AiryFSClient('https://example.com', 'my volume', {
      fetch: async () => Response.json({ ticket: 'one-time', expiresAt: 1 }),
    });
    const session = await openPty(client, { webSocket: FakeWebSocket as unknown as typeof WebSocket });
    const socket = FakeWebSocket.last!;
    expect(socket.url.toString()).toContain('wss://example.com/v1/volumes/my%20volume/exec/pty?ticket=one-time');
    const output: Uint8Array[] = [];
    session.onData((data) => output.push(data));
    session.write('x');
    session.resize(100, 40);
    expect(socket.sent[0]).toEqual(Uint8Array.of(0x00, 120));
    expect(socket.sent[1]).toEqual(Uint8Array.of(0x01, 0, 100, 0, 40));

    socket.emit(Uint8Array.of(0x10, 1, 2));
    const exit = new Uint8Array(6);
    exit[0] = 0x11;
    new DataView(exit.buffer).setInt32(1, 7, false);
    socket.emit(exit);
    await expect(session.closed).resolves.toEqual({ exitCode: 7 });
    expect(output).toEqual([Uint8Array.of(1, 2)]);
  });
});

class FakeWebSocket extends EventTarget {
  static last?: FakeWebSocket;
  readonly sent: Uint8Array[] = [];
  binaryType = 'blob';
  constructor(readonly url: string | URL) {
    super();
    FakeWebSocket.last = this;
    queueMicrotask(() => this.dispatchEvent(new Event('open')));
  }
  send(data: ArrayBuffer | ArrayBufferView): void {
    this.sent.push(data instanceof ArrayBuffer
      ? new Uint8Array(data)
      : new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice());
  }
  close(): void { this.dispatchEvent(new Event('close')); }
  emit(data: Uint8Array): void { this.dispatchEvent(new MessageEvent('message', { data: data.buffer })); }
}
