// ABOUTME: Tests CLI PTY ticket exchange and binary WebSocket framing independently of a terminal.
// ABOUTME: Verifies endpoint conversion, input, resize, output, and remote exit status.

import { describe, expect, it } from 'vitest';
import { AiryFSClient } from '../src/api/client.js';

describe('CLI PTY client', () => {
  it('opens a ticketed session and exchanges terminal frames', async () => {
    const client = new AiryFSClient('https://example.com', 'vol', async () => Response.json({ ticket: 't' }));
    const session = await client.openPty(FakeWebSocket as unknown as typeof WebSocket);
    const socket = FakeWebSocket.last!;
    expect(socket.url.toString()).toBe('wss://example.com/v1/volumes/vol/exec/pty?ticket=t');
    const output: Uint8Array[] = [];
    session.onData((data) => output.push(data));
    session.write(Uint8Array.of(1));
    session.resize(80, 24);
    expect(socket.sent).toEqual([Uint8Array.of(0, 1), Uint8Array.of(1, 0, 80, 0, 24)]);
    socket.emit(Uint8Array.of(0x10, 2));
    const exit = new Uint8Array(6);
    exit[0] = 0x11;
    new DataView(exit.buffer).setInt32(1, 3, false);
    socket.emit(exit);
    await expect(session.closed).resolves.toEqual({ exitCode: 3 });
    expect(output).toEqual([Uint8Array.of(2)]);
  });
});

class FakeWebSocket extends EventTarget {
  static last?: FakeWebSocket;
  sent: Uint8Array[] = [];
  binaryType = 'blob';
  constructor(readonly url: string | URL) { super(); FakeWebSocket.last = this; queueMicrotask(() => this.dispatchEvent(new Event('open'))); }
  send(data: ArrayBuffer | ArrayBufferView): void {
    this.sent.push(data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice());
  }
  close(): void { this.dispatchEvent(new Event('close')); }
  emit(data: Uint8Array): void { this.dispatchEvent(new MessageEvent('message', { data: data.buffer })); }
}
