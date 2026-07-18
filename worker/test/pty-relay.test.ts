// ABOUTME: Tests binary PTY WebSocket/TCP relay framing and single-use upgrade tickets.
// ABOUTME: Covers fragmented Container frames, client input framing, and ticket replay rejection.

import Database from 'better-sqlite3';
import { describe, expect, it } from 'vitest';
import { createPtyTicket, consumePtyTicket } from '../src/pty-tickets';
import { encodePtyTransportFrame, relayPty } from '../src/pty-relay';
import { initSchema } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('PTY relay', () => {
  it('preserves binary frames in both directions', async () => {
    const outbound = encodePtyTransportFrame(Uint8Array.of(0x10, 1, 2, 3));
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(outbound.slice(0, 3));
        controller.enqueue(outbound.slice(3));
        setTimeout(() => controller.close(), 0);
      },
    });
    const inbound: Uint8Array[] = [];
    const writable = new WritableStream<Uint8Array>({ write(chunk) { inbound.push(chunk.slice()); } });
    const webSocket = new FakeWebSocket();
    const relay = relayPty(webSocket as unknown as WebSocket, {
      readable, writable, close: async () => undefined,
    });
    webSocket.dispatchEvent(new MessageEvent('message', { data: Uint8Array.of(0x00, 9).buffer }));
    await relay;
    expect(webSocket.sent).toEqual([Uint8Array.of(0x10, 1, 2, 3)]);
    expect(inbound).toEqual([encodePtyTransportFrame(Uint8Array.of(0x00, 9))]);
  });

  it('consumes tickets exactly once', () => {
    const storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
    const ticket = createPtyTicket(storage.sql);
    expect(consumePtyTicket(storage.sql, ticket.ticket)).toBe(true);
    expect(consumePtyTicket(storage.sql, ticket.ticket)).toBe(false);
  });
});

class FakeWebSocket extends EventTarget {
  sent: Uint8Array[] = [];
  accept(): void {}
  send(data: ArrayBuffer | ArrayBufferView): void {
    this.sent.push(data instanceof ArrayBuffer
      ? new Uint8Array(data)
      : new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  }
  close(): void { this.dispatchEvent(new CloseEvent('close')); }
}
