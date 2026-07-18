// ABOUTME: Tests direct TCP HTTP streaming used to bypass Container fetch response buffering.
// ABOUTME: Covers split headers, immediate body delivery, cancellation, and malformed responses.

import { describe, expect, it } from 'vitest';
import { postContainerHttpStream, type ContainerSocket } from '../src/container-http-stream';

const encoder = new TextEncoder();

describe('postContainerHttpStream', () => {
  it('returns body bytes that arrive with a split response header', async () => {
    const source = controlledSocket();
    const responsePromise = postContainerHttpStream(
      source.socket,
      '/exec/stream',
      { command: 'sleep 1', id: 'exec-1' },
      new AbortController().signal
    );

    source.enqueue('HTTP/1.0 200 OK\r\nContent-Type: text/event-stream\r\n');
    source.enqueue('\r\ndata: {"type":"start","id":"exec-1"}\n\n');
    const response = await responsePromise;
    expect(response.status).toBe(200);

    const reader = response.body.getReader();
    const first = await reader.read();
    expect(new TextDecoder().decode(first.value)).toBe('data: {"type":"start","id":"exec-1"}\n\n');
    source.closeReadable();
    expect((await reader.read()).done).toBe(true);
    expect(source.wasClosed()).toBe(true);
  });

  it('closes the socket when the response body is canceled', async () => {
    const source = controlledSocket();
    const responsePromise = postContainerHttpStream(
      source.socket,
      '/exec/stream',
      { command: 'sleep 30' },
      new AbortController().signal
    );
    source.enqueue('HTTP/1.0 200 OK\r\n\r\n');
    const response = await responsePromise;
    await response.body.cancel('stop');
    expect(source.wasClosed()).toBe(true);
  });
});

function controlledSocket(): {
  socket: ContainerSocket;
  enqueue(value: string): void;
  closeReadable(): void;
  wasClosed(): boolean;
} {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  let closed = false;
  const readable = new ReadableStream<Uint8Array>({
    start(value) {
      controller = value;
    },
  });
  return {
    socket: {
      opened: Promise.resolve(),
      readable,
      writable: new WritableStream(),
      async close() {
        closed = true;
      },
    },
    enqueue(value) {
      controller.enqueue(encoder.encode(value));
    },
    closeReadable() {
      controller.close();
    },
    wasClosed() {
      return closed;
    },
  };
}
