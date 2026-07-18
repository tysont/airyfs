// ABOUTME: Verifies incremental internal-SSE to public-NDJSON translation.
// ABOUTME: Covers arbitrary byte boundaries, CRLF, incomplete frames, and size bounds.

import { describe, expect, it } from 'vitest';
import { sseToNdjson, SseStreamError } from '../src/sse-stream';

describe('sseToNdjson', () => {
  it('translates split SSE data frames and ignores comments', async () => {
    const source = stream([
      'data: {"type":"sta',
      'rt","id":"x"}\r\n\r\n: keepalive\n\n',
      'data: {"type":"exit","id":"x","exitCode":0}\n\n',
    ]);
    expect(await new Response(sseToNdjson(source)).text()).toBe(
      '{"type":"start","id":"x"}\n{"type":"exit","id":"x","exitCode":0}\n',
    );
  });

  it('rejects incomplete and oversized frames', async () => {
    await expect(new Response(sseToNdjson(stream(['data: {']))).text()).rejects.toBeInstanceOf(SseStreamError);
    await expect(new Response(sseToNdjson(stream([`data: ${'x'.repeat(1024 * 1024 + 1)}`]))).text())
      .rejects.toBeInstanceOf(SseStreamError);
  });

  it('filters internal heartbeat events', async () => {
    const source = stream([
      'data: {"type":"start","id":"one"}\n\n',
      'event: heartbeat\ndata: {}\n\n',
      'data: {"type":"exit","id":"one","exitCode":0}\n\n',
    ]);
    expect(await new Response(sseToNdjson(source)).text()).toBe(
      '{"type":"start","id":"one"}\n\n{"type":"exit","id":"one","exitCode":0}\n',
    );
  });
});

function stream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}
