// ABOUTME: Translates the Container's proxy-flushed SSE data frames into public NDJSON.
// ABOUTME: Preserves streaming and cancellation while bounding incomplete frame memory.

const MAX_SSE_BUFFER_BYTES = 1024 * 1024;

export class SseStreamError extends Error {}

export function sseToNdjson(source: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = '';

  const drain = (controller: TransformStreamDefaultController<Uint8Array>): void => {
    buffer = buffer.replaceAll('\r\n', '\n');
    let boundary = buffer.indexOf('\n\n');
    while (boundary >= 0) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const event = frame.split('\n').find((line) => line.startsWith('event:'))
        ?.slice(6).trim();
      const data = frame.split('\n')
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).replace(/^ /, ''))
        .join('\n');
      if (event === 'heartbeat') controller.enqueue(encoder.encode('\n'));
      else if (data) controller.enqueue(encoder.encode(`${data}\n`));
      boundary = buffer.indexOf('\n\n');
    }
    if (encoder.encode(buffer).byteLength > MAX_SSE_BUFFER_BYTES) {
      throw new SseStreamError('Container SSE frame exceeds 1 MiB');
    }
  };

  return source.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      buffer += decoder.decode(chunk, { stream: true });
      drain(controller);
    },
    flush(controller) {
      buffer += decoder.decode();
      drain(controller);
      if (buffer.trim()) throw new SseStreamError('Container SSE stream ended with an incomplete frame');
    },
  }));
}
