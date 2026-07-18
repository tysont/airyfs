// ABOUTME: Tests exec streaming route parsing and the single-flight stream-hold helper.
// ABOUTME: Covers the /exec/cancel suffix and release-exactly-once stream semantics.

import { describe, expect, it, vi } from 'vitest';
import { HttpError, parseV1Route } from '../src/files-api';
import { holdStreamUntilDone } from '../src/exec-stream';

describe('parseV1Route exec routes', () => {
  it('parses the exec resource without a suffix', () => {
    expect(parseV1Route('/v1/volumes/vol/exec')).toEqual({
      volume: 'vol', resource: 'exec', path: '/',
    });
  });

  it('accepts the cancel suffix on exec', () => {
    expect(parseV1Route('/v1/volumes/vol/exec/cancel')).toEqual({
      volume: 'vol', resource: 'exec', path: '/cancel',
    });
  });

  it('rejects any other exec suffix', () => {
    expect(() => parseV1Route('/v1/volumes/vol/exec/other')).toThrow(HttpError);
    expect(() => parseV1Route('/v1/volumes/vol/exec/cancel/extra')).toThrow(HttpError);
  });
});

function streamOf(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  let index = 0;
  return new ReadableStream({
    pull(controller) {
      if (index < chunks.length) controller.enqueue(chunks[index++]);
      else controller.close();
    },
  });
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<Uint8Array[]> {
  const reader = stream.getReader();
  const out: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    out.push(value);
  }
  return out;
}

describe('holdStreamUntilDone', () => {
  it('passes bytes through and releases once when the source drains', async () => {
    const release = vi.fn();
    const held = holdStreamUntilDone(streamOf([Uint8Array.of(1, 2), Uint8Array.of(3)]), release);

    const chunks = await drain(held);

    expect(Buffer.concat(chunks)).toEqual(Buffer.from([1, 2, 3]));
    expect(release).toHaveBeenCalledTimes(1);
  });

  it('releases once and cancels the source when the consumer cancels', async () => {
    const release = vi.fn();
    const cancel = vi.fn();
    const source = new ReadableStream<Uint8Array>({
      pull(controller) { controller.enqueue(Uint8Array.of(9)); },
      cancel(reason) { cancel(reason); },
    });

    const held = holdStreamUntilDone(source, release);
    const reader = held.getReader();
    await reader.read();
    await reader.cancel('stop');

    expect(release).toHaveBeenCalledTimes(1);
    expect(cancel).toHaveBeenCalledWith('stop');
  });

  it('releases when the source errors and surfaces the error', async () => {
    const release = vi.fn();
    const source = new ReadableStream<Uint8Array>({
      pull() { throw new Error('boom'); },
    });

    const held = holdStreamUntilDone(source, release);

    await expect(drain(held)).rejects.toThrow('boom');
    expect(release).toHaveBeenCalledTimes(1);
  });
});
