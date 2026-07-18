// ABOUTME: Verifies the incremental NDJSON decoder across arbitrary chunk boundaries.
// ABOUTME: Covers multi-line chunks, split lines, trailing lines, UTF-8 splits, and bounds.

import { describe, expect, it } from 'vitest';
import { NdjsonDecoder, NdjsonError, decodeNdjsonStream } from '../src/api/ndjson.js';

const encoder = new TextEncoder();

function bytes(text: string): Uint8Array {
  return encoder.encode(text);
}

describe('NdjsonDecoder', () => {
  it('parses multiple complete lines from one chunk', () => {
    const decoder = new NdjsonDecoder<{ n: number }>();
    expect(decoder.push(bytes('{"n":1}\n{"n":2}\n'))).toEqual([{ n: 1 }, { n: 2 }]);
    expect(decoder.flush()).toEqual([]);
  });

  it('reassembles a line split across arbitrary chunk boundaries', () => {
    const decoder = new NdjsonDecoder<{ hello: string }>();
    const full = '{"hello":"world"}\n';
    const collected: unknown[] = [];
    for (const character of full) collected.push(...decoder.push(bytes(character)));
    expect(collected).toEqual([{ hello: 'world' }]);
  });

  it('yields a trailing line without a newline only on flush', () => {
    const decoder = new NdjsonDecoder<{ done: boolean }>();
    expect(decoder.push(bytes('{"done":true}'))).toEqual([]);
    expect(decoder.flush()).toEqual([{ done: true }]);
  });

  it('handles a chunk that ends mid-line then continues', () => {
    const decoder = new NdjsonDecoder<{ a?: number; b?: number }>();
    expect(decoder.push(bytes('{"a":1}\n{"b'))).toEqual([{ a: 1 }]);
    expect(decoder.push(bytes('":2}\n'))).toEqual([{ b: 2 }]);
  });

  it('reassembles a multi-byte UTF-8 character split across chunks', () => {
    const decoder = new NdjsonDecoder<{ s: string }>();
    const payload = bytes('{"s":"\u2764"}\n'); // heart, 3 UTF-8 bytes
    const mid = payload.length - 3;
    expect(decoder.push(payload.subarray(0, mid))).toEqual([]);
    expect(decoder.push(payload.subarray(mid))).toEqual([{ s: '\u2764' }]);
  });

  it('ignores blank lines between records', () => {
    const decoder = new NdjsonDecoder<{ n: number }>();
    expect(decoder.push(bytes('{"n":1}\n\n{"n":2}\n'))).toEqual([{ n: 1 }, { n: 2 }]);
  });

  it('throws on a malformed line', () => {
    const decoder = new NdjsonDecoder();
    expect(() => decoder.push(bytes('not json\n'))).toThrow(NdjsonError);
  });

  it('bounds an unterminated line and rejects overflow', () => {
    const decoder = new NdjsonDecoder({ maxLineBytes: 16 });
    expect(() => decoder.push(bytes('{"x":"aaaaaaaaaaaaaaaaaaaa"'))).toThrow(NdjsonError);
  });

  it('rejects an oversized complete line delivered in one chunk', () => {
    const decoder = new NdjsonDecoder({ maxLineBytes: 16 });
    expect(() => decoder.push(bytes('{"x":"aaaaaaaaaaaaaaaaaaaa"}\n'))).toThrow(NdjsonError);
  });

  it('does not count completed lines against the line budget', () => {
    const decoder = new NdjsonDecoder<{ n: number }>({ maxLineBytes: 12 });
    // Many small complete lines stay under the per-line bound even though their
    // combined length exceeds it.
    expect(decoder.push(bytes('{"n":1}\n{"n":2}\n{"n":3}\n'))).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
  });
});

describe('decodeNdjsonStream', () => {
  it('decodes a whole stream into parsed values regardless of chunking', async () => {
    const parts = ['{"type":"start"', '}\n{"type":"exit"', ',"code":0}\n'];
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const part of parts) controller.enqueue(bytes(part));
        controller.close();
      },
    });

    const collected: unknown[] = [];
    for await (const event of decodeNdjsonStream(stream)) collected.push(event);
    expect(collected).toEqual([{ type: 'start' }, { type: 'exit', code: 0 }]);
  });
});
