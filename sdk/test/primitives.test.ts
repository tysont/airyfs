// ABOUTME: Pins runtime-neutral path and bounded NDJSON behavior in the SDK.
// ABOUTME: Guards browser/Worker compatibility by exercising only web-standard primitives.

import { describe, expect, it } from 'vitest';
import { NdjsonDecoder, NdjsonError, encodeRemotePath, remoteBasename, remoteDirname, resolveRemotePath } from '../src/index.js';

describe('remote paths', () => {
  it('normalizes without escaping root and encodes segments independently', () => {
    expect(resolveRemotePath('/a/b', '../../../../../c')).toBe('/c');
    expect(encodeRemotePath('/hello world/a#b')).toBe('hello%20world/a%23b');
    expect(remoteBasename('/a/b/')).toBe('b');
    expect(remoteDirname('/a/b/')).toBe('/a');
  });
});

describe('NdjsonDecoder', () => {
  it('decodes split lines and rejects malformed or oversized trailing input', () => {
    const decoder = new NdjsonDecoder<{ n: number }>();
    expect(decoder.push(new TextEncoder().encode('{"n":'))).toEqual([]);
    expect(decoder.push(new TextEncoder().encode('1}\n'))).toEqual([{ n: 1 }]);
    expect(() => new NdjsonDecoder({ maxLineBytes: 2 }).push(new TextEncoder().encode('123')))
      .toThrow(NdjsonError);
    const malformed = new NdjsonDecoder();
    malformed.push(new TextEncoder().encode('{'));
    expect(() => malformed.flush()).toThrow(NdjsonError);
  });
});
