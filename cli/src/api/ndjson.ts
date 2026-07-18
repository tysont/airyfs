// ABOUTME: Incremental NDJSON decoder tolerant of arbitrary chunk boundaries.
// ABOUTME: Bounds line length so a runaway server cannot exhaust client memory.

const DEFAULT_MAX_LINE_BYTES = 1024 * 1024;

export class NdjsonError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NdjsonError';
  }
}

/**
 * Feed raw byte chunks and receive parsed JSON values, split on newlines.
 *
 * Handles lines that span multiple chunks and multiple lines within one chunk.
 * A single line exceeding `maxLineBytes` (measured before UTF-8 decoding) throws
 * rather than buffering without bound. `flush` yields any trailing unterminated
 * line so callers do not silently drop a final value.
 */
export class NdjsonDecoder<T = unknown> {
  private readonly decoder = new TextDecoder();
  private readonly maxLineBytes: number;
  private buffer = '';
  private bufferBytes = 0;

  constructor(options: { maxLineBytes?: number } = {}) {
    this.maxLineBytes = options.maxLineBytes ?? DEFAULT_MAX_LINE_BYTES;
  }

  /** Decode a chunk and return every complete line parsed as JSON. */
  push(chunk: Uint8Array): T[] {
    this.bufferBytes += chunk.byteLength;
    this.buffer += this.decoder.decode(chunk, { stream: true });
    return this.drain();
  }

  /** Return the trailing line, if any, once the stream has ended. */
  flush(): T[] {
    this.buffer += this.decoder.decode();
    const trailing = this.buffer;
    this.buffer = '';
    this.bufferBytes = 0;
    if (!trailing) return [];
    if (trailing.trim() === '') return [];
    return [this.parse(trailing)];
  }

  private drain(): T[] {
    const values: T[] = [];
    let newline = this.buffer.indexOf('\n');
    while (newline >= 0) {
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      if (Buffer.byteLength(line, 'utf8') > this.maxLineBytes) {
        throw new NdjsonError(`NDJSON line exceeds ${this.maxLineBytes} bytes`);
      }
      // A completed line no longer counts against the pending-line budget.
      this.bufferBytes = Buffer.byteLength(this.buffer, 'utf8');
      if (line.trim() !== '') values.push(this.parse(line));
      newline = this.buffer.indexOf('\n');
    }
    if (this.bufferBytes > this.maxLineBytes) {
      throw new NdjsonError(`NDJSON line exceeds ${this.maxLineBytes} bytes`);
    }
    return values;
  }

  private parse(line: string): T {
    try {
      return JSON.parse(line) as T;
    } catch {
      throw new NdjsonError(`Malformed NDJSON line: ${line.slice(0, 120)}`);
    }
  }
}

/** Decode a whole byte stream into parsed NDJSON values as an async iterable. */
export async function* decodeNdjsonStream<T = unknown>(
  stream: ReadableStream<Uint8Array>,
  options?: { maxLineBytes?: number },
): AsyncGenerator<T> {
  const decoder = new NdjsonDecoder<T>(options);
  const reader = stream.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) for (const parsed of decoder.push(value)) yield parsed;
    }
    for (const parsed of decoder.flush()) yield parsed;
  } finally {
    reader.releaseLock();
  }
}
