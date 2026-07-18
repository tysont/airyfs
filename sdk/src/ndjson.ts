// ABOUTME: Incrementally decodes bounded NDJSON streams for exec events.
// ABOUTME: Uses web-standard encoders and streams in every supported runtime.

const DEFAULT_MAX_LINE_BYTES = 1024 * 1024;
const encoder = new TextEncoder();

export class NdjsonError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NdjsonError';
  }
}

export class NdjsonDecoder<T = unknown> {
  private readonly decoder = new TextDecoder();
  private readonly maxLineBytes: number;
  private buffer = '';

  constructor(options: { maxLineBytes?: number } = {}) {
    this.maxLineBytes = options.maxLineBytes ?? DEFAULT_MAX_LINE_BYTES;
  }

  push(chunk: Uint8Array): T[] {
    this.buffer += this.decoder.decode(chunk, { stream: true });
    return this.drain();
  }

  flush(): T[] {
    this.buffer += this.decoder.decode();
    const trailing = this.buffer;
    this.buffer = '';
    this.bound(trailing);
    return trailing.trim() === '' ? [] : [this.parse(trailing)];
  }

  private drain(): T[] {
    const values: T[] = [];
    let newline = this.buffer.indexOf('\n');
    while (newline >= 0) {
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      this.bound(line);
      if (line.trim() !== '') values.push(this.parse(line));
      newline = this.buffer.indexOf('\n');
    }
    this.bound(this.buffer);
    return values;
  }

  private bound(line: string): void {
    if (encoder.encode(line).byteLength > this.maxLineBytes) {
      throw new NdjsonError(`NDJSON line exceeds ${this.maxLineBytes} bytes`);
    }
  }

  private parse(line: string): T {
    try {
      return JSON.parse(line) as T;
    } catch {
      throw new NdjsonError(`Malformed NDJSON line: ${line.slice(0, 120)}`);
    }
  }
}

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
