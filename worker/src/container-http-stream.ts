// ABOUTME: Opens a streaming HTTP response over a Container TCP port without fetch proxy buffering.
// ABOUTME: Uses HTTP/1.0 connection-close framing so the response body can pass through incrementally.

const MAX_RESPONSE_HEADER_BYTES = 64 * 1024;

export interface ContainerSocket {
  opened: Promise<unknown>;
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  close(): Promise<void>;
}

export interface ContainerHttpStreamResponse {
  status: number;
  body: ReadableStream<Uint8Array>;
}

export async function postContainerHttpStream(
  socket: ContainerSocket,
  path: string,
  payload: unknown,
  signal: AbortSignal
): Promise<ContainerHttpStreamResponse> {
  signal.throwIfAborted();
  await socket.opened;

  const body = new TextEncoder().encode(JSON.stringify(payload));
  const header = new TextEncoder().encode([
    `POST ${path} HTTP/1.0`,
    'Host: localhost',
    'Content-Type: application/json',
    `Content-Length: ${body.byteLength}`,
    'Connection: close',
    '',
    '',
  ].join('\r\n'));
  const request = new Uint8Array(header.byteLength + body.byteLength);
  request.set(header);
  request.set(body, header.byteLength);

  const writer = socket.writable.getWriter();
  try {
    await writer.write(request);
  } catch (error) {
    await socket.close().catch(() => undefined);
    throw error;
  } finally {
    writer.releaseLock();
  }

  const reader = socket.readable.getReader();
  let buffered = new Uint8Array(0);
  let bodyStart = -1;
  while (bodyStart < 0) {
    const { done, value } = await reader.read();
    if (done) {
      await socket.close().catch(() => undefined);
      throw new Error('Container closed before sending an HTTP response');
    }
    const combined = new Uint8Array(buffered.byteLength + value.byteLength);
    combined.set(buffered);
    combined.set(value, buffered.byteLength);
    buffered = combined;
    bodyStart = findHeaderEnd(buffered);
    if (bodyStart < 0 && buffered.byteLength > MAX_RESPONSE_HEADER_BYTES) {
      await reader.cancel().catch(() => undefined);
      await socket.close().catch(() => undefined);
      throw new Error('Container HTTP response header exceeds 64 KiB');
    }
  }

  const headerText = new TextDecoder().decode(buffered.subarray(0, bodyStart - 4));
  const match = /^HTTP\/\d\.\d (\d{3})(?: |\r?$)/m.exec(headerText);
  if (!match) {
    await reader.cancel().catch(() => undefined);
    await socket.close().catch(() => undefined);
    throw new Error('Container returned a malformed HTTP status line');
  }

  let initial = buffered.subarray(bodyStart);
  let closed = false;
  const close = async (): Promise<void> => {
    if (closed) return;
    closed = true;
    signal.removeEventListener('abort', onAbort);
    await socket.close().catch(() => undefined);
  };
  const onAbort = (): void => {
    void reader.cancel(signal.reason).catch(() => undefined);
    void close();
  };
  signal.addEventListener('abort', onAbort, { once: true });

  return {
    status: Number(match[1]),
    body: new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          if (initial.byteLength > 0) {
            const chunk = initial;
            initial = new Uint8Array(0);
            controller.enqueue(chunk);
            return;
          }
          const { done, value } = await reader.read();
          if (done) {
            controller.close();
            await close();
          } else {
            controller.enqueue(value);
          }
        } catch (error) {
          controller.error(error);
          await close();
        }
      },
      async cancel(reason) {
        await reader.cancel(reason).catch(() => undefined);
        await close();
      },
    }),
  };
}

function findHeaderEnd(bytes: Uint8Array): number {
  for (let i = 3; i < bytes.byteLength; i++) {
    if (bytes[i - 3] === 13 && bytes[i - 2] === 10 && bytes[i - 1] === 13 && bytes[i] === 10) {
      return i + 1;
    }
  }
  return -1;
}
