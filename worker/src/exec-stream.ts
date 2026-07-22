// ABOUTME: Streaming-exec helpers kept free of Workers runtime imports for testability.
// ABOUTME: Holds the exec single-flight token for the full lifetime of a returned stream.

/**
 * Re-expose a source stream while running `release` exactly once when it drains,
 * errors, or is canceled. Used to keep the exec single-flight token held for the
 * full lifetime of a streaming command rather than only until the fetch returns.
 */
export function holdStreamUntilDone(
  source: ReadableStream<Uint8Array>,
  release: () => void
): ReadableStream<Uint8Array> {
  let released = false;
  const releaseOnce = (): void => {
    if (released) return;
    released = true;
    release();
  };
  const reader = source.getReader();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          releaseOnce();
          return;
        }
        controller.enqueue(value);
      } catch (error) {
        releaseOnce();
        controller.error(error);
      }
    },
    async cancel(reason) {
      releaseOnce();
      await reader.cancel(reason).catch(() => undefined);
    },
  });
}

export class ExecStreamLostError extends Error {
  constructor(message = 'Command outcome is unknown because the Container stream was lost') {
    super(message);
    this.name = 'ExecStreamLostError';
  }
}

export interface HeartbeatOptions {
  timeoutMs: number;
  onFailure: (error: ExecStreamLostError) => void | Promise<void>;
  onComplete?: () => void;
}

/** Fail a stream when neither output nor heartbeat bytes arrive before the deadline. */
export function enforceStreamHeartbeat(
  source: ReadableStream<Uint8Array>,
  options: HeartbeatOptions,
): ReadableStream<Uint8Array> {
  const reader = source.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let settled = false;
  let controllerRef: ReadableStreamDefaultController<Uint8Array>;

  const clear = (): void => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const fail = async (error: ExecStreamLostError): Promise<void> => {
    if (settled) return;
    settled = true;
    clear();
    await Promise.resolve(options.onFailure(error)).catch(() => undefined);
    controllerRef.error(error);
    await reader.cancel(error).catch(() => undefined);
  };
  const arm = (): void => {
    clear();
    timer = setTimeout(() => {
      void fail(new ExecStreamLostError('Command outcome is unknown because Container heartbeats stopped'));
    }, options.timeoutMs);
  };

  return new ReadableStream<Uint8Array>({
    start(controller) {
      controllerRef = controller;
      arm();
    },
    async pull(controller) {
      if (settled) return;
      try {
        const { done, value } = await reader.read();
        if (settled) return;
        if (done) {
          settled = true;
          clear();
          options.onComplete?.();
          controller.close();
          return;
        }
        arm();
        controller.enqueue(value);
      } catch {
        await fail(new ExecStreamLostError());
      }
    },
    async cancel(reason) {
      if (settled) return;
      settled = true;
      clear();
      options.onComplete?.();
      await reader.cancel(reason).catch(() => undefined);
    },
  });
}
