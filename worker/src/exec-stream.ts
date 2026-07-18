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
