// ABOUTME: Monitors Container control-plane liveness while a buffered command runs.
// ABOUTME: Trips only after consecutive probe failures and never retries command execution.

export interface ExecWatchdogOptions {
  signal: AbortSignal;
  initialDelayMs: number;
  intervalMs: number;
  maxFailures: number;
  probe: () => Promise<boolean>;
}

/** Resolve true after consecutive liveness failures, or false when stopped. */
export async function monitorExecLiveness(options: ExecWatchdogOptions): Promise<boolean> {
  if (!await delay(options.initialDelayMs, options.signal)) return false;

  let failures = 0;
  while (!options.signal.aborted) {
    try {
      failures = await options.probe() ? 0 : failures + 1;
    } catch {
      failures++;
    }
    if (failures >= options.maxFailures) return true;
    if (!await delay(options.intervalMs, options.signal)) return false;
  }
  return false;
}

function delay(ms: number, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve(true);
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve(false);
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}
