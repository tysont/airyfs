// ABOUTME: Tests conservative consecutive-failure handling for the exec watchdog.
// ABOUTME: Verifies recovery, cancellation, and failure thresholds with fake timers.

import { afterEach, describe, expect, it, vi } from 'vitest';
import { monitorExecLiveness } from '../src/exec-watchdog';

afterEach(() => vi.useRealTimers());

describe('monitorExecLiveness', () => {
  it('trips after consecutive failed probes', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const probe = vi.fn().mockResolvedValue(false);
    const result = monitorExecLiveness({
      signal: controller.signal,
      initialDelayMs: 10,
      intervalMs: 5,
      maxFailures: 3,
      probe,
    });

    await vi.advanceTimersByTimeAsync(20);

    await expect(result).resolves.toBe(true);
    expect(probe).toHaveBeenCalledTimes(3);
  });

  it('resets the failure count after a successful probe', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const probe = vi.fn()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(false);
    const result = monitorExecLiveness({
      signal: controller.signal,
      initialDelayMs: 1,
      intervalMs: 1,
      maxFailures: 3,
      probe,
    });

    await vi.advanceTimersByTimeAsync(5);

    await expect(result).resolves.toBe(true);
    expect(probe).toHaveBeenCalledTimes(5);
  });

  it('stops without tripping when aborted', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const probe = vi.fn().mockResolvedValue(true);
    const result = monitorExecLiveness({
      signal: controller.signal,
      initialDelayMs: 10,
      intervalMs: 5,
      maxFailures: 3,
      probe,
    });

    controller.abort();
    await expect(result).resolves.toBe(false);
    expect(probe).not.toHaveBeenCalled();
  });
});
