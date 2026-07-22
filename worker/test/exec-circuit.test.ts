// ABOUTME: Tests rolling failure thresholds, cooldown, and half-open recovery.
// ABOUTME: Uses an injected clock so circuit behavior is deterministic.

import { describe, expect, it } from 'vitest';
import { ExecCircuit } from '../src/exec-circuit';

describe('ExecCircuit', () => {
  it('opens after failures within the rolling window', () => {
    let now = 1_000;
    const circuit = new ExecCircuit({ threshold: 3, windowMs: 100, cooldownMs: 50, now: () => now });
    expect(circuit.admit()).toBe(0);
    circuit.recordFailure();
    now += 10;
    circuit.recordFailure();
    now += 10;
    expect(circuit.recordFailure()).toMatchObject({ state: 'open', failures: 3, retryAfterMs: 50 });
    expect(circuit.admit()).toBe(50);
  });

  it('forgets failures outside the rolling window', () => {
    let now = 1_000;
    const circuit = new ExecCircuit({ threshold: 2, windowMs: 100, cooldownMs: 50, now: () => now });
    circuit.recordFailure();
    now += 101;
    expect(circuit.recordFailure()).toMatchObject({ state: 'closed', failures: 1 });
  });

  it('allows one half-open attempt and closes after success', () => {
    let now = 1_000;
    const circuit = new ExecCircuit({ threshold: 1, windowMs: 100, cooldownMs: 50, now: () => now });
    circuit.recordFailure();
    now += 50;
    expect(circuit.admit()).toBe(0);
    expect(circuit.snapshot().state).toBe('half-open');
    expect(circuit.admit()).toBe(50);
    circuit.recordSuccess();
    expect(circuit.snapshot()).toMatchObject({ state: 'closed', failures: 0 });
  });

  it('reopens when the half-open attempt fails', () => {
    let now = 1_000;
    const circuit = new ExecCircuit({ threshold: 1, windowMs: 100, cooldownMs: 50, now: () => now });
    circuit.recordFailure();
    now += 50;
    circuit.admit();
    expect(circuit.recordFailure()).toMatchObject({ state: 'open', retryAfterMs: 50 });
  });

  it('releases an abandoned half-open claim after a bounded lease', () => {
    let now = 1_000;
    const circuit = new ExecCircuit({ threshold: 1, windowMs: 100, cooldownMs: 50, now: () => now });
    circuit.recordFailure();
    now += 50;
    expect(circuit.admit()).toBe(0);
    expect(circuit.admit()).toBe(50);
    now += 50;
    expect(circuit.admit()).toBe(0);
  });
});
