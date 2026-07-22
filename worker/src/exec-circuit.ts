// ABOUTME: Bounds repeated Container infrastructure failures with a rolling circuit breaker.
// ABOUTME: Allows one half-open recovery attempt after cooldown and never retries commands.

export interface ExecCircuitOptions {
  threshold: number;
  windowMs: number;
  cooldownMs: number;
  now?: () => number;
}

export interface ExecCircuitSnapshot {
  state: 'closed' | 'open' | 'half-open';
  failures: number;
  retryAfterMs: number;
}

export class ExecCircuit {
  private failures: number[] = [];
  private openUntil = 0;
  private halfOpenClaimed = false;
  private halfOpenLeaseUntil = 0;
  private readonly now: () => number;

  constructor(private readonly options: ExecCircuitOptions) {
    this.now = options.now ?? Date.now;
  }

  /** Return retry delay, or zero after claiming permission for this attempt. */
  admit(): number {
    const now = this.now();
    this.prune(now);
    if (this.openUntil > now) return this.openUntil - now;
    if (this.openUntil > 0) {
      if (this.halfOpenClaimed && this.halfOpenLeaseUntil > now) return this.halfOpenLeaseUntil - now;
      this.halfOpenClaimed = true;
      this.halfOpenLeaseUntil = now + this.options.cooldownMs;
    }
    return 0;
  }

  recordFailure(): ExecCircuitSnapshot {
    const now = this.now();
    this.prune(now);
    this.failures.push(now);
    if (this.halfOpenClaimed || this.failures.length >= this.options.threshold) {
      this.openUntil = now + this.options.cooldownMs;
      this.halfOpenClaimed = false;
      this.halfOpenLeaseUntil = 0;
    }
    return this.snapshot();
  }

  recordSuccess(): void {
    if (!this.halfOpenClaimed) return;
    this.failures = [];
    this.openUntil = 0;
    this.halfOpenClaimed = false;
    this.halfOpenLeaseUntil = 0;
  }

  snapshot(): ExecCircuitSnapshot {
    const now = this.now();
    this.prune(now);
    return {
      state: this.openUntil > now ? 'open' : this.openUntil > 0 ? 'half-open' : 'closed',
      failures: this.failures.length,
      retryAfterMs: Math.max(0, this.openUntil - now),
    };
  }

  private prune(now: number): void {
    const cutoff = now - this.options.windowMs;
    this.failures = this.failures.filter((timestamp) => timestamp >= cutoff);
  }
}
