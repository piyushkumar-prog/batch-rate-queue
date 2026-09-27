import { RateLimitConfig } from '../types';

/**
 * Token Bucket Rate Limiter
 *
 * Controls the rate of outbound API calls using the token bucket algorithm.
 * Tokens refill at a fixed rate. Each `acquire()` call consumes tokens (default 1).
 * If the bucket doesn't have enough tokens, the caller is queued and waits.
 *
 * Supports:
 * - **Cost-weighted acquisition**: `acquire(cost)` consumes `cost` tokens per call
 * - **Runtime reconfiguration**: `setRate()` changes the rate without restarting
 * - **Jitter**: ±10% on refill interval to prevent thundering-herd synchronization
 */
export class Throttler {
  protected tokens: number;
  protected maxTokens: number;
  protected refillIntervalMs: number;
  protected refillTimer: ReturnType<typeof setInterval> | null = null;
  protected waitQueue: Array<{ resolve: () => void; cost: number }> = [];
  protected destroyed = false;
  protected perMs: number;

  constructor(config: RateLimitConfig) {
    this.maxTokens = config.requests;
    this.tokens = config.requests;
    this.perMs = config.perMs;
    // How often to add one token: spread the allowed requests evenly across the window
    this.refillIntervalMs = config.perMs / config.requests;

    this.startRefill();
  }

  /**
   * Acquire rate-limit tokens.
   * Resolves immediately if enough tokens are available, otherwise waits.
   *
   * @param cost Number of tokens to consume. Defaults to 1 for backward compatibility.
   *             When costBased rate limiting is enabled, this represents the job's
   *             cost (e.g., estimated token count for LLM APIs).
   */
  async acquire(cost: number = 1): Promise<void> {
    if (this.destroyed) {
      throw new Error('Throttler has been destroyed');
    }

    if (cost <= 0) {
      return; // Zero or negative cost is free
    }

    if (cost > this.maxTokens) {
      throw new Error(
        `Cost ${cost} exceeds bucket capacity ${this.maxTokens}. ` +
        `A single job cannot cost more than the bucket's max tokens.`
      );
    }

    if (this.tokens >= cost) {
      this.tokens -= cost;
      return;
    }

    // Not enough tokens — queue the caller
    return new Promise<void>((resolve) => {
      this.waitQueue.push({ resolve, cost });
    });
  }

  /**
   * Get the number of currently available tokens.
   */
  getAvailableTokens(): number {
    return this.tokens;
  }

  /**
   * Get the number of callers waiting for tokens.
   */
  getWaitingCount(): number {
    return this.waitQueue.length;
  }

  /**
   * Get the current effective rate configuration.
   */
  getEffectiveRate(): { requests: number; perMs: number } {
    return { requests: this.maxTokens, perMs: this.perMs };
  }

  /**
   * Reconfigure the rate limit at runtime.
   * Stops the current refill timer and starts a new one with updated parameters.
   * Existing queued waiters are preserved and served under the new rate.
   */
  setRate(requests: number, perMs: number): void {
    if (requests <= 0 || perMs <= 0) {
      throw new Error('Rate must have positive requests and perMs');
    }

    this.maxTokens = requests;
    this.perMs = perMs;
    this.refillIntervalMs = perMs / requests;

    // Clamp current tokens to new max
    if (this.tokens > this.maxTokens) {
      this.tokens = this.maxTokens;
    }

    // Restart refill timer with new interval
    this.stopRefill();
    this.startRefill();
  }

  /**
   * Stop the refill timer and release any pending waiters.
   */
  destroy(): void {
    this.destroyed = true;
    this.stopRefill();
    // Release all waiters so they don't hang forever
    this.waitQueue.forEach(({ resolve }) => resolve());
    this.waitQueue = [];
  }

  /**
   * Start the interval that refills tokens at the configured rate with jitter.
   */
  protected startRefill(): void {
    this.refillTimer = setInterval(() => {
      if (this.destroyed) return;

      // Try to serve the next waiter if we have enough tokens
      if (this.waitQueue.length > 0) {
        const next = this.waitQueue[0];
        if (next.cost <= 1) {
          // Single-token waiter: serve immediately (original fast path)
          this.waitQueue.shift();
          next.resolve();
        } else {
          // Multi-token waiter: accumulate tokens until we have enough
          this.tokens++;
          if (this.tokens >= next.cost) {
            this.tokens -= next.cost;
            this.waitQueue.shift();
            next.resolve();
          }
        }
      } else if (this.tokens < this.maxTokens) {
        this.tokens++;
      }
    }, this.getJitteredInterval());

    // Unref so the timer doesn't prevent Node from exiting
    if (this.refillTimer && typeof this.refillTimer === 'object' && 'unref' in this.refillTimer) {
      this.refillTimer.unref();
    }
  }

  /**
   * Stop the refill timer.
   */
  protected stopRefill(): void {
    if (this.refillTimer) {
      clearInterval(this.refillTimer);
      this.refillTimer = null;
    }
  }

  /**
   * Add ±10% jitter to the refill interval to prevent synchronization.
   */
  protected getJitteredInterval(): number {
    const jitterFactor = 0.9 + Math.random() * 0.2; // 0.9 to 1.1
    return Math.round(this.refillIntervalMs * jitterFactor);
  }
}
