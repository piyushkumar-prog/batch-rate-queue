import { RateLimitConfig } from '../types';

/**
 * Token Bucket Rate Limiter
 *
 * Controls the rate of outbound API calls using the token bucket algorithm.
 * Tokens refill at a fixed rate. Each `acquire()` call consumes one token.
 * If the bucket is empty, the caller is queued and waits until a token is available.
 *
 * Includes optional jitter (±10%) to prevent thundering-herd synchronization
 * across multiple queue instances.
 */
export class Throttler {
  private tokens: number;
  private readonly maxTokens: number;
  private readonly refillIntervalMs: number;
  private refillTimer: ReturnType<typeof setInterval> | null = null;
  private waitQueue: Array<() => void> = [];
  private destroyed = false;

  constructor(config: RateLimitConfig) {
    this.maxTokens = config.requests;
    this.tokens = config.requests;
    // How often to add one token: spread the allowed requests evenly across the window
    this.refillIntervalMs = config.perMs / config.requests;

    this.startRefill();
  }

  /**
   * Acquire a rate-limit token.
   * Resolves immediately if a token is available, otherwise waits.
   */
  async acquire(): Promise<void> {
    if (this.destroyed) {
      throw new Error('Throttler has been destroyed');
    }

    if (this.tokens > 0) {
      this.tokens--;
      return;
    }

    // No tokens available — queue the caller
    return new Promise<void>((resolve) => {
      this.waitQueue.push(resolve);
    });
  }

  /**
   * Get the number of currently available tokens.
   */
  getAvailableTokens(): number {
    return this.tokens;
  }

  /**
   * Get the number of callers waiting for a token.
   */
  getWaitingCount(): number {
    return this.waitQueue.length;
  }

  /**
   * Stop the refill timer and reject any pending waiters.
   */
  destroy(): void {
    this.destroyed = true;
    if (this.refillTimer) {
      clearInterval(this.refillTimer);
      this.refillTimer = null;
    }
    // Release all waiters so they don't hang forever
    this.waitQueue.forEach((resolve) => resolve());
    this.waitQueue = [];
  }

  /**
   * Start the interval that refills tokens at the configured rate with jitter.
   */
  private startRefill(): void {
    this.refillTimer = setInterval(() => {
      if (this.destroyed) return;

      if (this.waitQueue.length > 0) {
        // Give the token directly to the next waiter
        const next = this.waitQueue.shift()!;
        next();
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
   * Add ±10% jitter to the refill interval to prevent synchronization.
   */
  private getJitteredInterval(): number {
    const jitterFactor = 0.9 + Math.random() * 0.2; // 0.9 to 1.1
    return Math.round(this.refillIntervalMs * jitterFactor);
  }
}
