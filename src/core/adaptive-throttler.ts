import { RateLimitConfig, AdaptiveThrottleConfig, RateChangeEvent, RateChangeReason } from '../types';
import { Throttler } from './throttler';

/**
 * Sliding window entry recording a worker outcome.
 */
interface OutcomeEntry {
  timestamp: number;
  success: boolean;
}

/**
 * Outcome recorded by the queue after each worker call.
 */
export interface WorkerOutcome {
  success: boolean;
  statusCode?: number;
  retryAfterSeconds?: number;
  error?: Error;
}

/**
 * Required fields from AdaptiveThrottleConfig after defaults are applied.
 */
type ResolvedAdaptiveConfig = Required<Omit<AdaptiveThrottleConfig, 'onRateChange'>> & {
  onRateChange?: (event: RateChangeEvent) => void;
};

/**
 * Adaptive Token Bucket Rate Limiter
 *
 * Extends the base Throttler with reactive/adaptive behavior.
 * Reads live signals from worker outcomes (error rates, 429s, Retry-After headers)
 * and automatically adjusts throughput:
 *
 * - When error rate exceeds threshold → throttle down by backoffFactor
 * - When Retry-After header is present → pause entirely for that duration
 * - When error rate drops below threshold → gradually recover by recoveryFactor
 * - Never goes below minRate or above maxRate (the original configured rate)
 *
 * This solves the confirmed pg-boss gap where static rate limiting has no way to
 * dynamically reduce delivery rate when the downstream service is struggling.
 */
export class AdaptiveThrottler extends Throttler {
  private slidingWindow: OutcomeEntry[] = [];
  private currentRate: number;
  private pausedUntil: number = 0;
  private recoveryTimer: ReturnType<typeof setInterval> | null = null;
  private readonly adaptiveConfig: ResolvedAdaptiveConfig;

  constructor(rateConfig: RateLimitConfig, adaptiveConfig: AdaptiveThrottleConfig) {
    super(rateConfig);

    this.currentRate = rateConfig.requests;

    // Merge defaults
    this.adaptiveConfig = {
      enabled: adaptiveConfig.enabled,
      signalSource: adaptiveConfig.signalSource ?? 'worker-errors',
      windowSize: adaptiveConfig.windowSize ?? 20,
      errorRateThreshold: adaptiveConfig.errorRateThreshold ?? 0.25,
      backoffFactor: adaptiveConfig.backoffFactor ?? 0.5,
      recoveryFactor: adaptiveConfig.recoveryFactor ?? 1.1,
      minRate: adaptiveConfig.minRate ?? 1,
      maxRate: adaptiveConfig.maxRate ?? rateConfig.requests,
      honorRetryAfter: adaptiveConfig.honorRetryAfter ?? true,
      onRateChange: adaptiveConfig.onRateChange,
    };

    // Start a periodic recovery probe that checks if we should recover
    this.startRecoveryProbe();
  }

  /**
   * Override acquire() to check if we're in a Retry-After pause.
   * If paused, waits until the pause expires before acquiring a token.
   */
  async acquire(cost: number = 1): Promise<void> {
    const now = Date.now();
    if (this.pausedUntil > now) {
      const waitMs = this.pausedUntil - now;
      await this.sleep(waitMs);
    }
    return super.acquire(cost);
  }

  /**
   * Record a worker outcome. Called by the queue after each worker completes.
   * This is the primary signal input for adaptive behavior.
   *
   * The throttler uses this to:
   * 1. Maintain a sliding window of recent outcomes
   * 2. Compute the current error rate
   * 3. Apply Retry-After pauses when present
   * 4. Throttle down when error rate exceeds threshold
   */
  recordOutcome(outcome: WorkerOutcome): void {
    if (!this.adaptiveConfig.enabled) return;

    const source = this.adaptiveConfig.signalSource;

    // Record in sliding window (for all signal sources)
    this.slidingWindow.push({
      timestamp: Date.now(),
      success: outcome.success,
    });

    // Trim window to configured size
    while (this.slidingWindow.length > this.adaptiveConfig.windowSize) {
      this.slidingWindow.shift();
    }

    // Handle Retry-After signal
    if (
      (source === 'response-headers' || source === 'both') &&
      this.adaptiveConfig.honorRetryAfter &&
      outcome.retryAfterSeconds &&
      outcome.retryAfterSeconds > 0
    ) {
      this.applyRetryAfterPause(outcome.retryAfterSeconds);
      return; // Retry-After takes priority over error-rate adjustments
    }

    // Handle error-rate-based throttling
    if (source === 'worker-errors' || source === 'both') {
      this.evaluateErrorRate();
    }
  }

  /**
   * Get the current effective rate (may differ from the original configured rate).
   */
  override getEffectiveRate(): { requests: number; perMs: number } {
    return { requests: Math.round(this.currentRate), perMs: this.perMs };
  }

  /**
   * Get the current error rate from the sliding window.
   * Returns a value between 0.0 and 1.0.
   */
  getErrorRate(): number {
    if (this.slidingWindow.length === 0) return 0;
    const failures = this.slidingWindow.filter(e => !e.success).length;
    return failures / this.slidingWindow.length;
  }

  /**
   * Check if the throttler is currently paused due to a Retry-After signal.
   */
  isPaused(): boolean {
    return this.pausedUntil > Date.now();
  }

  /**
   * Get the remaining pause duration in milliseconds.
   * Returns 0 if not paused.
   */
  getRemainingPauseMs(): number {
    const remaining = this.pausedUntil - Date.now();
    return remaining > 0 ? remaining : 0;
  }

  /**
   * Manually set the rate at runtime (for runtime-adjustable limits).
   * This is separate from adaptive adjustments and takes priority.
   */
  override setRate(requests: number, perMs: number): void {
    const previous = this.currentRate;
    this.currentRate = requests;
    this.adaptiveConfig.maxRate = requests; // Update the ceiling
    super.setRate(requests, perMs);

    this.emitRateChange(previous, requests, 'manual');
  }

  /**
   * Destroy the adaptive throttler, cleaning up all timers.
   */
  override destroy(): void {
    if (this.recoveryTimer) {
      clearInterval(this.recoveryTimer);
      this.recoveryTimer = null;
    }
    super.destroy();
  }

  // ---------------------------------------------------------------------------
  // Private: Adaptive logic
  // ---------------------------------------------------------------------------

  /**
   * Pause the throttler entirely for the specified duration (from Retry-After).
   */
  private applyRetryAfterPause(seconds: number): void {
    const pauseMs = seconds * 1000;
    this.pausedUntil = Date.now() + pauseMs;

    const previousRate = this.currentRate;
    // Also throttle down the rate for when the pause ends
    this.applyRateChange(
      Math.max(this.adaptiveConfig.minRate, this.currentRate * this.adaptiveConfig.backoffFactor),
      'retry-after'
    );
  }

  /**
   * Evaluate the current error rate and adjust the rate accordingly.
   */
  private evaluateErrorRate(): void {
    const errorRate = this.getErrorRate();

    // Need a minimum number of samples before making decisions
    if (this.slidingWindow.length < Math.min(5, this.adaptiveConfig.windowSize)) {
      return;
    }

    if (errorRate >= this.adaptiveConfig.errorRateThreshold) {
      // Error rate is high — throttle down
      const newRate = Math.max(
        this.adaptiveConfig.minRate,
        this.currentRate * this.adaptiveConfig.backoffFactor
      );

      if (Math.round(newRate) < Math.round(this.currentRate)) {
        this.applyRateChange(newRate, 'error-rate-high');
      }
    }
  }

  /**
   * Periodically check if conditions have improved and gradually recover the rate.
   * Runs every 5 seconds to avoid rapid oscillation.
   */
  private startRecoveryProbe(): void {
    this.recoveryTimer = setInterval(() => {
      if (this.destroyed || !this.adaptiveConfig.enabled) return;
      if (this.isPaused()) return; // Don't try to recover while paused

      const errorRate = this.getErrorRate();

      // Only recover if error rate is below half the threshold (hysteresis)
      // and current rate is below max
      if (
        errorRate < this.adaptiveConfig.errorRateThreshold * 0.5 &&
        this.currentRate < this.adaptiveConfig.maxRate
      ) {
        const newRate = Math.min(
          this.adaptiveConfig.maxRate,
          this.currentRate * this.adaptiveConfig.recoveryFactor
        );

        if (Math.round(newRate) > Math.round(this.currentRate)) {
          this.applyRateChange(newRate, 'recovery');
        }
      }
    }, 5000);

    // Unref so the timer doesn't prevent Node from exiting
    if (this.recoveryTimer && typeof this.recoveryTimer === 'object' && 'unref' in this.recoveryTimer) {
      this.recoveryTimer.unref();
    }
  }

  /**
   * Apply a rate change by updating the internal state and reconfiguring the base throttler.
   */
  private applyRateChange(newRate: number, reason: RateChangeReason): void {
    const previousRate = this.currentRate;
    this.currentRate = newRate;

    // Reconfigure the base throttler's refill timer
    const roundedRate = Math.max(1, Math.round(newRate));
    this.maxTokens = roundedRate;
    this.refillIntervalMs = this.perMs / roundedRate;

    // Clamp current tokens
    if (this.tokens > this.maxTokens) {
      this.tokens = this.maxTokens;
    }

    // Restart refill timer with new interval
    this.stopRefill();
    this.startRefill();

    this.emitRateChange(previousRate, newRate, reason);
  }

  /**
   * Emit a rate change event via the configured callback.
   */
  private emitRateChange(previousRate: number, newRate: number, reason: RateChangeReason): void {
    if (this.adaptiveConfig.onRateChange) {
      this.adaptiveConfig.onRateChange({
        previousRate: Math.round(previousRate),
        newRate: Math.round(newRate),
        reason,
        errorRate: this.getErrorRate(),
      });
    }
  }

  /**
   * Sleep utility.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
