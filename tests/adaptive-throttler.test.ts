import { describe, it, expect, afterEach, vi } from 'vitest';
import { AdaptiveThrottler, WorkerOutcome } from '../src/core/adaptive-throttler';

describe('AdaptiveThrottler', () => {
  let throttler: AdaptiveThrottler;

  afterEach(() => {
    if (throttler) throttler.destroy();
  });

  describe('basic operation (backward compat)', () => {
    it('should acquire tokens like a normal throttler when adaptive is disabled', async () => {
      throttler = new AdaptiveThrottler(
        { requests: 3, perMs: 1000 },
        { enabled: false }
      );

      const start = Date.now();
      await throttler.acquire();
      await throttler.acquire();
      await throttler.acquire();
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(50);
      expect(throttler.getAvailableTokens()).toBe(0);
    });

    it('should support cost-weighted acquire', async () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        { enabled: false }
      );

      await throttler.acquire(5); // Consume 5 tokens
      expect(throttler.getAvailableTokens()).toBe(5);

      await throttler.acquire(5); // Consume remaining 5
      expect(throttler.getAvailableTokens()).toBe(0);
    });
  });

  describe('sliding window tracking', () => {
    it('should track error rate in sliding window', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        { enabled: true, windowSize: 10 }
      );

      // Record 3 failures and 7 successes
      for (let i = 0; i < 7; i++) {
        throttler.recordOutcome({ success: true });
      }
      for (let i = 0; i < 3; i++) {
        throttler.recordOutcome({ success: false });
      }

      expect(throttler.getErrorRate()).toBeCloseTo(0.3, 1);
    });

    it('should evict old entries beyond window size', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        { enabled: true, windowSize: 5 }
      );

      // Record 5 failures (fills window)
      for (let i = 0; i < 5; i++) {
        throttler.recordOutcome({ success: false });
      }
      expect(throttler.getErrorRate()).toBe(1.0);

      // Record 5 successes (pushes out all failures)
      for (let i = 0; i < 5; i++) {
        throttler.recordOutcome({ success: true });
      }
      expect(throttler.getErrorRate()).toBe(0);
    });

    it('should return 0 error rate when window is empty', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        { enabled: true }
      );

      expect(throttler.getErrorRate()).toBe(0);
    });
  });

  describe('adaptive rate changes', () => {
    it('should throttle down when error rate exceeds threshold', () => {
      const rateChanges: Array<{ previousRate: number; newRate: number; reason: string }> = [];

      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        {
          enabled: true,
          windowSize: 10,
          errorRateThreshold: 0.25,
          backoffFactor: 0.5,
          onRateChange: (event) => rateChanges.push(event),
        }
      );

      // Record enough failures to exceed 25% threshold
      // We need at least 5 samples (min window check)
      for (let i = 0; i < 3; i++) {
        throttler.recordOutcome({ success: true });
      }
      for (let i = 0; i < 4; i++) {
        throttler.recordOutcome({ success: false });
      }

      // Should have throttled down
      expect(rateChanges.length).toBeGreaterThanOrEqual(1);
      const lastChange = rateChanges[rateChanges.length - 1];
      expect(lastChange.reason).toBe('error-rate-high');
      expect(lastChange.newRate).toBeLessThan(lastChange.previousRate);
    });

    it('should not throttle below minRate', () => {
      const rateChanges: Array<{ newRate: number }> = [];

      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        {
          enabled: true,
          windowSize: 5,
          errorRateThreshold: 0.2,
          backoffFactor: 0.1, // Aggressive backoff
          minRate: 2,
          onRateChange: (event) => rateChanges.push(event),
        }
      );

      // All failures — should throttle aggressively but stop at minRate
      for (let i = 0; i < 10; i++) {
        throttler.recordOutcome({ success: false });
      }

      // All recorded rate changes should have newRate >= minRate
      for (const change of rateChanges) {
        expect(change.newRate).toBeGreaterThanOrEqual(2);
      }
    });

    it('should respect backoffFactor', () => {
      const rateChanges: Array<{ previousRate: number; newRate: number }> = [];

      throttler = new AdaptiveThrottler(
        { requests: 20, perMs: 1000 },
        {
          enabled: true,
          windowSize: 5,
          errorRateThreshold: 0.2,
          backoffFactor: 0.5,
          minRate: 1,
          onRateChange: (event) => rateChanges.push(event),
        }
      );

      // All failures
      for (let i = 0; i < 5; i++) {
        throttler.recordOutcome({ success: false });
      }

      expect(rateChanges.length).toBeGreaterThanOrEqual(1);
      const firstChange = rateChanges[0];
      // 20 * 0.5 = 10
      expect(firstChange.newRate).toBe(10);
    });
  });

  describe('Retry-After handling', () => {
    it('should pause when Retry-After signal is received', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        {
          enabled: true,
          signalSource: 'response-headers',
          honorRetryAfter: true,
        }
      );

      throttler.recordOutcome({
        success: false,
        retryAfterSeconds: 5,
      });

      expect(throttler.isPaused()).toBe(true);
      expect(throttler.getRemainingPauseMs()).toBeGreaterThan(0);
      expect(throttler.getRemainingPauseMs()).toBeLessThanOrEqual(5000);
    });

    it('should not pause when honorRetryAfter is false', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        {
          enabled: true,
          signalSource: 'response-headers',
          honorRetryAfter: false,
        }
      );

      throttler.recordOutcome({
        success: false,
        retryAfterSeconds: 5,
      });

      expect(throttler.isPaused()).toBe(false);
    });

    it('should not process Retry-After when signalSource is worker-errors only', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        {
          enabled: true,
          signalSource: 'worker-errors',
          honorRetryAfter: true,
        }
      );

      throttler.recordOutcome({
        success: false,
        retryAfterSeconds: 5,
      });

      expect(throttler.isPaused()).toBe(false);
    });
  });

  describe('getEffectiveRate', () => {
    it('should return original rate when no adaptation has occurred', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        { enabled: true }
      );

      const rate = throttler.getEffectiveRate();
      expect(rate.requests).toBe(10);
      expect(rate.perMs).toBe(1000);
    });

    it('should return reduced rate after throttle-down', () => {
      throttler = new AdaptiveThrottler(
        { requests: 20, perMs: 1000 },
        {
          enabled: true,
          windowSize: 5,
          errorRateThreshold: 0.2,
          backoffFactor: 0.5,
          minRate: 1,
        }
      );

      // Trigger throttle-down
      for (let i = 0; i < 5; i++) {
        throttler.recordOutcome({ success: false });
      }

      const rate = throttler.getEffectiveRate();
      expect(rate.requests).toBeLessThan(20);
    });
  });

  describe('manual setRate', () => {
    it('should allow manual rate override', () => {
      const rateChanges: Array<{ newRate: number; reason: string }> = [];

      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        {
          enabled: true,
          onRateChange: (event) => rateChanges.push(event),
        }
      );

      throttler.setRate(5, 1000);

      expect(rateChanges.length).toBe(1);
      expect(rateChanges[0].newRate).toBe(5);
      expect(rateChanges[0].reason).toBe('manual');
      expect(throttler.getEffectiveRate().requests).toBe(5);
    });
  });

  describe('no-op when disabled', () => {
    it('should not track outcomes when disabled', () => {
      throttler = new AdaptiveThrottler(
        { requests: 10, perMs: 1000 },
        { enabled: false }
      );

      // These should be no-ops
      for (let i = 0; i < 10; i++) {
        throttler.recordOutcome({ success: false });
      }

      expect(throttler.getErrorRate()).toBe(0);
      expect(throttler.getEffectiveRate().requests).toBe(10);
    });
  });
});
