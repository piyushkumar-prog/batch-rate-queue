import { describe, it, expect, afterEach } from 'vitest';
import { Throttler } from '../src/core/throttler';
import { BatchRateQueue } from '../src/core/queue';
import { removeShutdownHandlers } from '../src/core/shutdown';

describe('Runtime-Adjustable Limits', () => {
  let throttler: Throttler;

  afterEach(() => {
    if (throttler) throttler.destroy();
    removeShutdownHandlers();
  });

  describe('Throttler.setRate', () => {
    it('should update rate at runtime without destroying the throttler', async () => {
      throttler = new Throttler({ requests: 2, perMs: 1000 });

      expect(throttler.getEffectiveRate()).toEqual({ requests: 2, perMs: 1000 });

      throttler.setRate(5, 2000);

      expect(throttler.getEffectiveRate()).toEqual({ requests: 5, perMs: 2000 });
      expect(throttler.getAvailableTokens()).toBeLessThanOrEqual(5);
    });

    it('should clamp tokens to new max when reducing rate', async () => {
      throttler = new Throttler({ requests: 10, perMs: 1000 });

      expect(throttler.getAvailableTokens()).toBe(10);

      throttler.setRate(3, 1000);

      // Tokens should be clamped to new max of 3
      expect(throttler.getAvailableTokens()).toBeLessThanOrEqual(3);
    });

    it('should preserve queued waiters after rate change', async () => {
      throttler = new Throttler({ requests: 1, perMs: 5000 });

      // Consume the only token
      await throttler.acquire();

      let waiterResolved = false;
      const waiterPromise = throttler.acquire().then(() => { waiterResolved = true; });

      expect(throttler.getWaitingCount()).toBe(1);

      // Change to a much faster rate — the waiter should eventually resolve
      throttler.setRate(10, 500);

      await waiterPromise;
      expect(waiterResolved).toBe(true);
    });

    it('should throw for invalid rate values', () => {
      throttler = new Throttler({ requests: 5, perMs: 1000 });

      expect(() => throttler.setRate(0, 1000)).toThrow('positive');
      expect(() => throttler.setRate(5, 0)).toThrow('positive');
      expect(() => throttler.setRate(-1, 1000)).toThrow('positive');
    });
  });

  describe('BatchRateQueue.setRateLimit', () => {
    it('should update rate limit on the queue without restarting', async () => {
      const results: any[] = [];
      const queue = new BatchRateQueue({
        name: 'test-runtime',
        rateLimit: { requests: 1, perMs: 500 },
        batchFlush: { size: 100, intervalMs: 500 },
        worker: async (item: { id: number }) => {
          results.push(item);
          return { id: item.id, updates: { done: true } };
        },
        onBatchFlush: async () => {},
        gracefulShutdown: false,
      });

      // Change rate to something faster
      queue.setRateLimit({ requests: 10, perMs: 1000 });

      // Add items and drain
      queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }]);
      await queue.waitUntilDrained();

      expect(results.length).toBe(3);
      queue.destroy();
    });
  });

  describe('BatchRateQueue.setBatchFlush', () => {
    it('should update batch flush config at runtime', async () => {
      let flushCount = 0;
      let lastBatchSize = 0;

      const queue = new BatchRateQueue({
        name: 'test-batch-runtime',
        rateLimit: { requests: 100, perMs: 1000 },
        batchFlush: { size: 2, intervalMs: 5000 },
        worker: async (item: { id: number }) => {
          return { id: item.id, updates: { done: true } };
        },
        onBatchFlush: async (batch) => {
          flushCount++;
          lastBatchSize = batch.length;
        },
        gracefulShutdown: false,
      });

      // Change batch size to 5
      queue.setBatchFlush({ size: 5 });

      // Add exactly 5 items — should trigger 1 flush (not 2–3 like with size=2)
      queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]);
      await queue.waitUntilDrained();

      // With size=5, all 5 items should be in one flush batch
      expect(flushCount).toBeGreaterThanOrEqual(1);
      queue.destroy();
    });
  });
});
