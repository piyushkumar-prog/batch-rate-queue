import { describe, it, expect, vi, afterEach } from 'vitest';
import { BatchRateQueue } from '../src/core/queue';
import { removeShutdownHandlers } from '../src/core/shutdown';
import { BufferItem } from '../src/types';

describe('BatchRateQueue', () => {
  let queue: BatchRateQueue<any>;

  afterEach(() => {
    if (queue) queue.destroy();
    removeShutdownHandlers();
  });

  it('should process all items through the worker', async () => {
    const processed: number[] = [];

    queue = new BatchRateQueue({
      name: 'test-basic',
      rateLimit: { requests: 100, perMs: 1000 }, // fast for testing
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number; value: string }) => {
        processed.push(item.id);
        return { id: item.id, updates: { processed: true } };
      },
      onBatchFlush: async () => {},
    });

    const items = Array.from({ length: 10 }, (_, i) => ({ id: i + 1, value: `item-${i}` }));
    queue.addMany(items);

    await queue.waitUntilDrained();

    expect(processed).toHaveLength(10);
    expect(processed.sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('should call onBatchFlush with buffered results', async () => {
    const flushedBatches: BufferItem[][] = [];

    queue = new BatchRateQueue({
      name: 'test-flush',
      rateLimit: { requests: 100, perMs: 1000 },
      batchFlush: { size: 5, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        return { id: item.id, updates: { done: true } };
      },
      onBatchFlush: async (batch) => {
        flushedBatches.push([...batch]);
      },
    });

    const items = Array.from({ length: 8 }, (_, i) => ({ id: i + 1 }));
    queue.addMany(items);

    await queue.waitUntilDrained();

    const totalFlushed = flushedBatches.reduce((sum, b) => sum + b.length, 0);
    expect(totalFlushed).toBe(8);

    // Verify the updates contain the expected data
    const allItems = flushedBatches.flat();
    expect(allItems.every((item) => item.updates.done === true)).toBe(true);
  });

  it('should respect rate limiting', async () => {
    const timestamps: number[] = [];

    queue = new BatchRateQueue({
      name: 'test-rate',
      rateLimit: { requests: 2, perMs: 500 }, // 2 per 500ms
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        timestamps.push(Date.now());
        return { id: item.id, updates: {} };
      },
      onBatchFlush: async () => {},
    });

    queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);

    await queue.waitUntilDrained();

    // First 2 should be near-instant, next 2 should be delayed by ~250ms each
    expect(timestamps).toHaveLength(4);

    // The gap between item 2 and item 3 should be noticeable (rate limit kicking in)
    const gap = timestamps[2] - timestamps[1];
    expect(gap).toBeGreaterThanOrEqual(100); // At least some delay
  }, 10000);

  it('should handle worker errors without crashing', async () => {
    const errors: string[] = [];

    queue = new BatchRateQueue({
      name: 'test-errors',
      rateLimit: { requests: 100, perMs: 1000 },
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        if (item.id === 2) throw new Error('API down');
        return { id: item.id, updates: { ok: true } };
      },
      onBatchFlush: async () => {},
      onError: (err) => {
        errors.push(err.message);
      },
    });

    queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }]);

    await queue.waitUntilDrained();

    const stats = queue.getStats();
    expect(stats.processed).toBe(2); // items 1 and 3
    expect(stats.failed).toBe(1); // item 2
    expect(errors.some((e) => e.includes('API down'))).toBe(true);
  }, 10000);

  it('should skip items when worker returns null', async () => {
    const flushedIds: Array<string | number> = [];

    queue = new BatchRateQueue({
      name: 'test-skip',
      rateLimit: { requests: 100, perMs: 1000 },
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        if (item.id % 2 === 0) return null; // skip even items
        return { id: item.id, updates: { odd: true } };
      },
      onBatchFlush: async (batch) => {
        batch.forEach((b) => flushedIds.push(b.id));
      },
    });

    queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);

    await queue.waitUntilDrained();

    expect(flushedIds.sort()).toEqual([1, 3]);
  });

  it('should report correct stats', async () => {
    queue = new BatchRateQueue({
      name: 'test-stats',
      rateLimit: { requests: 100, perMs: 1000 },
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        if (item.id === 3) throw new Error('fail');
        return { id: item.id, updates: {} };
      },
      onBatchFlush: async () => {},
      onError: () => {},
    });

    queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]);

    await queue.waitUntilDrained();

    const stats = queue.getStats();
    expect(stats.processed).toBe(4);
    expect(stats.failed).toBe(1);
    expect(stats.pending).toBe(0);
    expect(stats.buffered).toBe(0);
  }, 10000);

  it('should emit events correctly', async () => {
    const events: string[] = [];

    queue = new BatchRateQueue({
      name: 'test-events',
      rateLimit: { requests: 100, perMs: 1000 },
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        return { id: item.id, updates: {} };
      },
      onBatchFlush: async () => {},
    });

    queue.on('processed', () => events.push('processed'));
    queue.on('flush', () => events.push('flush'));
    queue.on('drain', () => events.push('drain'));

    queue.addMany([{ id: 1 }, { id: 2 }]);

    await queue.waitUntilDrained();

    expect(events).toContain('processed');
    expect(events).toContain('drain');
  });

  it('should support pause and resume', async () => {
    const processed: number[] = [];

    queue = new BatchRateQueue({
      name: 'test-pause',
      rateLimit: { requests: 1, perMs: 500 }, // Slow rate so pause can take effect
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        processed.push(item.id);
        return { id: item.id, updates: {} };
      },
      onBatchFlush: async () => {},
    });

    queue.addMany([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]);

    // Let it start processing (1 req/500ms — should process 1-2 items)
    await new Promise((r) => setTimeout(r, 300));

    queue.pause();

    const countAtPause = processed.length;
    expect(countAtPause).toBeGreaterThanOrEqual(1);
    expect(countAtPause).toBeLessThan(5);

    // Wait to verify nothing more is processed while paused
    await new Promise((r) => setTimeout(r, 800));
    expect(processed.length).toBe(countAtPause);

    // Resume
    queue.resume();

    await queue.waitUntilDrained();

    expect(processed).toHaveLength(5);
  }, 15000);

  it('should support adding items with add()', async () => {
    const processed: number[] = [];

    queue = new BatchRateQueue({
      name: 'test-single-add',
      rateLimit: { requests: 100, perMs: 1000 },
      batchFlush: { size: 50, intervalMs: 100 },
      gracefulShutdown: false,
      worker: async (item: { id: number }) => {
        processed.push(item.id);
        return { id: item.id, updates: {} };
      },
      onBatchFlush: async () => {},
    });

    queue.add({ id: 1 });
    queue.add({ id: 2 });

    await queue.waitUntilDrained();

    expect(processed.sort()).toEqual([1, 2]);
  });
});
