import { describe, it, expect, afterEach } from 'vitest';
import { BatchRateQueue } from '../src/core/queue';
import { removeShutdownHandlers } from '../src/core/shutdown';

describe('BudgetStats & Monitoring Dashboard', () => {
  let queue: BatchRateQueue<any>;

  afterEach(() => {
    if (queue) queue.destroy();
    removeShutdownHandlers();
  });

  it('should return complete structured budget statistics for a standard queue', () => {
    queue = new BatchRateQueue({
      name: 'geocoding-worker',
      rateLimit: { requests: 5, perMs: 1000 },
      batchFlush: { size: 20, intervalMs: 1000 },
      gracefulShutdown: false,
      worker: async (item) => ({ id: item.id, updates: { done: true } }),
      onBatchFlush: async () => {},
    });

    const stats = queue.getBudgetStats();

    // Queue info
    expect(stats.queue.name).toBe('geocoding-worker');
    expect(stats.queue.state).toBe('idle');
    expect(stats.queue.pending).toBe(0);
    expect(stats.queue.processed).toBe(0);
    expect(stats.queue.failed).toBe(0);

    // Rate limit info
    expect(stats.rateLimit.configured).toEqual({ requests: 5, perMs: 1000 });
    expect(stats.rateLimit.effective).toEqual({ requests: 5, perMs: 1000 });
    expect(stats.rateLimit.availableTokens).toBe(5);
    expect(stats.rateLimit.waitingCount).toBe(0);
    expect(stats.rateLimit.utilizationPct).toBe(0);

    // Buffer info
    expect(stats.buffer.currentSize).toBe(0);
    expect(stats.buffer.flushThreshold).toBe(20);
    expect(stats.buffer.totalFlushed).toBe(0);
    expect(stats.buffer.failedFlushes).toBe(0);

    // Backlog ETA
    expect(stats.backlogEta.estimatedSecondsRemaining).toBe(0);
    expect(stats.backlogEta.estimatedCompletionTime).toBeNull();
  });

  it('should calculate backlog ETA accurately when items are pending', async () => {
    queue = new BatchRateQueue({
      name: 'eta-test',
      rateLimit: { requests: 10, perMs: 1000 }, // 10 items / sec
      batchFlush: { size: 50, intervalMs: 2000 },
      gracefulShutdown: false,
      worker: async (item) => {
        await new Promise((r) => setTimeout(r, 10));
        return { id: item.id, updates: {} };
      },
      onBatchFlush: async () => {},
    });

    queue.pause(); // Pause so items stay pending
    queue.addMany(Array.from({ length: 50 }, (_, i) => ({ id: i })));

    const stats = queue.getBudgetStats();
    expect(stats.queue.pending).toBe(50);
    expect(stats.queue.state).toBe('paused');
    // 50 items @ 10/sec = 5 seconds
    expect(stats.backlogEta.estimatedSecondsRemaining).toBe(5);
    expect(stats.backlogEta.estimatedCompletionTime).not.toBeNull();
  });

  it('should include per-key stats when rateLimitKey is configured', async () => {
    queue = new BatchRateQueue({
      name: 'multi-tenant-stats',
      rateLimit: { requests: 10, perMs: 1000 },
      batchFlush: { size: 10, intervalMs: 1000 },
      rateLimitKey: (item: { tenant: string; id: number }) => item.tenant,
      fairShare: true,
      perKeyRateLimit: {
        'tenant-premium': { requests: 50, perMs: 1000 },
      },
      gracefulShutdown: false,
      worker: async (item) => ({ id: item.id, updates: {} }),
      onBatchFlush: async () => {},
    });

    // Populate key buckets
    queue.setKeyRateLimit('tenant-premium', { requests: 50, perMs: 1000 });
    queue.setKeyRateLimit('tenant-free', { requests: 5, perMs: 1000 });

    const stats = queue.getBudgetStats();
    expect(stats.keys).not.toBeNull();
    expect(stats.keys!['tenant-premium']).toBeDefined();
    expect(stats.keys!['tenant-free']).toBeDefined();
  });

  it('should preserve backward compatibility with getStats()', () => {
    queue = new BatchRateQueue({
      name: 'compat-test',
      rateLimit: { requests: 5, perMs: 1000 },
      batchFlush: { size: 10, intervalMs: 1000 },
      gracefulShutdown: false,
      worker: async (item) => ({ id: item.id, updates: {} }),
      onBatchFlush: async () => {},
    });

    const legacyStats = queue.getStats();
    expect(legacyStats).toEqual({
      processed: 0,
      failed: 0,
      buffered: 0,
      pending: 0,
      running: false,
    });
  });
});
