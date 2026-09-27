import { describe, it, expect, afterEach } from 'vitest';
import { MemoryIdempotencyStore, IdempotencyManager } from '../src/core/idempotency';
import { BatchRateQueue } from '../src/core/queue';
import { removeShutdownHandlers } from '../src/core/shutdown';

describe('Idempotency & External Side-Effect Deduplication', () => {
  let queue: BatchRateQueue<any>;

  afterEach(() => {
    if (queue) queue.destroy();
    removeShutdownHandlers();
  });

  describe('MemoryIdempotencyStore', () => {
    it('should store and retrieve idempotency records', () => {
      const store = new MemoryIdempotencyStore();
      const record = {
        key: 'tx_123',
        result: { id: 'tx_123', updates: { charged: true } },
        createdAt: Date.now(),
      };

      store.set('tx_123', record);
      const retrieved = store.get('tx_123');

      expect(retrieved).not.toBeNull();
      expect(retrieved?.result.updates).toEqual({ charged: true });
      expect(store.size).toBe(1);

      store.delete('tx_123');
      expect(store.get('tx_123')).toBeNull();
      store.destroy();
    });

    it('should respect TTL expiration', async () => {
      const store = new MemoryIdempotencyStore();
      const record = {
        key: 'tx_short',
        result: { id: 'tx_short', updates: { ok: true } },
        createdAt: Date.now(),
      };

      store.set('tx_short', record, 50); // 50ms TTL
      expect(store.get('tx_short')).not.toBeNull();

      await new Promise((r) => setTimeout(r, 70));
      expect(store.get('tx_short')).toBeNull();
      store.destroy();
    });
  });

  describe('Queue Integration (Crash Recovery & Deduplication)', () => {
    it('should execute worker on first run and bypass worker on duplicate redelivery', async () => {
      let workerExecutionCount = 0;

      queue = new BatchRateQueue({
        name: 'stripe-charge-dedup',
        rateLimit: { requests: 10, perMs: 1000 },
        batchFlush: { size: 10, intervalMs: 1000 },
        gracefulShutdown: false,
        idempotencyKey: (item: { chargeId: string; amount: number }) => item.chargeId,
        worker: async (item) => {
          workerExecutionCount++;
          return {
            id: item.chargeId,
            updates: { status: 'succeeded', amount: item.amount },
          };
        },
        onBatchFlush: async () => {},
      });

      // First run: fires external worker
      queue.add({ chargeId: 'ch_1001', amount: 50 });
      await queue.waitUntilDrained();

      expect(workerExecutionCount).toBe(1);
      expect(queue.getStats().processed).toBe(1);

      // Simulated redelivery after crash: same chargeId
      queue.add({ chargeId: 'ch_1001', amount: 50 });
      await queue.waitUntilDrained();

      // Worker should NOT be called again — returns cached result
      expect(workerExecutionCount).toBe(1);
      expect(queue.getStats().processed).toBe(2);

      // New distinct chargeId: should execute worker
      queue.add({ chargeId: 'ch_1002', amount: 75 });
      await queue.waitUntilDrained();

      expect(workerExecutionCount).toBe(2);
      expect(queue.getStats().processed).toBe(3);
    });
  });
});
