import { describe, it, expect, afterEach } from 'vitest';
import {
  ClaimCheckManager,
  MemoryPayloadStore,
  estimateByteSize,
  isClaimCheckRef,
} from '../src/core/claim-check';
import { BatchRateQueue } from '../src/core/queue';
import { removeShutdownHandlers } from '../src/core/shutdown';

describe('Claim-Check Pattern for Large Job Payloads', () => {
  let queue: BatchRateQueue<any>;

  afterEach(() => {
    if (queue) queue.destroy();
    removeShutdownHandlers();
  });

  describe('estimateByteSize & Ref Detection', () => {
    it('should estimate byte size accurately', () => {
      expect(estimateByteSize('hello')).toBe(10);
      expect(estimateByteSize({ id: 1, name: 'Alice' })).toBeGreaterThan(20);
      expect(estimateByteSize(null)).toBe(0);
    });

    it('should detect ClaimCheckRef correctly', () => {
      expect(isClaimCheckRef({ __claimCheck: true, claimId: 'c1', byteSize: 500 })).toBe(true);
      expect(isClaimCheckRef({ id: 1, text: 'normal' })).toBe(false);
      expect(isClaimCheckRef(null)).toBe(false);
    });
  });

  describe('ClaimCheckManager Offloading & Hydration', () => {
    it('should offload items exceeding threshold and keep small items inline', async () => {
      const manager = new ClaimCheckManager({
        thresholdBytes: 100, // 100 bytes threshold
      });

      const smallItem = { id: 1, text: 'hi' };
      const largeItem = { id: 2, hugePayload: 'X'.repeat(500) };

      const processedSmall = await manager.offloadIfNeeded(smallItem);
      const processedLarge = await manager.offloadIfNeeded(largeItem);

      expect(isClaimCheckRef(processedSmall)).toBe(false);
      expect(isClaimCheckRef(processedLarge)).toBe(true);

      // Hydration restores full object
      const hydratedLarge = await manager.hydrateIfNeeded(processedLarge);
      expect(hydratedLarge).toEqual(largeItem);

      await manager.destroy();
    });
  });

  describe('Queue Integration', () => {
    it('should transparently hydrate offloaded claim-check items for workers', async () => {
      let receivedItem: any = null;

      queue = new BatchRateQueue({
        name: 'claim-check-queue',
        rateLimit: { requests: 10, perMs: 1000 },
        batchFlush: { size: 10, intervalMs: 1000 },
        claimCheckThresholdBytes: 50, // 50 bytes triggers offloading
        gracefulShutdown: false,
        worker: async (item: { id: string; largeData: string }) => {
          receivedItem = item;
          return { id: item.id, updates: { processedLength: item.largeData.length } };
        },
        onBatchFlush: async () => {},
      });

      const payload = { id: 'job_large_1', largeData: 'A'.repeat(200) };
      const offloaded = await queue['claimCheckManager'].offloadIfNeeded(payload);
      expect(isClaimCheckRef(offloaded)).toBe(true);

      queue.add(offloaded as any);
      await queue.waitUntilDrained();

      expect(receivedItem).not.toBeNull();
      expect(receivedItem.largeData).toBe('A'.repeat(200));
      expect(queue.getStats().processed).toBe(1);
    });
  });
});
