import { describe, it, expect } from 'vitest';
import { FairScheduler } from '../src/core/fair-scheduler';

describe('FairScheduler (Deficit Round Robin)', () => {
  describe('basic operation', () => {
    it('should add and dequeue a single item', () => {
      const scheduler = new FairScheduler<{ id: number; key: string }>((item) => item.key);

      scheduler.add({ id: 1, key: 'a' });
      expect(scheduler.length).toBe(1);

      const result = scheduler.dequeue();
      expect(result).not.toBeNull();
      expect(result!.key).toBe('a');
      expect(result!.item.id).toBe(1);
      expect(scheduler.length).toBe(0);
    });

    it('should return null when empty', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      expect(scheduler.dequeue()).toBeNull();
    });

    it('should track total length correctly', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([
        { key: 'a' }, { key: 'a' }, { key: 'a' },
        { key: 'b' }, { key: 'b' },
      ]);

      expect(scheduler.length).toBe(5);
      expect(scheduler.keyCount).toBe(2);
    });
  });

  describe('fair-share interleaving', () => {
    it('should interleave items from different keys', () => {
      const scheduler = new FairScheduler<{ id: number; key: string }>((item) => item.key);

      // Simulate heavy tenant A (5 items) and light tenant B (3 items)
      scheduler.addMany([
        { id: 1, key: 'A' },
        { id: 2, key: 'A' },
        { id: 3, key: 'A' },
        { id: 4, key: 'A' },
        { id: 5, key: 'A' },
        { id: 6, key: 'B' },
        { id: 7, key: 'B' },
        { id: 8, key: 'B' },
      ]);

      // Dequeue all and track the key order
      const keyOrder: string[] = [];
      let result;
      while ((result = scheduler.dequeue()) !== null) {
        keyOrder.push(result.key);
      }

      expect(keyOrder).toHaveLength(8);

      // The first few items should interleave A and B
      // With DRR, the pattern should be: A, B, A, B, A, B, A, A
      // (B runs out after 3, then A gets all remaining)
      const firstSix = keyOrder.slice(0, 6);
      const aInFirstSix = firstSix.filter(k => k === 'A').length;
      const bInFirstSix = firstSix.filter(k => k === 'B').length;

      // Both A and B should appear in the first 6
      expect(aInFirstSix).toBe(3);
      expect(bInFirstSix).toBe(3);
    });

    it('should prevent tenant starvation', () => {
      const scheduler = new FairScheduler<{ key: string; data: string }>((item) => item.key);

      // Heavy tenant: 100 items
      for (let i = 0; i < 100; i++) {
        scheduler.add({ key: 'heavy', data: `item-${i}` });
      }
      // Light tenant: 3 items
      for (let i = 0; i < 3; i++) {
        scheduler.add({ key: 'light', data: `item-${i}` });
      }

      // Dequeue the first 10 items
      const first10: string[] = [];
      for (let i = 0; i < 10; i++) {
        const result = scheduler.dequeue();
        if (result) first10.push(result.key);
      }

      // Light tenant should appear in the first 10 (not starved)
      const lightCount = first10.filter(k => k === 'light').length;
      expect(lightCount).toBeGreaterThanOrEqual(3); // All 3 light items should be processed
    });
  });

  describe('three tenants', () => {
    it('should round-robin across 3 tenants fairly', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([
        { key: 'X' }, { key: 'X' }, { key: 'X' },
        { key: 'Y' }, { key: 'Y' }, { key: 'Y' },
        { key: 'Z' }, { key: 'Z' }, { key: 'Z' },
      ]);

      const keyOrder: string[] = [];
      let result;
      while ((result = scheduler.dequeue()) !== null) {
        keyOrder.push(result.key);
      }

      expect(keyOrder).toHaveLength(9);

      // First 3 should contain one of each key
      const first3 = keyOrder.slice(0, 3).sort();
      expect(first3).toEqual(['X', 'Y', 'Z']);

      // Middle 3 should also contain one of each key
      const mid3 = keyOrder.slice(3, 6).sort();
      expect(mid3).toEqual(['X', 'Y', 'Z']);
    });
  });

  describe('dequeueBatch', () => {
    it('should dequeue up to N items', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([
        { key: 'a' }, { key: 'a' },
        { key: 'b' }, { key: 'b' },
      ]);

      const batch = scheduler.dequeueBatch(3);
      expect(batch).toHaveLength(3);
      expect(scheduler.length).toBe(1);
    });

    it('should return fewer if not enough items', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.add({ key: 'a' });
      const batch = scheduler.dequeueBatch(5);
      expect(batch).toHaveLength(1);
    });
  });

  describe('key stats', () => {
    it('should report per-key pending counts', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([
        { key: 'a' }, { key: 'a' }, { key: 'a' },
        { key: 'b' },
      ]);

      expect(scheduler.getKeyLength('a')).toBe(3);
      expect(scheduler.getKeyLength('b')).toBe(1);
      expect(scheduler.getKeyLength('c')).toBe(0);
    });

    it('should list active keys', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([{ key: 'x' }, { key: 'y' }]);

      const keys = scheduler.getActiveKeys();
      expect(keys).toContain('x');
      expect(keys).toContain('y');
    });

    it('should return per-key stats map', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([{ key: 'a' }, { key: 'a' }, { key: 'b' }]);

      const stats = scheduler.getKeyStats();
      expect(stats.size).toBe(2);
      expect(stats.get('a')!.pending).toBe(2);
      expect(stats.get('b')!.pending).toBe(1);
    });
  });

  describe('cleanup after drain', () => {
    it('should remove keys when their queues are fully drained', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([{ key: 'a' }, { key: 'b' }]);

      scheduler.dequeue(); // a or b
      scheduler.dequeue(); // the other

      expect(scheduler.isEmpty()).toBe(true);
      expect(scheduler.keyCount).toBe(0);
    });
  });

  describe('clear', () => {
    it('should reset all state', () => {
      const scheduler = new FairScheduler<{ key: string }>((item) => item.key);

      scheduler.addMany([{ key: 'a' }, { key: 'b' }, { key: 'c' }]);
      expect(scheduler.length).toBe(3);

      scheduler.clear();
      expect(scheduler.length).toBe(0);
      expect(scheduler.keyCount).toBe(0);
      expect(scheduler.dequeue()).toBeNull();
    });
  });
});
