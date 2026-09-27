import { describe, it, expect, afterEach } from 'vitest';
import { KeyedThrottler } from '../src/core/keyed-throttler';

describe('KeyedThrottler', () => {
  let keyed: KeyedThrottler;

  afterEach(() => {
    if (keyed) keyed.destroy();
  });

  describe('lazy bucket creation', () => {
    it('should create buckets lazily on first acquire', async () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });

      expect(keyed.size).toBe(0);
      expect(keyed.hasKey('tenant-a')).toBe(false);

      await keyed.acquire('tenant-a');

      expect(keyed.size).toBe(1);
      expect(keyed.hasKey('tenant-a')).toBe(true);
    });

    it('should create independent buckets per key', async () => {
      keyed = new KeyedThrottler({ requests: 2, perMs: 1000 });

      // Each key gets its own pool of 2 tokens
      await keyed.acquire('tenant-a');
      await keyed.acquire('tenant-a');
      await keyed.acquire('tenant-b');
      await keyed.acquire('tenant-b');

      expect(keyed.size).toBe(2);

      const statsA = keyed.getKeyBucketStats('tenant-a');
      const statsB = keyed.getKeyBucketStats('tenant-b');

      expect(statsA).not.toBeNull();
      expect(statsA!.availableTokens).toBe(0);
      expect(statsB).not.toBeNull();
      expect(statsB!.availableTokens).toBe(0);
    });
  });

  describe('per-key rate isolation', () => {
    it('should rate-limit each key independently', async () => {
      keyed = new KeyedThrottler({ requests: 1, perMs: 1000 });

      // tenant-a uses its token
      await keyed.acquire('tenant-a');
      // tenant-b has its own token, should be immediate
      const start = Date.now();
      await keyed.acquire('tenant-b');
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(50); // instant, not blocked by tenant-a
    });

    it('should block within the same key when tokens are exhausted', async () => {
      keyed = new KeyedThrottler({ requests: 1, perMs: 500 });

      await keyed.acquire('tenant-a');

      const start = Date.now();
      await keyed.acquire('tenant-a');
      const elapsed = Date.now() - start;

      expect(elapsed).toBeGreaterThanOrEqual(350); // waited for refill
    });
  });

  describe('per-key config overrides', () => {
    it('should apply per-key config from constructor', async () => {
      keyed = new KeyedThrottler(
        { requests: 2, perMs: 1000 }, // default
        { 'premium': { requests: 10, perMs: 1000 } } // override
      );

      // Premium tenant should have 10 tokens
      for (let i = 0; i < 10; i++) {
        await keyed.acquire('premium');
      }

      const stats = keyed.getKeyBucketStats('premium');
      expect(stats).not.toBeNull();
      expect(stats!.availableTokens).toBe(0);
      expect(stats!.effectiveRate.requests).toBe(10);
    });

    it('should use default config for unspecified keys', async () => {
      keyed = new KeyedThrottler(
        { requests: 2, perMs: 1000 },
        { 'premium': { requests: 10, perMs: 1000 } }
      );

      await keyed.acquire('free-tier');

      const stats = keyed.getKeyBucketStats('free-tier');
      expect(stats).not.toBeNull();
      expect(stats!.effectiveRate.requests).toBe(2);
    });
  });

  describe('runtime reconfiguration', () => {
    it('should reconfigure an existing key live via setKeyRate', async () => {
      keyed = new KeyedThrottler({ requests: 2, perMs: 1000 });

      await keyed.acquire('tenant-a'); // creates bucket with default config

      keyed.setKeyRate('tenant-a', { requests: 20, perMs: 1000 });

      const stats = keyed.getKeyBucketStats('tenant-a');
      expect(stats).not.toBeNull();
      expect(stats!.effectiveRate.requests).toBe(20);
    });

    it('should store config for lazy creation if bucket does not exist', async () => {
      keyed = new KeyedThrottler({ requests: 2, perMs: 1000 });

      keyed.setKeyRate('future-tenant', { requests: 50, perMs: 1000 });

      expect(keyed.hasKey('future-tenant')).toBe(false); // not created yet

      await keyed.acquire('future-tenant'); // now created

      const stats = keyed.getKeyBucketStats('future-tenant');
      expect(stats!.effectiveRate.requests).toBe(50);
    });
  });

  describe('key removal', () => {
    it('should remove a key and destroy its bucket', async () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });

      await keyed.acquire('tenant-a');
      expect(keyed.hasKey('tenant-a')).toBe(true);

      keyed.removeKey('tenant-a');
      expect(keyed.hasKey('tenant-a')).toBe(false);
      expect(keyed.size).toBe(0);
    });

    it('should not throw when removing a non-existent key', () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });

      expect(() => keyed.removeKey('doesnt-exist')).not.toThrow();
    });
  });

  describe('stats and introspection', () => {
    it('should return null stats for non-existent key', () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });

      expect(keyed.getKeyBucketStats('nope')).toBeNull();
    });

    it('should list all active keys', async () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });

      await keyed.acquire('alpha');
      await keyed.acquire('beta');
      await keyed.acquire('gamma');

      const keys = keyed.getActiveKeys();
      expect(keys).toHaveLength(3);
      expect(keys).toContain('alpha');
      expect(keys).toContain('beta');
      expect(keys).toContain('gamma');
    });

    it('should return stats for all keys', async () => {
      keyed = new KeyedThrottler({ requests: 3, perMs: 1000 });

      await keyed.acquire('a');
      await keyed.acquire('a');
      await keyed.acquire('b');

      const allStats = keyed.getAllKeyStats();
      expect(allStats.size).toBe(2);
      expect(allStats.get('a')!.availableTokens).toBe(1); // 3-2=1
      expect(allStats.get('b')!.availableTokens).toBe(2); // 3-1=2
    });
  });

  describe('cost-weighted per-key', () => {
    it('should support cost-weighted acquire per key', async () => {
      keyed = new KeyedThrottler({ requests: 10, perMs: 1000 });

      await keyed.acquire('tenant-a', 7); // 10-7=3 remaining

      const stats = keyed.getKeyBucketStats('tenant-a');
      expect(stats!.availableTokens).toBe(3);
    });
  });

  describe('destroy', () => {
    it('should destroy all buckets', async () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });

      await keyed.acquire('a');
      await keyed.acquire('b');

      keyed.destroy();
      expect(keyed.size).toBe(0);
    });

    it('should throw on acquire after destroy', async () => {
      keyed = new KeyedThrottler({ requests: 5, perMs: 1000 });
      keyed.destroy();

      await expect(keyed.acquire('a')).rejects.toThrow('destroyed');
    });
  });
});
