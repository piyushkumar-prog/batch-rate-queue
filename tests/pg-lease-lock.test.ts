import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { PgLeaseLock } from '../src/distributed/pg-lease-lock';

/**
 * Mock PostgreSQL Pool for testing PgLeaseLock with PgBouncer transaction pooling behavior.
 */
class MockLockPgPool {
  public table: Map<string, any> = new Map();

  async query(sql: string, params: any[] = []): Promise<{ rows: any[] }> {
    const normalized = sql.trim().replace(/\s+/g, ' ');

    if (normalized.startsWith('CREATE TABLE')) {
      return { rows: [] };
    }

    if (normalized.includes('INSERT INTO _brq_distributed_locks')) {
      const [key, holderId, ttlMs] = params;
      const now = Date.now();
      const existing = this.table.get(key);

      if (!existing || existing.expires_at.getTime() <= now || existing.holder_id === holderId) {
        const fenceToken = existing ? existing.fence_token + 1 : 1;
        const newRecord = {
          lock_key: key,
          holder_id: holderId,
          acquired_at: new Date(now),
          expires_at: new Date(now + Number(ttlMs)),
          fence_token: fenceToken,
        };
        this.table.set(key, newRecord);
        return {
          rows: [
            {
              lock_key: key,
              holder_id: holderId,
              fence_token: fenceToken,
            },
          ],
        };
      }

      // Lock is held by someone else and has not expired
      return { rows: [] };
    }

    if (normalized.includes('UPDATE _brq_distributed_locks SET expires_at = NOW() +')) {
      const [key, holderId, ttlMs] = params;
      const existing = this.table.get(key);
      if (existing && existing.holder_id === holderId) {
        existing.expires_at = new Date(Date.now() + Number(ttlMs));
        return { rows: [{ lock_key: key }] };
      }
      return { rows: [] };
    }

    if (normalized.includes("SET expires_at = NOW() - INTERVAL '1 second'")) {
      const [key, holderId] = params;
      const existing = this.table.get(key);
      if (existing && existing.holder_id === holderId) {
        existing.expires_at = new Date(Date.now() - 1000);
      }
      return { rows: [] };
    }

    if (normalized.includes('SELECT') && normalized.includes('FROM _brq_distributed_locks')) {
      const [key] = params;
      const existing = this.table.get(key);
      if (!existing) return { rows: [] };

      const isExpired = existing.expires_at.getTime() <= Date.now();
      return {
        rows: [
          {
            lock_key: existing.lock_key,
            holder_id: existing.holder_id,
            acquired_at: existing.acquired_at,
            expires_at: existing.expires_at,
            fence_token: existing.fence_token,
            is_expired: isExpired,
          },
        ],
      };
    }

    return { rows: [] };
  }

  async connect(): Promise<any> {
    return {
      query: this.query.bind(this),
      release: () => {},
    };
  }
}

describe('PgLeaseLock (PgBouncer-Safe Distributed Lock)', () => {
  let pool: MockLockPgPool;
  let lock: PgLeaseLock;

  beforeEach(() => {
    pool = new MockLockPgPool();
  });

  afterEach(async () => {
    if (lock) await lock.destroy();
  });

  describe('Acquisition & Mutual Exclusion', () => {
    it('should allow first worker to acquire lock and block second worker', async () => {
      const lockA = new PgLeaseLock({
        pool,
        lockKey: 'cron:daily-reconciliation',
        holderId: 'pod-1',
        ttlMs: 5000,
      });

      const lockB = new PgLeaseLock({
        pool,
        lockKey: 'cron:daily-reconciliation',
        holderId: 'pod-2',
        ttlMs: 5000,
      });

      const acquiredA = await lockA.acquire();
      expect(acquiredA).toBe(true);
      expect(lockA.isHeld()).toBe(true);

      // Pod 2 should be blocked
      const acquiredB = await lockB.acquire();
      expect(acquiredB).toBe(false);
      expect(lockB.isHeld()).toBe(false);

      await lockA.destroy();
      await lockB.destroy();
    });

    it('should allow second worker to acquire after first releases', async () => {
      const lockA = new PgLeaseLock({
        pool,
        lockKey: 'job:partition-cleanup',
        holderId: 'worker-1',
        ttlMs: 5000,
      });

      const lockB = new PgLeaseLock({
        pool,
        lockKey: 'job:partition-cleanup',
        holderId: 'worker-2',
        ttlMs: 5000,
      });

      await lockA.acquire();
      await lockA.release();

      const acquiredB = await lockB.acquire();
      expect(acquiredB).toBe(true);

      await lockA.destroy();
      await lockB.destroy();
    });

    it('should allow takeover when lease TTL expires (crash recovery)', async () => {
      const lockA = new PgLeaseLock({
        pool,
        lockKey: 'leader:scheduler',
        holderId: 'crashed-pod',
        ttlMs: 50, // 50ms lease
      });

      const lockB = new PgLeaseLock({
        pool,
        lockKey: 'leader:scheduler',
        holderId: 'healthy-pod',
        ttlMs: 5000,
      });

      await lockA.acquire();

      // Wait for lockA lease to expire
      await new Promise((r) => setTimeout(r, 120));

      // Healthy pod should successfully take over
      const acquiredB = await lockB.acquire();
      expect(acquiredB).toBe(true);

      const state = await lockB.getState();
      expect(state?.holderId).toBe('healthy-pod');
      expect(state?.fenceToken).toBe(2);

      await lockA.destroy();
      await lockB.destroy();
    });
  });

  describe('runWithLock Helper', () => {
    it('should execute protected function and release lock on completion', async () => {
      lock = new PgLeaseLock({
        pool,
        lockKey: 'task:hourly-sync',
        ttlMs: 5000,
      });

      let executed = false;
      const result = await lock.runWithLock(async () => {
        executed = true;
        return 42;
      });

      expect(executed).toBe(true);
      expect(result).toBe(42);
      expect(lock.isHeld()).toBe(false);
    });
  });
});
