import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { PgTokenBucket } from '../src/distributed/pg-token-bucket';

/**
 * In-memory Mock PostgreSQL Pool for distributed token bucket tests.
 * Simulates PostgreSQL transactional locking and time-based calculations.
 */
class MockPgPool {
  public tables: Map<string, Map<string, any>> = new Map();
  public queries: string[] = [];

  constructor() {
    this.tables.set('_brq_rate_buckets', new Map());
  }

  async query(sql: string, params: any[] = []): Promise<{ rows: any[] }> {
    this.queries.push(sql);
    const normalized = sql.trim().replace(/\s+/g, ' ');

    if (normalized.startsWith('CREATE TABLE')) {
      return { rows: [] };
    }

    if (normalized.includes('INSERT INTO _brq_rate_buckets')) {
      const [key, tokens, maxTokens, refillRate] = params;
      const table = this.tables.get('_brq_rate_buckets')!;
      if (!table.has(key)) {
        table.set(key, {
          bucket_key: key,
          tokens: Number(tokens),
          max_tokens: Number(maxTokens),
          refill_rate: Number(refillRate),
          last_refill_at: new Date(),
          updated_at: new Date(),
        });
      }
      return { rows: [] };
    }

    if (normalized.includes('SELECT') && normalized.includes('FROM _brq_rate_buckets')) {
      const [key] = params;
      const table = this.tables.get('_brq_rate_buckets')!;
      const row = table.get(key);
      if (!row) return { rows: [] };

      const now = Date.now();
      const lastRefill = new Date(row.last_refill_at).getTime();
      const elapsedMs = Math.max(0, now - lastRefill);

      return {
        rows: [
          {
            bucket_key: row.bucket_key,
            tokens: row.tokens,
            max_tokens: row.max_tokens,
            refill_rate: row.refill_rate,
            last_refill_at: row.last_refill_at,
            elapsed_ms: elapsedMs,
          },
        ],
      };
    }

    if (normalized.includes('UPDATE _brq_rate_buckets')) {
      if (normalized.includes('SET max_tokens = $2')) {
        const [key, maxTokens, refillRate] = params;
        const table = this.tables.get('_brq_rate_buckets')!;
        const row = table.get(key);
        if (row) {
          row.max_tokens = Number(maxTokens);
          row.refill_rate = Number(refillRate);
          row.tokens = Math.min(row.tokens, Number(maxTokens));
          row.updated_at = new Date();
        }
        return { rows: [] };
      }

      if (normalized.includes('SET tokens = $2')) {
        const [key, remainingTokens] = params;
        const table = this.tables.get('_brq_rate_buckets')!;
        const row = table.get(key);
        if (row) {
          row.tokens = Number(remainingTokens);
          row.last_refill_at = new Date();
          row.updated_at = new Date();
        }
        return { rows: [] };
      }
    }

    if (normalized === 'BEGIN' || normalized === 'COMMIT' || normalized === 'ROLLBACK') {
      return { rows: [] };
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

describe('PgTokenBucket (Distributed Rate Limiting)', () => {
  let pool: MockPgPool;
  let bucket: PgTokenBucket;

  beforeEach(() => {
    pool = new MockPgPool();
  });

  afterEach(async () => {
    if (bucket) await bucket.destroy();
  });

  describe('initialization', () => {
    it('should throw if pool is missing', () => {
      expect(
        () =>
          new PgTokenBucket({
            pool: null as any,
            bucketKey: 'test-bucket',
            rateLimit: { requests: 10, perMs: 1000 },
          })
      ).toThrow('requires a PostgreSQL pool');
    });

    it('should throw if bucketKey is missing', () => {
      expect(
        () =>
          new PgTokenBucket({
            pool,
            bucketKey: '',
            rateLimit: { requests: 10, perMs: 1000 },
          })
      ).toThrow('requires a bucketKey');
    });

    it('should throw if rateLimit is invalid', () => {
      expect(
        () =>
          new PgTokenBucket({
            pool,
            bucketKey: 'test',
            rateLimit: { requests: 0, perMs: 1000 },
          })
      ).toThrow('valid rateLimit');
    });

    it('should initialize schema and initial row idempotently', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'api:stripe',
        rateLimit: { requests: 5, perMs: 1000 },
      });

      await bucket.initialize();
      await bucket.initialize(); // Second call should be no-op

      const state = await bucket.getState();
      expect(state.bucketKey).toBe('api:stripe');
      expect(state.maxTokens).toBe(5);
      expect(state.tokens).toBe(5);
      expect(state.refillRate).toBe(5 / 1000);
    });
  });

  describe('token acquisition', () => {
    it('should acquire single token immediately when available', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:acquire',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await bucket.acquire(1);

      const state = await bucket.getState();
      expect(state.tokens).toBeCloseTo(9, 0);
    });

    it('should acquire cost-weighted tokens', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:cost',
        rateLimit: { requests: 100, perMs: 1000 },
      });

      await bucket.acquire(25);
      let state = await bucket.getState();
      expect(state.tokens).toBeCloseTo(75, 0);

      await bucket.acquire(50);
      state = await bucket.getState();
      expect(state.tokens).toBeCloseTo(25, 0);
    });

    it('should throw when cost exceeds bucket capacity', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:cap',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await expect(bucket.acquire(15)).rejects.toThrow('exceeds maximum bucket capacity');
    });

    it('should no-op when acquiring 0 or negative tokens', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:zero',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await bucket.acquire(0);
      await bucket.acquire(-5);

      const state = await bucket.getState();
      expect(state.tokens).toBeCloseTo(10, 0);
    });

    it('should throw after being destroyed', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:destroy',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await bucket.destroy();
      await expect(bucket.acquire(1)).rejects.toThrow('destroyed');
    });
  });

  describe('multi-process distributed sharing', () => {
    it('should share token depletion across two instances with the same bucketKey', async () => {
      const process1 = new PgTokenBucket({
        pool,
        bucketKey: 'shared:nominatim',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      const process2 = new PgTokenBucket({
        pool,
        bucketKey: 'shared:nominatim',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await process1.acquire(3);
      await process2.acquire(4);

      const state = await process1.getState();
      expect(state.tokens).toBeCloseTo(3, 0); // 10 - 3 - 4 = 3

      const state2 = await process2.getState();
      expect(state2.tokens).toBeCloseTo(3, 0);

      await process1.destroy();
      await process2.destroy();
    });

    it('should keep different bucketKeys isolated', async () => {
      const bucketA = new PgTokenBucket({
        pool,
        bucketKey: 'tenant:alpha',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      const bucketB = new PgTokenBucket({
        pool,
        bucketKey: 'tenant:beta',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await bucketA.acquire(7);
      await bucketB.acquire(2);

      const stateA = await bucketA.getState();
      const stateB = await bucketB.getState();

      expect(stateA.tokens).toBeCloseTo(3, 0);
      expect(stateB.tokens).toBeCloseTo(8, 0);

      await bucketA.destroy();
      await bucketB.destroy();
    });
  });

  describe('runtime rate updates', () => {
    it('should update maxTokens and refillRate in the database via setRate', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:reconfig',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await bucket.initialize();
      await bucket.setRate({ requests: 20, perMs: 1000 });

      const state = await bucket.getState();
      expect(state.maxTokens).toBe(20);
      expect(state.refillRate).toBe(20 / 1000);
    });

    it('should throw on invalid setRate parameters', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:bad-reconfig',
        rateLimit: { requests: 10, perMs: 1000 },
      });

      await expect(bucket.setRate({ requests: -1, perMs: 1000 })).rejects.toThrow('Invalid rate limit');
      await expect(bucket.setRate({ requests: 10, perMs: 0 })).rejects.toThrow('Invalid rate limit');
    });
  });

  describe('getAvailableTokens', () => {
    it('should return available tokens without modifying state', async () => {
      bucket = new PgTokenBucket({
        pool,
        bucketKey: 'test:avail',
        rateLimit: { requests: 50, perMs: 1000 },
      });

      await bucket.acquire(15);
      const available = await bucket.getAvailableTokens();
      expect(available).toBe(35);
    });
  });
});
