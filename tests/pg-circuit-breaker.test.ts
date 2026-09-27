import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { PgCircuitBreaker } from '../src/distributed/pg-circuit-breaker';

/**
 * Mock PostgreSQL Pool for distributed circuit breaker testing.
 */
class MockBreakerPgPool {
  public table: Map<string, any> = new Map();

  async query(sql: string, params: any[] = []): Promise<{ rows: any[] }> {
    const normalized = sql.trim().replace(/\s+/g, ' ');

    if (normalized.startsWith('CREATE TABLE')) {
      return { rows: [] };
    }

    if (normalized.includes('INSERT INTO _brq_circuit_breakers')) {
      const [key, cooldownMs, threshold] = params;
      if (!this.table.has(key)) {
        this.table.set(key, {
          breaker_key: key,
          state: 'closed',
          failure_count: 0,
          success_count: 0,
          last_failure: null,
          opened_at: null,
          cooldown_ms: Number(cooldownMs),
          failure_threshold: Number(threshold),
          updated_at: new Date(),
        });
      }
      return { rows: [] };
    }

    if (normalized.includes('SELECT') && normalized.includes('FROM _brq_circuit_breakers')) {
      const [key] = params;
      const row = this.table.get(key);
      if (!row) return { rows: [] };

      const now = Date.now();
      const openTime = row.opened_at ? new Date(row.opened_at).getTime() : 0;
      const elapsed = openTime ? Math.max(0, now - openTime) : 0;

      return {
        rows: [
          {
            ...row,
            open_elapsed_ms: elapsed,
          },
        ],
      };
    }

    if (normalized.includes('UPDATE _brq_circuit_breakers')) {
      const [key] = params;
      const row = this.table.get(key);
      if (!row) return { rows: [] };

      if (normalized.includes("SET state = 'half-open'")) {
        if (row.state === 'open') {
          row.state = 'half-open';
          row.updated_at = new Date();
        }
        return { rows: [] };
      }

      if (normalized.includes("SET state = 'closed'")) {
        row.state = 'closed';
        row.failure_count = 0;
        row.success_count = (row.success_count || 0) + 1;
        row.opened_at = null;
        row.updated_at = new Date();
        return { rows: [] };
      }

      if (normalized.includes("SET state = 'open'")) {
        const failureCount = params[1];
        row.state = 'open';
        row.failure_count = Number(failureCount);
        row.opened_at = new Date();
        row.last_failure = new Date();
        row.updated_at = new Date();
        return { rows: [] };
      }

      if (normalized.includes('SET failure_count = $2')) {
        const failureCount = params[1];
        row.failure_count = Number(failureCount);
        row.last_failure = new Date();
        row.updated_at = new Date();
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

describe('PgCircuitBreaker (Distributed Failure Circuit)', () => {
  let pool: MockBreakerPgPool;
  let breaker: PgCircuitBreaker;

  beforeEach(() => {
    pool = new MockBreakerPgPool();
  });

  afterEach(async () => {
    if (breaker) await breaker.destroy();
  });

  describe('initialization', () => {
    it('should throw if pool is missing', () => {
      expect(() => new PgCircuitBreaker({ pool: null as any, breakerKey: 'test' })).toThrow(
        'requires a PostgreSQL pool'
      );
    });

    it('should throw if breakerKey is missing', () => {
      expect(() => new PgCircuitBreaker({ pool, breakerKey: '' })).toThrow('requires a breakerKey');
    });

    it('should initialize schema with default closed state', async () => {
      breaker = new PgCircuitBreaker({
        pool,
        breakerKey: 'api:orders',
        failureThreshold: 3,
        cooldownMs: 500,
      });

      await breaker.initialize();
      const state = await breaker.getState();

      expect(state.breakerKey).toBe('api:orders');
      expect(state.state).toBe('closed');
      expect(state.failureCount).toBe(0);
      expect(state.failureThreshold).toBe(3);
      expect(state.cooldownMs).toBe(500);
    });
  });

  describe('state transitions', () => {
    it('should allow requests when closed', async () => {
      breaker = new PgCircuitBreaker({
        pool,
        breakerKey: 'api:closed-test',
        failureThreshold: 3,
      });

      const allowed = await breaker.allowRequest();
      expect(allowed).toBe(true);
    });

    it('should trip to OPEN when failure threshold is reached', async () => {
      breaker = new PgCircuitBreaker({
        pool,
        breakerKey: 'api:trip-test',
        failureThreshold: 3,
        cooldownMs: 500,
      });

      expect(await breaker.recordFailure()).toBe(false); // 1 failure
      expect(await breaker.recordFailure()).toBe(false); // 2 failures
      const tripped = await breaker.recordFailure(); // 3 failures -> trips
      expect(tripped).toBe(true);

      const state = await breaker.getState();
      expect(state.state).toBe('open');
      expect(state.failureCount).toBe(3);
      expect(state.openedAt).not.toBeNull();

      // Subsequent allowRequest should be denied
      const allowed = await breaker.allowRequest();
      expect(allowed).toBe(false);
    });

    it('should transition to HALF-OPEN after cooldown expires and recover on success', async () => {
      breaker = new PgCircuitBreaker({
        pool,
        breakerKey: 'api:half-open-test',
        failureThreshold: 2,
        cooldownMs: 50,
      });

      await breaker.recordFailure();
      await breaker.recordFailure(); // Tripped to OPEN

      expect(await breaker.allowRequest()).toBe(false);

      // Wait for cooldown
      await new Promise((r) => setTimeout(r, 120));

      // allowRequest should transition to half-open and return true
      const allowedTrial = await breaker.allowRequest();
      expect(allowedTrial).toBe(true);

      const halfOpenState = await breaker.getState();
      expect(halfOpenState.state).toBe('half-open');

      // Record success -> closes circuit
      await breaker.recordSuccess();

      const recoveredState = await breaker.getState();
      expect(recoveredState.state).toBe('closed');
      expect(recoveredState.failureCount).toBe(0);
      expect(recoveredState.successCount).toBeGreaterThan(0);
    });

    it('should immediately re-trip to OPEN if trial request fails during HALF-OPEN', async () => {
      breaker = new PgCircuitBreaker({
        pool,
        breakerKey: 'api:retrip-test',
        failureThreshold: 2,
        cooldownMs: 50,
      });

      await breaker.recordFailure();
      await breaker.recordFailure(); // OPEN

      await new Promise((r) => setTimeout(r, 120));
      await breaker.allowRequest(); // Transitions to HALF-OPEN

      // Trial request fails
      await breaker.recordFailure();

      const state = await breaker.getState();
      expect(state.state).toBe('open');
    });

    it('should reset state cleanly with reset()', async () => {
      breaker = new PgCircuitBreaker({
        pool,
        breakerKey: 'api:reset-test',
        failureThreshold: 2,
      });

      await breaker.recordFailure();
      await breaker.recordFailure(); // OPEN

      await breaker.reset();
      const state = await breaker.getState();
      expect(state.state).toBe('closed');
      expect(state.failureCount).toBe(0);
    });
  });

  describe('cross-process coordination', () => {
    it('should allow replica 2 to immediately see when replica 1 trips the circuit', async () => {
      const replica1 = new PgCircuitBreaker({
        pool,
        breakerKey: 'shared:openai',
        failureThreshold: 2,
      });

      const replica2 = new PgCircuitBreaker({
        pool,
        breakerKey: 'shared:openai',
        failureThreshold: 2,
      });

      expect(await replica2.allowRequest()).toBe(true);

      // Replica 1 encounters downstream outages
      await replica1.recordFailure();
      await replica1.recordFailure(); // Trips breaker

      // Replica 2 immediately blocks further calls
      expect(await replica2.allowRequest()).toBe(false);

      const state = await replica2.getState();
      expect(state.state).toBe('open');

      await replica1.destroy();
      await replica2.destroy();
    });
  });
});
