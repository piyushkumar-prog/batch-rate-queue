export type CircuitState = 'closed' | 'open' | 'half-open';

export interface PgCircuitBreakerOptions {
  /** PostgreSQL Pool or Client instance (e.g. from 'pg') */
  pool: any;
  /** Unique identifier for this circuit breaker */
  breakerKey: string;
  /** Number of consecutive failures before tripping the breaker. @default 5 */
  failureThreshold?: number;
  /** Cooldown time in milliseconds before transitioning from OPEN to HALF-OPEN. @default 30000 */
  cooldownMs?: number;
  /** Table name for storing breaker state. @default '_brq_circuit_breakers' */
  tableName?: string;
  /** Auto-create table schema if it does not exist. @default true */
  autoCreateSchema?: boolean;
}

export interface PgCircuitBreakerState {
  breakerKey: string;
  state: CircuitState;
  failureCount: number;
  successCount: number;
  lastFailure: Date | null;
  openedAt: Date | null;
  cooldownMs: number;
  failureThreshold: number;
}

/**
 * PostgreSQL-backed distributed circuit breaker.
 *
 * Coordinates circuit breaker state across multiple worker processes / pods.
 * When an external downstream service fails repeatedly, any single pod can trip
 * the circuit breaker and ALL pods will immediately stop pounding the failing service.
 *
 * State transitions:
 *   CLOSED    -> (consecutive failures >= threshold) -> OPEN
 *   OPEN      -> (cooldown elapsed)                  -> HALF-OPEN
 *   HALF-OPEN -> (success)                           -> CLOSED
 *   HALF-OPEN -> (failure)                           -> OPEN (cooldown resets)
 */
export class PgCircuitBreaker {
  private readonly pool: any;
  private readonly breakerKey: string;
  private readonly failureThreshold: number;
  private readonly cooldownMs: number;
  private readonly tableName: string;
  private readonly autoCreateSchema: boolean;
  private initialized = false;
  private destroyed = false;

  constructor(options: PgCircuitBreakerOptions) {
    if (!options.pool) {
      throw new Error('PgCircuitBreaker requires a PostgreSQL pool or client instance');
    }
    if (!options.breakerKey) {
      throw new Error('PgCircuitBreaker requires a breakerKey');
    }

    this.pool = options.pool;
    this.breakerKey = options.breakerKey;
    this.failureThreshold = Math.max(1, options.failureThreshold ?? 5);
    this.cooldownMs = Math.max(100, options.cooldownMs ?? 30000);
    this.tableName = options.tableName ?? '_brq_circuit_breakers';
    this.autoCreateSchema = options.autoCreateSchema ?? true;
  }

  /**
   * Initialize table schema and initial breaker row.
   * Safe to call multiple times (idempotent).
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    if (this.autoCreateSchema) {
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          breaker_key VARCHAR(255) PRIMARY KEY,
          state VARCHAR(20) NOT NULL DEFAULT 'closed',
          failure_count INTEGER NOT NULL DEFAULT 0,
          success_count INTEGER NOT NULL DEFAULT 0,
          last_failure TIMESTAMPTZ,
          opened_at TIMESTAMPTZ,
          cooldown_ms INTEGER NOT NULL DEFAULT 30000,
          failure_threshold INTEGER NOT NULL DEFAULT 5,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);
    }

    await this.pool.query(
      `
      INSERT INTO ${this.tableName} (
        breaker_key, state, failure_count, success_count, cooldown_ms, failure_threshold, updated_at
      )
      VALUES ($1, 'closed', 0, 0, $2, $3, NOW())
      ON CONFLICT (breaker_key) DO NOTHING;
    `,
      [this.breakerKey, this.cooldownMs, this.failureThreshold]
    );

    this.initialized = true;
  }

  /**
   * Check whether a request is allowed through the circuit.
   *
   * - CLOSED: returns true
   * - OPEN: returns false unless cooldown has elapsed, in which case it transitions to HALF-OPEN and returns true
   * - HALF-OPEN: returns true (trial request)
   */
  async allowRequest(): Promise<boolean> {
    if (this.destroyed) return false;
    await this.ensureInitialized();

    const client = typeof this.pool.connect === 'function' ? await this.pool.connect() : this.pool;
    const isClientDedicated = client !== this.pool;

    try {
      const res = await client.query(
        `
        SELECT state, failure_count, opened_at, cooldown_ms,
               EXTRACT(EPOCH FROM (NOW() - opened_at)) * 1000 AS open_elapsed_ms
        FROM ${this.tableName}
        WHERE breaker_key = $1;
      `,
        [this.breakerKey]
      );

      if (res.rows.length === 0) {
        return true;
      }

      const row = res.rows[0];
      const state = (row.state || 'closed') as CircuitState;

      if (state === 'closed') {
        return true;
      }

      if (state === 'open') {
        const cooldown = Number(row.cooldown_ms || this.cooldownMs);
        const elapsed = Number(row.open_elapsed_ms || 0);

        if (elapsed >= cooldown) {
          // Cooldown elapsed — transition to half-open
          await client.query(
            `
            UPDATE ${this.tableName}
            SET state = 'half-open', updated_at = NOW()
            WHERE breaker_key = $1 AND state = 'open';
          `,
            [this.breakerKey]
          );
          return true;
        }

        return false;
      }

      if (state === 'half-open') {
        return true;
      }

      return true;
    } finally {
      if (isClientDedicated && typeof client.release === 'function') {
        client.release();
      }
    }
  }

  /**
   * Record a successful execution.
   * - Resets failure count
   * - If state was HALF-OPEN, closes the circuit.
   */
  async recordSuccess(): Promise<void> {
    if (this.destroyed) return;
    await this.ensureInitialized();

    await this.pool.query(
      `
      UPDATE ${this.tableName}
      SET state = 'closed',
          failure_count = 0,
          success_count = success_count + 1,
          opened_at = NULL,
          updated_at = NOW()
      WHERE breaker_key = $1;
    `,
      [this.breakerKey]
    );
  }

  /**
   * Record a failure.
   * - Increments failure count
   * - Trips breaker to OPEN if failures >= threshold or if state was HALF-OPEN.
   *
   * @returns true if the failure caused the circuit to trip to OPEN
   */
  async recordFailure(): Promise<boolean> {
    if (this.destroyed) return false;
    await this.ensureInitialized();

    const client = typeof this.pool.connect === 'function' ? await this.pool.connect() : this.pool;
    const isClientDedicated = client !== this.pool;

    try {
      await client.query('BEGIN');

      const res = await client.query(
        `
        SELECT state, failure_count, failure_threshold
        FROM ${this.tableName}
        WHERE breaker_key = $1
        FOR UPDATE;
      `,
        [this.breakerKey]
      );

      if (res.rows.length === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      const row = res.rows[0];
      const currentState = (row.state || 'closed') as CircuitState;
      const currentFailures = Number(row.failure_count || 0) + 1;
      const threshold = Number(row.failure_threshold || this.failureThreshold);

      let newState: CircuitState = currentState;
      let tripped = false;

      if (currentState === 'half-open' || currentFailures >= threshold) {
        newState = 'open';
        tripped = currentState !== 'open';
        await client.query(
          `
          UPDATE ${this.tableName}
          SET state = 'open',
              failure_count = $2,
              opened_at = NOW(),
              last_failure = NOW(),
              updated_at = NOW()
          WHERE breaker_key = $1;
        `,
          [this.breakerKey, currentFailures]
        );
      } else {
        await client.query(
          `
          UPDATE ${this.tableName}
          SET failure_count = $2,
              last_failure = NOW(),
              updated_at = NOW()
          WHERE breaker_key = $1;
        `,
          [this.breakerKey, currentFailures]
        );
      }

      await client.query('COMMIT');
      return tripped;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch (_) {}
      return false;
    } finally {
      if (isClientDedicated && typeof client.release === 'function') {
        client.release();
      }
    }
  }

  /**
   * Get current state of the circuit breaker.
   */
  async getState(): Promise<PgCircuitBreakerState> {
    await this.ensureInitialized();

    const res = await this.pool.query(
      `
      SELECT breaker_key, state, failure_count, success_count,
             last_failure, opened_at, cooldown_ms, failure_threshold,
             EXTRACT(EPOCH FROM (NOW() - opened_at)) * 1000 AS open_elapsed_ms
      FROM ${this.tableName}
      WHERE breaker_key = $1;
    `,
      [this.breakerKey]
    );

    if (res.rows.length === 0) {
      return {
        breakerKey: this.breakerKey,
        state: 'closed',
        failureCount: 0,
        successCount: 0,
        lastFailure: null,
        openedAt: null,
        cooldownMs: this.cooldownMs,
        failureThreshold: this.failureThreshold,
      };
    }

    const row = res.rows[0];
    let state = (row.state || 'closed') as CircuitState;
    const cooldown = Number(row.cooldown_ms || this.cooldownMs);
    const elapsed = Number(row.open_elapsed_ms || 0);

    // If open and cooldown elapsed, report as half-open
    if (state === 'open' && elapsed >= cooldown) {
      state = 'half-open';
    }

    return {
      breakerKey: row.breaker_key,
      state,
      failureCount: Number(row.failure_count || 0),
      successCount: Number(row.success_count || 0),
      lastFailure: row.last_failure ? new Date(row.last_failure) : null,
      openedAt: row.opened_at ? new Date(row.opened_at) : null,
      cooldownMs: cooldown,
      failureThreshold: Number(row.failure_threshold || this.failureThreshold),
    };
  }

  /**
   * Force-reset the circuit breaker back to CLOSED state.
   */
  async reset(): Promise<void> {
    await this.ensureInitialized();
    await this.pool.query(
      `
      UPDATE ${this.tableName}
      SET state = 'closed',
          failure_count = 0,
          opened_at = NULL,
          updated_at = NOW()
      WHERE breaker_key = $1;
    `,
      [this.breakerKey]
    );
  }

  /**
   * Destroy instance.
   */
  async destroy(): Promise<void> {
    this.destroyed = true;
  }

  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }
}
