import { RateLimitConfig } from '../types';

export interface PgTokenBucketOptions {
  /** PostgreSQL Pool or Client instance (e.g., from 'pg') */
  pool: any;
  /** Unique key for this bucket in the database. */
  bucketKey: string;
  /** Rate limit configuration */
  rateLimit: RateLimitConfig;
  /** Table name for storing rate buckets. @default '_brq_rate_buckets' */
  tableName?: string;
  /** Auto-create table schema if it does not exist. @default true */
  autoCreateSchema?: boolean;
  /** Minimum retry interval in ms when token bucket is empty. @default 50 */
  retryIntervalMs?: number;
  /** Max wait time in ms before throwing timeout error on acquire. @default 30000 */
  acquireTimeoutMs?: number;
}

export interface PgTokenBucketState {
  bucketKey: string;
  tokens: number;
  maxTokens: number;
  refillRate: number;
  lastRefillAt: Date;
}

/**
 * PostgreSQL-backed distributed token bucket.
 *
 * Allows multiple Node.js worker processes / Kubernetes pods to collectively
 * respect an external rate limit (e.g., 10 req/sec) without requiring Redis.
 *
 * Concurrency safety is achieved via PostgreSQL transactions with
 * `SELECT ... FOR UPDATE` row-level locks and time-delta token refilling.
 */
export class PgTokenBucket {
  private readonly pool: any;
  private readonly bucketKey: string;
  private readonly tableName: string;
  private readonly autoCreateSchema: boolean;
  private readonly retryIntervalMs: number;
  private readonly acquireTimeoutMs: number;
  private rateLimit: RateLimitConfig;
  private initialized = false;
  private destroyed = false;

  constructor(options: PgTokenBucketOptions) {
    if (!options.pool) {
      throw new Error('PgTokenBucket requires a PostgreSQL pool or client instance');
    }
    if (!options.bucketKey) {
      throw new Error('PgTokenBucket requires a bucketKey');
    }
    if (!options.rateLimit || options.rateLimit.requests <= 0 || options.rateLimit.perMs <= 0) {
      throw new Error('PgTokenBucket requires valid rateLimit with requests > 0 and perMs > 0');
    }

    this.pool = options.pool;
    this.bucketKey = options.bucketKey;
    this.rateLimit = { ...options.rateLimit };
    this.tableName = options.tableName ?? '_brq_rate_buckets';
    this.autoCreateSchema = options.autoCreateSchema ?? true;
    this.retryIntervalMs = options.retryIntervalMs ?? 50;
    this.acquireTimeoutMs = options.acquireTimeoutMs ?? 30000;
  }

  /**
   * Initialize table schema and initial bucket row.
   * Safe to call multiple times (idempotent).
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    if (this.autoCreateSchema) {
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          bucket_key VARCHAR(255) PRIMARY KEY,
          tokens DOUBLE PRECISION NOT NULL,
          max_tokens DOUBLE PRECISION NOT NULL,
          refill_rate DOUBLE PRECISION NOT NULL,
          last_refill_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);
    }

    const refillRate = this.rateLimit.requests / this.rateLimit.perMs;
    await this.pool.query(
      `
      INSERT INTO ${this.tableName} (bucket_key, tokens, max_tokens, refill_rate, last_refill_at, updated_at)
      VALUES ($1, $2, $3, $4, NOW(), NOW())
      ON CONFLICT (bucket_key) DO NOTHING;
    `,
      [this.bucketKey, this.rateLimit.requests, this.rateLimit.requests, refillRate]
    );

    this.initialized = true;
  }

  /**
   * Acquire `cost` tokens from the shared PostgreSQL token bucket.
   * Blocks until enough tokens are available or until acquireTimeoutMs is exceeded.
   *
   * @param cost Number of tokens to acquire (default: 1)
   */
  async acquire(cost: number = 1): Promise<void> {
    if (this.destroyed) {
      throw new Error('PgTokenBucket has been destroyed');
    }
    if (cost <= 0) return;
    if (cost > this.rateLimit.requests) {
      throw new Error(
        `Requested cost (${cost}) exceeds maximum bucket capacity (${this.rateLimit.requests})`
      );
    }

    await this.ensureInitialized();

    const deadline = Date.now() + this.acquireTimeoutMs;

    while (Date.now() <= deadline) {
      if (this.destroyed) {
        throw new Error('PgTokenBucket has been destroyed');
      }

      const client = typeof this.pool.connect === 'function' ? await this.pool.connect() : this.pool;
      const isClientDedicated = client !== this.pool;

      try {
        await client.query('BEGIN');

        // Ensure row exists
        const refillRate = this.rateLimit.requests / this.rateLimit.perMs;
        await client.query(
          `
          INSERT INTO ${this.tableName} (bucket_key, tokens, max_tokens, refill_rate, last_refill_at, updated_at)
          VALUES ($1, $2, $3, $4, NOW(), NOW())
          ON CONFLICT (bucket_key) DO NOTHING;
        `,
          [this.bucketKey, this.rateLimit.requests, this.rateLimit.requests, refillRate]
        );

        // Lock row and calculate elapsed time
        const selectRes = await client.query(
          `
          SELECT bucket_key, tokens, max_tokens, refill_rate,
                 EXTRACT(EPOCH FROM (NOW() - last_refill_at)) * 1000 AS elapsed_ms
          FROM ${this.tableName}
          WHERE bucket_key = $1
          FOR UPDATE;
        `,
          [this.bucketKey]
        );

        if (selectRes.rows.length === 0) {
          await client.query('ROLLBACK');
          await this.sleep(this.retryIntervalMs);
          continue;
        }

        const row = selectRes.rows[0];
        const maxTokens = Number(row.max_tokens);
        const currentRefillRate = Number(row.refill_rate);
        const elapsedMs = Math.max(0, Number(row.elapsed_ms || 0));
        const storedTokens = Number(row.tokens);

        // Compute refilled tokens capped at max_tokens
        const refilledTokens = Math.min(maxTokens, storedTokens + elapsedMs * currentRefillRate);

        if (refilledTokens >= cost) {
          const remainingTokens = refilledTokens - cost;
          await client.query(
            `
            UPDATE ${this.tableName}
            SET tokens = $2, last_refill_at = NOW(), updated_at = NOW()
            WHERE bucket_key = $1;
          `,
            [this.bucketKey, remainingTokens]
          );
          await client.query('COMMIT');
          return;
        } else {
          // Update refilled tokens so elapsed time calculation starts fresh
          await client.query(
            `
            UPDATE ${this.tableName}
            SET tokens = $2, last_refill_at = NOW(), updated_at = NOW()
            WHERE bucket_key = $1;
          `,
            [this.bucketKey, refilledTokens]
          );
          await client.query('COMMIT');

          const deficit = cost - refilledTokens;
          const waitNeeded = currentRefillRate > 0 ? Math.ceil(deficit / currentRefillRate) : this.retryIntervalMs;
          const waitTime = Math.max(this.retryIntervalMs, waitNeeded);

          if (Date.now() + waitTime > deadline) {
            throw new Error(`PgTokenBucket acquire timeout after ${this.acquireTimeoutMs}ms`);
          }

          await this.sleep(waitTime);
        }
      } catch (err) {
        try {
          await client.query('ROLLBACK');
        } catch (_) {
          // Ignore rollback errors
        }
        if (err instanceof Error && err.message.includes('acquire timeout')) {
          throw err;
        }
        // Transient error — short wait and retry
        await this.sleep(this.retryIntervalMs);
      } finally {
        if (isClientDedicated && typeof client.release === 'function') {
          client.release();
        }
      }
    }

    throw new Error(`PgTokenBucket acquire timeout after ${this.acquireTimeoutMs}ms`);
  }

  /**
   * Get current state of the shared token bucket.
   */
  async getState(): Promise<PgTokenBucketState> {
    await this.ensureInitialized();

    const res = await this.pool.query(
      `
      SELECT bucket_key, tokens, max_tokens, refill_rate, last_refill_at,
             EXTRACT(EPOCH FROM (NOW() - last_refill_at)) * 1000 AS elapsed_ms
      FROM ${this.tableName}
      WHERE bucket_key = $1;
    `,
      [this.bucketKey]
    );

    if (res.rows.length === 0) {
      return {
        bucketKey: this.bucketKey,
        tokens: this.rateLimit.requests,
        maxTokens: this.rateLimit.requests,
        refillRate: this.rateLimit.requests / this.rateLimit.perMs,
        lastRefillAt: new Date(),
      };
    }

    const row = res.rows[0];
    const maxTokens = Number(row.max_tokens);
    const refillRate = Number(row.refill_rate);
    const elapsedMs = Math.max(0, Number(row.elapsed_ms || 0));
    const storedTokens = Number(row.tokens);
    const effectiveTokens = Math.min(maxTokens, storedTokens + elapsedMs * refillRate);

    return {
      bucketKey: row.bucket_key,
      tokens: effectiveTokens,
      maxTokens,
      refillRate,
      lastRefillAt: new Date(row.last_refill_at),
    };
  }

  /**
   * Get current available tokens (calculated non-destructively).
   */
  async getAvailableTokens(): Promise<number> {
    const state = await this.getState();
    return state.tokens;
  }

  /**
   * Update the rate limit configuration in the database for this bucket.
   */
  async setRate(config: RateLimitConfig): Promise<void> {
    if (config.requests <= 0 || config.perMs <= 0) {
      throw new Error('Invalid rate limit parameters: requests and perMs must be > 0');
    }

    this.rateLimit = { ...config };
    await this.ensureInitialized();

    const refillRate = config.requests / config.perMs;
    await this.pool.query(
      `
      UPDATE ${this.tableName}
      SET max_tokens = $2,
          refill_rate = $3,
          tokens = LEAST(tokens, $2),
          updated_at = NOW()
      WHERE bucket_key = $1;
    `,
      [this.bucketKey, config.requests, refillRate]
    );
  }

  /**
   * Destroy this instance and clean up resources.
   */
  async destroy(): Promise<void> {
    this.destroyed = true;
  }

  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
