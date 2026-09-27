export interface PgLeaseLockOptions {
  /** PostgreSQL Pool or Client instance (e.g. from 'pg') */
  pool: any;
  /** Unique lock name */
  lockKey: string;
  /** Lease time-to-live in milliseconds. @default 30000 (30 seconds) */
  ttlMs?: number;
  /** Unique identifier for this instance/process. Defaults to random UUID. */
  holderId?: string;
  /** Table name for storing locks. @default '_brq_distributed_locks' */
  tableName?: string;
  /** Auto-create table schema if it does not exist. @default true */
  autoCreateSchema?: boolean;
}

export interface PgLockState {
  lockKey: string;
  holderId: string;
  acquiredAt: Date;
  expiresAt: Date;
  fenceToken: number;
  isHeldByMe: boolean;
  isExpired: boolean;
}

/**
 * PostgreSQL-Backed Distributed Lease Lock
 *
 * Designed specifically for **PgBouncer in Transaction Pooling Mode** (Supabase, Neon, AWS RDS Proxy).
 *
 * Why this is needed:
 * Standard `pg_advisory_lock` is session-scoped. In transaction-pooling mode, connections
 * are returned to the pool immediately after each transaction ends, releasing session locks
 * unpredictably or leaking them to other requests.
 *
 * This implementation uses dedicated table rows with atomic lease timeouts and fencing tokens
 * computed using database-side UTC intervals (`NOW() + ttl * INTERVAL '1 millisecond'`).
 * This makes it 100% immune to PgBouncer connection multiplexing, server clock drift, and DST shifts.
 */
export class PgLeaseLock {
  private readonly pool: any;
  private readonly lockKey: string;
  private readonly ttlMs: number;
  private readonly holderId: string;
  private readonly tableName: string;
  private readonly autoCreateSchema: boolean;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private initialized = false;
  private destroyed = false;
  private held = false;

  constructor(options: PgLeaseLockOptions) {
    if (!options.pool) {
      throw new Error('PgLeaseLock requires a PostgreSQL pool or client instance');
    }
    if (!options.lockKey) {
      throw new Error('PgLeaseLock requires a lockKey');
    }

    this.pool = options.pool;
    this.lockKey = options.lockKey;
    this.ttlMs = Math.max(50, options.ttlMs ?? 30000);
    this.holderId =
      options.holderId ??
      `holder_${process.pid}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    this.tableName = options.tableName ?? '_brq_distributed_locks';
    this.autoCreateSchema = options.autoCreateSchema ?? true;
  }

  /**
   * Initialize table schema. Safe to call multiple times (idempotent).
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    if (this.autoCreateSchema) {
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          lock_key VARCHAR(255) PRIMARY KEY,
          holder_id VARCHAR(255) NOT NULL,
          acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          expires_at TIMESTAMPTZ NOT NULL,
          fence_token BIGINT NOT NULL DEFAULT 1
        );
      `);
    }

    this.initialized = true;
  }

  /**
   * Attempt to acquire the distributed lease lock.
   * Returns true if successfully acquired, false if held by another active worker.
   */
  async acquire(): Promise<boolean> {
    if (this.destroyed) return false;
    await this.ensureInitialized();

    const client = typeof this.pool.connect === 'function' ? await this.pool.connect() : this.pool;
    const isClientDedicated = client !== this.pool;

    try {
      const res = await client.query(
        `
        INSERT INTO ${this.tableName} (lock_key, holder_id, acquired_at, expires_at, fence_token)
        VALUES ($1, $2, NOW(), NOW() + ($3 * INTERVAL '1 millisecond'), 1)
        ON CONFLICT (lock_key) DO UPDATE
        SET holder_id = $2,
            acquired_at = NOW(),
            expires_at = NOW() + ($3 * INTERVAL '1 millisecond'),
            fence_token = ${this.tableName}.fence_token + 1
        WHERE ${this.tableName}.expires_at <= NOW() OR ${this.tableName}.holder_id = $2
        RETURNING lock_key, holder_id, fence_token;
      `,
        [this.lockKey, this.holderId, this.ttlMs]
      );

      const acquired = res.rows.length > 0 && res.rows[0].holder_id === this.holderId;
      this.held = acquired;
      return acquired;
    } catch (err) {
      return false;
    } finally {
      if (isClientDedicated && typeof client.release === 'function') {
        client.release();
      }
    }
  }

  /**
   * Renew an existing lease lock before it expires.
   */
  async renew(): Promise<boolean> {
    if (this.destroyed || !this.held) return false;
    await this.ensureInitialized();

    try {
      const res = await this.pool.query(
        `
        UPDATE ${this.tableName}
        SET expires_at = NOW() + ($3 * INTERVAL '1 millisecond')
        WHERE lock_key = $1 AND holder_id = $2
        RETURNING lock_key;
      `,
        [this.lockKey, this.holderId, this.ttlMs]
      );

      const renewed = res.rows.length > 0;
      if (!renewed) {
        this.held = false;
      }
      return renewed;
    } catch (_) {
      return false;
    }
  }

  /**
   * Release the distributed lease lock.
   */
  async release(): Promise<void> {
    if (!this.held) return;
    this.stopHeartbeat();

    try {
      await this.pool.query(
        `
        UPDATE ${this.tableName}
        SET expires_at = NOW() - INTERVAL '1 second'
        WHERE lock_key = $1 AND holder_id = $2;
      `,
        [this.lockKey, this.holderId]
      );
    } catch (_) {
      // Ignore release errors
    } finally {
      this.held = false;
    }
  }

  /**
   * Execute a function while holding the distributed lock, with automatic heartbeat renewal.
   * If lock acquisition fails, returns null without executing fn.
   */
  async runWithLock<R>(fn: () => Promise<R>): Promise<R | null> {
    const acquired = await this.acquire();
    if (!acquired) return null;

    this.startHeartbeat();

    try {
      return await fn();
    } finally {
      await this.release();
    }
  }

  /**
   * Inspect current lock state from database.
   */
  async getState(): Promise<PgLockState | null> {
    await this.ensureInitialized();

    const res = await this.pool.query(
      `
      SELECT lock_key, holder_id, acquired_at, expires_at, fence_token,
             (expires_at <= NOW()) AS is_expired
      FROM ${this.tableName}
      WHERE lock_key = $1;
    `,
      [this.lockKey]
    );

    if (res.rows.length === 0) return null;

    const row = res.rows[0];
    const isExpired = Boolean(row.is_expired);
    const isHeldByMe = row.holder_id === this.holderId && !isExpired;

    return {
      lockKey: row.lock_key,
      holderId: row.holder_id,
      acquiredAt: new Date(row.acquired_at),
      expiresAt: new Date(row.expires_at),
      fenceToken: Number(row.fence_token || 1),
      isHeldByMe,
      isExpired,
    };
  }

  /**
   * Whether the lock is currently held by this instance.
   */
  isHeld(): boolean {
    return this.held;
  }

  /**
   * Get the holder ID of this lock instance.
   */
  getHolderId(): string {
    return this.holderId;
  }

  /**
   * Destroy this instance and release any active locks.
   */
  async destroy(): Promise<void> {
    this.destroyed = true;
    this.stopHeartbeat();
    await this.release();
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    const intervalMs = Math.max(500, Math.floor(this.ttlMs / 3));

    this.heartbeatTimer = setInterval(async () => {
      if (this.held && !this.destroyed) {
        await this.renew();
      }
    }, intervalMs);

    if (this.heartbeatTimer && typeof this.heartbeatTimer === 'object' && 'unref' in this.heartbeatTimer) {
      this.heartbeatTimer.unref();
    }
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }
}
