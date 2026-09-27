import { WorkerResult } from '../types';

/**
 * An idempotency entry representing a previously completed external side-effect.
 */
export interface IdempotencyRecord {
  key: string;
  result: WorkerResult;
  createdAt: number;
  expiresAt?: number;
}

/**
 * Storage interface for idempotency records.
 * Can be implemented for Redis, PostgreSQL, SQLite, or In-Memory.
 */
export interface IdempotencyStore {
  get(key: string): Promise<IdempotencyRecord | null> | IdempotencyRecord | null;
  set(key: string, record: IdempotencyRecord, ttlMs?: number): Promise<void> | void;
  delete(key: string): Promise<void> | void;
  clear(): Promise<void> | void;
}

/**
 * Default in-memory Idempotency Store with automatic TTL eviction.
 */
export class MemoryIdempotencyStore implements IdempotencyStore {
  private records: Map<string, IdempotencyRecord> = new Map();
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;
  private readonly maxEntries: number;

  constructor(options?: { maxEntries?: number; cleanupIntervalMs?: number }) {
    this.maxEntries = options?.maxEntries ?? 50000;
    const cleanupIntervalMs = options?.cleanupIntervalMs ?? 60000;

    this.cleanupTimer = setInterval(() => this.purgeExpired(), cleanupIntervalMs);
    if (this.cleanupTimer && typeof this.cleanupTimer === 'object' && 'unref' in this.cleanupTimer) {
      this.cleanupTimer.unref();
    }
  }

  get(key: string): IdempotencyRecord | null {
    const record = this.records.get(key);
    if (!record) return null;

    if (record.expiresAt && record.expiresAt < Date.now()) {
      this.records.delete(key);
      return null;
    }

    return record;
  }

  set(key: string, record: IdempotencyRecord, ttlMs?: number): void {
    if (this.records.size >= this.maxEntries) {
      this.purgeExpired();
      if (this.records.size >= this.maxEntries) {
        // Evict oldest entry
        const oldestKey = this.records.keys().next().value;
        if (oldestKey) this.records.delete(oldestKey);
      }
    }

    const expiresAt = ttlMs ? Date.now() + ttlMs : record.expiresAt;
    this.records.set(key, { ...record, expiresAt });
  }

  delete(key: string): void {
    this.records.delete(key);
  }

  clear(): void {
    this.records.clear();
  }

  destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    this.clear();
  }

  get size(): number {
    return this.records.size;
  }

  private purgeExpired(): void {
    const now = Date.now();
    for (const [key, record] of this.records.entries()) {
      if (record.expiresAt && record.expiresAt < now) {
        this.records.delete(key);
      }
    }
  }
}

/**
 * Idempotency Manager
 *
 * Prevents duplicate external side-effects when jobs are redelivered after crashes.
 * Checks for existing cached executions before firing worker calls and records
 * successful executions atomically with the result.
 */
export class IdempotencyManager {
  private readonly store: IdempotencyStore;
  private readonly defaultTtlMs: number;

  constructor(options?: { store?: IdempotencyStore; defaultTtlMs?: number }) {
    this.store = options?.store ?? new MemoryIdempotencyStore();
    this.defaultTtlMs = options?.defaultTtlMs ?? 24 * 60 * 60 * 1000; // 24 hours
  }

  /**
   * Check if a task with this idempotency key was already completed.
   * Returns the previously recorded WorkerResult or null if not yet executed.
   */
  async check(key: string): Promise<WorkerResult | null> {
    const record = await this.store.get(key);
    return record ? record.result : null;
  }

  /**
   * Record a completed execution result under the idempotency key.
   */
  async record(key: string, result: WorkerResult, ttlMs?: number): Promise<void> {
    const record: IdempotencyRecord = {
      key,
      result,
      createdAt: Date.now(),
      expiresAt: Date.now() + (ttlMs ?? this.defaultTtlMs),
    };
    await this.store.set(key, record, ttlMs ?? this.defaultTtlMs);
  }

  /**
   * Clear the idempotency store.
   */
  async clear(): Promise<void> {
    await this.store.clear();
  }

  /**
   * Destroy the idempotency manager.
   */
  destroy(): void {
    if (this.store instanceof MemoryIdempotencyStore) {
      this.store.destroy();
    }
  }
}
