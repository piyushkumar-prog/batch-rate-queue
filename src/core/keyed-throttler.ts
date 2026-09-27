import { RateLimitConfig } from '../types';
import { Throttler } from './throttler';

/**
 * Per-key rate bucket statistics.
 */
export interface KeyBucketStats {
  availableTokens: number;
  waitingCount: number;
  effectiveRate: { requests: number; perMs: number };
}

/**
 * Keyed Throttler — Per-Key / Per-Tenant Rate Limiting
 *
 * Manages a map of independent Throttler instances, one per key.
 * Keys are created lazily on first use and can be removed/reconfigured at runtime.
 *
 * This solves the gap that BullMQ removed in v3 (group-based rate limiting)
 * and made paid-only because their implementation "was not solid enough."
 *
 * Use cases:
 * - Per-tenant API rate limits (different SLA tiers)
 * - Per-destination rate limits (different APIs have different limits)
 * - Per-API-key rate limits (rate limit each key independently)
 *
 * @example
 * ```ts
 * const keyed = new KeyedThrottler({ requests: 10, perMs: 1000 });
 *
 * // Each tenant gets independent rate limiting
 * await keyed.acquire('tenant-a'); // Uses default: 10 req/sec
 * await keyed.acquire('tenant-b'); // Independent bucket
 *
 * // Override rate for specific tenant
 * keyed.setKeyRate('premium-tenant', { requests: 100, perMs: 1000 });
 * ```
 */
export class KeyedThrottler {
  private buckets: Map<string, Throttler> = new Map();
  private readonly defaultConfig: RateLimitConfig;
  private perKeyConfigs: Map<string, RateLimitConfig> = new Map();
  private destroyed = false;

  constructor(defaultConfig: RateLimitConfig, perKeyOverrides?: Record<string, RateLimitConfig>) {
    this.defaultConfig = defaultConfig;

    if (perKeyOverrides) {
      for (const [key, config] of Object.entries(perKeyOverrides)) {
        this.perKeyConfigs.set(key, config);
      }
    }
  }

  /**
   * Acquire a token for the given key.
   * Lazily creates a new bucket if this is the first request for this key.
   *
   * @param key The rate-limit key (e.g., tenant ID, API key)
   * @param cost Number of tokens to consume. Defaults to 1.
   */
  async acquire(key: string, cost: number = 1): Promise<void> {
    if (this.destroyed) {
      throw new Error('KeyedThrottler has been destroyed');
    }

    const bucket = this.getOrCreateBucket(key);
    return bucket.acquire(cost);
  }

  /**
   * Set a custom rate limit for a specific key.
   * If the bucket already exists, reconfigures it live (no restart needed).
   * If the bucket doesn't exist yet, stores the config for lazy creation.
   */
  setKeyRate(key: string, config: RateLimitConfig): void {
    this.perKeyConfigs.set(key, config);

    const existingBucket = this.buckets.get(key);
    if (existingBucket) {
      existingBucket.setRate(config.requests, config.perMs);
    }
  }

  /**
   * Remove a key's bucket (e.g., tenant offboarded, API key revoked).
   * The bucket is destroyed and its resources are freed.
   */
  removeKey(key: string): void {
    const bucket = this.buckets.get(key);
    if (bucket) {
      bucket.destroy();
      this.buckets.delete(key);
    }
    this.perKeyConfigs.delete(key);
  }

  /**
   * Check if a bucket exists for the given key.
   */
  hasKey(key: string): boolean {
    return this.buckets.has(key);
  }

  /**
   * Get the list of all active keys (keys with created buckets).
   */
  getActiveKeys(): string[] {
    return Array.from(this.buckets.keys());
  }

  /**
   * Get the total number of active key buckets.
   */
  get size(): number {
    return this.buckets.size;
  }

  /**
   * Get stats for a specific key's bucket.
   * Returns null if the key doesn't have an active bucket.
   */
  getKeyBucketStats(key: string): KeyBucketStats | null {
    const bucket = this.buckets.get(key);
    if (!bucket) return null;

    return {
      availableTokens: bucket.getAvailableTokens(),
      waitingCount: bucket.getWaitingCount(),
      effectiveRate: bucket.getEffectiveRate(),
    };
  }

  /**
   * Get stats for all active and configured keys.
   */
  getAllKeyStats(): Map<string, KeyBucketStats> {
    const stats = new Map<string, KeyBucketStats>();
    const allKeys = new Set([...this.buckets.keys(), ...this.perKeyConfigs.keys()]);
    for (const key of allKeys) {
      const bucket = this.getOrCreateBucket(key);
      stats.set(key, {
        availableTokens: bucket.getAvailableTokens(),
        waitingCount: bucket.getWaitingCount(),
        effectiveRate: bucket.getEffectiveRate(),
      });
    }
    return stats;
  }

  /**
   * Destroy all buckets and clean up.
   */
  destroy(): void {
    this.destroyed = true;
    for (const bucket of this.buckets.values()) {
      bucket.destroy();
    }
    this.buckets.clear();
    this.perKeyConfigs.clear();
  }

  /**
   * Get or create a bucket for the given key.
   * Uses per-key config if available, otherwise falls back to default.
   */
  private getOrCreateBucket(key: string): Throttler {
    let bucket = this.buckets.get(key);
    if (!bucket) {
      const config = this.perKeyConfigs.get(key) ?? this.defaultConfig;
      bucket = new Throttler(config);
      this.buckets.set(key, bucket);
    }
    return bucket;
  }
}
