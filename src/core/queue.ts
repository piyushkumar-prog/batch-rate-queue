import { EventEmitter } from 'events';
import {
  BatchRateQueueOptions,
  BatchFlushConfig,
  BufferItem,
  QueueStats,
  RateLimitConfig,
  WorkerResult,
  QueueEvents,
  RateChangeEvent,
  BudgetStats,
} from '../types';
import { Throttler } from './throttler';
import { AdaptiveThrottler, WorkerOutcome } from './adaptive-throttler';
import { KeyedThrottler } from './keyed-throttler';
import { FairScheduler } from './fair-scheduler';
import { WriteBuffer } from './buffer';
import { setupGracefulShutdown } from './shutdown';
import { PgTokenBucket } from '../distributed/pg-token-bucket';
import { PgCircuitBreaker, CircuitState } from '../distributed/pg-circuit-breaker';
import { IdempotencyManager } from './idempotency';
import { ClaimCheckManager, isClaimCheckRef } from './claim-check';
import { parseRateLimitHeaders } from './header-parser';

/**
 * BatchRateQueue
 *
 * The main orchestrator that ties together:
 *   - A token-bucket rate limiter (Throttler, AdaptiveThrottler, KeyedThrottler, or PgTokenBucket)
 *   - An optional fair-share scheduler (FairScheduler)
 *   - An optional distributed circuit breaker (PgCircuitBreaker)
 *   - Built-in idempotency deduplication (IdempotencyManager)
 *   - Automatic claim-check payload offloading (ClaimCheckManager)
 *   - Multi-provider rate-limit header parser (parseRateLimitHeaders)
 *   - An in-memory write buffer with partial batch failure isolation (WriteBuffer)
 *   - A self-scheduling drain loop
 *   - Graceful shutdown handling
 */
export class BatchRateQueue<T> extends EventEmitter {
  readonly name: string;

  private readonly options: Required<
    Pick<BatchRateQueueOptions<T>, 'maxRetries' | 'concurrency' | 'gracefulShutdown'>
  > &
    BatchRateQueueOptions<T>;

  private readonly throttler: Throttler;
  private readonly keyedThrottler: KeyedThrottler | null;
  private readonly scheduler: FairScheduler<T> | null;
  private readonly pgTokenBucket: PgTokenBucket | null;
  private readonly circuitBreaker: PgCircuitBreaker | null;
  private readonly idempotencyManager: IdempotencyManager | null;
  private readonly claimCheckManager: ClaimCheckManager;
  private readonly buffer: WriteBuffer;
  private readonly isAdaptive: boolean;
  private readonly isCostBased: boolean;
  private readonly isKeyed: boolean;
  private readonly isFairShare: boolean;
  private readonly isDistributed: boolean;

  private pendingItems: any[] = [];
  private processedCount = 0;
  private failedCount = 0;
  private running = false;
  private paused = false;
  private draining = false;
  private shutdownCleanup: (() => void) | null = null;
  private lastBreakerState: {
    state: CircuitState;
    failureCount: number;
    lastFailure: Date | null;
    openedAt: Date | null;
  } | null = null;

  constructor(options: BatchRateQueueOptions<T>) {
    super();

    this.name = options.name;

    // Merge defaults
    this.options = {
      maxRetries: 3,
      concurrency: 1,
      gracefulShutdown: true,
      onError: (err, ctx) => console.error(`[BatchRateQueue:${this.name}] Error in ${ctx}:`, err),
      ...options,
    };

    this.isCostBased = !!this.options.rateLimit.costBased;
    this.isAdaptive = !!this.options.adaptiveThrottle?.enabled;
    this.isKeyed = !!this.options.rateLimitKey;
    this.isFairShare = !!this.options.fairShare && this.isKeyed;
    this.isDistributed = !!this.options.distributed;

    // Initialize distributed rate limiter if configured
    if (this.isDistributed && this.options.distributed) {
      this.pgTokenBucket = new PgTokenBucket({
        pool: this.options.distributed.pool,
        bucketKey: this.options.distributed.bucketKey ?? this.name,
        rateLimit: this.options.rateLimit,
        tableName: this.options.distributed.tableName,
        autoCreateSchema: this.options.distributed.autoCreateSchema,
        retryIntervalMs: this.options.distributed.retryIntervalMs,
        acquireTimeoutMs: this.options.distributed.acquireTimeoutMs,
      });
    } else {
      this.pgTokenBucket = null;
    }

    // Initialize distributed circuit breaker if configured
    if (this.options.circuitBreaker && this.options.circuitBreaker.enabled !== false) {
      this.circuitBreaker = new PgCircuitBreaker({
        pool: this.options.circuitBreaker.pool,
        breakerKey: this.options.circuitBreaker.breakerKey ?? `${this.name}:breaker`,
        failureThreshold: this.options.circuitBreaker.failureThreshold,
        cooldownMs: this.options.circuitBreaker.cooldownMs,
        tableName: this.options.circuitBreaker.tableName,
        autoCreateSchema: this.options.circuitBreaker.autoCreateSchema,
      });
    } else {
      this.circuitBreaker = null;
    }

    // Initialize idempotency manager if configured
    if (this.options.idempotencyKey) {
      this.idempotencyManager = new IdempotencyManager({
        store: this.options.idempotencyStore,
        defaultTtlMs: this.options.idempotencyTtlMs,
      });
    } else {
      this.idempotencyManager = null;
    }

    // Initialize claim-check manager for large payloads
    this.claimCheckManager = new ClaimCheckManager({
      store: this.options.payloadStore,
      thresholdBytes: this.options.claimCheckThresholdBytes,
    });

    // Initialize local rate limiter (adaptive or standard)
    if (this.isAdaptive && this.options.adaptiveThrottle) {
      const adaptiveConfig = {
        ...this.options.adaptiveThrottle,
        onRateChange: (event: RateChangeEvent) => {
          this.options.adaptiveThrottle?.onRateChange?.(event);
          this.emit('rateLimitChanged', event);
        },
      };
      this.throttler = new AdaptiveThrottler(this.options.rateLimit, adaptiveConfig);
    } else {
      this.throttler = new Throttler(this.options.rateLimit);
    }

    // Initialize per-key throttler if rateLimitKey is set
    if (this.isKeyed) {
      this.keyedThrottler = new KeyedThrottler(
        this.options.rateLimit,
        this.options.perKeyRateLimit,
      );
    } else {
      this.keyedThrottler = null;
    }

    // Initialize fair-share scheduler if enabled
    if (this.isFairShare && this.options.rateLimitKey) {
      this.scheduler = new FairScheduler<T>(this.options.rateLimitKey);
    } else {
      this.scheduler = null;
    }

    // Initialize write buffer with partial batch failure isolation
    this.buffer = new WriteBuffer({
      config: this.options.batchFlush,
      maxRetries: this.options.maxRetries,
      onFlush: this.options.onBatchFlush,
      onError: (err, ctx) => {
        this.options.onError!(err, ctx);
        this.emit('error', err, ctx);
      },
      onFlushComplete: (count) => {
        this.emit('flush', count);
      },
      errorClassifier: this.options.errorClassifier,
      isolateBatchFailures: this.options.isolateBatchFailures ?? true,
    });

    // Prevent unhandled 'error' event crashes from EventEmitter.
    this.on('error', () => {});

    // Register graceful shutdown
    if (this.options.gracefulShutdown) {
      this.shutdownCleanup = setupGracefulShutdown(this.buffer, async () => {
        this.running = false;
        this.emit('drain');
      });
    }
  }

  /**
   * Enqueue a single item for processing.
   */
  add(item: T): void {
    if (this.scheduler) {
      this.scheduler.add(item);
    } else {
      this.pendingItems.push(item);
    }

    // Auto-start if not running
    if (!this.running && !this.paused) {
      this.start();
    }
  }

  /**
   * Enqueue multiple items for processing.
   */
  addMany(items: T[]): void {
    if (this.scheduler) {
      this.scheduler.addMany(items);
    } else {
      this.pendingItems.push(...items);
    }

    // Auto-start if not running
    if (!this.running && !this.paused) {
      this.start();
    }
  }

  /**
   * Start the drain loop. Called automatically by add/addMany.
   */
  start(): void {
    if (this.running) return;
    this.running = true;
    this.paused = false;
    this.drain();
  }

  /**
   * Stop the queue. In-flight items finish, but no new items are picked up.
   * The buffer is flushed before stopping.
   */
  async stop(): Promise<void> {
    this.running = false;
    this.draining = false;

    // Flush remaining buffer
    await this.buffer.flushAll();

    this.emit('idle');
  }

  /**
   * Pause processing. Items remain in the pending queue.
   */
  pause(): void {
    this.paused = true;
  }

  /**
   * Resume processing after a pause.
   */
  resume(): void {
    if (!this.paused) return;
    this.paused = false;

    if (this.getPendingCount() > 0 && !this.draining) {
      this.drain();
    }
  }

  /**
   * Get current queue statistics.
   */
  getStats(): QueueStats {
    return {
      processed: this.processedCount,
      failed: this.failedCount,
      buffered: this.buffer.length,
      pending: this.getPendingCount(),
      running: this.running,
    };
  }

  /**
   * Get comprehensive budget and throughput statistics.
   * Designed for dashboard endpoints, Prometheus metrics, or health checks.
   */
  getBudgetStats(): BudgetStats {
    const effectiveRate = this.throttler.getEffectiveRate();
    const availableTokens = this.throttler.getAvailableTokens();
    const maxTokens = this.options.rateLimit.requests;
    const utilizationPct = Math.max(
      0,
      Math.min(100, Math.round(((maxTokens - availableTokens) / maxTokens) * 100))
    );

    const requestsPerSec = effectiveRate.requests / (effectiveRate.perMs / 1000);
    const pending = this.getPendingCount();
    const estimatedSecondsRemaining =
      requestsPerSec > 0 && pending > 0 ? Math.ceil(pending / requestsPerSec) : 0;

    const estimatedCompletionTime =
      pending > 0 && estimatedSecondsRemaining > 0
        ? new Date(Date.now() + estimatedSecondsRemaining * 1000).toISOString()
        : null;

    let keysStats: Record<string, any> | null = null;
    if (this.keyedThrottler) {
      const statsMap = this.keyedThrottler.getAllKeyStats();
      const schedulerMap = this.scheduler?.getKeyStats();
      const obj: Record<string, any> = {};

      for (const [k, v] of statsMap.entries()) {
        obj[k] = {
          availableTokens: v.availableTokens,
          waitingCount: v.waitingCount,
          pending: schedulerMap?.get(k)?.pending ?? 0,
          deficit: schedulerMap?.get(k)?.deficit ?? 0,
        };
      }
      keysStats = obj;
    }

    return {
      queue: {
        name: this.name,
        state: this.paused ? 'paused' : this.running ? 'running' : 'idle',
        pending,
        processed: this.processedCount,
        failed: this.failedCount,
      },
      rateLimit: {
        configured: { ...this.options.rateLimit },
        effective: effectiveRate,
        availableTokens,
        waitingCount: this.throttler.getWaitingCount(),
        utilizationPct,
      },
      buffer: {
        currentSize: this.buffer.length,
        flushThreshold: this.options.batchFlush.size,
        totalFlushed: this.buffer.totalFlushed,
        failedFlushes: this.buffer.failedFlushes,
      },
      backlogEta: {
        estimatedSecondsRemaining,
        estimatedCompletionTime,
      },
      keys: keysStats,
      circuitBreaker: this.lastBreakerState,
    };
  }

  /**
   * Update the rate limit configuration at runtime.
   * Takes effect immediately on the next token refill cycle.
   * Does NOT require restarting the queue.
   */
  setRateLimit(config: Partial<RateLimitConfig>): void {
    const newRequests = config.requests ?? this.options.rateLimit.requests;
    const newPerMs = config.perMs ?? this.options.rateLimit.perMs;

    this.throttler.setRate(newRequests, newPerMs);
    if (this.pgTokenBucket) {
      this.pgTokenBucket.setRate({ requests: newRequests, perMs: newPerMs }).catch(() => {});
    }

    // Update stored config
    this.options.rateLimit.requests = newRequests;
    this.options.rateLimit.perMs = newPerMs;
    if (config.costBased !== undefined) {
      this.options.rateLimit.costBased = config.costBased;
    }

    this.emit('rateLimitChanged', {
      previousRate: this.options.rateLimit.requests,
      newRate: newRequests,
      reason: 'manual' as const,
      errorRate: this.isAdaptive ? (this.throttler as AdaptiveThrottler).getErrorRate() : 0,
    });
  }

  /**
   * Set a custom rate limit for a specific key (tenant/API key/destination).
   * Only works when `rateLimitKey` is configured.
   */
  setKeyRateLimit(key: string, config: RateLimitConfig): void {
    if (!this.keyedThrottler) {
      throw new Error('setKeyRateLimit requires rateLimitKey to be configured');
    }
    this.keyedThrottler.setKeyRate(key, config);
  }

  /**
   * Remove a key's rate bucket (e.g., tenant offboarded).
   * Only works when `rateLimitKey` is configured.
   */
  removeKeyRateLimit(key: string): void {
    if (!this.keyedThrottler) {
      throw new Error('removeKeyRateLimit requires rateLimitKey to be configured');
    }
    this.keyedThrottler.removeKey(key);
  }

  /**
   * Update the batch flush configuration at runtime.
   * Takes effect immediately.
   */
  setBatchFlush(config: Partial<BatchFlushConfig>): void {
    this.buffer.updateConfig(config);
    Object.assign(this.options.batchFlush, config);
  }

  /**
   * Destroy the queue, cleaning up all timers, handles, and connections.
   */
  destroy(): void {
    this.running = false;
    this.draining = false;
    this.throttler.destroy();
    this.keyedThrottler?.destroy();
    this.scheduler?.clear();
    this.pgTokenBucket?.destroy().catch(() => {});
    this.circuitBreaker?.destroy().catch(() => {});
    this.idempotencyManager?.destroy();
    this.claimCheckManager.destroy().catch(() => {});
    this.buffer.destroy();

    if (this.shutdownCleanup) {
      this.shutdownCleanup();
      this.shutdownCleanup = null;
    }

    this.removeAllListeners();
  }

  /**
   * Wait until the queue is fully drained (all items processed and buffer flushed).
   * Returns a promise that resolves when the 'drain' event fires.
   */
  waitUntilDrained(): Promise<void> {
    if (this.getPendingCount() === 0 && this.buffer.length === 0 && !this.draining) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.once('drain', resolve);
    });
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Get the total number of pending items across all sources.
   */
  private getPendingCount(): number {
    if (this.scheduler) {
      return this.scheduler.length;
    }
    return this.pendingItems.length;
  }

  /**
   * Dequeue the next batch of items for processing.
   * Uses the fair scheduler if enabled, otherwise splices from the FIFO array.
   */
  private dequeueItems(count: number): Array<{ key: string; item: T }> {
    if (this.scheduler) {
      return this.scheduler.dequeueBatch(count);
    }

    // Standard FIFO dequeue
    const items = this.pendingItems.splice(0, count);
    const keyExtractor = this.options.rateLimitKey;

    return items.map((item) => ({
      key: keyExtractor ? keyExtractor(item) : '__default__',
      item,
    }));
  }

  /**
   * Acquire a rate-limit token for the given key and cost.
   * Routes to PgTokenBucket (if distributed), KeyedThrottler (if keyed),
   * or standard in-memory Throttler.
   */
  private async acquireToken(key: string, cost: number): Promise<void> {
    if (this.pgTokenBucket) {
      return this.pgTokenBucket.acquire(cost);
    }
    if (this.keyedThrottler && key !== '__default__') {
      return this.keyedThrottler.acquire(key, cost);
    }
    return this.throttler.acquire(cost);
  }

  // ---------------------------------------------------------------------------
  // Private: drain loop
  // ---------------------------------------------------------------------------

  /**
   * Self-scheduling drain loop.
   * Processes items from the pending queue with rate limiting and concurrency control.
   */
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;

    try {
      while (this.getPendingCount() > 0 && this.running && !this.paused) {
        // Process items up to concurrency limit
        const concurrency = this.options.concurrency;
        const batch = this.dequeueItems(concurrency);

        if (batch.length === 0) break;

        const promises = batch.map(async ({ key, item }) => {
          try {
            // Hydrate claim-check payload if offloaded
            const actualItem: T = await this.claimCheckManager.hydrateIfNeeded(item);

            // Check idempotency if configured
            let idempKey: string | null = null;
            if (this.idempotencyManager && this.options.idempotencyKey) {
              idempKey = this.options.idempotencyKey(actualItem);
              const cachedResult = await this.idempotencyManager.check(idempKey);

              if (cachedResult) {
                // Return cached result without firing side-effect
                if (cachedResult.id) {
                  const bufferItem: BufferItem = {
                    id: cachedResult.id,
                    updates: cachedResult.updates,
                    retries: 0,
                  };
                  this.buffer.add(bufferItem);
                }
                this.processedCount++;
                this.emit('processed', cachedResult);
                return;
              }
            }

            // Check circuit breaker if configured
            if (this.circuitBreaker) {
              const allowed = await this.circuitBreaker.allowRequest();
              if (!allowed) {
                // Circuit is OPEN — re-enqueue item and wait briefly
                if (this.scheduler) {
                  this.scheduler.add(item);
                } else {
                  this.pendingItems.unshift(item);
                }
                const cbState = await this.circuitBreaker.getState();
                this.lastBreakerState = {
                  state: cbState.state,
                  failureCount: cbState.failureCount,
                  lastFailure: cbState.lastFailure,
                  openedAt: cbState.openedAt,
                };
                this.emit('circuitBreakerTripped', {
                  breakerKey: cbState.breakerKey,
                  failureCount: cbState.failureCount,
                  openedAt: cbState.openedAt ?? new Date(),
                });
                await new Promise((resolve) =>
                  setTimeout(resolve, Math.min(1000, cbState.cooldownMs))
                );
                return;
              }
            }

            // Determine cost for this item
            const cost =
              this.isCostBased && this.options.costExtractor
                ? this.options.costExtractor(actualItem)
                : 1;

            // Acquire rate-limit tokens (may block)
            await this.acquireToken(key, cost);

            if (!this.running || this.paused) {
              // Re-enqueue the item if stopped or paused during the wait
              if (this.scheduler) {
                this.scheduler.add(item);
              } else {
                this.pendingItems.unshift(item);
              }
              return;
            }

            // Call the user's worker function
            const result = await this.options.worker(actualItem);

            // Record in idempotency manager upon success
            if (result && idempKey && this.idempotencyManager) {
              await this.idempotencyManager.record(idempKey, result);
            }

            // Record success for circuit breaker
            if (this.circuitBreaker) {
              const hadTripped =
                this.lastBreakerState?.state === 'open' ||
                this.lastBreakerState?.state === 'half-open';
              await this.circuitBreaker.recordSuccess();
              const cbState = await this.circuitBreaker.getState();
              this.lastBreakerState = {
                state: cbState.state,
                failureCount: cbState.failureCount,
                lastFailure: cbState.lastFailure,
                openedAt: cbState.openedAt,
              };
              if (hadTripped) {
                this.emit('circuitBreakerReset', {
                  breakerKey: cbState.breakerKey,
                });
              }
            }

            // Record success for adaptive throttling & parse rate limit headers
            if (this.isAdaptive) {
              let retryAfterSeconds = result?.meta?.retryAfterSeconds;
              if (result?.meta?.headers && !retryAfterSeconds) {
                const parsed = parseRateLimitHeaders(result.meta.headers);
                if (parsed.retryAfterMs) {
                  retryAfterSeconds = Math.ceil(parsed.retryAfterMs / 1000);
                }
              }

              const outcome: WorkerOutcome = {
                success: true,
                statusCode: result?.meta?.statusCode,
                retryAfterSeconds,
              };
              (this.throttler as AdaptiveThrottler).recordOutcome(outcome);
            }

            if (result) {
              // Push result into the write buffer
              const bufferItem: BufferItem = {
                id: result.id,
                updates: result.updates,
                retries: 0,
              };
              this.buffer.add(bufferItem);
              this.processedCount++;
              this.emit('processed', result);
            } else {
              // Worker returned null — skip this item
              this.processedCount++;
            }
          } catch (error: any) {
            this.failedCount++;
            const err = error instanceof Error ? error : new Error(String(error));

            // Record failure for circuit breaker
            if (this.circuitBreaker) {
              const tripped = await this.circuitBreaker.recordFailure();
              const cbState = await this.circuitBreaker.getState();
              this.lastBreakerState = {
                state: cbState.state,
                failureCount: cbState.failureCount,
                lastFailure: cbState.lastFailure,
                openedAt: cbState.openedAt,
              };
              if (tripped) {
                this.emit('circuitBreakerTripped', {
                  breakerKey: cbState.breakerKey,
                  failureCount: cbState.failureCount,
                  openedAt: cbState.openedAt ?? new Date(),
                });
              }
            }

            // Record failure for adaptive throttling
            if (this.isAdaptive) {
              const headers = error?.headers ?? error?.response?.headers;
              const parsed = parseRateLimitHeaders(headers);

              let retryAfterSeconds: number | undefined;
              if (parsed.retryAfterMs) {
                retryAfterSeconds = Math.ceil(parsed.retryAfterMs / 1000);
              }

              const outcome: WorkerOutcome = {
                success: false,
                statusCode: error?.status ?? error?.statusCode ?? error?.response?.status,
                retryAfterSeconds,
                error: err,
              };

              (this.throttler as AdaptiveThrottler).recordOutcome(outcome);
            }

            this.options.onError!(err, `worker(${JSON.stringify(item).slice(0, 100)})`);
            this.emit('error', err, 'worker');
          }
        });

        await Promise.all(promises);
      }
    } finally {
      this.draining = false;
    }

    // If we've drained all pending items, flush the buffer and emit drain
    if (this.getPendingCount() === 0 && this.running) {
      await this.buffer.flushAll();
      this.running = false;
      this.emit('drain');
      this.emit('idle');
    }
  }
}
