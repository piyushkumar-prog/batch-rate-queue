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
} from '../types';
import { Throttler } from './throttler';
import { AdaptiveThrottler, WorkerOutcome } from './adaptive-throttler';
import { KeyedThrottler } from './keyed-throttler';
import { FairScheduler } from './fair-scheduler';
import { WriteBuffer } from './buffer';
import { setupGracefulShutdown } from './shutdown';

/**
 * BatchRateQueue
 *
 * The main orchestrator that ties together:
 *   - A token-bucket rate limiter (Throttler, AdaptiveThrottler, or KeyedThrottler)
 *   - An optional fair-share scheduler (FairScheduler)
 *   - An in-memory write buffer (WriteBuffer)
 *   - A self-scheduling drain loop
 *   - Graceful shutdown handling
 *
 * v1.1.0 additions:
 *   - Adaptive throttling (reads live 429s, error rates, Retry-After headers)
 *   - Cost-weighted rate limiting (for LLM token-based budgets)
 *   - Runtime-adjustable rate limits (setRateLimit / setBatchFlush)
 *   - Pluggable error classifiers (httpRateLimitClassifier, llmApiClassifier, etc.)
 *
 * v1.2.0 additions:
 *   - Per-key/per-tenant rate limiting (rateLimitKey + perKeyRateLimit)
 *   - Weighted fair-share scheduling (fairShare: true) via Deficit Round Robin
 *
 * Usage:
 * ```ts
 * const queue = new BatchRateQueue({
 *   name: 'my-enrichment',
 *   rateLimit: { requests: 2, perMs: 1000 },
 *   batchFlush: { size: 50, intervalMs: 2000 },
 *   worker: async (item) => {
 *     const result = await callApi(item);
 *     return { id: item.id, updates: result };
 *   },
 *   onBatchFlush: async (batch) => {
 *     await db.batchUpdate(batch);
 *   },
 * });
 *
 * await queue.addMany(items);
 * ```
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
  private readonly buffer: WriteBuffer;
  private readonly isAdaptive: boolean;
  private readonly isCostBased: boolean;
  private readonly isKeyed: boolean;
  private readonly isFairShare: boolean;

  private pendingItems: T[] = [];
  private processedCount = 0;
  private failedCount = 0;
  private running = false;
  private paused = false;
  private draining = false;
  private shutdownCleanup: (() => void) | null = null;

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

    // Initialize rate limiter (adaptive or standard)
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

    // Initialize write buffer
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
   * Update the rate limit configuration at runtime.
   * Takes effect immediately on the next token refill cycle.
   * Does NOT require restarting the queue.
   */
  setRateLimit(config: Partial<RateLimitConfig>): void {
    const newRequests = config.requests ?? this.options.rateLimit.requests;
    const newPerMs = config.perMs ?? this.options.rateLimit.perMs;

    this.throttler.setRate(newRequests, newPerMs);

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
   *
   * @param key The rate-limit key
   * @param config The rate limit for this specific key
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
   * Destroy the queue, cleaning up all timers and handlers.
   */
  destroy(): void {
    this.running = false;
    this.draining = false;
    this.throttler.destroy();
    this.keyedThrottler?.destroy();
    this.scheduler?.clear();
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
   *
   * Returns an array of { key, item } where key is the rate-limit key
   * (or '__default__' if keyed throttling is not enabled).
   */
  private dequeueItems(count: number): Array<{ key: string; item: T }> {
    if (this.scheduler) {
      return this.scheduler.dequeueBatch(count);
    }

    // Standard FIFO dequeue
    const items = this.pendingItems.splice(0, count);
    const keyExtractor = this.options.rateLimitKey;

    return items.map(item => ({
      key: keyExtractor ? keyExtractor(item) : '__default__',
      item,
    }));
  }

  /**
   * Acquire a rate-limit token for the given key and cost.
   * Routes to keyed throttler if per-key rate limiting is enabled,
   * otherwise uses the single shared throttler.
   */
  private async acquireToken(key: string, cost: number): Promise<void> {
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
            // Determine cost for this item
            const cost = this.isCostBased && this.options.costExtractor
              ? this.options.costExtractor(item)
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
            const result = await this.options.worker(item);

            // Record success for adaptive throttling
            if (this.isAdaptive) {
              const outcome: WorkerOutcome = {
                success: true,
                statusCode: result?.meta?.statusCode,
                retryAfterSeconds: result?.meta?.retryAfterSeconds,
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

            // Record failure for adaptive throttling
            if (this.isAdaptive) {
              const outcome: WorkerOutcome = {
                success: false,
                statusCode: error?.status ?? error?.statusCode ?? error?.response?.status,
                retryAfterSeconds: undefined,
                error: err,
              };

              // Extract Retry-After from error if available
              const retryAfter =
                error?.headers?.['retry-after'] ??
                error?.response?.headers?.['retry-after'];
              if (retryAfter) {
                const seconds = parseInt(retryAfter, 10);
                if (!isNaN(seconds) && seconds > 0) {
                  outcome.retryAfterSeconds = seconds;
                }
              }

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
