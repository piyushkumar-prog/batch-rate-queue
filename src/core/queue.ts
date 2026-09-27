import { EventEmitter } from 'events';
import { BatchRateQueueOptions, BufferItem, QueueStats, WorkerResult, QueueEvents } from '../types';
import { Throttler } from './throttler';
import { WriteBuffer } from './buffer';
import { setupGracefulShutdown } from './shutdown';

/**
 * BatchRateQueue
 *
 * The main orchestrator that ties together:
 *   - A token-bucket rate limiter (Throttler)
 *   - An in-memory write buffer (WriteBuffer)
 *   - A self-scheduling drain loop
 *   - Graceful shutdown handling
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
  private readonly buffer: WriteBuffer;

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

    // Initialize rate limiter
    this.throttler = new Throttler(this.options.rateLimit);

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
    });

    // Prevent unhandled 'error' event crashes from EventEmitter.
    // If the user hasn't registered an error listener, swallow the event
    // (errors are still reported via onError callback).
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
    this.pendingItems.push(item);

    // Auto-start if not running
    if (!this.running && !this.paused) {
      this.start();
    }
  }

  /**
   * Enqueue multiple items for processing.
   */
  addMany(items: T[]): void {
    this.pendingItems.push(...items);

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

    if (this.pendingItems.length > 0 && !this.draining) {
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
      pending: this.pendingItems.length,
      running: this.running,
    };
  }

  /**
   * Destroy the queue, cleaning up all timers and handlers.
   */
  destroy(): void {
    this.running = false;
    this.draining = false;
    this.throttler.destroy();
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
    if (this.pendingItems.length === 0 && this.buffer.length === 0 && !this.draining) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.once('drain', resolve);
    });
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
      while (this.pendingItems.length > 0 && this.running && !this.paused) {
        // Process items up to concurrency limit
        const concurrency = this.options.concurrency;
        const batch = this.pendingItems.splice(0, concurrency);

        const promises = batch.map(async (item) => {
          try {
            // Acquire a rate-limit token (may block)
            await this.throttler.acquire();

            if (!this.running || this.paused) {
              // Re-enqueue the item if stopped or paused during the wait
              this.pendingItems.unshift(item);
              return;
            }

            // Call the user's worker function
            const result = await this.options.worker(item);

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
    if (this.pendingItems.length === 0 && this.running) {
      await this.buffer.flushAll();
      this.running = false;
      this.emit('drain');
      this.emit('idle');
    }
  }
}
