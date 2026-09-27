import { BatchFlushConfig, BufferItem } from '../types';

/**
 * Detects whether an error is a transient database connection error.
 * These errors warrant a retry with backoff rather than immediate failure.
 *
 * Covers:
 * - Prisma error codes P1001 (unreachable), P2010 (raw query failure)
 * - Common TCP/connection error messages
 */
export function isDbConnectionError(error: any): boolean {
  const code = error?.code;
  if (code === 'P1001' || code === 'P2010') return true;

  const msg = (error?.message || String(error)).toLowerCase();
  if (
    msg.includes('connection timeout') ||
    msg.includes('connection terminated') ||
    msg.includes('reach database') ||
    msg.includes('server has closed the connection') ||
    msg.includes('econnrefused') ||
    msg.includes('econnreset')
  ) {
    return true;
  }

  return false;
}

/**
 * In-Memory Write Buffer
 *
 * Accumulates `BufferItem`s in memory and flushes them to the database
 * via the user-provided `onFlush` callback. Flushing is triggered by either:
 *   1. The buffer reaching the configured `size` threshold.
 *   2. The periodic `intervalMs` timer firing.
 *
 * On transient DB connection errors, items are re-queued with an incremented
 * retry counter and exponential backoff is applied. Items that exceed
 * `maxRetries` are dropped and reported via `onError`.
 */
export class WriteBuffer {
  private buffer: BufferItem[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private flushing = false;
  private destroyed = false;

  private readonly config: BatchFlushConfig;
  private readonly maxRetries: number;
  private readonly onFlush: (batch: BufferItem[]) => Promise<void>;
  private readonly onError: (error: Error, context: string) => void;
  private readonly onFlushComplete: (count: number) => void;

  constructor(options: {
    config: BatchFlushConfig;
    maxRetries: number;
    onFlush: (batch: BufferItem[]) => Promise<void>;
    onError: (error: Error, context: string) => void;
    onFlushComplete?: (count: number) => void;
  }) {
    this.config = options.config;
    this.maxRetries = options.maxRetries;
    this.onFlush = options.onFlush;
    this.onError = options.onError;
    this.onFlushComplete = options.onFlushComplete ?? (() => {});

    this.startFlushInterval();
  }

  /**
   * Add an item to the buffer.
   * If the buffer reaches the size threshold, a flush is triggered immediately.
   */
  add(item: BufferItem): void {
    if (this.destroyed) return;

    this.buffer.push(item);

    if (this.buffer.length >= this.config.size) {
      // Fire-and-forget — errors are handled inside flush()
      this.flush().catch(() => {});
    }
  }

  /**
   * Get the current number of items in the buffer.
   */
  get length(): number {
    return this.buffer.length;
  }

  /**
   * Flush up to `config.size` items from the buffer to the database.
   * Handles transient errors with retry and exponential backoff.
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0 || this.flushing) return;

    this.flushing = true;

    try {
      // Take up to `size` items from the front of the buffer
      const batch = this.buffer.splice(0, this.config.size);

      try {
        await this.onFlush(batch);
        this.onFlushComplete(batch.length);
      } catch (error: any) {
        if (isDbConnectionError(error)) {
          // Re-queue items that haven't exceeded max retries
          const retryable: BufferItem[] = [];
          const dropped: BufferItem[] = [];

          for (const item of batch) {
            if (item.retries < this.maxRetries) {
              item.retries++;
              retryable.push(item);
            } else {
              dropped.push(item);
            }
          }

          // Put retryable items back at the front
          if (retryable.length > 0) {
            this.buffer.unshift(...retryable);
          }

          // Report dropped items
          for (const item of dropped) {
            this.onError(
              new Error(`Buffer item ${item.id} dropped after ${this.maxRetries} retries`),
              'buffer.flush.maxRetries'
            );
          }

          // Exponential backoff: 2^retries * 1000ms, capped at 30s
          const maxRetryInBatch = Math.max(...batch.map((i) => i.retries), 1);
          const backoffMs = Math.min(Math.pow(2, maxRetryInBatch) * 1000, 30000);
          await this.sleep(backoffMs);
        } else {
          // Non-transient error — report and drop the batch
          this.onError(error instanceof Error ? error : new Error(String(error)), 'buffer.flush');
        }
      }
    } finally {
      this.flushing = false;
    }
  }

  /**
   * Flush ALL remaining items in the buffer (may require multiple passes).
   * Used during graceful shutdown.
   */
  async flushAll(): Promise<void> {
    while (this.buffer.length > 0) {
      await this.flush();
    }
  }

  /**
   * Stop the flush interval timer and clean up.
   */
  destroy(): void {
    this.destroyed = true;
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  /**
   * Start the periodic flush interval.
   */
  private startFlushInterval(): void {
    this.flushTimer = setInterval(() => {
      if (!this.destroyed && this.buffer.length > 0) {
        this.flush().catch(() => {});
      }
    }, this.config.intervalMs);

    // Unref so the timer doesn't prevent Node from exiting
    if (this.flushTimer && typeof this.flushTimer === 'object' && 'unref' in this.flushTimer) {
      this.flushTimer.unref();
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
