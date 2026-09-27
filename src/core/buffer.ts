import { BatchFlushConfig, BufferItem, ErrorClassifier } from '../types';
import { dbConnectionClassifier } from './error-classifier';

/**
 * Detects whether an error is a transient database connection error.
 * These errors warrant a retry with backoff rather than immediate failure.
 *
 * Covers:
 * - Prisma error codes P1001 (unreachable), P2010 (raw query failure)
 * - Common TCP/connection error messages
 *
 * @deprecated Use `dbConnectionClassifier` from `error-classifier.ts` instead.
 *             Kept for backward compatibility.
 */
export function isDbConnectionError(error: any): boolean {
  const result = dbConnectionClassifier(error);
  return result !== null && result.verdict !== 'fail-fast';
}

/**
 * In-Memory Write Buffer
 *
 * Accumulates `BufferItem`s in memory and flushes them to the database
 * via the user-provided `onFlush` callback. Flushing is triggered by either:
 *   1. The buffer reaching the configured `size` threshold.
 *   2. The periodic `intervalMs` timer firing.
 *
 * On transient errors (classified by the error classifier), items are re-queued
 * with an incremented retry counter and exponential backoff is applied. Items
 * that exceed `maxRetries` are dropped and reported via `onError`.
 */
export class WriteBuffer {
  private buffer: BufferItem[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private flushing = false;
  private destroyed = false;

  private config: BatchFlushConfig;
  private readonly maxRetries: number;
  private readonly onFlush: (batch: BufferItem[]) => Promise<void>;
  private readonly onError: (error: Error, context: string) => void;
  private readonly onFlushComplete: (count: number) => void;
  private readonly errorClassifier: ErrorClassifier;

  private readonly isolateBatchFailures: boolean;

  /** Total items successfully flushed (for stats/dashboard). */
  totalFlushed: number = 0;
  /** Total flush operations that failed. */
  failedFlushes: number = 0;

  constructor(options: {
    config: BatchFlushConfig;
    maxRetries: number;
    onFlush: (batch: BufferItem[]) => Promise<void>;
    onError: (error: Error, context: string) => void;
    onFlushComplete?: (count: number) => void;
    errorClassifier?: ErrorClassifier;
    isolateBatchFailures?: boolean;
  }) {
    this.config = options.config;
    this.maxRetries = options.maxRetries;
    this.onFlush = options.onFlush;
    this.onError = options.onError;
    this.onFlushComplete = options.onFlushComplete ?? (() => {});
    this.errorClassifier = options.errorClassifier ?? dbConnectionClassifier;
    this.isolateBatchFailures = options.isolateBatchFailures ?? true;

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
   * Update the flush configuration at runtime without restarting the buffer.
   */
  updateConfig(config: Partial<BatchFlushConfig>): void {
    if (config.size !== undefined) this.config.size = config.size;
    if (config.intervalMs !== undefined) {
      this.config.intervalMs = config.intervalMs;
      // Restart the flush interval with new timing
      this.stopFlushInterval();
      this.startFlushInterval();
    }
  }

  /**
   * Flush up to `config.size` items from the buffer to the database.
   * Uses the error classifier to determine retry/fail behavior.
   * On non-transient constraint errors, isolates failures item-by-item so
   * valid rows still commit and only the failing row is dropped.
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0 || this.flushing) return;

    this.flushing = true;

    try {
      // Take up to `size` items from the front of the buffer
      const batch = this.buffer.splice(0, this.config.size);

      try {
        await this.onFlush(batch);
        this.totalFlushed += batch.length;
        this.onFlushComplete(batch.length);
      } catch (error: any) {
        this.failedFlushes++;

        // Classify the error using the pluggable classifier
        const classification = this.errorClassifier(error);

        if (classification && (classification.verdict === 'retry-backoff' || classification.verdict === 'retry-after')) {
          // Transient error — re-queue items that haven't exceeded max retries
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
              new Error(`Buffer item ${item.id} dropped after ${this.maxRetries} retries (${classification.reason})`),
              'buffer.flush.maxRetries'
            );
          }

          // Determine backoff duration
          let backoffMs: number;
          if (classification.verdict === 'retry-after' && classification.retryAfterMs) {
            backoffMs = classification.retryAfterMs;
          } else {
            // Exponential backoff: 2^retries * 1000ms, capped at 30s
            const maxRetryInBatch = Math.max(...batch.map((i) => i.retries), 1);
            backoffMs = Math.min(Math.pow(2, maxRetryInBatch) * 1000, 30000);
          }

          await this.sleep(backoffMs);
        } else if (this.isolateBatchFailures && batch.length > 1) {
          // Partial batch failure isolation:
          // Try writing items individually so that valid rows commit and only the
          // failing row(s) are isolated and reported.
          for (const item of batch) {
            try {
              await this.onFlush([item]);
              this.totalFlushed += 1;
              this.onFlushComplete(1);
            } catch (itemErr: any) {
              this.onError(
                itemErr instanceof Error ? itemErr : new Error(String(itemErr)),
                `buffer.flush.item(${item.id})`
              );
            }
          }
        } else {
          // Non-transient single item or isolation disabled — report and drop
          this.onError(error instanceof Error ? error : new Error(String(error)), 'buffer.flush');
        }
      }
    } finally {
      this.flushing = false;
    }
  }

  /**
   * Flush ALL remaining items in the buffer (may require multiple passes).
   * Used during graceful shutdown and explicit drain.
   */
  async flushAll(): Promise<void> {
    while (this.buffer.length > 0 || this.flushing) {
      if (this.flushing) {
        await this.sleep(10);
      } else if (this.buffer.length > 0) {
        await this.flush();
      }
    }
  }

  /**
   * Stop the flush interval timer and clean up.
   */
  destroy(): void {
    this.destroyed = true;
    this.stopFlushInterval();
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

  /**
   * Stop the flush interval timer.
   */
  private stopFlushInterval(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
