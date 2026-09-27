/**
 * Rate limit configuration.
 * Controls how many requests are allowed within a given time window.
 */
export interface RateLimitConfig {
  /** Maximum number of requests allowed in the time window */
  requests: number;
  /** Time window in milliseconds */
  perMs: number;
}

/**
 * Batch flush configuration.
 * Controls when the in-memory write buffer is flushed to the database.
 */
export interface BatchFlushConfig {
  /** Flush when the buffer reaches this many items */
  size: number;
  /** Flush every N milliseconds regardless of buffer size */
  intervalMs: number;
}

/**
 * An item in the write buffer, representing a pending database update.
 */
export interface BufferItem {
  /** The record identifier */
  id: string | number;
  /** Key-value pairs to update on the record */
  updates: Record<string, any>;
  /** Number of times this item has been retried after a transient failure */
  retries: number;
}

/**
 * Runtime statistics for the queue.
 */
export interface QueueStats {
  /** Total items successfully processed by the worker */
  processed: number;
  /** Total items that failed permanently (exhausted retries) */
  failed: number;
  /** Items currently sitting in the write buffer awaiting flush */
  buffered: number;
  /** Items still waiting to be picked up by the drain loop */
  pending: number;
  /** Whether the queue is currently processing */
  running: boolean;
}

/**
 * Result returned by the user's worker function.
 * Contains the record ID and the updates to apply.
 */
export interface WorkerResult {
  /** The record identifier to update */
  id: string | number;
  /** Key-value pairs to write to the database */
  updates: Record<string, any>;
}

/**
 * Configuration options for creating a BatchRateQueue.
 *
 * @typeParam T - The type of each job item in the queue.
 */
export interface BatchRateQueueOptions<T> {
  /** A descriptive name for this queue (used in logs) */
  name: string;

  /** Rate limit configuration for the external API calls */
  rateLimit: RateLimitConfig;

  /** Batch flush configuration for database writes */
  batchFlush: BatchFlushConfig;

  /**
   * The worker function called for each job item.
   * Should call the external API and return the result to buffer for DB write.
   * Return `null` to skip writing for this item.
   */
  worker: (item: T) => Promise<WorkerResult | null>;

  /**
   * Called when the buffer flushes a batch of results.
   * This is where the actual database writes happen (e.g., a Prisma $transaction).
   */
  onBatchFlush: (batch: BufferItem[]) => Promise<void>;

  /**
   * Called when an unrecoverable error occurs.
   * Defaults to `console.error`.
   */
  onError?: (error: Error, context: string) => void;

  /**
   * Maximum number of retries for transient DB write failures per buffer item.
   * @default 3
   */
  maxRetries?: number;

  /**
   * Whether to register SIGINT/SIGTERM handlers for graceful shutdown.
   * @default true
   */
  gracefulShutdown?: boolean;

  /**
   * Concurrency — how many worker calls can be in-flight simultaneously.
   * Rate limiting still applies; this controls parallelism within the rate limit.
   * @default 1
   */
  concurrency?: number;
}

/**
 * Events emitted by the BatchRateQueue.
 */
export interface QueueEvents {
  /** Emitted when a single item is processed by the worker */
  processed: (item: WorkerResult) => void;
  /** Emitted when a batch flush completes */
  flush: (count: number) => void;
  /** Emitted when all enqueued items have been processed and flushed */
  drain: () => void;
  /** Emitted when the queue becomes idle (nothing left to process) */
  idle: () => void;
  /** Emitted on any error */
  error: (error: Error, context: string) => void;
}
