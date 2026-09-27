/**
 * Rate limit configuration.
 * Controls how many requests are allowed within a given time window.
 */
export interface RateLimitConfig {
  /** Maximum number of requests (or token units when costBased=true) allowed in the time window */
  requests: number;
  /** Time window in milliseconds */
  perMs: number;

  /**
   * When true, each job consumes a variable number of tokens from the bucket
   * instead of always consuming 1. The cost is provided per-item via the worker
   * result's `meta.actualCost` or pre-estimated in the queue's `costExtractor`.
   * @default false
   */
  costBased?: boolean;
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
 * Metadata that can be returned alongside a WorkerResult.
 * Used by adaptive throttling to read live signals from API responses.
 */
export interface WorkerResultMeta {
  /** HTTP status code from the API response */
  statusCode?: number;
  /** Retry-After header value in seconds (from 429 responses) */
  retryAfterSeconds?: number;
  /** Raw response headers for advanced signal parsing */
  headers?: Record<string, string>;
  /**
   * Actual cost consumed by this job (e.g., actual token count from LLM API).
   * When costBased rate limiting is enabled, this allows the throttler to
   * self-correct between estimated and actual cost.
   */
  actualCost?: number;
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
  /**
   * Optional metadata from the API response for adaptive throttling.
   * When adaptive throttling is enabled, the throttler reads signals from
   * this metadata to automatically adjust throughput.
   */
  meta?: WorkerResultMeta;
}

// ---------------------------------------------------------------------------
// Adaptive Throttling
// ---------------------------------------------------------------------------

/**
 * Reason why the adaptive throttler changed its rate.
 */
export type RateChangeReason = 'error-rate-high' | 'retry-after' | 'recovery' | 'manual';

/**
 * Event payload emitted when the adaptive throttler adjusts its rate.
 */
export interface RateChangeEvent {
  previousRate: number;
  newRate: number;
  reason: RateChangeReason;
  /** Current error rate (0.0 - 1.0) in the sliding window */
  errorRate: number;
}

/**
 * Adaptive throttling configuration.
 * When enabled, the throttler reads live signals from worker results
 * and adjusts throughput automatically.
 */
export interface AdaptiveThrottleConfig {
  /** Enable adaptive throttling. @default false */
  enabled: boolean;

  /**
   * The signal the throttler reacts to.
   * - 'worker-errors': Read error rate from worker failures
   * - 'response-headers': Read Retry-After / X-RateLimit-* headers from WorkerResult.meta
   * - 'both': Combine both signals
   * @default 'worker-errors'
   */
  signalSource?: 'worker-errors' | 'response-headers' | 'both';

  /**
   * Sliding window size (number of recent results) to compute error rate over.
   * @default 20
   */
  windowSize?: number;

  /**
   * Error rate threshold (0.0 - 1.0) that triggers throttle-down.
   * E.g., 0.3 means "if 30% of recent requests failed, slow down."
   * @default 0.25
   */
  errorRateThreshold?: number;

  /**
   * Factor to multiply current rate by when throttling down.
   * E.g., 0.5 means "halve the rate."
   * @default 0.5
   */
  backoffFactor?: number;

  /**
   * Factor to multiply current rate by when recovering (error rate drops below threshold).
   * Applied gradually, not instantly, to avoid oscillation.
   * @default 1.1
   */
  recoveryFactor?: number;

  /**
   * Minimum rate (requests per window) the adaptive throttler will never go below.
   * Prevents complete stall. @default 1
   */
  minRate?: number;

  /**
   * Maximum rate — the original configured rate. The adaptive throttler
   * will never exceed this, even during recovery.
   * Automatically set from `rateLimit.requests` if not provided.
   */
  maxRate?: number;

  /**
   * When a 429 response includes a Retry-After header (seconds), pause
   * the throttler entirely for that duration instead of using backoffFactor.
   * @default true
   */
  honorRetryAfter?: boolean;

  /**
   * Callback invoked whenever the throttler adjusts its rate.
   * Useful for logging/monitoring.
   */
  onRateChange?: (event: RateChangeEvent) => void;
}

// ---------------------------------------------------------------------------
// Error Classification
// ---------------------------------------------------------------------------

/**
 * The verdict for how to handle an error.
 * - 'retry-backoff': Transient error, retry with exponential backoff
 * - 'retry-after': Retry after a specific delay (e.g., from Retry-After header)
 * - 'fail-fast': Permanent error, do not retry
 * - 'ignore': Not an error, ignore it
 */
export type ErrorVerdict = 'retry-backoff' | 'retry-after' | 'fail-fast' | 'ignore';

/**
 * Classification result from an ErrorClassifier.
 */
export interface ErrorClassification {
  verdict: ErrorVerdict;
  /** Delay in milliseconds before retrying. Only used when verdict is 'retry-after'. */
  retryAfterMs?: number;
  /** Human-readable reason for the classification. */
  reason: string;
}

/**
 * A function that inspects an error and returns a classification,
 * or null if it doesn't recognize the error (pass to next classifier).
 */
export type ErrorClassifier = (error: any) => ErrorClassification | null;

// ---------------------------------------------------------------------------
// Queue Options (extended)
// ---------------------------------------------------------------------------

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

  /**
   * Adaptive throttling configuration.
   * When enabled, the throttler automatically adjusts throughput based on
   * live signals (429s, error rates, Retry-After headers) from the downstream API.
   */
  adaptiveThrottle?: AdaptiveThrottleConfig;

  /**
   * Pluggable error classifier for the worker.
   * Determines whether a worker error should be retried, backed off, or failed fast.
   * If not provided, all worker errors are treated as permanent failures.
   * For the write buffer, the classifier replaces the hardcoded isDbConnectionError check.
   */
  errorClassifier?: ErrorClassifier;

  /**
   * Extract the rate-limit cost from an item before the worker runs.
   * Used when `rateLimit.costBased` is true to pre-acquire the estimated
   * number of tokens from the bucket.
   *
   * @example
   * ```ts
   * costExtractor: (item) => Math.ceil(item.text.length / 4) // rough token estimate
   * ```
   * @default () => 1
   */
  costExtractor?: (item: T) => number;
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
  /** Emitted when the adaptive throttler adjusts the rate */
  rateLimitChanged: (event: RateChangeEvent) => void;
}
