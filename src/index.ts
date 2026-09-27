// Core
export { BatchRateQueue } from './core/queue';
export { Throttler } from './core/throttler';
export { AdaptiveThrottler } from './core/adaptive-throttler';
export type { WorkerOutcome } from './core/adaptive-throttler';
export { KeyedThrottler } from './core/keyed-throttler';
export type { KeyBucketStats } from './core/keyed-throttler';
export { FairScheduler } from './core/fair-scheduler';
export { WriteBuffer, isDbConnectionError } from './core/buffer';
export { setupGracefulShutdown, removeShutdownHandlers } from './core/shutdown';

// Distributed
export { PgTokenBucket } from './distributed/pg-token-bucket';
export type { PgTokenBucketOptions, PgTokenBucketState } from './distributed/pg-token-bucket';
export { PgCircuitBreaker } from './distributed/pg-circuit-breaker';
export type { PgCircuitBreakerOptions, PgCircuitBreakerState, CircuitState } from './distributed/pg-circuit-breaker';

// Error Classifiers
export {
  httpRateLimitClassifier,
  llmApiClassifier,
  dbConnectionClassifier,
  composeClassifiers,
  defaultBufferClassifier,
} from './core/error-classifier';

// Adapters
export { createPrismaFlushHandler } from './adapters/prisma';
export { createPgFlushHandler } from './adapters/pg';
export { createSqliteFlushHandler } from './adapters/sqlite';

// Types
export type {
  BatchRateQueueOptions,
  RateLimitConfig,
  BatchFlushConfig,
  BufferItem,
  QueueStats,
  BudgetStats,
  DistributedConfig,
  CircuitBreakerConfig,
  WorkerResult,
  WorkerResultMeta,
  QueueEvents,
  AdaptiveThrottleConfig,
  RateChangeEvent,
  RateChangeReason,
  ErrorVerdict,
  ErrorClassification,
  ErrorClassifier,
} from './types';

// ---- Convenience factory ----

import { BatchRateQueue } from './core/queue';
import { BatchRateQueueOptions } from './types';

/**
 * Create a new BatchRateQueue instance.
 *
 * This is the primary entry point for the library.
 *
 * @example
 * ```ts
 * import { createBatchRateQueue } from 'batch-rate-queue';
 *
 * const queue = createBatchRateQueue({
 *   name: 'nominatim-enrichment',
 *   rateLimit: { requests: 2, perMs: 1000 },
 *   batchFlush: { size: 50, intervalMs: 2000 },
 *   worker: async (item) => {
 *     const geo = await fetchGeo(item.address);
 *     return { id: item.id, updates: { lat: geo.lat, lng: geo.lng } };
 *   },
 *   onBatchFlush: async (batch) => {
 *     await prisma.$transaction(
 *       batch.map(u => prisma.location.update({ where: { id: u.id }, data: u.updates }))
 *     );
 *   },
 * });
 *
 * await queue.addMany(locations);
 * await queue.waitUntilDrained();
 * ```
 */
export function createBatchRateQueue<T>(options: BatchRateQueueOptions<T>): BatchRateQueue<T> {
  return new BatchRateQueue<T>(options);
}
