# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-09-27

### Added

- **Per-Key / Per-Tenant Rate Limiting** (`KeyedThrottler`):
  - Each unique key (tenant ID, API key, destination) gets its own independent token bucket
  - Lazy bucket creation — no overhead for keys that haven't been seen yet
  - `rateLimitKey` option on queue: `(item) => item.tenantId`
  - `perKeyRateLimit` option: different rate limits per key (e.g., premium vs. free tier)
  - `queue.setKeyRateLimit(key, config)` — live per-key rate changes without restart
  - `queue.removeKeyRateLimit(key)` — remove a key's bucket (e.g., tenant offboarded)
  - Per-key stats via `KeyedThrottler.getAllKeyStats()`
  - Cost-weighted acquire per key: `keyed.acquire('tenant-a', tokenCount)`
  - This is the feature BullMQ removed in v3 and made paid-only
- **Weighted Fair-Share Scheduling** (`FairScheduler`):
  - Deficit Round Robin (DRR) algorithm prevents tenant starvation
  - `fairShare: true` option on queue (requires `rateLimitKey`)
  - A tenant with 10K items no longer blocks a tenant with 10 items
  - Per-key queue stats: pending count, deficit counter
  - `dequeueBatch(count)` for concurrent dequeue
  - Automatic cleanup of empty per-key queues
- 29 new tests (114 total, all passing)

### Changed

- Queue drain loop now routes through `dequeueItems()` and `acquireToken()` for clean keyed/non-keyed dispatch
- `pendingItems` array is bypassed when `FairScheduler` is active; `getPendingCount()` abstracts the source

## [1.1.0] - 2026-09-27

### Added

- **Reactive / Adaptive Throttling** (`AdaptiveThrottler`):
  - Reads live signals (429s, Retry-After headers, rising error rates) from worker outcomes
  - Automatically reduces throughput when downstream APIs are struggling
  - Gradually recovers when error rate drops (with hysteresis to prevent oscillation)
  - Honors `Retry-After` headers by pausing the throttler for the specified duration
  - Configurable `backoffFactor`, `recoveryFactor`, `errorRateThreshold`, `minRate`
  - `onRateChange` callback + `rateLimitChanged` event for monitoring
- **Cost-Weighted Rate Limiting**:
  - `acquire(cost)` consumes variable tokens per job (e.g., LLM token counts)
  - `rateLimit.costBased: true` enables token-budget mode
  - `costExtractor` option to estimate cost before worker runs
  - Backward compatible: `acquire()` with no args still costs 1
- **Runtime-Adjustable Limits**:
  - `queue.setRateLimit({ requests, perMs })` — live rate changes without restart
  - `queue.setBatchFlush({ size, intervalMs })` — live buffer config changes
  - `throttler.setRate(requests, perMs)` — reconfigures the token bucket in-place
- **Pluggable Error Classifiers**:
  - `ErrorClassifier` type: inspect errors → `retry-backoff` / `retry-after` / `fail-fast` / `ignore`
  - `httpRateLimitClassifier` — HTTP 429/502/503/504 with Retry-After parsing
  - `llmApiClassifier` — OpenAI / Anthropic error patterns (rate limits, overloaded, invalid keys)
  - `dbConnectionClassifier` — Prisma, PostgreSQL deadlocks, TCP connection errors
  - `composeClassifiers()` — combine multiple classifiers into a pipeline
  - `errorClassifier` option on queue replaces hardcoded `isDbConnectionError` in write buffer
- **WorkerResult.meta** — optional metadata (statusCode, retryAfterSeconds, actualCost, headers) for adaptive throttling signals
- **WriteBuffer stats** — `totalFlushed` and `failedFlushes` counters for monitoring
- 54 new tests (85 total, all passing)

### Changed

- `Throttler` class members changed from `private` to `protected` for `AdaptiveThrottler` extension
- `WriteBuffer` now accepts an optional `errorClassifier` (defaults to `dbConnectionClassifier` for backward compat)
- `isDbConnectionError()` is now a thin wrapper over `dbConnectionClassifier` (deprecated but still exported)

## [1.0.0] - 2026-09-27

### Added

- **Core engine**: `BatchRateQueue` orchestrator with self-scheduling drain loop
- **Token-bucket rate limiter** (`Throttler`) with ±10% jitter to prevent thundering-herd synchronization
- **In-memory write buffer** (`WriteBuffer`) with dual flush triggers:
  - Size threshold (e.g., flush every 50 items)
  - Time interval (e.g., flush every 2 seconds)
- **Transient error detection** (`isDbConnectionError`) for Prisma codes (P1001, P2010) and TCP errors (ECONNREFUSED, ECONNRESET, connection timeout, etc.)
- **Exponential backoff** on transient DB failures with configurable max retries (default: 3)
- **Graceful shutdown** — SIGINT/SIGTERM handlers flush the buffer before process exit
- **Database adapters**:
  - `createPrismaFlushHandler` — Prisma `$transaction()` batch updates
  - `createPgFlushHandler` — Raw PostgreSQL (`pg`) parameterized batch updates
  - `createSqliteFlushHandler` — `better-sqlite3` transactional batch updates
- **Lifecycle control**: `start()`, `stop()`, `pause()`, `resume()`, `destroy()`
- **Async drain**: `waitUntilDrained()` returns a Promise that resolves when all items are processed and flushed
- **Event emitter**: `processed`, `flush`, `drain`, `idle`, `error` events
- **Concurrency control**: configurable parallel worker calls within the rate limit
- **Runtime stats**: `getStats()` returns processed, failed, buffered, pending counts
- Full TypeScript types with declarations and source maps
- Comprehensive test suite (31 tests)
