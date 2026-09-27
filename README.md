# BatchRateQueue

> **Zero-Redis Resilient Background Worker, Throttling & Write-Buffering Layer**

BatchRateQueue is an embedded Node.js throttling and write-buffering layer for background processing. It gives you precise token-bucket rate limiting, automatic reactive/adaptive throttling, per-tenant fair-share scheduling, distributed rate limiting with PostgreSQL, and high-throughput transactional database batching — **without requiring Redis or external worker infrastructure**.

---

## Why BatchRateQueue?

Most queues (BullMQ, Celery, pg-boss) are designed to distribute heavy tasks across worker nodes, but they introduce major operational pain when dealing with rate-limited APIs and database writes:

1. **The Redis Tax:** You are forced to deploy, monitor, and pay for Redis clusters just to throttle a few API calls.
2. **Database Hammering:** Naive workers execute an individual `UPDATE` for every processed item, causing lock contention, connection timeouts, and pool exhaustion.
3. **Static Rate Limits:** Downstream services slow down or return `429 Too Many Requests`, but traditional queues continue hammering at full speed.
4. **Tenant Starvation:** One heavy customer enqueueing 50,000 items starves smaller customers.
5. **Single-Dimension Limiting:** Modern APIs (e.g. OpenAI, Anthropic) limit by **token count and cost**, not just request count.

**BatchRateQueue solves all of this in a single, lightweight package with 0 runtime dependencies.**

---

## Key Features

- ⚡ **True Token Bucket Limiting:** Smooth token accumulation with jitter to prevent window-boundary burst spikes.
- 📉 **Batch SQL Write-Buffering:** Accumulates updates in memory and flushes them in atomic batch transactions (e.g., 50 updates per SQL query).
- 🧠 **Reactive / Adaptive Throttling:** Automatically detects 429s, Retry-After headers, and rising error rates to throttle down throughput dynamically and recover smoothly.
- 🪙 **Cost-Weighted Rate Limiting:** Enforce token-based budgets (e.g., 40,000 LLM tokens/min) where each job consumes variable units.
- 🏢 **Multi-Tenant / Per-Key Rate Limiting:** Independent rate buckets per tenant, API key, or destination within a single queue (the feature BullMQ removed and made paid-only).
- ⚖️ **Deficit Round Robin (DRR) Fair-Share Scheduling:** Prevents tenant starvation by round-robining fairly across tenants.
- 🐘 **Zero-Redis Distributed Rate Limiting:** Optional PostgreSQL-backed shared token bucket (`PgTokenBucket`) to coordinate rate limits across multiple Kubernetes pods or Node.js processes.
- 🛡️ **Distributed Circuit Breaker:** PostgreSQL-backed shared circuit breaker (`PgCircuitBreaker`) that trips across all pods when downstream APIs fail.
- 🔄 **Runtime-Adjustable Limits:** Update rate limits (`setRateLimit`) and batch sizes (`setBatchFlush`) on live workers without restarts.
- 🔍 **Pluggable Error Classifiers:** Built-in error classifiers for HTTP 429/503, OpenAI/Anthropic rate limits, and Prisma/TCP connection errors.
- 📊 **Lightweight Budget Dashboard Data:** Export real-time token utilization %, buffer metrics, per-key stats, and backlog completion ETAs (`getBudgetStats()`).
- 🔌 **Native DB Adapters:** Pre-built transaction adapters for Prisma, PostgreSQL (`pg`), and SQLite (`better-sqlite3`).
- 🛑 **Graceful Shutdown:** Commits pending in-memory buffers on `SIGINT` / `SIGTERM` before process termination.

---

## Installation

```bash
npm install batch-rate-queue
```

*Optional peer dependency for distributed PostgreSQL features:*
```bash
npm install pg
```

---

## Quick Start

```typescript
import { createBatchRateQueue, createPrismaFlushHandler } from 'batch-rate-queue';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const queue = createBatchRateQueue({
  name: 'nominatim-enrichment',
  rateLimit: { requests: 2, perMs: 1000 },       // Max 2 API calls/second
  batchFlush: { size: 50, intervalMs: 2000 },     // Flush 50 writes every 2 seconds
  worker: async (item) => {
    const geo = await fetchGeo(item.address);
    return {
      id: item.id,
      updates: { lat: geo.lat, lng: geo.lng, enriched: true }
    };
  },
  onBatchFlush: createPrismaFlushHandler(prisma, 'location'),
});

// Enqueue 10,000 items instantly
queue.addMany(locations);

// Await completion when needed
await queue.waitUntilDrained();
console.log(queue.getStats());
```

---

## Advanced Use Cases

### 1. LLM Token-Budget Limiting (Cost-Weighted)

When calling LLMs (OpenAI, Anthropic), rates are limited by **tokens per minute (TPM)** rather than simple request count:

```typescript
import { createBatchRateQueue } from 'batch-rate-queue';

const queue = createBatchRateQueue({
  name: 'openai-embeddings',
  // Budget: 40,000 tokens per minute
  rateLimit: { requests: 40000, perMs: 60000, costBased: true },
  batchFlush: { size: 50, intervalMs: 3000 },
  // Pre-acquire estimated token budget before execution
  costExtractor: (item) => Math.ceil(item.text.length / 4),
  worker: async (item) => {
    const response = await openai.embeddings.create({
      model: 'text-embedding-3-small',
      input: item.text,
    });

    return {
      id: item.id,
      updates: { embedding: response.data[0].embedding },
      meta: { actualCost: response.usage.total_tokens }, // Report actual usage
    };
  },
  onBatchFlush: async (batch) => {
    await db.saveEmbeddings(batch);
  },
});
```

---

### 2. Multi-Tenant Rate Limiting & Fair-Share Scheduling

Give each tenant their own rate limit and prevent large tenants from blocking smaller ones:

```typescript
const queue = createBatchRateQueue({
  name: 'multi-tenant-sync',
  rateLimit: { requests: 10, perMs: 1000 }, // Default: 10 req/s
  batchFlush: { size: 100, intervalMs: 2000 },
  // Extract tenant ID to isolate rate buckets
  rateLimitKey: (item) => item.tenantId,
  // Custom SLA limits per tenant
  perKeyRateLimit: {
    'tenant-enterprise': { requests: 100, perMs: 1000 },
    'tenant-free': { requests: 2, perMs: 1000 },
  },
  // Deficit Round Robin scheduling: prevents 10k items from tenant A starving tenant B
  fairShare: true,
  worker: async (item) => {
    return await syncTenantData(item);
  },
  onBatchFlush: async (batch) => {
    await db.bulkUpdate(batch);
  },
});

// Update a tenant's rate dynamically at runtime
queue.setKeyRateLimit('tenant-free', { requests: 10, perMs: 1000 });
```

---

### 3. Reactive / Adaptive Throttling

Automatically back off when downstream APIs return `429`, `503`, or `Retry-After` headers, and gradually recover when error rates normalize:

```typescript
import { createBatchRateQueue, httpRateLimitClassifier } from 'batch-rate-queue';

const queue = createBatchRateQueue({
  name: 'resilient-enrichment',
  rateLimit: { requests: 20, perMs: 1000 },
  batchFlush: { size: 50, intervalMs: 2000 },
  adaptiveThrottle: {
    enabled: true,
    signalSource: 'both',
    errorRateThreshold: 0.2, // If >20% recent requests fail, back off
    backoffFactor: 0.5,      // Halve the throughput
    recoveryFactor: 1.1,     // Smooth 10% recovery steps
    honorRetryAfter: true,   // Respect Retry-After header duration
    onRateChange: (e) => {
      console.log(`Rate adjusted: ${e.previousRate} -> ${e.newRate} (${e.reason})`);
    },
  },
  errorClassifier: httpRateLimitClassifier,
  worker: async (item) => callExternalApi(item),
  onBatchFlush: async (batch) => db.flush(batch),
});
```

---

### 4. Distributed Rate Limiting Across Pods (Zero-Redis)

Coordinate rate limits across all application replicas using a shared PostgreSQL token bucket:

```typescript
import { Pool } from 'pg';
import { createBatchRateQueue } from 'batch-rate-queue';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const queue = createBatchRateQueue({
  name: 'stripe-metered-billing',
  rateLimit: { requests: 25, perMs: 1000 }, // Global 25 req/s across all pods
  batchFlush: { size: 100, intervalMs: 2000 },
  distributed: {
    pool,
    bucketKey: 'global:stripe-api',
  },
  worker: async (item) => stripe.charges.create(item.chargeData),
  onBatchFlush: async (batch) => db.recordCharges(batch),
});
```

---

### 5. Distributed Circuit Breaker

Trip a shared circuit breaker in PostgreSQL so all replicas immediately stop hammering a failing service:

```typescript
const queue = createBatchRateQueue({
  name: 'crm-sync',
  rateLimit: { requests: 10, perMs: 1000 },
  batchFlush: { size: 50, intervalMs: 2000 },
  circuitBreaker: {
    pool,
    breakerKey: 'salesforce-api',
    failureThreshold: 5,   // Trip after 5 consecutive failures
    cooldownMs: 30000,     // 30s cooldown before trying HALF-OPEN
  },
  worker: async (item) => syncToSalesforce(item),
  onBatchFlush: async (batch) => db.save(batch),
});

queue.on('circuitBreakerTripped', (e) => {
  console.warn(`[ALERT] Circuit breaker ${e.breakerKey} tripped to OPEN!`);
});
```

---

### 6. Dashboard & Monitoring Stats

Expose rich throughput, budget utilization, and backlog ETAs for health checks or Prometheus/Grafana:

```typescript
app.get('/metrics/queue', (req, res) => {
  const stats = queue.getBudgetStats();
  res.json({
    state: stats.queue.state,
    pending: stats.queue.pending,
    utilization: `${stats.rateLimit.utilizationPct}%`,
    effectiveRate: `${stats.rateLimit.effective.requests} req / ${stats.rateLimit.effective.perMs}ms`,
    estimatedSecondsRemaining: stats.backlogEta.estimatedSecondsRemaining,
    estimatedCompletionTime: stats.backlogEta.estimatedCompletionTime,
    perKeyStats: stats.keys,
  });
});
```

---

## API Reference

### `createBatchRateQueue(options)` Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | `string` | *required* | Queue identifier for logging & metrics |
| `rateLimit` | `RateLimitConfig` | *required* | Rate limit window (`requests`, `perMs`, `costBased`) |
| `batchFlush` | `BatchFlushConfig` | *required* | Write buffer config (`size`, `intervalMs`) |
| `worker` | `(item: T) => Promise<WorkerResult \| null>` | *required* | Worker function per item |
| `onBatchFlush` | `(batch: BufferItem[]) => Promise<void>` | *required* | Transactional bulk write handler |
| `concurrency` | `number` | `1` | Worker concurrency within rate limit |
| `maxRetries` | `number` | `3` | Max transient DB retries |
| `costExtractor` | `(item: T) => number` | `() => 1` | Pre-acquire token cost extractor |
| `rateLimitKey` | `(item: T) => string` | `undefined` | Key extractor for per-tenant rate isolation |
| `perKeyRateLimit` | `Record<string, RateLimitConfig>` | `undefined` | Per-key rate limit overrides |
| `fairShare` | `boolean` | `false` | Enable Deficit Round Robin scheduling |
| `adaptiveThrottle` | `AdaptiveThrottleConfig` | `undefined` | Auto-backoff and recovery config |
| `errorClassifier` | `ErrorClassifier` | `dbConnectionClassifier` | Custom error classifier function |
| `distributed` | `DistributedConfig` | `undefined` | PostgreSQL shared token bucket config |
| `circuitBreaker` | `CircuitBreakerConfig` | `undefined` | PostgreSQL shared circuit breaker config |
| `gracefulShutdown` | `boolean` | `true` | Auto-commit buffers on `SIGINT`/`SIGTERM` |

---

### Queue Methods

- `queue.add(item)`: Enqueue a single item.
- `queue.addMany(items)`: Enqueue an array of items.
- `queue.setRateLimit(config)`: Change rate limit dynamically without restarting workers.
- `queue.setKeyRateLimit(key, config)`: Set/update a specific tenant's rate limit.
- `queue.removeKeyRateLimit(key)`: Remove a tenant's rate bucket.
- `queue.setBatchFlush(config)`: Change batch size or interval live.
- `queue.getStats()`: Returns basic queue stats (`processed`, `failed`, `buffered`, `pending`, `running`).
- `queue.getBudgetStats()`: Returns detailed budget, ETA, per-key, and circuit breaker metrics.
- `queue.pause()` / `queue.resume()`: Temporarily halt or resume processing.
- `queue.waitUntilDrained()`: Promise that resolves when all items are processed and buffers flushed.
- `queue.destroy()`: Gracefully tears down timers, listeners, and handlers.

---

## License

MIT © [Piyush Kumar](https://github.com/piyushkumar-prog)
