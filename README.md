# BatchRateQueue

[![npm version](https://img.shields.io/badge/npm-v1.4.0-blue.svg)](https://www.npmjs.com/package/batch-rate-queue)
[![Tests](https://img.shields.io/badge/tests-161%20passing-brightgreen.svg)](https://github.com/piyushkumar-prog/batch-rate-queue)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0%20runtime-purple.svg)](https://www.npmjs.com/package/batch-rate-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Zero-Redis Resilient Background Worker, Throttling & Write-Buffering Layer for Node.js & TypeScript**
>
> 📖 *Looking for the full architectural whitepaper? Download the 6-page [BatchRateQueue Guide (PDF)](./batch-rate-queue-guide.pdf).*

BatchRateQueue is an embedded Node.js throttling and write-buffering layer for background processing. It gives you precise token-bucket rate limiting, automatic reactive/adaptive throttling, per-tenant fair-share scheduling, distributed rate limiting with PostgreSQL, side-effect idempotency deduplication, partial batch failure isolation, and high-throughput transactional database batching — **without requiring Redis or external worker infrastructure**.

---

## Why BatchRateQueue?

Most queues (BullMQ, Celery, pg-boss, graphile-worker) are designed to distribute heavy tasks across worker nodes, but they introduce major operational pain when dealing with rate-limited APIs and database writes:

1. **The Redis Tax:** You are forced to deploy, monitor, and pay for Redis clusters just to throttle a few API calls.
2. **Database Hammering:** Naive workers execute an individual `UPDATE` for every processed item, causing lock contention, connection timeouts, and pool exhaustion.
3. **Double-Firing External Side-Effects:** If a worker crashes after calling an external API (Stripe, Twilio, SendGrid) but before marking the job done, redelivery duplicates the charge or SMS.
4. **All-or-Nothing Batch Failures:** If 1 row out of 50 in a SQL batch violates a constraint, Postgres rolls back the *entire* transaction, losing 49 good writes.
5. **Static Rate Limits:** Downstream services slow down or return `429 Too Many Requests`, but traditional queues continue hammering at full speed.
6. **Tenant Starvation:** One heavy customer enqueueing 50,000 items starves smaller customers.
7. **Single-Dimension Limiting:** Modern APIs (e.g. OpenAI, Anthropic) limit by **token count and cost**, not just request count.
8. **PgBouncer Incompatibility:** Session-scoped locks (`pg_advisory_lock`) break silently under PgBouncer transaction-pooling mode (Supabase, Neon, AWS RDS Proxy).

**BatchRateQueue solves all of this in a single, lightweight package with 0 runtime dependencies.**

---

## Key Features

- ⚡ **True Token Bucket Limiting:** Smooth token accumulation with jitter to prevent window-boundary burst spikes.
- 📉 **Batch SQL Write-Buffering:** Accumulates updates in memory and flushes them in atomic batch transactions (e.g., 50 updates per SQL query).
- 🛡️ **Partial Batch Failure Isolation:** If row 30 of 50 violates a DB constraint, BatchRateQueue automatically salvages and commits the 49 valid rows while isolating the bad row.
- 🔒 **Idempotency & Deduplication:** Built-in idempotency key manager prevents duplicate external API calls on crash recovery.
- 🧠 **Reactive / Adaptive Throttling:** Automatically detects 429s, Retry-After headers, and rising error rates to throttle down throughput dynamically and recover smoothly.
- 🌐 **Multi-Provider Header Parser:** Parses rate-limit headers across RFC 9745, OpenAI, Anthropic, GitHub, Vercel, and HTTP-Date `Retry-After`.
- 🪙 **Cost-Weighted Rate Limiting:** Enforce token-based budgets (e.g., 40,000 LLM tokens/min) where each job consumes variable units.
- 🏢 **Multi-Tenant / Per-Key Rate Limiting:** Independent rate buckets per tenant, API key, or destination within a single queue (the feature BullMQ removed and made paid-only).
- ⚖️ **Deficit Round Robin (DRR) Fair-Share Scheduling:** Prevents tenant starvation by round-robining fairly across tenants.
- 📦 **Claim-Check Pattern for Large Payloads:** Offloads payloads >64KB to prevent DB table bloat and preserve high-throughput SKIP LOCKED scans.
- 🐘 **Zero-Redis Distributed Rate Limiting:** Optional PostgreSQL-backed shared token bucket (`PgTokenBucket`) to coordinate rate limits across multiple Kubernetes pods.
- 🚦 **PostgreSQL Distributed Circuit Breaker:** PostgreSQL-backed shared circuit breaker (`PgCircuitBreaker`) that trips across all pods when downstream APIs fail.
- 🔑 **PgBouncer-Safe Lease Locking:** Distributed lease-based lock (`PgLeaseLock`) designed specifically for PgBouncer transaction pooling mode.
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

## Advanced Production Recipes

### 1. Idempotency for External Side-Effects (Stripe / Twilio)

Prevent duplicate charges or emails when workers crash before recording completion:

```typescript
const queue = createBatchRateQueue({
  name: 'billing-charges',
  rateLimit: { requests: 20, perMs: 1000 },
  batchFlush: { size: 50, intervalMs: 2000 },
  // Deduplicate by charge idempotency key
  idempotencyKey: (item) => `charge_${item.orderId}`,
  idempotencyTtlMs: 24 * 60 * 60 * 1000, // 24-hour cache
  worker: async (item) => {
    const charge = await stripe.charges.create({
      amount: item.amount,
      currency: 'usd',
    });
    return { id: item.id, updates: { chargeId: charge.id, status: 'paid' } };
  },
  onBatchFlush: async (batch) => db.saveCharges(batch),
});
```

---

### 2. LLM Token-Budget Limiting (OpenAI / Anthropic TPM)

Enforce tokens-per-minute (TPM) budgets where each task consumes variable prompt/completion tokens:

```typescript
import { createBatchRateQueue, llmApiClassifier } from 'batch-rate-queue';

const queue = createBatchRateQueue({
  name: 'openai-embeddings',
  // Budget: 40,000 tokens per minute
  rateLimit: { requests: 40000, perMs: 60000, costBased: true },
  batchFlush: { size: 50, intervalMs: 3000 },
  // Estimate tokens before acquiring
  costExtractor: (item) => Math.ceil(item.text.length / 4),
  errorClassifier: llmApiClassifier,
  worker: async (item) => {
    const response = await openai.embeddings.create({
      model: 'text-embedding-3-small',
      input: item.text,
    });
    return {
      id: item.id,
      updates: { embedding: response.data[0].embedding },
      meta: { actualCost: response.usage.total_tokens }, // Exact actual tokens used
    };
  },
  onBatchFlush: async (batch) => db.saveEmbeddings(batch),
});
```

---

### 3. Multi-Tenant Rate Limiting & Fair-Share Scheduling

Isolate rate limits per customer and eliminate tenant starvation via Deficit Round Robin (DRR):

```typescript
const queue = createBatchRateQueue({
  name: 'multi-tenant-sync',
  rateLimit: { requests: 10, perMs: 1000 }, // Default: 10 req/s
  batchFlush: { size: 100, intervalMs: 2000 },
  rateLimitKey: (item) => item.tenantId,
  perKeyRateLimit: {
    'enterprise-corp': { requests: 100, perMs: 1000 },
    'free-tier-user':  { requests: 2, perMs: 1000 },
  },
  // DRR Scheduling: 10k items from tenant A will never block tenant B
  fairShare: true,
  worker: async (item) => syncTenantData(item),
  onBatchFlush: async (batch) => db.bulkUpdate(batch),
});
```

---

### 4. Zero-Redis Distributed Rate Limiting Across Pods

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

### 5. PgBouncer-Safe Distributed Lease Lock

Execute distributed scheduled jobs safely under PgBouncer transaction-pooling mode (Supabase, Neon, AWS RDS Proxy):

```typescript
import { PgLeaseLock } from 'batch-rate-queue';

const lock = new PgLeaseLock({
  pool,
  lockKey: 'cron:daily-reconciliation',
  ttlMs: 30000, // 30-second lease
});

// Automatically acquires, heartbeats, executes, and releases
await lock.runWithLock(async () => {
  console.log('Running daily reconciliation with guaranteed single-replica execution!');
  await reconcileAccounts();
});
```

---

### 6. Real-Time Dashboard & Monitoring Data

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
| `idempotencyKey` | `(item: T) => string` | `undefined` | Deduplication key extractor preventing duplicate external calls |
| `idempotencyTtlMs` | `number` | `86400000` | Idempotency record TTL (24h default) |
| `isolateBatchFailures` | `boolean` | `true` | Salvage 49 valid rows when 1 row fails in a 50-item SQL batch |
| `claimCheckThresholdBytes` | `number` | `65536` | Offload payloads >64KB to preserve fast DB index scans |
| `rateLimitKey` | `(item: T) => string` | `undefined` | Key extractor for per-tenant rate isolation |
| `perKeyRateLimit` | `Record<string, RateLimitConfig>` | `undefined` | Custom SLA rate limits per tenant/key |
| `fairShare` | `boolean` | `false` | Enable Deficit Round Robin scheduling |
| `costExtractor` | `(item: T) => number` | `() => 1` | Pre-acquire token cost extractor (LLM tokens/TPM) |
| `adaptiveThrottle` | `AdaptiveThrottleConfig` | `undefined` | Auto-backoff on 429 / Retry-After and smooth recovery |
| `errorClassifier` | `ErrorClassifier` | `dbConnectionClassifier` | Custom error classifier function |
| `distributed` | `DistributedConfig` | `undefined` | PostgreSQL shared token bucket config |
| `circuitBreaker` | `CircuitBreakerConfig` | `undefined` | PostgreSQL shared circuit breaker config |
| `gracefulShutdown` | `boolean` | `true` | Auto-commit buffers on `SIGINT`/`SIGTERM` |

---

## License

MIT © [Piyush Kumar](https://github.com/piyushkumar-prog)
