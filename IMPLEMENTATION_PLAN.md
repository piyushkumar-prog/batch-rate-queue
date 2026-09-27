# BatchRateQueue — Implementation Plan & Technical Specification

> **Tool Category:** Zero-Redis Resilient Background Worker with Batch SQL Write-Buffering  
> **Target Package:** `batch-rate-queue` (NPM)  
> **Source Origin in Project 1:**  
> - `d:/project1/vendor_backend/src/services/geocodingJobQueue.ts` (1,096 lines)  
> - `d:/project1/SOLUTION_SUMMARY.md`  
> - `d:/project1/BEFORE_AFTER_COMPARISON.md`

---

## 1. Executive Vision & Problem Statement

### The Problem in Production Systems
Many backend systems must enrich thousands of records via third-party APIs that enforce strict rate limits:
- Geocoding (Nominatim: 1 req/sec)
- KYC verification APIs (2-5 req/sec)
- Financial or government data APIs
- LLM API inference endpoints

If a developer runs this sequentially in an HTTP handler, **requests time out and users are blocked for hours** (as seen in Project 1, where 12,000 locations took 3+ hours).
If the developer uses standard queue tools (BullMQ, Celery):
1. **Infrastructure Tax:** They are forced to deploy, monitor, and pay for Redis clusters.
2. **Database Hammering:** Naive workers execute an individual `UPDATE` for every completed item, creating lock contention, connection timeouts, and connection pool exhaustion.

### The Solution: BatchRateQueue
BatchRateQueue is an embedded Node.js worker library that:
1. Runs inside the primary server process backed directly by SQL (Postgres, MySQL, or SQLite)—**no Redis needed**.
2. Throttles requests according to exact rate-limit specs (e.g. 1 req/sec).
3. **Buffers database writes in memory and flushes them in raw transactional batches (50 writes per transaction)**.
4. Detects transient DB drops (`P1001`, `P2010`, connection reset) and backs off gracefully.
5. Handles `SIGINT`/`SIGTERM` by committing in-memory buffers before exiting and resetting in-flight jobs to `'pending'`.

---

## 2. Core Architecture & Resiliency Flow

```
[ POST /upload ] ──> Insert 10k rows ──> Enqueue Job in DB ──> Return 200 OK (5s)
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      │             BATCHRATE WORKER                  │
                      │                                               │
                      │  1. Self-scheduling drain loop                │
                      │  2. Token bucket throttler (1-3 req/sec)      │
                      │  3. In-memory buffer: dbWriteBuffer[]         │
                      │  4. Flusher: every 2s, flushes 50 writes      │
                      │  5. Graceful shutdown handler                 │
                      └───────────────────────┬───────────────────────┘
                                              │ Raw SQL Batch Transaction
                                              ▼
                                    [ Database (Postgres) ]
```

---

## 3. Reference Implementation from Project 1

Extracted from `vendor_backend/src/services/geocodingJobQueue.ts`:

### A. Write Buffer & Transient Connection Detection
```typescript
interface BufferItem {
    id: number;
    updates: Record<string, any>;
    retries: number;
}

const dbWriteBuffer: BufferItem[] = [];

function isDbConnectionError(error: any): boolean {
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
    ) return true;

    return false;
}
```

### B. High-Throughput Batch Flusher
```typescript
async function flushWriteBuffer(): Promise<void> {
    if (dbWriteBuffer.length === 0) return;

    // Take up to 50 items from buffer
    const batch = dbWriteBuffer.splice(0, 50);

    try {
        // Atomic bulk execution in a single transaction
        await prisma.$transaction(
            batch.map(item =>
                prisma.custom_project_locations.update({
                    where: { id: item.id },
                    data: item.updates
                })
            )
        );
        processedCount += batch.length;
    } catch (error) {
        if (isDbConnectionError(error)) {
            // Re-queue items with retry count limit
            batch.forEach(item => {
                if (item.retries < 3) {
                    item.retries++;
                    dbWriteBuffer.unshift(item);
                }
            });
            // Apply exponential backoff
            await sleep(2000);
        } else {
            console.error('Batch write error:', error);
        }
    }
}
```

### C. Graceful Shutdown & Recovery Loop
```typescript
function setupGracefulShutdown() {
    const handleShutdown = async () => {
        if (isShuttingDown) return;
        isShuttingDown = true;
        console.log('[Worker] Gracefully shutting down...');
        
        cancelRequested = true;
        
        // 1. Flush any pending buffered writes
        if (dbWriteBuffer.length > 0) {
            await flushWriteBuffer();
        }
        
        // 2. Mark active job as pending so it resumes on container restart
        if (currentJobId) {
            await prisma.geocoding_jobs.update({
                where: { id: currentJobId },
                data: { status: 'pending' }
            });
        }
        process.exit(0);
    };

    process.on('SIGINT', handleShutdown);
    process.on('SIGTERM', handleShutdown);
}
```

---

## 4. Standalone Tool Directory Structure

```
D:/batch-rate-queue/
├── package.json
├── tsconfig.json
├── README.md
├── src/
│   ├── index.ts                # Main export API
│   ├── core/
│   │   ├── queue.ts            # Job scheduler & state manager
│   │   ├── throttler.ts        # Token bucket & leaky bucket rate limiter
│   │   ├── buffer.ts           # In-memory batch write buffer
│   │   └── shutdown.ts         # SIGINT/SIGTERM lifecycle manager
│   ├── adapters/
│   │   ├── prisma.ts           # Prisma transaction adapter
│   │   ├── pg.ts               # Raw PostgreSQL adapter
│   │   └── sqlite.ts           # SQLite adapter
│   └── types.ts
└── tests/
    ├── throttler.test.ts
    └── buffer.test.ts
```

---

## 5. Public API Specification

```typescript
import { createBatchRateQueue } from 'batch-rate-queue';
import { prisma } from './db';

const queue = createBatchRateQueue({
  name: 'nominatim-enrichment',
  rateLimit: { requests: 2, perMs: 1000 }, // Max 2 calls/sec
  batchFlush: { size: 50, intervalMs: 2000 }, // Bulk write 50 updates at once
  worker: async (jobItem) => {
    // Call 3rd-party API
    const geo = await fetchGeo(jobItem.address);
    return {
      id: jobItem.id,
      updates: { lat: geo.lat, lng: geo.lng, enriched: true }
    };
  },
  onBatchFlush: async (batchUpdates) => {
    // Single atomic transaction!
    await prisma.$transaction(
      batchUpdates.map(u => prisma.location.update({ where: { id: u.id }, data: u.updates }))
    );
  }
});

// Enqueue 10,000 items instantly:
await queue.addMany(locations);
```

---

## 6. Implementation Roadmap

### Phase 1: Throttler & Buffer Engine (Days 1–4)
- [ ] Build token-bucket rate limiter with jitter.
- [ ] Build in-memory write buffer with automatic interval and threshold flushing.
- [ ] Write stress tests simulating 10,000 items.

### Phase 2: SQL Persistence & Crash Resilience (Days 5–8)
- [ ] Add SQL job table schema generator.
- [ ] Implement graceful shutdown hook handling `SIGINT`/`SIGTERM`.
- [ ] Test kill-and-resume recovery.

### Phase 3: Open Source Packaging (Days 9–12)
- [ ] Add Prisma and Knex adapters.
- [ ] Publish documentation and benchmarks comparing against BullMQ.
