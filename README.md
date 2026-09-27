# BatchRateQueue

> Zero-Redis resilient background worker with batch SQL write-buffering and token-bucket rate limiting.

BatchRateQueue is an embedded Node.js worker library that runs inside your server process — **no Redis, no external dependencies**. It throttles outbound API calls via a token-bucket rate limiter and buffers database writes in memory, flushing them in atomic batch transactions.

## Features

- **Token-bucket rate limiter** with jitter to prevent thundering-herd synchronization
- **In-memory write buffer** with dual flush triggers (size threshold + time interval)
- **Transient error detection** for Prisma (P1001, P2010) and raw TCP errors (ECONNREFUSED, ECONNRESET)
- **Exponential backoff** on transient DB failures with configurable max retries
- **Graceful shutdown** — flushes buffers on `SIGINT`/`SIGTERM` before exit
- **Database adapters** for Prisma, raw PostgreSQL (`pg`), and SQLite (`better-sqlite3`)
- **Zero dependencies** in production (only devDependencies for building/testing)

## Installation

```bash
npm install batch-rate-queue
```

## Quick Start

```typescript
import { createBatchRateQueue } from 'batch-rate-queue';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const queue = createBatchRateQueue({
  name: 'nominatim-enrichment',
  rateLimit: { requests: 2, perMs: 1000 },     // Max 2 API calls/sec
  batchFlush: { size: 50, intervalMs: 2000 },   // Flush 50 writes every 2s
  worker: async (item) => {
    const geo = await fetchGeo(item.address);
    return { id: item.id, updates: { lat: geo.lat, lng: geo.lng, enriched: true } };
  },
  onBatchFlush: async (batch) => {
    await prisma.$transaction(
      batch.map(u => prisma.location.update({ where: { id: u.id }, data: u.updates }))
    );
  },
});

// Enqueue 10,000 items — returns instantly
queue.addMany(locations);

// Wait for everything to finish
await queue.waitUntilDrained();
console.log(queue.getStats());
```

## Using Database Adapters

Instead of writing `onBatchFlush` manually, use a built-in adapter:

### Prisma

```typescript
import { createBatchRateQueue, createPrismaFlushHandler } from 'batch-rate-queue';

const queue = createBatchRateQueue({
  name: 'enrichment',
  rateLimit: { requests: 2, perMs: 1000 },
  batchFlush: { size: 50, intervalMs: 2000 },
  worker: async (item) => ({ id: item.id, updates: { enriched: true } }),
  onBatchFlush: createPrismaFlushHandler(prisma, 'location'),
});
```

### Raw PostgreSQL (pg)

```typescript
import { Pool } from 'pg';
import { createBatchRateQueue, createPgFlushHandler } from 'batch-rate-queue';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const queue = createBatchRateQueue({
  // ...
  onBatchFlush: createPgFlushHandler(pool, 'locations'),
});
```

### SQLite (better-sqlite3)

```typescript
import Database from 'better-sqlite3';
import { createBatchRateQueue, createSqliteFlushHandler } from 'batch-rate-queue';

const db = new Database('app.db');

const queue = createBatchRateQueue({
  // ...
  onBatchFlush: createSqliteFlushHandler(db, 'locations'),
});
```

## API Reference

### `createBatchRateQueue<T>(options): BatchRateQueue<T>`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | `string` | *required* | Queue name (used in logs) |
| `rateLimit` | `{ requests, perMs }` | *required* | Token bucket config |
| `batchFlush` | `{ size, intervalMs }` | *required* | Write buffer flush config |
| `worker` | `(item: T) => Promise<WorkerResult \| null>` | *required* | Process each item, return result or `null` to skip |
| `onBatchFlush` | `(batch: BufferItem[]) => Promise<void>` | *required* | Atomic batch write callback |
| `onError` | `(error, context) => void` | `console.error` | Error handler |
| `maxRetries` | `number` | `3` | Max retries for transient DB errors |
| `gracefulShutdown` | `boolean` | `true` | Register SIGINT/SIGTERM handlers |
| `concurrency` | `number` | `1` | Parallel worker calls within rate limit |

### `BatchRateQueue<T>` Methods

| Method | Description |
|--------|-------------|
| `add(item)` | Enqueue a single item |
| `addMany(items)` | Enqueue multiple items |
| `start()` | Start processing (auto-called by add/addMany) |
| `stop()` | Stop processing, flush buffer |
| `pause()` | Pause processing (items stay queued) |
| `resume()` | Resume after pause |
| `getStats()` | Get `{ processed, failed, buffered, pending, running }` |
| `waitUntilDrained()` | Returns a Promise that resolves when all items are processed and flushed |
| `destroy()` | Clean up all timers and handlers |

### Events

| Event | Payload | Description |
|-------|---------|-------------|
| `processed` | `WorkerResult` | A single item was processed |
| `flush` | `number` | A batch was flushed (count of items) |
| `drain` | — | All items processed and buffer flushed |
| `idle` | — | Queue is idle |
| `error` | `Error, string` | An error occurred |

## Architecture

```
[ Your App ] ──> addMany(10k items) ──> Return instantly
                                              │
                    ┌─────────────────────────┴─────────────────────────┐
                    │              BATCHRATE WORKER                     │
                    │                                                   │
                    │  1. Self-scheduling drain loop                    │
                    │  2. Token bucket throttler (configurable req/s)   │
                    │  3. In-memory buffer: BufferItem[]                │
                    │  4. Flusher: every N ms or at size threshold      │
                    │  5. Graceful shutdown handler                     │
                    └─────────────────────────┬─────────────────────────┘
                                              │ Atomic Batch Transaction
                                              ▼
                                    [ Your Database ]
```

## License

MIT
