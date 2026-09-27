# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
