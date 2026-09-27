import { describe, it, expect, vi, afterEach } from 'vitest';
import { WriteBuffer, isDbConnectionError } from '../src/core/buffer';

describe('isDbConnectionError', () => {
  it('should detect Prisma P1001 error', () => {
    expect(isDbConnectionError({ code: 'P1001', message: 'Cannot reach database' })).toBe(true);
  });

  it('should detect Prisma P2010 error', () => {
    expect(isDbConnectionError({ code: 'P2010', message: 'Raw query failed' })).toBe(true);
  });

  it('should detect connection timeout in message', () => {
    expect(isDbConnectionError(new Error('Connection timeout after 30s'))).toBe(true);
  });

  it('should detect ECONNREFUSED', () => {
    expect(isDbConnectionError(new Error('connect ECONNREFUSED 127.0.0.1:5432'))).toBe(true);
  });

  it('should detect ECONNRESET', () => {
    expect(isDbConnectionError(new Error('read ECONNRESET'))).toBe(true);
  });

  it('should detect connection terminated', () => {
    expect(isDbConnectionError(new Error('Connection terminated unexpectedly'))).toBe(true);
  });

  it('should detect server closed connection', () => {
    expect(isDbConnectionError(new Error('server has closed the connection unexpectedly'))).toBe(true);
  });

  it('should return false for non-connection errors', () => {
    expect(isDbConnectionError(new Error('Unique constraint violation'))).toBe(false);
    expect(isDbConnectionError(new Error('Syntax error'))).toBe(false);
    expect(isDbConnectionError({ code: 'P2002', message: 'Unique constraint' })).toBe(false);
  });

  it('should handle string errors', () => {
    expect(isDbConnectionError('econnrefused')).toBe(true);
    expect(isDbConnectionError('some other error')).toBe(false);
  });
});

describe('WriteBuffer', () => {
  let buffer: WriteBuffer;

  afterEach(() => {
    if (buffer) buffer.destroy();
  });

  it('should flush when size threshold is reached', async () => {
    const flushed: any[][] = [];

    buffer = new WriteBuffer({
      config: { size: 3, intervalMs: 60000 }, // high interval so only size triggers
      maxRetries: 3,
      onFlush: async (batch) => {
        flushed.push(batch);
      },
      onError: () => {},
    });

    buffer.add({ id: 1, updates: { a: 1 }, retries: 0 });
    buffer.add({ id: 2, updates: { a: 2 }, retries: 0 });

    // Not yet flushed (size=3, only 2 added)
    expect(flushed.length).toBe(0);

    buffer.add({ id: 3, updates: { a: 3 }, retries: 0 });

    // Wait a tick for the async flush to complete
    await new Promise((r) => setTimeout(r, 50));

    expect(flushed.length).toBe(1);
    expect(flushed[0]).toHaveLength(3);
    expect(flushed[0].map((i: any) => i.id)).toEqual([1, 2, 3]);
  });

  it('should flush on interval timer', async () => {
    const flushed: any[][] = [];

    buffer = new WriteBuffer({
      config: { size: 100, intervalMs: 100 }, // low interval, high size
      maxRetries: 3,
      onFlush: async (batch) => {
        flushed.push(batch);
      },
      onError: () => {},
    });

    buffer.add({ id: 1, updates: { x: 'y' }, retries: 0 });
    buffer.add({ id: 2, updates: { x: 'z' }, retries: 0 });

    // Should not have flushed yet (size=100 not reached)
    expect(flushed.length).toBe(0);

    // Wait for the interval to fire
    await new Promise((r) => setTimeout(r, 200));

    expect(flushed.length).toBe(1);
    expect(flushed[0]).toHaveLength(2);
  });

  it('should retry transient errors and drop after max retries', async () => {
    let callCount = 0;
    const errors: string[] = [];

    buffer = new WriteBuffer({
      config: { size: 1, intervalMs: 60000 },
      maxRetries: 1,
      onFlush: async () => {
        callCount++;
        const err: any = new Error('connect ECONNREFUSED 127.0.0.1:5432');
        err.code = 'ECONNREFUSED';
        throw err;
      },
      onError: (err, ctx) => {
        errors.push(ctx);
      },
    });

    buffer.add({ id: 1, updates: { a: 1 }, retries: 0 });

    // Wait for initial flush + retry + backoff (2^1 * 1000 = 2s)
    await new Promise((r) => setTimeout(r, 4000));

    // Force remaining flushes
    await buffer.flushAll().catch(() => {});

    // The item should eventually be dropped after maxRetries=1
    expect(callCount).toBeGreaterThanOrEqual(2);
    // Should have reported the dropped item
    expect(errors).toContain('buffer.flush.maxRetries');
  }, 15000);

  it('should report non-transient errors immediately', async () => {
    const errors: Array<{ error: Error; context: string }> = [];

    buffer = new WriteBuffer({
      config: { size: 1, intervalMs: 60000 },
      maxRetries: 3,
      onFlush: async () => {
        throw new Error('Unique constraint violation');
      },
      onError: (error, context) => {
        errors.push({ error, context });
      },
    });

    buffer.add({ id: 1, updates: { a: 1 }, retries: 0 });

    await new Promise((r) => setTimeout(r, 100));

    expect(errors.length).toBe(1);
    expect(errors[0].context).toBe('buffer.flush');
    expect(errors[0].error.message).toContain('Unique constraint');
  });

  it('should flush all items on flushAll', async () => {
    const flushed: any[][] = [];

    buffer = new WriteBuffer({
      config: { size: 2, intervalMs: 60000 },
      maxRetries: 3,
      onFlush: async (batch) => {
        flushed.push([...batch]);
      },
      onError: () => {},
    });

    // Add 5 items (flush size=2, so needs 3 flushes)
    for (let i = 1; i <= 5; i++) {
      buffer.add({ id: i, updates: { v: i }, retries: 0 });
    }

    // Wait for the first size-triggered flush
    await new Promise((r) => setTimeout(r, 50));

    // Flush everything remaining
    await buffer.flushAll();

    // All 5 items should have been flushed across multiple batches
    const totalFlushed = flushed.reduce((sum, batch) => sum + batch.length, 0);
    expect(totalFlushed).toBe(5);
  });

  it('should call onFlushComplete with batch count', async () => {
    const counts: number[] = [];

    buffer = new WriteBuffer({
      config: { size: 2, intervalMs: 60000 },
      maxRetries: 3,
      onFlush: async () => {},
      onError: () => {},
      onFlushComplete: (count) => {
        counts.push(count);
      },
    });

    buffer.add({ id: 1, updates: {}, retries: 0 });
    buffer.add({ id: 2, updates: {}, retries: 0 });

    await new Promise((r) => setTimeout(r, 50));

    expect(counts).toContain(2);
  });
});
