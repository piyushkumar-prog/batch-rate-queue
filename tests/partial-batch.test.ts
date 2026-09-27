import { describe, it, expect, afterEach } from 'vitest';
import { WriteBuffer } from '../src/core/buffer';
import { BufferItem } from '../src/types';

describe('Partial Batch Failure Isolation (WriteBuffer)', () => {
  let buffer: WriteBuffer;

  afterEach(() => {
    if (buffer) buffer.destroy();
  });

  it('should salvage valid items when a single item in a batch violates a constraint', async () => {
    const committedIds: Array<string | number> = [];
    const errorLogs: Array<{ error: Error; context: string }> = [];

    buffer = new WriteBuffer({
      config: { size: 50, intervalMs: 1000 },
      maxRetries: 3,
      isolateBatchFailures: true,
      onFlush: async (batch: BufferItem[]) => {
        // If batch contains the bad item (id: 30), simulate Postgres transaction abort
        const hasBadItem = batch.some((item) => item.id === 30);

        if (batch.length > 1 && hasBadItem) {
          const err: any = new Error('duplicate key value violates unique constraint "users_email_key"');
          err.code = '23505'; // PostgreSQL unique_violation code
          throw err;
        }

        // Single item write of bad item fails
        if (batch.length === 1 && batch[0].id === 30) {
          const err: any = new Error('duplicate key value violates unique constraint');
          err.code = '23505';
          throw err;
        }

        // Normal successful batch or item write
        for (const item of batch) {
          committedIds.push(item.id);
        }
      },
      onError: (error, context) => {
        errorLogs.push({ error, context });
      },
    });

    // Enqueue 50 items (ids: 1 to 50, with #30 being the bad one)
    for (let i = 1; i <= 50; i++) {
      buffer.add({ id: i, updates: { row: i }, retries: 0 });
    }

    await buffer.flushAll();

    // All 49 valid items should have been salvaged and committed!
    expect(committedIds.length).toBe(49);
    expect(committedIds.includes(30)).toBe(false);
    expect(committedIds.includes(1)).toBe(true);
    expect(committedIds.includes(50)).toBe(true);

    // Buffer statistics
    expect(buffer.totalFlushed).toBe(49);
    expect(errorLogs.length).toBe(1);
    expect(errorLogs[0].context).toContain('buffer.flush.item(30)');
  });
});
