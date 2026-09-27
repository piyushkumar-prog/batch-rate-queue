import { BufferItem } from '../types';

/**
 * PostgreSQL (pg) Database Adapter
 *
 * Creates an `onBatchFlush` handler that executes batch updates
 * using raw parameterized SQL queries within a pg Pool transaction.
 *
 * @param pool - A `pg.Pool` instance.
 * @param tableName - The table to update.
 * @param idColumn - The name of the primary key column. Defaults to `'id'`.
 * @returns An async function suitable for `BatchRateQueueOptions.onBatchFlush`.
 *
 * @example
 * ```ts
 * import { Pool } from 'pg';
 * import { createPgFlushHandler } from 'batch-rate-queue/adapters/pg';
 *
 * const pool = new Pool({ connectionString: process.env.DATABASE_URL });
 * const onBatchFlush = createPgFlushHandler(pool, 'locations');
 * ```
 */
export function createPgFlushHandler(
  pool: any,
  tableName: string,
  idColumn: string = 'id'
): (batch: BufferItem[]) => Promise<void> {
  return async (batch: BufferItem[]) => {
    if (batch.length === 0) return;

    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      for (const item of batch) {
        const keys = Object.keys(item.updates);
        if (keys.length === 0) continue;

        // Build parameterized SET clause: "col1" = $1, "col2" = $2, ...
        const setClauses = keys.map((key, i) => `"${key}" = $${i + 1}`);
        const values = keys.map((key) => item.updates[key]);

        // The ID is the last parameter
        const idParamIndex = values.length + 1;
        values.push(item.id);

        const sql = `UPDATE "${tableName}" SET ${setClauses.join(', ')} WHERE "${idColumn}" = $${idParamIndex}`;
        await client.query(sql, values);
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  };
}
