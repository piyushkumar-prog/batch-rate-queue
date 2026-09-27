import { BufferItem } from '../types';

/**
 * SQLite Database Adapter (better-sqlite3)
 *
 * Creates an `onBatchFlush` handler that executes batch updates
 * using better-sqlite3's synchronous transaction API.
 *
 * @param db - A `better-sqlite3` database instance.
 * @param tableName - The table to update.
 * @param idColumn - The name of the primary key column. Defaults to `'id'`.
 * @returns An async function suitable for `BatchRateQueueOptions.onBatchFlush`.
 *
 * @example
 * ```ts
 * import Database from 'better-sqlite3';
 * import { createSqliteFlushHandler } from 'batch-rate-queue/adapters/sqlite';
 *
 * const db = new Database('app.db');
 * const onBatchFlush = createSqliteFlushHandler(db, 'locations');
 * ```
 */
export function createSqliteFlushHandler(
  db: any,
  tableName: string,
  idColumn: string = 'id'
): (batch: BufferItem[]) => Promise<void> {
  return async (batch: BufferItem[]) => {
    if (batch.length === 0) return;

    // better-sqlite3 transactions are synchronous
    const runTransaction = db.transaction((items: BufferItem[]) => {
      for (const item of items) {
        const keys = Object.keys(item.updates);
        if (keys.length === 0) continue;

        // Build SET clause: "col1" = ?, "col2" = ?, ...
        const setClauses = keys.map((key) => `"${key}" = ?`);
        const values = keys.map((key) => item.updates[key]);
        values.push(item.id);

        const sql = `UPDATE "${tableName}" SET ${setClauses.join(', ')} WHERE "${idColumn}" = ?`;
        db.prepare(sql).run(...values);
      }
    });

    runTransaction(batch);
  };
}
