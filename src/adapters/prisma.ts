import { BufferItem } from '../types';

/**
 * Prisma Database Adapter
 *
 * Creates an `onBatchFlush` handler that executes batch updates
 * inside a Prisma `$transaction()`.
 *
 * @param prismaClient - Your Prisma client instance (must have `$transaction`).
 * @param modelName - The Prisma model name to update (e.g., 'user', 'location').
 * @returns An async function suitable for `BatchRateQueueOptions.onBatchFlush`.
 *
 * @example
 * ```ts
 * import { PrismaClient } from '@prisma/client';
 * import { createPrismaFlushHandler } from 'batch-rate-queue/adapters/prisma';
 *
 * const prisma = new PrismaClient();
 * const onBatchFlush = createPrismaFlushHandler(prisma, 'location');
 * ```
 */
export function createPrismaFlushHandler(
  prismaClient: any,
  modelName: string
): (batch: BufferItem[]) => Promise<void> {
  return async (batch: BufferItem[]) => {
    if (batch.length === 0) return;

    const model = prismaClient[modelName];
    if (!model) {
      throw new Error(
        `Prisma model "${modelName}" not found. Available models can be accessed as prisma.<modelName>.`
      );
    }

    // Build an array of update operations
    const operations = batch.map((item) =>
      model.update({
        where: { id: item.id },
        data: item.updates,
      })
    );

    // Execute all updates in a single atomic transaction
    await prismaClient.$transaction(operations);
  };
}
