import { WriteBuffer } from './buffer';

/**
 * Graceful Shutdown Manager
 *
 * Registers `SIGINT` and `SIGTERM` handlers to ensure that:
 *   1. The in-memory write buffer is fully flushed before the process exits.
 *   2. Any cleanup callback (e.g., resetting in-flight jobs to 'pending') is invoked.
 *
 * The handler is idempotent — multiple signals won't cause double-flush.
 */

type ShutdownCleanup = () => void;

let isShuttingDown = false;
const registeredHandlers: Array<{ signal: string; handler: () => void }> = [];

/**
 * Register graceful shutdown handlers for a queue's buffer.
 *
 * @param buffer - The WriteBuffer instance to flush on shutdown.
 * @param onBeforeExit - Optional async callback invoked after flush, before exit.
 *                       Use this to reset in-flight job statuses, close connections, etc.
 * @returns A cleanup function that removes the registered signal handlers.
 */
export function setupGracefulShutdown(
  buffer: WriteBuffer,
  onBeforeExit?: () => Promise<void>
): ShutdownCleanup {
  const handleShutdown = async () => {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log('[BatchRateQueue] Gracefully shutting down...');

    try {
      // 1. Flush all pending buffered writes
      if (buffer.length > 0) {
        console.log(`[BatchRateQueue] Flushing ${buffer.length} buffered writes...`);
        await buffer.flushAll();
        console.log('[BatchRateQueue] Buffer flushed.');
      }

      // 2. Run user cleanup (e.g., reset in-flight job status)
      if (onBeforeExit) {
        await onBeforeExit();
      }
    } catch (error) {
      console.error('[BatchRateQueue] Error during shutdown:', error);
    }

    console.log('[BatchRateQueue] Shutdown complete.');
    process.exit(0);
  };

  const sigintHandler = () => { handleShutdown(); };
  const sigtermHandler = () => { handleShutdown(); };

  process.on('SIGINT', sigintHandler);
  process.on('SIGTERM', sigtermHandler);

  registeredHandlers.push(
    { signal: 'SIGINT', handler: sigintHandler },
    { signal: 'SIGTERM', handler: sigtermHandler }
  );

  // Return cleanup function
  return () => {
    removeShutdownHandlers();
  };
}

/**
 * Remove all registered shutdown handlers.
 * Useful for tests to prevent handler leaks.
 */
export function removeShutdownHandlers(): void {
  for (const { signal, handler } of registeredHandlers) {
    process.removeListener(signal, handler);
  }
  registeredHandlers.length = 0;
  isShuttingDown = false;
}

/**
 * Check if a shutdown is currently in progress.
 */
export function getShutdownState(): boolean {
  return isShuttingDown;
}
