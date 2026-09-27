/**
 * Deficit Round Robin (DRR) Fair-Share Scheduler
 *
 * Instead of a single FIFO array where one heavy tenant's 10K items block
 * everyone else, this scheduler maintains per-key queues and round-robins
 * across them using the Deficit Round Robin algorithm.
 *
 * Algorithm:
 * 1. Each key has a "deficit counter" initialized to 0
 * 2. On each scheduling round, add the key's quantum (default 1) to its deficit
 * 3. Dequeue one item from the key (costs 1 deficit)
 * 4. Move to the next key in round-robin order
 *
 * This ensures that a tenant with 10,000 items and a tenant with 10 items
 * get proportional access to the processing pipeline, preventing starvation.
 *
 * Based on the DRR algorithm from Shreedhar & Varghese (1996),
 * as discussed in Microsoft/Brown's 2DFQ paper and Inngest's engineering blog.
 *
 * @typeParam T - The type of each job item.
 *
 * @example
 * ```ts
 * const scheduler = new FairScheduler<Job>((job) => job.tenantId);
 *
 * scheduler.addMany([
 *   ...Array(1000).fill({ tenantId: 'heavy', data: '...' }),
 *   ...Array(5).fill({ tenantId: 'light', data: '...' }),
 * ]);
 *
 * // Dequeue interleaves: heavy, light, heavy, light, heavy, light, ...
 * // until light's queue is empty, then heavy gets all remaining slots.
 * ```
 */
export class FairScheduler<T> {
  private keyQueues: Map<string, T[]> = new Map();
  private deficits: Map<string, number> = new Map();
  private activeKeys: string[] = []; // Round-robin order
  private currentIndex: number = 0;
  private totalItems: number = 0;

  constructor(private readonly keyExtractor: (item: T) => string) {}

  /**
   * Add a single item, automatically routed to the correct per-key queue.
   */
  add(item: T): void {
    const key = this.keyExtractor(item);
    this.addToKey(key, item);
  }

  /**
   * Add multiple items, each routed to its per-key queue.
   */
  addMany(items: T[]): void {
    for (const item of items) {
      this.add(item);
    }
  }

  /**
   * Dequeue the next item using Deficit Round Robin.
   *
   * Returns the key and item, or null if all queues are empty.
   * The key is needed by the queue to route to the correct per-key throttler.
   */
  dequeue(): { key: string; item: T } | null {
    if (this.totalItems === 0 || this.activeKeys.length === 0) {
      return null;
    }

    // Try each active key in round-robin until we find one with items
    const startIndex = this.currentIndex;
    let tried = 0;

    while (tried < this.activeKeys.length) {
      const key = this.activeKeys[this.currentIndex];
      const queue = this.keyQueues.get(key);

      if (queue && queue.length > 0) {
        // Add quantum to deficit
        const deficit = (this.deficits.get(key) ?? 0) + 1;

        if (deficit >= 1) {
          // Dequeue one item
          const item = queue.shift()!;
          this.totalItems--;
          this.deficits.set(key, deficit - 1);

          // Advance to next key for next call
          this.currentIndex = (this.currentIndex + 1) % this.activeKeys.length;

          // Clean up empty queues
          if (queue.length === 0) {
            this.removeKey(key);
            // Adjust index after removal
            if (this.activeKeys.length > 0) {
              this.currentIndex = this.currentIndex % this.activeKeys.length;
            }
          }

          return { key, item };
        }

        // Deficit not enough (shouldn't happen with quantum=1, but defensive)
        this.deficits.set(key, deficit);
      } else {
        // Empty queue — remove it
        this.removeKey(key);
        if (this.activeKeys.length === 0) return null;
        this.currentIndex = this.currentIndex % this.activeKeys.length;
        continue; // Don't increment tried since array shifted
      }

      this.currentIndex = (this.currentIndex + 1) % this.activeKeys.length;
      tried++;
    }

    return null;
  }

  /**
   * Dequeue multiple items (up to `count`).
   * Returns an array of { key, item } pairs.
   */
  dequeueBatch(count: number): Array<{ key: string; item: T }> {
    const results: Array<{ key: string; item: T }> = [];
    for (let i = 0; i < count; i++) {
      const result = this.dequeue();
      if (!result) break;
      results.push(result);
    }
    return results;
  }

  /**
   * Total pending items across all keys.
   */
  get length(): number {
    return this.totalItems;
  }

  /**
   * Number of distinct active keys with pending items.
   */
  get keyCount(): number {
    return this.activeKeys.length;
  }

  /**
   * Get all active keys.
   */
  getActiveKeys(): string[] {
    return [...this.activeKeys];
  }

  /**
   * Get the number of pending items for a specific key.
   */
  getKeyLength(key: string): number {
    return this.keyQueues.get(key)?.length ?? 0;
  }

  /**
   * Get per-key statistics.
   */
  getKeyStats(): Map<string, { pending: number; deficit: number }> {
    const stats = new Map<string, { pending: number; deficit: number }>();
    for (const key of this.activeKeys) {
      stats.set(key, {
        pending: this.keyQueues.get(key)?.length ?? 0,
        deficit: this.deficits.get(key) ?? 0,
      });
    }
    return stats;
  }

  /**
   * Check if there are any pending items.
   */
  isEmpty(): boolean {
    return this.totalItems === 0;
  }

  /**
   * Clear all queues and reset state.
   */
  clear(): void {
    this.keyQueues.clear();
    this.deficits.clear();
    this.activeKeys = [];
    this.currentIndex = 0;
    this.totalItems = 0;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Add an item to a specific key's queue.
   * Creates the key queue if it doesn't exist.
   */
  private addToKey(key: string, item: T): void {
    let queue = this.keyQueues.get(key);
    if (!queue) {
      queue = [];
      this.keyQueues.set(key, queue);
      this.activeKeys.push(key);
      this.deficits.set(key, 0);
    }
    queue.push(item);
    this.totalItems++;
  }

  /**
   * Remove a key from the active rotation.
   */
  private removeKey(key: string): void {
    const idx = this.activeKeys.indexOf(key);
    if (idx !== -1) {
      this.activeKeys.splice(idx, 1);
      // Adjust currentIndex if the removed key was before it
      if (idx < this.currentIndex) {
        this.currentIndex--;
      }
    }
    this.keyQueues.delete(key);
    this.deficits.delete(key);
  }
}
