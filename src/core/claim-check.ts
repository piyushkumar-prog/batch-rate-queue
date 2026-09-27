/**
 * Claim-Check Pattern for Large Job Payloads
 *
 * When job payloads are large (e.g., 64KB - 10MB batches of records or heavy prompt contexts),
 * storing them directly in PostgreSQL job queue tables bloats the database, degrades
 * SKIP LOCKED index scans, and exhausts memory buffers.
 *
 * The Claim-Check pattern automatically offloads large payloads to a payload store
 * (in-memory or external blob storage), replacing the queue item with a lightweight reference token.
 * On pickup, the payload is transparently hydrated before passing it to the worker.
 */

export interface ClaimCheckRef {
  __claimCheck: true;
  claimId: string;
  byteSize: number;
}

export interface PayloadStore {
  put(claimId: string, payload: any): Promise<void> | void;
  get(claimId: string): Promise<any> | any;
  delete(claimId: string): Promise<void> | void;
  clear(): Promise<void> | void;
}

/**
 * Default in-memory payload store.
 */
export class MemoryPayloadStore implements PayloadStore {
  private payloads: Map<string, any> = new Map();

  put(claimId: string, payload: any): void {
    this.payloads.set(claimId, payload);
  }

  get(claimId: string): any {
    return this.payloads.get(claimId);
  }

  delete(claimId: string): void {
    this.payloads.delete(claimId);
  }

  clear(): void {
    this.payloads.clear();
  }

  get size(): number {
    return this.payloads.size;
  }
}

/**
 * Check if an object is a ClaimCheckRef.
 */
export function isClaimCheckRef(val: any): val is ClaimCheckRef {
  return typeof val === 'object' && val !== null && val.__claimCheck === true && typeof val.claimId === 'string';
}

/**
 * Estimate the byte size of an object or primitive.
 */
export function estimateByteSize(obj: any): number {
  if (obj === null || obj === undefined) return 0;
  if (typeof obj === 'string') return obj.length * 2;
  if (typeof obj === 'number') return 8;
  if (typeof obj === 'boolean') return 4;
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(obj)) return obj.length;

  try {
    const json = JSON.stringify(obj);
    return json ? json.length * 2 : 0;
  } catch (_) {
    return 1024; // Fallback estimate
  }
}

/**
 * Claim-Check Manager
 */
export class ClaimCheckManager {
  private readonly store: PayloadStore;
  private readonly thresholdBytes: number;
  private idCounter = 0;

  constructor(options?: { store?: PayloadStore; thresholdBytes?: number }) {
    this.store = options?.store ?? new MemoryPayloadStore();
    this.thresholdBytes = options?.thresholdBytes ?? 64 * 1024; // 64 KB default threshold
  }

  /**
   * Offload the item payload to storage if it exceeds the threshold size.
   * Returns a lightweight ClaimCheckRef or the original item if within limits.
   */
  async offloadIfNeeded<T>(item: T): Promise<T | ClaimCheckRef> {
    if (isClaimCheckRef(item)) return item;

    const size = estimateByteSize(item);
    if (size >= this.thresholdBytes) {
      const claimId = `claim_${Date.now()}_${++this.idCounter}_${Math.random().toString(36).slice(2, 8)}`;
      await this.store.put(claimId, item);
      return {
        __claimCheck: true,
        claimId,
        byteSize: size,
      };
    }

    return item;
  }

  /**
   * Hydrate an item back to its full payload if it is a ClaimCheckRef.
   */
  async hydrateIfNeeded<T>(item: T | ClaimCheckRef, autoDelete = false): Promise<T> {
    if (isClaimCheckRef(item)) {
      const payload = await this.store.get(item.claimId);
      if (payload === undefined) {
        throw new Error(`Claim-check payload not found for claimId: ${item.claimId}`);
      }
      if (autoDelete) {
        await this.store.delete(item.claimId);
      }
      return payload as T;
    }
    return item;
  }

  /**
   * Delete an offloaded payload.
   */
  async delete(claimId: string): Promise<void> {
    await this.store.delete(claimId);
  }

  /**
   * Clean up resources.
   */
  async destroy(): Promise<void> {
    await this.store.clear();
  }
}
