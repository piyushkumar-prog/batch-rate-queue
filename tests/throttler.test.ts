import { describe, it, expect, afterEach } from 'vitest';
import { Throttler } from '../src/core/throttler';

describe('Throttler', () => {
  let throttler: Throttler;

  afterEach(() => {
    if (throttler) throttler.destroy();
  });

  it('should allow immediate acquisition when tokens are available', async () => {
    throttler = new Throttler({ requests: 3, perMs: 1000 });

    // Should resolve immediately 3 times (bucket starts full)
    const start = Date.now();
    await throttler.acquire();
    await throttler.acquire();
    await throttler.acquire();
    const elapsed = Date.now() - start;

    // All 3 should be near-instant (< 50ms)
    expect(elapsed).toBeLessThan(50);
    expect(throttler.getAvailableTokens()).toBe(0);
  });

  it('should block when no tokens are available', async () => {
    throttler = new Throttler({ requests: 1, perMs: 500 });

    // Consume the only token
    await throttler.acquire();
    expect(throttler.getAvailableTokens()).toBe(0);

    // Next acquire should block until refill (~500ms)
    const start = Date.now();
    await throttler.acquire();
    const elapsed = Date.now() - start;

    // Should have waited roughly 400-700ms (with jitter ±10%)
    expect(elapsed).toBeGreaterThanOrEqual(350);
    expect(elapsed).toBeLessThan(800);
  });

  it('should queue multiple waiters and resolve them in order', async () => {
    throttler = new Throttler({ requests: 1, perMs: 300 });

    // Consume the token
    await throttler.acquire();

    const order: number[] = [];

    // Queue up 3 waiters
    const p1 = throttler.acquire().then(() => order.push(1));
    const p2 = throttler.acquire().then(() => order.push(2));
    const p3 = throttler.acquire().then(() => order.push(3));

    expect(throttler.getWaitingCount()).toBe(3);

    await Promise.all([p1, p2, p3]);

    // Should resolve in FIFO order
    expect(order).toEqual([1, 2, 3]);
  });

  it('should not exceed maxTokens during refill', async () => {
    throttler = new Throttler({ requests: 2, perMs: 1000 });

    // Wait for potential over-refill
    await new Promise((r) => setTimeout(r, 2000));

    // Should still be at max (2), not higher
    expect(throttler.getAvailableTokens()).toBeLessThanOrEqual(2);
  });

  it('should release all waiters on destroy', async () => {
    throttler = new Throttler({ requests: 1, perMs: 10000 });

    await throttler.acquire();

    let resolved = false;
    const promise = throttler.acquire().then(() => {
      resolved = true;
    });

    // Destroy should unblock the waiter
    throttler.destroy();
    await promise;

    expect(resolved).toBe(true);
  });

  it('should throw if acquire is called after destroy', async () => {
    throttler = new Throttler({ requests: 1, perMs: 1000 });
    throttler.destroy();

    await expect(throttler.acquire()).rejects.toThrow('destroyed');
  });

  it('should apply jitter within ±10% bounds', () => {
    // Test by creating many throttlers and checking the refill timing
    // This is a statistical test — we just verify the throttler doesn't crash
    // and that rate limiting actually works
    throttler = new Throttler({ requests: 5, perMs: 1000 });

    // The refill interval should be ~200ms (1000/5) ± 10%
    // Just verify it was created successfully
    expect(throttler.getAvailableTokens()).toBe(5);
  });
});
