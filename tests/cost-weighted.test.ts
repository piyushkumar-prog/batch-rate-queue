import { describe, it, expect, afterEach } from 'vitest';
import { Throttler } from '../src/core/throttler';

describe('Cost-Weighted Rate Limiting', () => {
  let throttler: Throttler;

  afterEach(() => {
    if (throttler) throttler.destroy();
  });

  it('should deduct the correct number of tokens for cost > 1', async () => {
    throttler = new Throttler({ requests: 10, perMs: 1000 });

    await throttler.acquire(3); // Consume 3 tokens
    expect(throttler.getAvailableTokens()).toBe(7);

    await throttler.acquire(5); // Consume 5 tokens
    expect(throttler.getAvailableTokens()).toBe(2);
  });

  it('should block when cost exceeds available tokens', async () => {
    throttler = new Throttler({ requests: 5, perMs: 500 });

    await throttler.acquire(4); // Consume 4, leaving 1
    expect(throttler.getAvailableTokens()).toBe(1);

    // Acquiring 3 should block until enough tokens refill
    const start = Date.now();
    await throttler.acquire(3);
    const elapsed = Date.now() - start;

    // Should have waited for at least 2 token refills (~200ms at 500ms/5 = 100ms each)
    expect(elapsed).toBeGreaterThanOrEqual(100);
  });

  it('should throw if cost exceeds bucket capacity', async () => {
    throttler = new Throttler({ requests: 5, perMs: 1000 });

    await expect(throttler.acquire(6)).rejects.toThrow('exceeds bucket capacity');
  });

  it('should treat cost=0 as free (no token consumption)', async () => {
    throttler = new Throttler({ requests: 5, perMs: 1000 });

    await throttler.acquire(0);
    expect(throttler.getAvailableTokens()).toBe(5); // Unchanged
  });

  it('should treat negative cost as free', async () => {
    throttler = new Throttler({ requests: 5, perMs: 1000 });

    await throttler.acquire(-1);
    expect(throttler.getAvailableTokens()).toBe(5); // Unchanged
  });

  it('should default to cost=1 for backward compatibility', async () => {
    throttler = new Throttler({ requests: 5, perMs: 1000 });

    await throttler.acquire(); // No args = cost 1
    expect(throttler.getAvailableTokens()).toBe(4);

    await throttler.acquire(); // No args = cost 1
    expect(throttler.getAvailableTokens()).toBe(3);
  });

  it('should handle cost=1 same as original behavior', async () => {
    throttler = new Throttler({ requests: 3, perMs: 1000 });

    // Same as original test — 3 immediate acquires
    const start = Date.now();
    await throttler.acquire(1);
    await throttler.acquire(1);
    await throttler.acquire(1);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(50);
    expect(throttler.getAvailableTokens()).toBe(0);
  });
});
