import { describe, it, expect } from 'vitest';
import { parseRateLimitHeaders, parseRetryAfter } from '../src/core/header-parser';

describe('Unified Rate Limit Header Parser', () => {
  describe('RFC 9745 Standard Headers', () => {
    it('should parse combined RateLimit header', () => {
      const headers = {
        ratelimit: 'limit=100, remaining=45, reset=30.5',
      };

      const result = parseRateLimitHeaders(headers);
      expect(result.provider).toBe('rfc9745');
      expect(result.limit).toBe(100);
      expect(result.remaining).toBe(45);
      expect(result.resetAfterMs).toBe(30500);
    });
  });

  describe('Standard X-RateLimit (GitHub, Twitter, Stripe)', () => {
    it('should parse standard X-RateLimit headers with epoch seconds', () => {
      const futureEpoch = Math.floor(Date.now() / 1000) + 60; // 60s in future
      const headers = {
        'X-RateLimit-Limit': '5000',
        'X-RateLimit-Remaining': '4999',
        'X-RateLimit-Reset': String(futureEpoch),
      };

      const result = parseRateLimitHeaders(headers);
      expect(result.provider).toBe('standard');
      expect(result.limit).toBe(5000);
      expect(result.remaining).toBe(4999);
      expect(result.resetAfterMs).toBeGreaterThanOrEqual(58000);
      expect(result.resetAfterMs).toBeLessThanOrEqual(62000);
    });

    it('should parse lowercase ratelimit-* headers (Vercel / Cloudflare)', () => {
      const headers = {
        'ratelimit-limit': '120',
        'ratelimit-remaining': '10',
        'ratelimit-reset': '15',
      };

      const result = parseRateLimitHeaders(headers);
      expect(result.limit).toBe(120);
      expect(result.remaining).toBe(10);
      expect(result.resetAfterMs).toBe(15000);
    });
  });

  describe('OpenAI Rate Limit Headers', () => {
    it('should parse OpenAI duration strings (e.g. 20ms, 6m0s, 1s)', () => {
      const headers = {
        'x-ratelimit-limit-requests': '500',
        'x-ratelimit-remaining-requests': '495',
        'x-ratelimit-reset-requests': '20ms',
        'x-ratelimit-limit-tokens': '40000',
        'x-ratelimit-remaining-tokens': '38000',
        'x-ratelimit-reset-tokens': '6m0s',
      };

      const result = parseRateLimitHeaders(headers);
      expect(result.provider).toBe('openai');
      expect(result.requestsLimit).toBe(500);
      expect(result.requestsRemaining).toBe(495);
      expect(result.requestsResetAfterMs).toBe(20);
      expect(result.tokensLimit).toBe(40000);
      expect(result.tokensRemaining).toBe(38000);
      expect(result.tokensResetAfterMs).toBe(360000); // 6 minutes = 360,000 ms
    });
  });

  describe('Anthropic Rate Limit Headers', () => {
    it('should parse Anthropic token and request limits', () => {
      const headers = {
        'anthropic-ratelimit-requests-limit': '1000',
        'anthropic-ratelimit-requests-remaining': '950',
        'anthropic-ratelimit-requests-reset': '2',
        'anthropic-ratelimit-tokens-limit': '80000',
        'anthropic-ratelimit-tokens-remaining': '75000',
        'anthropic-ratelimit-tokens-reset': '5',
      };

      const result = parseRateLimitHeaders(headers);
      expect(result.provider).toBe('anthropic');
      expect(result.requestsLimit).toBe(1000);
      expect(result.requestsRemaining).toBe(950);
      expect(result.requestsResetAfterMs).toBe(2000);
      expect(result.tokensLimit).toBe(80000);
      expect(result.tokensRemaining).toBe(75000);
      expect(result.tokensResetAfterMs).toBe(5000);
    });
  });

  describe('Retry-After Header Formats', () => {
    it('should parse integer seconds', () => {
      expect(parseRetryAfter('120')).toBe(120000);
      expect(parseRetryAfter('  5  ')).toBe(5000);
    });

    it('should parse HTTP-Date (RFC 7231)', () => {
      const futureDate = new Date(Date.now() + 45000).toUTCString();
      const delayMs = parseRetryAfter(futureDate);

      expect(delayMs).toBeGreaterThanOrEqual(43000);
      expect(delayMs).toBeLessThanOrEqual(47000);
    });

    it('should return 0 on invalid header values', () => {
      expect(parseRetryAfter('invalid-date')).toBe(0);
      expect(parseRetryAfter('')).toBe(0);
    });
  });
});
