import { describe, it, expect } from 'vitest';
import {
  httpRateLimitClassifier,
  llmApiClassifier,
  dbConnectionClassifier,
  composeClassifiers,
} from '../src/core/error-classifier';

describe('Error Classifier', () => {
  describe('httpRateLimitClassifier', () => {
    it('should classify HTTP 429 as retry-backoff', () => {
      const result = httpRateLimitClassifier({ status: 429 });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
      expect(result!.reason).toContain('429');
    });

    it('should classify HTTP 429 with Retry-After as retry-after', () => {
      const result = httpRateLimitClassifier({
        status: 429,
        headers: { 'retry-after': '30' },
      });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-after');
      expect(result!.retryAfterMs).toBe(30000);
    });

    it('should classify HTTP 503 as retry-backoff', () => {
      const result = httpRateLimitClassifier({ status: 503 });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
      expect(result!.reason).toContain('503');
    });

    it('should classify HTTP 502 as retry-backoff', () => {
      const result = httpRateLimitClassifier({ status: 502 });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify HTTP 504 as retry-backoff', () => {
      const result = httpRateLimitClassifier({ status: 504 });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should return null for unrecognized errors', () => {
      const result = httpRateLimitClassifier({ status: 400 });
      expect(result).toBeNull();
    });

    it('should handle response.status shape', () => {
      const result = httpRateLimitClassifier({ response: { status: 429 } });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should handle response.headers Retry-After', () => {
      const result = httpRateLimitClassifier({
        statusCode: 429,
        response: { headers: { 'retry-after': '10' } },
      });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-after');
      expect(result!.retryAfterMs).toBe(10000);
    });
  });

  describe('llmApiClassifier', () => {
    it('should classify rate_limit_exceeded as retry-backoff', () => {
      const result = llmApiClassifier({ code: 'rate_limit_exceeded' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify server_error as retry-backoff', () => {
      const result = llmApiClassifier({ code: 'server_error' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify overloaded as retry-backoff', () => {
      const result = llmApiClassifier({ code: 'overloaded' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify Anthropic overloaded_error as retry-backoff', () => {
      const result = llmApiClassifier({ type: 'overloaded_error' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify invalid_api_key as fail-fast', () => {
      const result = llmApiClassifier({ code: 'invalid_api_key' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('fail-fast');
    });

    it('should classify invalid_request_error as fail-fast', () => {
      const result = llmApiClassifier({ code: 'invalid_request_error' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('fail-fast');
    });

    it('should classify insufficient_quota as fail-fast', () => {
      const result = llmApiClassifier({ code: 'insufficient_quota' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('fail-fast');
    });

    it('should classify context_length_exceeded as fail-fast', () => {
      const result = llmApiClassifier({ code: 'context_length_exceeded' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('fail-fast');
    });

    it('should return null for unrecognized error shapes', () => {
      const result = llmApiClassifier({ message: 'some random error' });
      expect(result).toBeNull();
    });

    it('should handle nested error.error.code shape', () => {
      const result = llmApiClassifier({ error: { code: 'rate_limit_exceeded' } });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });
  });

  describe('dbConnectionClassifier', () => {
    it('should classify Prisma P1001 as retry-backoff', () => {
      const result = dbConnectionClassifier({ code: 'P1001' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify ECONNREFUSED as retry-backoff', () => {
      const result = dbConnectionClassifier({ code: 'ECONNREFUSED' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should classify PostgreSQL deadlock 40P01 as retry-backoff', () => {
      const result = dbConnectionClassifier({ code: '40P01' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
      expect(result!.reason).toContain('deadlock');
    });

    it('should classify connection timeout message as retry-backoff', () => {
      const result = dbConnectionClassifier({ message: 'Connection timeout expired' });
      expect(result).not.toBeNull();
      expect(result!.verdict).toBe('retry-backoff');
    });

    it('should return null for non-connection errors', () => {
      const result = dbConnectionClassifier({ message: 'Unique constraint violation' });
      expect(result).toBeNull();
    });
  });

  describe('composeClassifiers', () => {
    it('should return the first matching classifier result', () => {
      const composed = composeClassifiers(
        httpRateLimitClassifier,
        llmApiClassifier,
        dbConnectionClassifier,
      );

      // HTTP 429 should match the HTTP classifier first
      const result = composed({ status: 429 });
      expect(result).not.toBeNull();
      expect(result!.reason).toContain('429');
    });

    it('should fall through to later classifiers', () => {
      const composed = composeClassifiers(
        httpRateLimitClassifier,
        dbConnectionClassifier,
      );

      // ECONNREFUSED doesn't match HTTP, but matches DB
      const result = composed({ code: 'ECONNREFUSED' });
      expect(result).not.toBeNull();
      expect(result!.reason).toContain('ECONNREFUSED');
    });

    it('should return null when no classifier matches', () => {
      const composed = composeClassifiers(
        httpRateLimitClassifier,
        llmApiClassifier,
      );

      const result = composed({ message: 'totally unknown error' });
      expect(result).toBeNull();
    });
  });
});
