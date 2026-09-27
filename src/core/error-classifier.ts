import { ErrorClassification, ErrorClassifier } from '../types';

// ---------------------------------------------------------------------------
// Built-in classifiers
// ---------------------------------------------------------------------------

/**
 * Classifier: HTTP 429 / 503 with optional Retry-After header.
 *
 * Recognizes standard HTTP rate-limit and service-unavailable responses.
 * Looks for status codes in common error shapes (error.status, error.statusCode,
 * error.response.status) and parses Retry-After when available.
 */
export const httpRateLimitClassifier: ErrorClassifier = (error: any): ErrorClassification | null => {
  const status =
    error?.status ??
    error?.statusCode ??
    error?.response?.status ??
    error?.response?.statusCode;

  if (status === 429) {
    const retryAfter =
      error?.headers?.['retry-after'] ??
      error?.response?.headers?.['retry-after'];

    if (retryAfter) {
      const seconds = parseInt(retryAfter, 10);
      if (!isNaN(seconds) && seconds > 0) {
        return {
          verdict: 'retry-after',
          retryAfterMs: seconds * 1000,
          reason: `HTTP 429 Too Many Requests (Retry-After: ${seconds}s)`,
        };
      }
    }
    return { verdict: 'retry-backoff', reason: 'HTTP 429 Too Many Requests' };
  }

  if (status === 503) {
    return { verdict: 'retry-backoff', reason: 'HTTP 503 Service Unavailable' };
  }

  if (status === 502) {
    return { verdict: 'retry-backoff', reason: 'HTTP 502 Bad Gateway' };
  }

  if (status === 504) {
    return { verdict: 'retry-backoff', reason: 'HTTP 504 Gateway Timeout' };
  }

  return null;
};

/**
 * Classifier: OpenAI / Anthropic LLM API error patterns.
 *
 * Recognizes error codes and types from major LLM API providers:
 * - OpenAI: rate_limit_exceeded, server_error, overloaded, insufficient_quota
 * - Anthropic: overloaded_error, rate_limit_error, api_error
 *
 * Permanent errors (invalid_api_key, invalid_request_error) are classified
 * as fail-fast to prevent wasting retries.
 */
export const llmApiClassifier: ErrorClassifier = (error: any): ErrorClassification | null => {
  const code = error?.code ?? error?.error?.code;
  const type = error?.type ?? error?.error?.type;
  const errorMessage = error?.message ?? error?.error?.message ?? '';

  // OpenAI / Anthropic rate limit
  if (code === 'rate_limit_exceeded' || type === 'rate_limit_error') {
    // Check for Retry-After in the error
    const retryAfter = error?.headers?.['retry-after'] ?? error?.error?.headers?.['retry-after'];
    if (retryAfter) {
      const seconds = parseInt(retryAfter, 10);
      if (!isNaN(seconds) && seconds > 0) {
        return {
          verdict: 'retry-after',
          retryAfterMs: seconds * 1000,
          reason: `LLM rate limit exceeded (Retry-After: ${seconds}s)`,
        };
      }
    }
    return { verdict: 'retry-backoff', reason: `LLM rate limit exceeded: ${code ?? type}` };
  }

  // OpenAI server error / overloaded
  if (code === 'server_error' || code === 'overloaded') {
    return { verdict: 'retry-backoff', reason: `LLM server error: ${code}` };
  }

  // Anthropic overloaded
  if (type === 'overloaded_error' || type === 'api_error') {
    return { verdict: 'retry-backoff', reason: `LLM API error: ${type}` };
  }

  // OpenAI insufficient quota (billing issue — don't retry)
  if (code === 'insufficient_quota') {
    return { verdict: 'fail-fast', reason: 'LLM insufficient quota (billing issue)' };
  }

  // Permanent errors — no point retrying
  if (
    code === 'invalid_request_error' ||
    code === 'invalid_api_key' ||
    type === 'authentication_error' ||
    type === 'invalid_request_error'
  ) {
    return { verdict: 'fail-fast', reason: `LLM permanent error: ${code ?? type}` };
  }

  // Model not found
  if (code === 'model_not_found' || errorMessage.includes('does not exist')) {
    return { verdict: 'fail-fast', reason: `LLM model not found: ${errorMessage.slice(0, 80)}` };
  }

  // Context length exceeded
  if (code === 'context_length_exceeded' || errorMessage.includes('maximum context length')) {
    return { verdict: 'fail-fast', reason: 'LLM context length exceeded' };
  }

  return null;
};

/**
 * Classifier: Database connection errors.
 *
 * Recognizes transient database connection errors from common drivers:
 * - Prisma error codes: P1001 (unreachable), P2010 (raw query failure)
 * - PostgreSQL: deadlock (40P01), serialization failure (40001)
 * - TCP/connection errors: ECONNREFUSED, ECONNRESET, ETIMEDOUT
 */
export const dbConnectionClassifier: ErrorClassifier = (error: any): ErrorClassification | null => {
  const code = error?.code;

  // Prisma error codes
  if (code === 'P1001' || code === 'P2010') {
    return { verdict: 'retry-backoff', reason: `Prisma connection error: ${code}` };
  }

  // PostgreSQL deadlock / serialization failure
  if (code === '40P01') {
    return { verdict: 'retry-backoff', reason: 'PostgreSQL deadlock detected (40P01)' };
  }
  if (code === '40001') {
    return { verdict: 'retry-backoff', reason: 'PostgreSQL serialization failure (40001)' };
  }

  // TCP / connection errors
  if (code === 'ECONNREFUSED' || code === 'ECONNRESET' || code === 'ETIMEDOUT' || code === 'EPIPE') {
    return { verdict: 'retry-backoff', reason: `TCP connection error: ${code}` };
  }

  // Message-based detection for drivers that don't set error codes
  const msg = (error?.message || String(error)).toLowerCase();
  if (
    msg.includes('connection timeout') ||
    msg.includes('connection terminated') ||
    msg.includes('reach database') ||
    msg.includes('server has closed the connection') ||
    msg.includes('econnrefused') ||
    msg.includes('econnreset')
  ) {
    return { verdict: 'retry-backoff', reason: `Database connection error: ${msg.slice(0, 80)}` };
  }

  return null;
};

// ---------------------------------------------------------------------------
// Composer utility
// ---------------------------------------------------------------------------

/**
 * Compose multiple error classifiers into a single pipeline.
 * The first classifier to return a non-null result wins.
 *
 * @example
 * ```ts
 * const classifier = composeClassifiers(
 *   httpRateLimitClassifier,
 *   llmApiClassifier,
 *   dbConnectionClassifier,
 * );
 * ```
 */
export function composeClassifiers(...classifiers: ErrorClassifier[]): ErrorClassifier {
  return (error: any): ErrorClassification | null => {
    for (const classifier of classifiers) {
      const result = classifier(error);
      if (result !== null) return result;
    }
    return null;
  };
}

/**
 * The default classifier used by the write buffer when no custom classifier is provided.
 * Equivalent to the v1.0 isDbConnectionError behavior, wrapped in the classifier interface.
 */
export const defaultBufferClassifier: ErrorClassifier = dbConnectionClassifier;
