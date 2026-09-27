/**
 * Unified Rate Limit Header Parser
 *
 * Parses rate-limit and backoff headers across diverse API provider conventions:
 * - RFC 9745 Standard: `RateLimit: limit=100, remaining=50, reset=60` and `RateLimit-Policy`
 * - Standard X-RateLimit: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
 * - Lowercase / Cloudflare / Vercel: `ratelimit-limit`, `ratelimit-remaining`, `ratelimit-reset`
 * - OpenAI: `x-ratelimit-limit-requests`, `x-ratelimit-remaining-tokens`, `x-ratelimit-reset-requests` (e.g. `20ms`, `6m0s`)
 * - Anthropic: `anthropic-ratelimit-requests-limit`, `anthropic-ratelimit-tokens-remaining`, `anthropic-ratelimit-tokens-reset`
 * - Retry-After: Integer seconds or HTTP-Date (RFC 7231, e.g. "Wed, 21 Oct 2026 07:28:00 GMT")
 */

export interface ParsedRateLimitInfo {
  /** Maximum requests allowed in the window */
  limit?: number;
  /** Remaining requests in current window */
  remaining?: number;
  /** Milliseconds until the rate limit resets */
  resetAfterMs?: number;
  /** Milliseconds to wait as requested by Retry-After header */
  retryAfterMs?: number;

  /** Token-specific limits (OpenAI, Anthropic) */
  tokensLimit?: number;
  tokensRemaining?: number;
  tokensResetAfterMs?: number;

  /** Request-specific limits (OpenAI, Anthropic) */
  requestsLimit?: number;
  requestsRemaining?: number;
  requestsResetAfterMs?: number;

  /** Detected header format provider */
  provider?: 'rfc9745' | 'openai' | 'anthropic' | 'standard' | 'generic';
}

/**
 * Parse headers from Headers object, raw Record, or Axios/Fetch response.
 */
export function parseRateLimitHeaders(
  headers: Record<string, any> | Headers | undefined | null
): ParsedRateLimitInfo {
  if (!headers) return {};

  const map = normalizeHeaders(headers);
  const result: ParsedRateLimitInfo = {};

  // 1. Parse Retry-After (seconds or HTTP-Date)
  const retryAfterRaw = map.get('retry-after');
  if (retryAfterRaw) {
    result.retryAfterMs = parseRetryAfter(retryAfterRaw);
  }

  // 2. Parse IETF RFC 9745 (Combined `RateLimit` header, e.g. "limit=100, remaining=50, reset=60")
  const rfcRateLimit = map.get('ratelimit');
  if (rfcRateLimit && rfcRateLimit.includes('=')) {
    result.provider = 'rfc9745';
    const parts = rfcRateLimit.split(',').map((s) => s.trim());
    for (const part of parts) {
      const [key, val] = part.split('=').map((s) => s.trim().toLowerCase());
      if (key === 'limit' && val) result.limit = parseInt(val, 10);
      if (key === 'remaining' && val) result.remaining = parseInt(val, 10);
      if (key === 'reset' && val) {
        const sec = parseFloat(val);
        if (!isNaN(sec)) result.resetAfterMs = Math.round(sec * 1000);
      }
    }
  }

  // 3. Parse OpenAI Rate Limit Headers
  if (
    map.has('x-ratelimit-limit-requests') ||
    map.has('x-ratelimit-limit-tokens') ||
    map.has('x-ratelimit-remaining-tokens')
  ) {
    result.provider = 'openai';
    if (map.has('x-ratelimit-limit-requests')) {
      result.requestsLimit = parseInt(map.get('x-ratelimit-limit-requests')!, 10);
      result.limit = result.requestsLimit;
    }
    if (map.has('x-ratelimit-remaining-requests')) {
      result.requestsRemaining = parseInt(map.get('x-ratelimit-remaining-requests')!, 10);
      result.remaining = result.requestsRemaining;
    }
    if (map.has('x-ratelimit-reset-requests')) {
      result.requestsResetAfterMs = parseTimeStringToMs(map.get('x-ratelimit-reset-requests')!);
      result.resetAfterMs = result.requestsResetAfterMs;
    }

    if (map.has('x-ratelimit-limit-tokens')) {
      result.tokensLimit = parseInt(map.get('x-ratelimit-limit-tokens')!, 10);
    }
    if (map.has('x-ratelimit-remaining-tokens')) {
      result.tokensRemaining = parseInt(map.get('x-ratelimit-remaining-tokens')!, 10);
    }
    if (map.has('x-ratelimit-reset-tokens')) {
      result.tokensResetAfterMs = parseTimeStringToMs(map.get('x-ratelimit-reset-tokens')!);
    }
  }

  // 4. Parse Anthropic Rate Limit Headers
  if (
    map.has('anthropic-ratelimit-requests-limit') ||
    map.has('anthropic-ratelimit-tokens-limit') ||
    map.has('anthropic-ratelimit-tokens-remaining')
  ) {
    result.provider = 'anthropic';
    if (map.has('anthropic-ratelimit-requests-limit')) {
      result.requestsLimit = parseInt(map.get('anthropic-ratelimit-requests-limit')!, 10);
      result.limit = result.requestsLimit;
    }
    if (map.has('anthropic-ratelimit-requests-remaining')) {
      result.requestsRemaining = parseInt(map.get('anthropic-ratelimit-requests-remaining')!, 10);
      result.remaining = result.requestsRemaining;
    }
    if (map.has('anthropic-ratelimit-requests-reset')) {
      result.requestsResetAfterMs = parseResetToMs(map.get('anthropic-ratelimit-requests-reset')!);
      result.resetAfterMs = result.requestsResetAfterMs;
    }

    if (map.has('anthropic-ratelimit-tokens-limit')) {
      result.tokensLimit = parseInt(map.get('anthropic-ratelimit-tokens-limit')!, 10);
    }
    if (map.has('anthropic-ratelimit-tokens-remaining')) {
      result.tokensRemaining = parseInt(map.get('anthropic-ratelimit-tokens-remaining')!, 10);
    }
    if (map.has('anthropic-ratelimit-tokens-reset')) {
      result.tokensResetAfterMs = parseResetToMs(map.get('anthropic-ratelimit-tokens-reset')!);
    }
  }

  // 5. Standard X-RateLimit / ratelimit headers (GitHub, Twitter, Vercel, Stripe)
  if (!result.limit) {
    const limitKey = findFirstHeader(map, [
      'x-ratelimit-limit',
      'ratelimit-limit',
      'x-rate-limit-limit',
      'x-rate-limit',
    ]);
    if (limitKey) {
      const parsed = parseInt(map.get(limitKey)!, 10);
      if (!isNaN(parsed)) result.limit = parsed;
    }
  }

  if (result.remaining === undefined) {
    const remainingKey = findFirstHeader(map, [
      'x-ratelimit-remaining',
      'ratelimit-remaining',
      'x-rate-limit-remaining',
    ]);
    if (remainingKey) {
      const parsed = parseInt(map.get(remainingKey)!, 10);
      if (!isNaN(parsed)) result.remaining = parsed;
    }
  }

  if (result.resetAfterMs === undefined) {
    const resetKey = findFirstHeader(map, [
      'x-ratelimit-reset',
      'ratelimit-reset',
      'x-rate-limit-reset',
      'ratelimit-reset-after',
    ]);
    if (resetKey) {
      result.resetAfterMs = parseResetToMs(map.get(resetKey)!);
    }
  }

  if (!result.provider && (result.limit !== undefined || result.remaining !== undefined)) {
    result.provider = 'standard';
  }

  return result;
}

/**
 * Parse Retry-After value: supports seconds (e.g. "120") or HTTP-Date (RFC 7231).
 */
export function parseRetryAfter(value: string): number {
  const trimmed = value.trim();
  const seconds = parseInt(trimmed, 10);

  if (!isNaN(seconds) && /^\d+$/.test(trimmed)) {
    return Math.max(0, seconds * 1000);
  }

  // Try parsing as HTTP-Date: "Wed, 21 Oct 2026 07:28:00 GMT"
  const parsedDate = Date.parse(trimmed);
  if (!isNaN(parsedDate)) {
    const deltaMs = parsedDate - Date.now();
    return Math.max(0, deltaMs);
  }

  return 0;
}

/**
 * Parse a reset header value (can be epoch timestamp, delta seconds, or duration string).
 */
function parseResetToMs(val: string): number {
  const trimmed = val.trim();

  // Check if duration string (e.g. "6m0s", "500ms", "2s")
  if (/[a-zA-Z]/.test(trimmed) && !trimmed.includes('GMT') && !trimmed.includes('UTC')) {
    return parseTimeStringToMs(trimmed);
  }

  // Check if HTTP-Date
  const parsedDate = Date.parse(trimmed);
  if (isNaN(Number(trimmed)) && !isNaN(parsedDate)) {
    return Math.max(0, parsedDate - Date.now());
  }

  const num = parseFloat(trimmed);
  if (isNaN(num)) return 0;

  const now = Date.now();

  // If timestamp in seconds (> 1,000,000,000 and < 10,000,000,000)
  if (num > 1000000000 && num < 10000000000) {
    const epochMs = num * 1000;
    return Math.max(0, Math.round(epochMs - now));
  }

  // If timestamp in milliseconds (> 1,000,000,000,000)
  if (num > 1000000000000) {
    return Math.max(0, Math.round(num - now));
  }

  // Otherwise treat as delta seconds
  return Math.max(0, Math.round(num * 1000));
}

/**
 * Parse human duration strings used by OpenAI (e.g. "6m0s", "100ms", "1s", "2h30m").
 */
function parseTimeStringToMs(val: string): number {
  const trimmed = val.trim().toLowerCase();
  let totalMs = 0;

  const regex = /(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)/g;
  let match: RegExpExecArray | null;
  let matchedAny = false;

  while ((match = regex.exec(trimmed)) !== null) {
    matchedAny = true;
    const amount = parseFloat(match[1]);
    const unit = match[2];

    switch (unit) {
      case 'ms':
        totalMs += amount;
        break;
      case 's':
        totalMs += amount * 1000;
        break;
      case 'm':
        totalMs += amount * 60 * 1000;
        break;
      case 'h':
        totalMs += amount * 60 * 60 * 1000;
        break;
      case 'd':
        totalMs += amount * 24 * 60 * 60 * 1000;
        break;
    }
  }

  if (!matchedAny) {
    const parsedNum = parseFloat(trimmed);
    if (!isNaN(parsedNum)) {
      return parseResetToMs(trimmed);
    }
  }

  return Math.round(totalMs);
}

/**
 * Normalize headers from any structure to a case-insensitive Map.
 */
function normalizeHeaders(headers: any): Map<string, string> {
  const map = new Map<string, string>();

  if (typeof headers.forEach === 'function') {
    // Standard fetch Headers object
    headers.forEach((val: string, key: string) => {
      map.set(key.toLowerCase(), String(val));
    });
  } else if (typeof headers === 'object') {
    for (const [key, val] of Object.entries(headers)) {
      if (val !== undefined && val !== null) {
        map.set(key.toLowerCase(), Array.isArray(val) ? val.join(', ') : String(val));
      }
    }
  }

  return map;
}

function findFirstHeader(map: Map<string, string>, candidates: string[]): string | undefined {
  return candidates.find((c) => map.has(c));
}
