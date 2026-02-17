const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;
const MAX_CACHE_ENTRIES = 200;

type CacheEntry<T> = {
  value: T;
  expiresAt: number;
};

const cacheStore = new Map<string, CacheEntry<unknown>>();
const inFlightRequests = new Map<string, Promise<unknown>>();

const now = () => Date.now();

const normalizeKeyPart = (value: unknown): string => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return String(value).trim().toUpperCase();
};

const isExpired = (entry: CacheEntry<unknown>, at: number): boolean => entry.expiresAt <= at;

const pruneExpiredEntries = () => {
  const current = now();
  for (const [key, entry] of cacheStore.entries()) {
    if (isExpired(entry, current)) {
      cacheStore.delete(key);
    }
  }
};

const pruneOverflowEntries = () => {
  if (cacheStore.size <= MAX_CACHE_ENTRIES) return;

  const overflow = cacheStore.size - MAX_CACHE_ENTRIES;
  const entries = Array.from(cacheStore.entries())
    .sort((left, right) => left[1].expiresAt - right[1].expiresAt)
    .slice(0, overflow);

  for (const [key] of entries) {
    cacheStore.delete(key);
  }
};

export const buildAnalyticsCacheKey = (scope: string, parts: unknown[]): string => {
  const prefix = normalizeKeyPart(scope) || 'ANALYTICS';
  const normalizedParts = parts.map((part) => normalizeKeyPart(part));
  return `${prefix}::${normalizedParts.join('|')}`;
};

export const getCachedAnalyticsValue = <T>(key: string): T | null => {
  if (!key) return null;
  const entry = cacheStore.get(key);
  if (!entry) return null;

  const current = now();
  if (isExpired(entry, current)) {
    cacheStore.delete(key);
    return null;
  }

  return entry.value as T;
};

export const setCachedAnalyticsValue = <T>(key: string, value: T, ttlMs = DEFAULT_CACHE_TTL_MS): T => {
  if (!key) return value;

  const ttl = Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : DEFAULT_CACHE_TTL_MS;
  cacheStore.set(key, {
    value,
    expiresAt: now() + ttl,
  });

  pruneExpiredEntries();
  pruneOverflowEntries();

  return value;
};

export const getOrFetchCachedAnalytics = async <T>(
  key: string,
  fetcher: () => Promise<T>,
  options?: { ttlMs?: number; forceRefresh?: boolean }
): Promise<T> => {
  if (!key) {
    return fetcher();
  }

  if (!options?.forceRefresh) {
    const cached = getCachedAnalyticsValue<T>(key);
    if (cached !== null) {
      return cached;
    }
  }

  const existingRequest = inFlightRequests.get(key);
  if (existingRequest) {
    return existingRequest as Promise<T>;
  }

  const pending = fetcher()
    .then((result) => setCachedAnalyticsValue(key, result, options?.ttlMs))
    .finally(() => {
      inFlightRequests.delete(key);
    });

  inFlightRequests.set(key, pending as Promise<unknown>);
  return pending;
};

export const invalidateAnalyticsCacheByPrefix = (scopePrefix: string) => {
  const normalizedPrefix = normalizeKeyPart(scopePrefix);
  if (!normalizedPrefix) return;

  for (const key of cacheStore.keys()) {
    if (key.startsWith(`${normalizedPrefix}::`)) {
      cacheStore.delete(key);
    }
  }

  for (const key of inFlightRequests.keys()) {
    if (key.startsWith(`${normalizedPrefix}::`)) {
      inFlightRequests.delete(key);
    }
  }
};

export const clearAnalyticsCache = () => {
  cacheStore.clear();
  inFlightRequests.clear();
};
