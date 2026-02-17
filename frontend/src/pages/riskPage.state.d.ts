export const RETURN_ZERO_EPSILON: number;

export function resolveRiskThresholdValue(
  thresholdPct: unknown,
  fallbackValue?: unknown
): number;

export function classifyReturnValue(
  value: unknown,
  epsilon?: number
): 'positive' | 'negative' | 'neutral';

export function filterScatterRowsByReturn<T extends { returnPct: unknown }>(
  rows: T[],
  filter: unknown,
  epsilon?: number
): T[];
