export const RETURN_ZERO_EPSILON = 0.001;

const toNumber = (value) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
};

export const resolveRiskThresholdValue = (thresholdPct, fallbackValue = 15) => {
  const numeric = toNumber(thresholdPct);
  if (numeric > 0) return numeric;
  return toNumber(fallbackValue) > 0 ? toNumber(fallbackValue) : 15;
};

export const classifyReturnValue = (value, epsilon = RETURN_ZERO_EPSILON) => {
  const numeric = toNumber(value);
  if (numeric > epsilon) return 'positive';
  if (numeric < -epsilon) return 'negative';
  return 'neutral';
};

export const filterScatterRowsByReturn = (rows, filter, epsilon = RETURN_ZERO_EPSILON) => {
  const normalizedFilter = String(filter || 'all').toLowerCase();
  const normalizedRows = Array.isArray(rows) ? rows : [];

  if (normalizedFilter === 'all') return normalizedRows;

  return normalizedRows.filter((row) => classifyReturnValue(row?.returnPct, epsilon) === normalizedFilter);
};
