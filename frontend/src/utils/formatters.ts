const DEFAULT_CURRENCY = 'BRL';
const CURRENCY_CODE_REGEX = /^[A-Z]{3}$/;

const normalizeCurrencyCode = (currency: string): string => {
  const normalized = currency.trim().toUpperCase();
  if (!CURRENCY_CODE_REGEX.test(normalized)) return DEFAULT_CURRENCY;
  return normalized;
};

export const formatCurrency = (value: number, currency = DEFAULT_CURRENCY, locale = 'pt-BR'): string => {
  const normalizedCurrency = normalizeCurrencyCode(currency || DEFAULT_CURRENCY);
  const normalizedValue = Number.isFinite(value) ? value : 0;

  try {
    return new Intl.NumberFormat(locale, {
      style: 'currency',
      currency: normalizedCurrency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(normalizedValue);
  } catch {
    // Guard against unsupported locale/currency combinations so the UI never crashes while rendering tables.
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: DEFAULT_CURRENCY,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(normalizedValue);
  }
};

export const formatDate = (dateStr: string, locale = 'pt-BR'): string => {
  return new Date(dateStr).toLocaleDateString(locale, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
};

export const formatNumber = (value: number, decimals = 2): string => {
  return value.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
};

export const formatPercent = (value: number): string => {
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
};
