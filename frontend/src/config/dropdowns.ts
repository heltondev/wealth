import type { DropdownConfigMap, DropdownOption } from '../services/api';

export const DEFAULT_DROPDOWN_CONFIG: DropdownConfigMap = {
  'assets.form.assetClass': {
    label: 'Assets / Asset Class',
    options: [
      { value: 'stock', label: 'Stock' },
      { value: 'fii', label: 'FII' },
      { value: 'bond', label: 'Bond' },
      { value: 'crypto', label: 'Crypto' },
      { value: 'rsu', label: 'RSU' },
    ],
  },
  'assets.form.country': {
    label: 'Assets / Country',
    options: [
      { value: 'BR', label: 'Brazil' },
      { value: 'US', label: 'United States' },
      { value: 'CA', label: 'Canada' },
    ],
  },
  'assets.form.currency': {
    label: 'Assets / Currency',
    options: [
      { value: 'BRL', label: 'BRL' },
      { value: 'USD', label: 'USD' },
      { value: 'CAD', label: 'CAD' },
    ],
  },
  'assets.filters.status': {
    label: 'Assets / Status Filter',
    options: [
      { value: 'active', label: 'Active' },
      { value: 'inactive', label: 'Inactive' },
      { value: 'all', label: 'All' },
    ],
  },
  'transactions.filters.type': {
    label: 'Transactions / Type Filter',
    options: [
      { value: 'all', label: 'All' },
      { value: 'buy', label: 'Buy' },
      { value: 'sell', label: 'Sell' },
      { value: 'dividend', label: 'Dividend' },
      { value: 'jcp', label: 'JCP' },
      { value: 'tax', label: 'Tax' },
      { value: 'subscription', label: 'Subscription' },
      { value: 'transfer', label: 'Transfer' },
    ],
  },
  'transactions.filters.status': {
    label: 'Transactions / Status Filter',
    options: [
      { value: 'all', label: 'All' },
      { value: 'confirmed', label: 'Confirmed' },
      { value: 'pending', label: 'Pending' },
      { value: 'failed', label: 'Failed' },
      { value: 'canceled', label: 'Canceled' },
      { value: 'unknown', label: 'Unknown' },
    ],
  },
  'settings.profile.preferredCurrency': {
    label: 'Settings / Preferred Currency',
    options: [
      { value: 'BRL', label: 'BRL' },
      { value: 'USD', label: 'USD' },
      { value: 'CAD', label: 'CAD' },
    ],
  },
  'settings.aliases.source': {
    label: 'Settings / Alias Source',
    options: [
      { value: 'manual', label: 'Manual' },
      { value: 'b3', label: 'B3' },
      { value: 'itau', label: 'Itau' },
      { value: 'robinhood', label: 'Robinhood' },
      { value: 'equate', label: 'Equate' },
      { value: 'coinbase', label: 'Coinbase' },
    ],
  },
  'settings.preferences.language': {
    label: 'Settings / Language',
    options: [
      { value: 'en', label: 'English' },
      { value: 'pt', label: 'Português' },
    ],
  },
  'tables.pagination.itemsPerPage': {
    label: 'Tables / Items Per Page',
    options: [
      { value: '5', label: '5' },
      { value: '10', label: '10' },
      { value: '25', label: '25' },
      { value: '50', label: '50' },
    ],
  },
};

export function sanitizeDropdownOptions(options: DropdownOption[]): DropdownOption[] {
  const seen = new Set<string>();
  const normalized: DropdownOption[] = [];

  for (const option of options || []) {
    const value = String(option?.value ?? '').trim();
    if (!value || seen.has(value)) continue;
    const label = String(option?.label ?? value).trim() || value;
    normalized.push({ value, label });
    seen.add(value);
  }

  return normalized;
}

export function normalizeDropdownConfig(config: DropdownConfigMap | undefined): DropdownConfigMap {
  const merged: DropdownConfigMap = {};
  const keys = new Set<string>([
    ...Object.keys(DEFAULT_DROPDOWN_CONFIG),
    ...Object.keys(config || {}),
  ]);

  for (const key of keys) {
    const fallback = DEFAULT_DROPDOWN_CONFIG[key];
    const current = config?.[key];
    const options = Array.isArray(current?.options)
      ? current.options
      : (fallback?.options || []);

    merged[key] = {
      label: String(current?.label || fallback?.label || key),
      options: sanitizeDropdownOptions(options),
    };
  }

  return merged;
}

export function getDropdownOptions(config: DropdownConfigMap, key: string): DropdownOption[] {
  return config[key]?.options || [];
}
