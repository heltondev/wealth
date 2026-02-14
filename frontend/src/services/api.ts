const API_URL = import.meta.env.VITE_API_URL || '/api';

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  // When Cognito is added, inject the token here:
  // const session = await fetchAuthSession();
  // headers['Authorization'] = session.tokens?.idToken?.toString() || '';

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.error || `API error: ${response.statusText}`);
  }

  return response.json();
}

// --- Types ---

export interface Portfolio {
  portfolioId: string;
  name: string;
  description: string;
  baseCurrency: string;
  createdAt: string;
  updatedAt: string;
}

export interface Asset {
  assetId: string;
  portfolioId: string;
  ticker: string;
  name: string;
  assetClass: 'stock' | 'fii' | 'bond' | 'crypto' | 'rsu';
  country: 'BR' | 'US' | 'CA';
  currency: string;
  status: string;
  quantity?: number;
  source?: string | null;
  createdAt: string;
}

export interface Transaction {
  transId: string;
  portfolioId: string;
  assetId: string;
  type: 'buy' | 'sell' | 'dividend' | 'jcp' | 'tax' | 'subscription' | 'transfer';
  date: string;
  quantity: number;
  price: number;
  currency: string;
  amount: number;
  status: string;
  sourceDocId: string | null;
  createdAt: string;
}

export interface Alias {
  normalizedName: string;
  ticker: string;
  source: string;
  createdAt: string;
}

export interface UserSettings {
  displayName?: string;
  email?: string;
  preferredCurrency?: string;
  locale?: string;
  updatedAt?: string;
}

// --- API Methods ---

export const api = {
  // Portfolios
  getPortfolios: () => request<Portfolio[]>('/portfolios'),
  createPortfolio: (data: Partial<Portfolio>) =>
    request<Portfolio>('/portfolios', { method: 'POST', body: JSON.stringify(data) }),
  getPortfolio: (id: string) => request<Portfolio>(`/portfolios/${id}`),
  updatePortfolio: (id: string, data: Partial<Portfolio>) =>
    request<Portfolio>(`/portfolios/${id}`, { method: 'PUT', body: JSON.stringify(data) }),
  deletePortfolio: (id: string) =>
    request<{ message: string; id: string }>(`/portfolios/${id}`, { method: 'DELETE' }),

  // Assets
  getAssets: (portfolioId: string) => request<Asset[]>(`/portfolios/${portfolioId}/assets`),
  createAsset: (portfolioId: string, data: Record<string, string>) =>
    request<Asset>(`/portfolios/${portfolioId}/assets`, { method: 'POST', body: JSON.stringify(data) }),
  getAsset: (portfolioId: string, assetId: string) =>
    request<Asset>(`/portfolios/${portfolioId}/assets/${assetId}`),
  updateAsset: (portfolioId: string, assetId: string, data: Partial<Asset>) =>
    request<Asset>(`/portfolios/${portfolioId}/assets/${assetId}`, { method: 'PUT', body: JSON.stringify(data) }),
  deleteAsset: (portfolioId: string, assetId: string) =>
    request<{ message: string; id: string }>(`/portfolios/${portfolioId}/assets/${assetId}`, { method: 'DELETE' }),

  // Transactions
  getTransactions: (portfolioId: string) =>
    request<Transaction[]>(`/portfolios/${portfolioId}/transactions`),
  createTransaction: (portfolioId: string, data: Partial<Transaction>) =>
    request<Transaction>(`/portfolios/${portfolioId}/transactions`, { method: 'POST', body: JSON.stringify(data) }),
  deleteTransaction: (portfolioId: string, transId: string) =>
    request<{ message: string; id: string }>(`/portfolios/${portfolioId}/transactions/${transId}`, { method: 'DELETE' }),

  // Settings
  getProfile: () => request<UserSettings>('/settings/profile'),
  updateProfile: (data: Partial<UserSettings>) =>
    request<UserSettings>('/settings/profile', { method: 'PUT', body: JSON.stringify(data) }),

  // Aliases
  getAliases: () => request<Alias[]>('/settings/aliases'),
  createAlias: (data: Partial<Alias>) =>
    request<Alias>('/settings/aliases', { method: 'POST', body: JSON.stringify(data) }),
};
