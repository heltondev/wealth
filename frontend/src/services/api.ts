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

const fileToBase64 = (file: File): Promise<string> =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('Failed to read file'));
    reader.onload = () => {
      const result = reader.result;
      if (typeof result !== 'string') {
        reject(new Error('Failed to read file'));
        return;
      }
      const commaIndex = result.indexOf(',');
      resolve(commaIndex >= 0 ? result.slice(commaIndex + 1) : result);
    };
    reader.readAsDataURL(file);
  });

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
  assetClass: string;
  country: string;
  currency: string;
  status: string;
  quantity?: number;
  currentPrice?: number | null;
  currentValue?: number | null;
  lastPriceSource?: string | null;
  lastPriceAt?: string | null;
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
  institution?: string | null;
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

export interface DropdownOption {
  value: string;
  label: string;
}

export interface DropdownConfig {
  label: string;
  options: DropdownOption[];
}

export type DropdownConfigMap = Record<string, DropdownConfig>;

export interface DropdownSettings {
  dropdowns: DropdownConfigMap;
  updatedAt?: string | null;
}

export interface DashboardAllocationItem {
  key: string;
  value: number;
  weight_pct: number;
}

export interface DashboardResponse {
  portfolioId: string;
  currency: string;
  total_value_brl: number;
  allocation_by_class: DashboardAllocationItem[];
  allocation_by_currency: DashboardAllocationItem[];
  allocation_by_sector: DashboardAllocationItem[];
  evolution: Array<{ date: string; value: number }>;
  evolution_period?: string;
  return_absolute: number;
  return_percent: number;
  fetched_at: string;
}

export interface DividendMonthlyItem {
  period: string;
  amount: number;
}

export interface DividendCalendarEvent {
  ticker?: string;
  eventType?: string;
  eventTitle?: string;
  eventDate?: string;
  details?: unknown;
  [key: string]: unknown;
}

export interface DividendsResponse {
  portfolioId: string;
  monthly_dividends: DividendMonthlyItem[];
  total_last_12_months: number;
  total_in_period?: number;
  average_monthly_income?: number;
  annualized_income?: number;
  period_months?: number;
  period_from?: string;
  period_to?: string;
  projected_monthly_income: number;
  projected_annual_income: number;
  yield_on_cost_realized: number;
  dividend_yield_current?: number;
  calendar: DividendCalendarEvent[];
  calendar_upcoming?: DividendCalendarEvent[];
  fetched_at: string;
}

export interface TaxMonthlyItem {
  month: string;
  gross_sales: Record<string, number>;
  realized_gain: Record<string, number>;
  tax_due: Record<string, number>;
  dividends: number;
  jcp: number;
}

export interface TaxReportResponse {
  portfolioId: string;
  year: number;
  monthly: TaxMonthlyItem[];
  total_tax_due: number;
  total_dividends_isentos: number;
  total_jcp_tributavel: number;
  carry_loss_by_class: Record<string, number>;
  data_source?: string;
  fetched_at?: string;
  is_scraped?: boolean;
}

export interface RebalanceTarget {
  targetId?: string;
  scope: 'assetClass' | 'asset' | string;
  value: string;
  percent: number;
  updatedAt?: string | null;
}

export interface RebalanceTargetsResponse {
  portfolioId: string;
  targets: RebalanceTarget[];
  fetched_at?: string;
}

export interface RebalanceDriftItem {
  scope: 'assetClass' | 'asset' | string;
  scope_key: string;
  assetClass?: string;
  assetId?: string;
  ticker?: string | null;
  current_value: number;
  target_value: number;
  target_weight_pct: number;
  current_weight_pct: number;
  drift_value: number;
  drift_pct: number;
}

export interface RebalanceSuggestionItem {
  scope: 'assetClass' | 'asset' | string;
  assetClass?: string;
  assetId?: string | null;
  ticker?: string | null;
  recommended_amount: number;
  current_value: number;
  target_value: number;
}

export interface RebalanceSuggestionResponse {
  portfolioId: string;
  scope: 'assetClass' | 'asset' | string;
  contribution: number;
  current_total: number;
  target_total_after_contribution: number;
  targets: Record<string, number>;
  drift?: RebalanceDriftItem[];
  suggestions: RebalanceSuggestionItem[];
  fetched_at?: string;
}

export interface RiskConcentrationItem {
  assetId: string;
  ticker: string;
  market_value: number;
  weight_pct: number;
}

export interface RiskCorrelationItem {
  left: string;
  right: string;
  correlation: number;
}

export interface RiskScatterItem {
  ticker: string;
  volatility: number;
  return_pct: number;
}

export interface RiskInflationAdjustedPoint {
  date: string;
  nominal_value: number;
  real_value: number;
}

export interface RiskFxExposureItem {
  value: number;
  weight_pct: number;
}

export interface BenchmarkReturnItem {
  benchmark: string;
  symbol?: string;
  return_pct: number;
}

export interface BenchmarkNormalizedPoint {
  date: string;
  value: number;
}

export interface BenchmarksResponse {
  portfolioId: string;
  period: string;
  from: string;
  to: string;
  portfolio_return_pct: number;
  benchmarks: BenchmarkReturnItem[];
  selected_benchmark: BenchmarkReturnItem | null;
  alpha: number | null;
  normalized_series: {
    portfolio: BenchmarkNormalizedPoint[];
    benchmarks: Record<string, BenchmarkNormalizedPoint[]>;
  };
  fetched_at: string;
}

export interface CompareAssetFundamentals {
  pe: number | null;
  pb: number | null;
  roe: number | null;
  roa: number | null;
  roic: number | null;
  netDebtEbitda: number | null;
  payout: number | null;
  evEbitda: number | null;
  lpa: number | null;
  vpa: number | null;
  netMargin: number | null;
  ebitMargin: number | null;
}

export interface CompareAssetRiskSnapshot {
  volatility: number | null;
  drawdown: number | null;
}

export interface CompareAssetRow {
  ticker: string;
  name: string;
  assetClass: string;
  currency: string;
  current_price: number | null;
  fair_price: number | null;
  margin_of_safety_pct: number | null;
  fundamentals: CompareAssetFundamentals | null;
  risk: CompareAssetRiskSnapshot | null;
}

export interface CompareAssetsResponse {
  tickers: string[];
  comparison: CompareAssetRow[];
  fetched_at: string;
}

export interface MultiCurrencyPortfolioSummary {
  start_value_brl: number;
  end_value_brl: number;
  start_value_original_brl: number;
  end_value_original_brl: number;
  return_brl_pct: number;
  return_original_pct: number;
  fx_impact_pct: number;
  fx_impact_brl: number;
  foreign_exposure_pct: number;
}

export interface MultiCurrencyEvolutionPoint {
  date: string;
  value_brl: number;
  value_original_brl: number;
  fx_impact_brl: number;
}

export interface MultiCurrencyByCurrencyItem {
  currency: string;
  start_value_brl: number;
  end_value_brl: number;
  start_value_original_brl: number;
  end_value_original_brl: number;
  fx_start: number;
  fx_current: number;
  weight_pct: number;
  return_brl_pct: number;
  return_original_pct: number;
  fx_impact_pct: number;
  fx_impact_brl: number;
}

export interface MultiCurrencyByAssetItem {
  assetId: string;
  ticker: string;
  name: string;
  asset_class: string;
  currency: string;
  quantity: number;
  fx_start: number;
  fx_current: number;
  start_value_native: number;
  end_value_native: number;
  start_value_brl: number;
  end_value_brl: number;
  start_value_original_brl: number;
  end_value_original_brl: number;
  return_brl_pct: number;
  return_original_pct: number;
  fx_impact_pct: number;
  fx_impact_brl: number;
}

export interface MultiCurrencyResponse {
  portfolioId: string;
  period: string;
  from: string;
  to: string;
  portfolio: MultiCurrencyPortfolioSummary;
  evolution: MultiCurrencyEvolutionPoint[];
  by_currency: MultiCurrencyByCurrencyItem[];
  by_asset: MultiCurrencyByAssetItem[];
  fx_rates: {
    latest: Record<string, number>;
    start: Record<string, number>;
    end: Record<string, number>;
  };
  fetched_at: string;
}

export interface RiskResponse {
  portfolioId: string;
  concentration: RiskConcentrationItem[];
  concentration_alerts: RiskConcentrationItem[];
  volatility_by_asset: Record<string, number>;
  drawdown_by_asset: Record<string, number>;
  portfolio_drawdown: number;
  portfolio_volatility: number;
  correlation_matrix: RiskCorrelationItem[];
  risk_return_scatter: RiskScatterItem[];
  fx_exposure: Record<string, RiskFxExposureItem>;
  inflation_adjusted_value: RiskInflationAdjustedPoint[];
  fetched_at: string;
}

export interface AlertEvent {
  eventId: string;
  ruleId?: string;
  type: string;
  message: string;
  eventAt: string;
  read?: boolean;
  dedupeKey?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface Goal {
  goalId: string;
  type: string;
  targetAmount: number;
  targetDate?: string | null;
  currency?: string;
  label?: string | null;
  status?: string;
}

export interface AlertRule {
  ruleId: string;
  type: string;
  enabled: boolean;
  portfolioId?: string | null;
  params: Record<string, unknown>;
  description?: string | null;
}

export interface ReportRecord {
  reportId: string;
  reportType: string;
  period?: string | null;
  locale?: string | null;
  storage: {
    type?: string;
    bucket?: string;
    key?: string;
    uri?: string;
    path?: string;
    [key: string]: unknown;
  };
  data_source?: string | null;
  fetched_at?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface ReportContentResponse {
  reportId: string;
  reportType: string | null;
  period: string | null;
  createdAt: string | null;
  contentType: string;
  filename: string;
  sizeBytes: number;
  dataBase64: string;
  fetched_at: string;
}

export interface ParserDescriptor {
  id: string;
  provider: string;
}

export interface ImportStatsByEntity {
  created: number;
  skipped: number;
  updated?: number;
  filtered?: number;
}

export interface ImportB3Response {
  portfolioId: string;
  parser: string;
  provider: string;
  detectionMode: 'auto' | 'manual' | string;
  sourceFile: string;
  importedAt: string;
  stats: {
    assets: ImportStatsByEntity;
    transactions: ImportStatsByEntity;
    aliases: ImportStatsByEntity;
  };
  warnings: string[];
}

export interface ContributionPayload {
  contributionId?: string;
  date?: string;
  amount: number;
  currency?: string;
  destination?: string;
  notes?: string;
}

export interface SimulationPayload {
  monthlyAmount: number;
  rate: number;
  years: number;
  ticker?: string;
  initialAmount?: number;
  portfolioId?: string;
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

  // Core refresh services
  refreshMarketData: (portfolioId: string, assetId?: string) =>
    request(`/portfolios/${portfolioId}/market-data/refresh`, {
      method: 'POST',
      body: JSON.stringify({ assetId: assetId || null }),
    }),
  refreshPriceHistory: (portfolioId: string, assetId?: string) =>
    request(`/portfolios/${portfolioId}/price-history`, {
      method: 'POST',
      body: JSON.stringify({ assetId: assetId || null }),
    }),
  getPriceAtDate: (portfolioId: string, ticker: string, date: string) =>
    request(`/portfolios/${portfolioId}/price-history?action=priceAtDate&ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(date)}`),
  getAverageCost: (portfolioId: string, ticker: string, method = 'fifo') =>
    request(`/portfolios/${portfolioId}/price-history?action=averageCost&ticker=${encodeURIComponent(ticker)}&method=${encodeURIComponent(method)}`),
  getPortfolioMetrics: (portfolioId: string, method = 'fifo') =>
    request(`/portfolios/${portfolioId}/price-history?action=metrics&method=${encodeURIComponent(method)}`),
  getPriceChart: (portfolioId: string, ticker: string, chartType = 'price_history', period = '1A', method = 'fifo') =>
    request(`/portfolios/${portfolioId}/price-history?action=chart&ticker=${encodeURIComponent(ticker)}&chartType=${encodeURIComponent(chartType)}&period=${encodeURIComponent(period)}&method=${encodeURIComponent(method)}`),

  // Settings
  getProfile: () => request<UserSettings>('/settings/profile'),
  updateProfile: (data: Partial<UserSettings>) =>
    request<UserSettings>('/settings/profile', { method: 'PUT', body: JSON.stringify(data) }),
  getDropdownSettings: () => request<DropdownSettings>('/settings/dropdowns'),
  updateDropdownSettings: (data: DropdownSettings) =>
    request<DropdownSettings>('/settings/dropdowns', { method: 'PUT', body: JSON.stringify(data) }),

  // Aliases
  getAliases: () => request<Alias[]>('/settings/aliases'),
  createAlias: (data: Partial<Alias>) =>
    request<Alias>('/settings/aliases', { method: 'POST', body: JSON.stringify(data) }),
  listParsers: () => request<ParserDescriptor[]>('/parsers'),
  importB3File: async (portfolioId: string, file: File, parserId?: string) => {
    const fileContentBase64 = await fileToBase64(file);
    return request<ImportB3Response>(`/portfolios/${encodeURIComponent(portfolioId)}/import`, {
      method: 'POST',
      body: JSON.stringify({
        fileName: file.name,
        parserId: parserId || null,
        fileContentBase64,
      }),
    });
  },

  // Dashboard + analytics
  getDashboard: (portfolioId: string, period = 'MAX') =>
    request<DashboardResponse>(
      `/portfolios/${portfolioId}/dashboard?period=${encodeURIComponent(period)}`
    ),
  getDividends: (portfolioId: string, params?: { fromDate?: string; method?: string; periodMonths?: number }) => {
    const query = new URLSearchParams();
    if (params?.fromDate) query.set('fromDate', params.fromDate);
    if (params?.method) query.set('method', params.method);
    if (typeof params?.periodMonths === 'number' && Number.isFinite(params.periodMonths) && params.periodMonths > 0) {
      query.set('periodMonths', String(Math.round(params.periodMonths)));
    }
    const suffix = query.toString() ? `?${query.toString()}` : '';
    return request<DividendsResponse>(`/portfolios/${portfolioId}/dividends${suffix}`);
  },
  getTaxReport: (portfolioId: string, year: number) =>
    request<TaxReportResponse>(`/portfolios/${portfolioId}/tax?year=${encodeURIComponent(String(year))}`),
  getRebalanceTargets: (portfolioId: string) =>
    request<RebalanceTargetsResponse>(`/portfolios/${portfolioId}/rebalance/targets`),
  getRebalanceSuggestion: (portfolioId: string, amount: number, scope = 'assetClass') =>
    request<RebalanceSuggestionResponse>(
      `/portfolios/${portfolioId}/rebalance/suggestion?amount=${encodeURIComponent(String(amount))}&scope=${encodeURIComponent(scope)}`
    ),
  setRebalanceTargets: (portfolioId: string, targets: RebalanceTarget[]) =>
    request<RebalanceTargetsResponse>(`/portfolios/${portfolioId}/rebalance/targets`, { method: 'POST', body: JSON.stringify({ targets }) }),
  getRisk: (portfolioId: string, params?: { concentrationThreshold?: number }) => {
    const query = new URLSearchParams();
    if (
      typeof params?.concentrationThreshold === 'number' &&
      Number.isFinite(params.concentrationThreshold) &&
      params.concentrationThreshold > 0
    ) {
      query.set('concentrationThreshold', String(params.concentrationThreshold));
    }
    const suffix = query.toString() ? `?${query.toString()}` : '';
    return request<RiskResponse>(`/portfolios/${portfolioId}/risk${suffix}`);
  },
  getBenchmarks: (portfolioId: string, benchmark = 'IBOV', period = '1A') =>
    request<BenchmarksResponse>(`/portfolios/${portfolioId}/benchmarks?benchmark=${encodeURIComponent(benchmark)}&period=${encodeURIComponent(period)}`),
  getMultiCurrency: (portfolioId: string, period = '1Y', method = 'fifo') =>
    request<MultiCurrencyResponse>(
      `/portfolios/${portfolioId}/multi-currency?period=${encodeURIComponent(period)}&method=${encodeURIComponent(method)}`
    ),

  // Contributions
  createContribution: (portfolioId: string, data: ContributionPayload) =>
    request(`/portfolios/${portfolioId}/contributions`, { method: 'POST', body: JSON.stringify(data) }),
  getContributions: (portfolioId: string) =>
    request(`/portfolios/${portfolioId}/contributions`),

  // Assets advanced
  getAssetFairPrice: (ticker: string, portfolioId?: string) => {
    const query = portfolioId ? `?portfolioId=${encodeURIComponent(portfolioId)}` : '';
    return request(`/assets/${encodeURIComponent(ticker)}${query}`);
  },
  getAssetDetails: (ticker: string, portfolioId?: string) => {
    const query = new URLSearchParams({ action: 'details' });
    if (portfolioId) query.set('portfolioId', portfolioId);
    return request(`/assets/${encodeURIComponent(ticker)}?${query.toString()}`);
  },
  getAssetFinancials: (ticker: string, portfolioId?: string) => {
    const query = new URLSearchParams({ action: 'financials' });
    if (portfolioId) query.set('portfolioId', portfolioId);
    return request(`/assets/${encodeURIComponent(ticker)}?${query.toString()}`);
  },
  getAssetEvents: (ticker: string, portfolioId?: string) => {
    const query = new URLSearchParams({ action: 'events' });
    if (portfolioId) query.set('portfolioId', portfolioId);
    return request(`/assets/${encodeURIComponent(ticker)}?${query.toString()}`);
  },
  getAssetNews: (ticker: string, portfolioId?: string) => {
    const query = new URLSearchParams({ action: 'news' });
    if (portfolioId) query.set('portfolioId', portfolioId);
    return request(`/assets/${encodeURIComponent(ticker)}?${query.toString()}`);
  },
  getFiiUpdates: (ticker: string, portfolioId?: string) => {
    const query = new URLSearchParams({ action: 'fii-updates' });
    if (portfolioId) query.set('portfolioId', portfolioId);
    return request(`/assets/${encodeURIComponent(ticker)}?${query.toString()}`);
  },
  getFiiEmissions: (ticker: string, portfolioId?: string) => {
    const query = new URLSearchParams({ action: 'fii-emissions' });
    if (portfolioId) query.set('portfolioId', portfolioId);
    return request(`/assets/${encodeURIComponent(ticker)}?${query.toString()}`);
  },
  screenAssets: (filters: Record<string, unknown>) =>
    request('/assets/screen', { method: 'POST', body: JSON.stringify(filters) }),
  compareAssets: (tickers: string[], portfolioId?: string) =>
    request<CompareAssetsResponse>('/assets/compare', { method: 'POST', body: JSON.stringify({ tickers, portfolioId }) }),

  // Fixed income + costs
  getFixedIncomeComparison: (portfolioId?: string) => {
    const query = portfolioId ? `?portfolioId=${encodeURIComponent(portfolioId)}` : '';
    return request(`/fixed-income${query}`);
  },
  calculatePrivateFixedIncome: (payload: Record<string, unknown>) =>
    request('/fixed-income', { method: 'POST', body: JSON.stringify(payload) }),
  getCostAnalysis: (portfolioId?: string) => {
    const query = portfolioId ? `?portfolioId=${encodeURIComponent(portfolioId)}` : '';
    return request(`/costs${query}`);
  },

  // Goals
  getGoals: () => request<Goal[]>('/users/me/goals'),
  createGoal: (goal: Partial<Goal>) =>
    request<Goal>('/users/me/goals', { method: 'POST', body: JSON.stringify(goal) }),
  updateGoal: (goalId: string, goal: Partial<Goal>) =>
    request<Goal>(`/users/me/goals/${goalId}`, { method: 'PUT', body: JSON.stringify(goal) }),
  deleteGoal: (goalId: string) =>
    request<{ deleted: boolean; goalId: string }>(`/users/me/goals/${goalId}`, { method: 'DELETE' }),
  getGoalProgress: (goalId: string) =>
    request(`/users/me/goals/${goalId}/progress`),

  // Alerts
  getAlerts: () => request<{ rules: AlertRule[]; events: AlertEvent[] }>('/users/me/alerts'),
  createAlertRule: (rule: Partial<AlertRule>) =>
    request<AlertRule>('/users/me/alerts', { method: 'POST', body: JSON.stringify(rule) }),
  updateAlertRule: (ruleId: string, rule: Partial<AlertRule>) =>
    request<AlertRule>(`/users/me/alerts/${ruleId}`, { method: 'PUT', body: JSON.stringify(rule) }),
  deleteAlertRule: (ruleId: string) =>
    request<{ deleted: boolean; ruleId: string }>(`/users/me/alerts/${ruleId}`, { method: 'DELETE' }),
  evaluateAlerts: (portfolioId: string) =>
    request('/users/me/alerts?action=evaluate', { method: 'POST', body: JSON.stringify({ portfolioId }) }),

  // Jobs
  refreshEconomicIndicators: () =>
    request('/jobs/economic-data/refresh', { method: 'POST', body: JSON.stringify({}) }),
  refreshCorporateEvents: (payload: { ticker?: string; portfolioId?: string }) =>
    request('/jobs/corporate-events/refresh', { method: 'POST', body: JSON.stringify(payload) }),
  refreshNews: (payload: { ticker?: string; portfolioId?: string }) =>
    request('/jobs/news/refresh', { method: 'POST', body: JSON.stringify(payload) }),
  runAlertEvaluation: (portfolioId: string) =>
    request('/jobs/alerts/refresh', { method: 'POST', body: JSON.stringify({ portfolioId }) }),

  // Reports
  generateReport: (reportType: string, period?: string, portfolioId?: string, locale?: string) =>
    request<ReportRecord>('/reports/generate', {
      method: 'POST',
      body: JSON.stringify({
        reportType,
        period: period || null,
        portfolioId: portfolioId || null,
        locale: locale || null,
      }),
    }),
  listReports: () => request<ReportRecord[]>('/reports'),
  getReport: (reportId: string) =>
    request<ReportRecord>(`/reports/${encodeURIComponent(reportId)}`),
  getReportContent: (reportId: string) =>
    request<ReportContentResponse>(`/reports/${encodeURIComponent(reportId)}?action=content`),
  deleteReport: (reportId: string) =>
    request<{ deleted: boolean; reportId: string; fetched_at: string }>(`/reports/${encodeURIComponent(reportId)}`, { method: 'DELETE' }),

  // Simulation
  simulate: (payload: SimulationPayload) =>
    request('/simulate', { method: 'POST', body: JSON.stringify(payload) }),

  // Community
  listIdeas: (limit?: number) =>
    request(limit ? `/community/ideas?limit=${encodeURIComponent(String(limit))}` : '/community/ideas'),
  publishIdea: (payload: { title: string; content: string; tags?: string[] }) =>
    request('/community/ideas', { method: 'POST', body: JSON.stringify(payload) }),
  getLeagueRanking: () => request('/community/ranking'),
};
