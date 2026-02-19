import { fetchAuthSession } from 'aws-amplify/auth';
import { isAmplifyAuthConfigured } from '../aws-exports';
import { logger } from '../utils/logger';

const API_URL = import.meta.env.VITE_API_URL || '/api';
const API_GET_CACHE_TTL_MS = Number(import.meta.env.VITE_API_GET_CACHE_TTL_MS || 15_000);
const API_GET_CACHE_MAX_ENTRIES = Number(import.meta.env.VITE_API_GET_CACHE_MAX_ENTRIES || 300);

type ApiGetCacheEntry = {
  data: unknown;
  expiresAt: number;
};

const apiGetCacheStore = new Map<string, ApiGetCacheEntry>();
const apiGetInFlightRequests = new Map<string, Promise<unknown>>();

const normalizeMethod = (method?: string): string => String(method || 'GET').trim().toUpperCase();

const normalizeGetCacheKey = (path: string): string => String(path || '').trim();

const pruneExpiredGetCacheEntries = (at = Date.now()) => {
  for (const [key, entry] of apiGetCacheStore.entries()) {
    if (entry.expiresAt <= at) {
      apiGetCacheStore.delete(key);
    }
  }
};

const pruneOverflowGetCacheEntries = () => {
  const overflow = apiGetCacheStore.size - API_GET_CACHE_MAX_ENTRIES;
  if (overflow <= 0) return;

  const oldest = Array.from(apiGetCacheStore.entries())
    .sort((left, right) => left[1].expiresAt - right[1].expiresAt)
    .slice(0, overflow);

  for (const [key] of oldest) {
    apiGetCacheStore.delete(key);
  }
};

const shouldCacheGetRequest = (path: string): boolean => {
  const normalizedPath = String(path || '').toLowerCase();
  if (!normalizedPath) return false;
  if (normalizedPath.includes('/health/')) return false;
  if (normalizedPath.startsWith('/settings/backup')) return false;
  if (normalizedPath.startsWith('/settings/cache')) return false;
  if (normalizedPath.startsWith('/reports/') && normalizedPath.includes('action=content')) return false;
  return true;
};

const clearApiGetCache = () => {
  apiGetCacheStore.clear();
  apiGetInFlightRequests.clear();
};

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const method = normalizeMethod(options.method);
  const cacheKey = normalizeGetCacheKey(path);
  const useGetCache = method === 'GET' && shouldCacheGetRequest(path);

  if (useGetCache) {
    const cached = apiGetCacheStore.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.data as T;
    }
    if (cached && cached.expiresAt <= Date.now()) {
      apiGetCacheStore.delete(cacheKey);
    }

    const inFlight = apiGetInFlightRequests.get(cacheKey);
    if (inFlight) {
      return inFlight as Promise<T>;
    }
  }

  const executeRequest = async (): Promise<T> => {
    const headers = new Headers(options.headers);
    headers.set('Content-Type', 'application/json');

    if (isAmplifyAuthConfigured) {
      try {
        const session = await fetchAuthSession();
        const idToken = session.tokens?.idToken?.toString();
        headers.set('Authorization', idToken || '');
      } catch (error) {
        logger.warn('Unable to resolve auth session', error);
      }
    }

    const response = await fetch(`${API_URL}${path}`, {
      ...options,
      method,
      headers,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error || `API error: ${response.statusText}`);
    }

    const parsed = await response.json();

    if (useGetCache) {
      const ttlMs = Number.isFinite(API_GET_CACHE_TTL_MS) && API_GET_CACHE_TTL_MS > 0
        ? API_GET_CACHE_TTL_MS
        : 15_000;
      apiGetCacheStore.set(cacheKey, {
        data: parsed,
        expiresAt: Date.now() + ttlMs,
      });
      pruneExpiredGetCacheEntries();
      pruneOverflowGetCacheEntries();
    } else if (method !== 'GET') {
      clearApiGetCache();
    }

    return parsed as T;
  };

  if (!useGetCache) {
    return executeRequest();
  }

  const pending = executeRequest()
    .finally(() => {
      apiGetInFlightRequests.delete(cacheKey);
    });
  apiGetInFlightRequests.set(cacheKey, pending as Promise<unknown>);
  return pending;
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

export interface BackupSnapshot {
  schemaVersion: number;
  exportedAt: string;
  exportedBy?: string;
  data: {
    userItems: Record<string, unknown>[];
    portfolioItems: Record<string, unknown>[];
    aliases: Record<string, unknown>[];
  };
  stats?: {
    userItems: number;
    portfolios?: number;
    portfolioItems: number;
    aliases: number;
    totalItems: number;
  };
}

export interface BackupImportResponse {
  mode: 'replace' | 'merge' | string;
  importedAt: string;
  stats: {
    userItems: number;
    portfolioItems: number;
    aliases: number;
    totalItems: number;
  };
}

export interface CacheLayerDiagnostics {
  entries: number;
  defaultTtlMs: number;
  hitCount: number;
  missCount: number;
  requests: number;
  hitRatePct: number;
  [key: string]: unknown;
}

export interface CacheDiagnosticsResponse {
  responseCache: CacheLayerDiagnostics;
  scraperCache: CacheLayerDiagnostics;
  fetchedAt: string;
}

export interface CacheClearResponse extends CacheDiagnosticsResponse {
  cleared: {
    responseCache: boolean;
    scraperCache: boolean;
  };
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
  [currency: string]: string | number;
}

export interface DividendCurrencyKpis {
  total_in_period: number;
  average_monthly_income: number;
  annualized_income: number;
  yield_on_cost_realized: number;
  dividend_yield_current: number;
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
  by_currency?: Record<string, DividendCurrencyKpis>;
  currencies?: string[];
  calendar?: DividendCalendarEvent[];
  calendar_upcoming?: DividendCalendarEvent[];
  fetched_at: string;
}

export interface DividendCalendarMonthResponse {
  month: string;
  events: DividendCalendarEvent[];
}

export interface PortfolioEventNoticeItem {
  id: string;
  dedupe_key?: string | null;
  ticker: string;
  eventType: string;
  eventTitle: string;
  eventDate: string;
  notice_kind?: string;
  severity?: 'low' | 'medium' | 'high' | string;
  read?: boolean;
  readAt?: string | null;
  firstSeenAt?: string | null;
  lastSeenAt?: string | null;
  occurrences?: number;
  details?: Record<string, unknown> | null;
  data_source?: string | null;
  updatedAt?: string | null;
}

export interface PortfolioEventNoticesResponse {
  portfolioId: string;
  today: string;
  week_end: string;
  lookahead_days: number;
  tracked_tickers: number;
  status_filter?: string;
  severity_filter?: string | null;
  total_count?: number;
  unread_count?: number;
  read_count?: number;
  today_count: number;
  week_count: number;
  unread_today_count?: number;
  unread_week_count?: number;
  by_kind?: Record<string, number>;
  by_severity?: Record<string, number>;
  today_by_kind?: Record<string, number>;
  week_by_kind?: Record<string, number>;
  today_events: PortfolioEventNoticeItem[];
  week_events: PortfolioEventNoticeItem[];
  items?: PortfolioEventNoticeItem[];
  fetched_at: string;
}

export interface SyncPortfolioEventInboxResponse extends PortfolioEventNoticesResponse {
  sync?: {
    created: number;
    updated: number;
    reopened: number;
    deleted: number;
    refresh_sources: boolean;
    tracked_tickers: number;
    considered_events: number;
    from_date: string;
    to_date: string;
    synced_at: string;
  };
}

export interface TaxMonthlyItem {
  month: string;
  gross_sales: Record<string, number>;
  realized_gain: Record<string, number>;
  tax_due: Record<string, number>;
  dividends: number;
  jcp: number;
  carry_loss_by_class?: Record<string, number>;
  explain_by_class?: Record<string, TaxMonthlyClassTrace>;
}

export interface TaxExemptionTrace {
  type: string | null;
  applied: boolean;
  eligible: boolean;
  reason: string;
  limit_brl: number | null;
  requires_positive_gain: boolean;
  window_start: string | null;
  window_end: string | null;
}

export interface TaxMonthlyClassTrace {
  asset_class: string;
  rule_id: string;
  rule_label: string;
  tax_rate: number;
  gross_sales: number;
  realized_gain: number;
  carry_in: number;
  adjusted_gain: number;
  taxable_gain: number;
  tax_due: number;
  carry_out: number;
  decision: string;
  exemption: TaxExemptionTrace | null;
}

export interface TaxRuleConfig {
  asset_class: string;
  rule_id: string;
  label: string;
  rate: number;
  exemption: {
    type?: string | null;
    limit_brl?: number;
    requires_positive_gain?: boolean;
    windows?: Array<Record<string, string | null>>;
  } | null;
}

export interface TaxReportResponse {
  portfolioId: string;
  year: number;
  monthly: TaxMonthlyItem[];
  total_tax_due: number;
  total_dividends_isentos: number;
  total_jcp_tributavel: number;
  carry_loss_start_by_class?: Record<string, number>;
  carry_loss_by_class: Record<string, number>;
  tax_rules_by_class?: Record<string, TaxRuleConfig>;
  trace_version?: number;
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
  display_currency?: string | null;
  fx_rate_to_brl?: number | null;
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
  display_currency?: string | null;
  fx_rate_to_brl?: number | null;
  recommended_amount: number;
  current_value: number;
  target_value: number;
}

export interface RebalanceContributionInput {
  amount_brl: number;
  amount_usd: number;
  usd_brl_rate: number;
  total_brl: number;
}

export interface RebalanceContributionPool {
  currency: string;
  countries: string[];
  amount_native: number;
  amount_brl: number;
  allocated_brl: number;
  unallocated_brl: number;
  eligible_keys: number;
  strategy: string;
}

export interface RebalanceContributionBreakdown {
  mode: 'single' | 'by_currency' | string;
  input?: RebalanceContributionInput | null;
  pools?: RebalanceContributionPool[];
  unallocated_brl?: number;
}

export interface RebalanceThesisConflict {
  scope: 'thesisScope' | 'assetClass' | string;
  scope_key: string;
  type: 'below_min' | 'above_max' | string;
  actual_pct: number;
  min_pct?: number | null;
  max_pct?: number | null;
  target_pct?: number | null;
  title?: string | null;
  country?: string | null;
  asset_class?: string | null;
  related_scope_keys?: string[];
}

export interface RebalanceThesisDiagnostics {
  active_scope_count: number;
  scopes_with_target_count: number;
  applied_scope_count: number;
  applied_scope_keys: string[];
  ignored_scope_keys: string[];
  tracked_asset_count: number;
  covered_asset_count: number;
  assets_without_scope_count: number;
  covered_value_pct: number;
  uncovered_scope_keys: string[];
  conflicts: RebalanceThesisConflict[];
}

export interface RebalanceSuggestionResponse {
  portfolioId: string;
  scope: 'assetClass' | 'asset' | string;
  contribution: number;
  contribution_mode?: 'single' | 'by_currency' | string;
  contribution_input?: RebalanceContributionInput | null;
  contribution_breakdown?: RebalanceContributionBreakdown | null;
  current_total: number;
  target_total_after_contribution: number;
  target_source?: 'manual' | 'thesis' | 'equal_weight' | string;
  targets: Record<string, number>;
  thesis_diagnostics?: RebalanceThesisDiagnostics;
  drift?: RebalanceDriftItem[];
  suggestions: RebalanceSuggestionItem[];
  fetched_at?: string;
}

export type ThesisCountry = 'BR' | 'US' | 'CA';
export type ThesisAssetClass =
  | 'FII'
  | 'TESOURO'
  | 'ETF'
  | 'STOCK'
  | 'REIT'
  | 'BOND'
  | 'CRYPTO'
  | 'CASH'
  | 'RSU';

export interface ThesisRecord {
  thesisId: string | null;
  portfolioId: string;
  scopeKey: string;
  country: ThesisCountry | string;
  assetClass: ThesisAssetClass | string;
  title: string;
  thesisText: string;
  targetAllocation: number | null;
  minAllocation: number | null;
  maxAllocation: number | null;
  triggers: string;
  actionPlan: string;
  riskNotes: string;
  status: 'active' | 'archived' | string;
  version: number;
  createdAt: string | null;
  updatedAt: string | null;
  archivedAt: string | null;
}

export interface ThesisListResponse {
  portfolioId: string;
  taxonomy: {
    countries: ThesisCountry[];
    assetClasses: ThesisAssetClass[];
  };
  items: ThesisRecord[];
  history?: ThesisRecord[];
}

export interface ThesisDetailResponse {
  portfolioId: string;
  scopeKey: string;
  current: ThesisRecord | null;
  history: ThesisRecord[];
}

export interface ThesisUpsertPayload {
  scopeKey?: string;
  country?: ThesisCountry | string;
  assetClass?: ThesisAssetClass | string;
  title: string;
  thesisText: string;
  targetAllocation?: number | null;
  minAllocation?: number | null;
  maxAllocation?: number | null;
  triggers?: string;
  actionPlan?: string;
  riskNotes?: string;
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
  has_series?: boolean;
  current_points?: number | null;
  month_min?: number | null;
  month_max?: number | null;
  week52_min?: number | null;
  week52_max?: number | null;
  points_source?: string | null;
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
  available_benchmarks?: string[];
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
  value_brl_assets: number;
  value_usd_assets: number;
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
  includedReports?: Array<{
    reportId: string;
    reportType: string | null;
    period: string | null;
    createdAt: string | null;
  }>;
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

export interface ImportAssetReportEntry {
  assetId: string | null;
  ticker: string | null;
  name: string | null;
  assetClass: string | null;
  country: string | null;
  currency: string | null;
  quantity: number;
  currentPrice: number | null;
  currentValue: number | null;
  status: string | null;
  reason?: string;
}

export interface ImportTransactionReportEntry {
  transId: string | null;
  ticker: string | null;
  type: string | null;
  date: string | null;
  quantity: number;
  price: number;
  amount: number;
  currency: string;
  source: string | null;
  dedupKey?: string;
  reason?: string;
}

export interface ImportAliasReportEntry {
  normalizedName: string | null;
  ticker: string | null;
  source: string | null;
  reason?: string;
}

export interface ImportB3Response {
  portfolioId: string;
  parser: string;
  provider: string;
  detectionMode: 'auto' | 'manual' | string;
  dryRun?: boolean;
  sourceFile: string;
  importedAt: string;
  stats: {
    assets: ImportStatsByEntity;
    transactions: ImportStatsByEntity;
    aliases: ImportStatsByEntity;
  };
  report: {
    assets: {
      created: ImportAssetReportEntry[];
      updated: ImportAssetReportEntry[];
      skipped: ImportAssetReportEntry[];
    };
    transactions: {
      created: ImportTransactionReportEntry[];
      skipped: ImportTransactionReportEntry[];
      filtered: ImportTransactionReportEntry[];
    };
    aliases: {
      created: ImportAliasReportEntry[];
      skipped: ImportAliasReportEntry[];
    };
  };
  warnings: string[];
}

export interface ImportB3RequestOptions {
  parserId?: string;
  dryRun?: boolean;
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
  exportBackup: () => request<BackupSnapshot>('/settings/backup'),
  importBackup: (
    backup: BackupSnapshot | Record<string, unknown>,
    mode: 'replace' | 'merge' = 'replace'
  ) =>
    request<BackupImportResponse>('/settings/backup', {
      method: 'POST',
      body: JSON.stringify({
        mode,
        backup,
      }),
    }),
  getCacheDiagnostics: () => request<CacheDiagnosticsResponse>('/settings/cache'),
  clearCaches: (scope: 'all' | 'response' | 'scraper' = 'all') =>
    request<CacheClearResponse>('/settings/cache', {
      method: 'POST',
      body: JSON.stringify({
        action: 'clear',
        scope,
      }),
    }),

  // Aliases
  getAliases: () => request<Alias[]>('/settings/aliases'),
  createAlias: (data: Partial<Alias>) =>
    request<Alias>('/settings/aliases', { method: 'POST', body: JSON.stringify(data) }),
  listParsers: () => request<ParserDescriptor[]>('/parsers'),
  importB3File: async (portfolioId: string, file: File, options?: ImportB3RequestOptions) => {
    const fileContentBase64 = await fileToBase64(file);
    return request<ImportB3Response>(`/portfolios/${encodeURIComponent(portfolioId)}/import`, {
      method: 'POST',
      body: JSON.stringify({
        fileName: file.name,
        parserId: options?.parserId || null,
        dryRun: Boolean(options?.dryRun),
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
  getDividendCalendar: (portfolioId: string, month: string) =>
    request<DividendCalendarMonthResponse>(`/portfolios/${portfolioId}/dividends/calendar?month=${encodeURIComponent(month)}`),
  getPortfolioEventInbox: (
    portfolioId: string,
    params?: {
      lookaheadDays?: number;
      status?: 'all' | 'read' | 'unread' | string;
      severity?: 'low' | 'medium' | 'high' | string;
      limit?: number;
      sync?: boolean;
      refreshSources?: boolean;
    }
  ) => {
    const query = new URLSearchParams();
    if (typeof params?.lookaheadDays === 'number' && Number.isFinite(params.lookaheadDays) && params.lookaheadDays > 0) {
      query.set('lookaheadDays', String(Math.round(params.lookaheadDays)));
    }
    if (params?.status) query.set('status', params.status);
    if (params?.severity) query.set('severity', params.severity);
    if (typeof params?.limit === 'number' && Number.isFinite(params.limit) && params.limit > 0) {
      query.set('limit', String(Math.round(params.limit)));
    }
    if (typeof params?.sync === 'boolean') query.set('sync', params.sync ? 'true' : 'false');
    if (typeof params?.refreshSources === 'boolean') {
      query.set('refreshSources', params.refreshSources ? 'true' : 'false');
    }
    const suffix = query.toString() ? `?${query.toString()}` : '';
    return request<PortfolioEventNoticesResponse>(
      `/portfolios/${encodeURIComponent(portfolioId)}/event-inbox${suffix}`
    );
  },
  syncPortfolioEventInbox: (
    portfolioId: string,
    params?: {
      lookaheadDays?: number;
      includePastDays?: number;
      pruneDaysPast?: number;
      refreshSources?: boolean;
    }
  ) =>
    request<SyncPortfolioEventInboxResponse>(
      `/portfolios/${encodeURIComponent(portfolioId)}/event-inbox/sync`,
      {
        method: 'POST',
        body: JSON.stringify({
          lookaheadDays: params?.lookaheadDays ?? 7,
          includePastDays: params?.includePastDays ?? null,
          pruneDaysPast: params?.pruneDaysPast ?? null,
          refreshSources: params?.refreshSources ?? true,
        }),
      }
    ),
  setPortfolioEventInboxRead: (portfolioId: string, eventId: string, read: boolean) =>
    request<{ portfolioId: string; id: string; read: boolean; readAt?: string | null; updatedAt?: string }>(
      `/portfolios/${encodeURIComponent(portfolioId)}/event-inbox/${encodeURIComponent(eventId)}`,
      {
        method: 'PUT',
        body: JSON.stringify({ read }),
      }
    ),
  markAllPortfolioEventInboxRead: (
    portfolioId: string,
    payload?: { read?: boolean; scope?: 'all' | 'today' | 'week' | 'unread' | string; lookaheadDays?: number }
  ) =>
    request<{ portfolioId: string; scope: string; read: boolean; updated_count: number; updatedAt?: string }>(
      `/portfolios/${encodeURIComponent(portfolioId)}/event-inbox/read`,
      {
        method: 'POST',
        body: JSON.stringify({
          read: payload?.read ?? true,
          scope: payload?.scope ?? 'all',
          lookaheadDays: payload?.lookaheadDays ?? 7,
        }),
      }
    ),
  getPortfolioEventNotices: (portfolioId: string, lookaheadDays = 7) =>
    request<PortfolioEventNoticesResponse>(
      `/portfolios/${encodeURIComponent(portfolioId)}/event-notices?lookaheadDays=${encodeURIComponent(String(lookaheadDays))}`
    ),
  getTaxReport: (portfolioId: string, year: number) =>
    request<TaxReportResponse>(`/portfolios/${portfolioId}/tax?year=${encodeURIComponent(String(year))}`),
  getRebalanceTargets: (portfolioId: string) =>
    request<RebalanceTargetsResponse>(`/portfolios/${portfolioId}/rebalance/targets`),
  getRebalanceSuggestion: (
    portfolioId: string,
    amount: number,
    scope = 'assetClass',
    options?: { amountBrl?: number; amountUsd?: number }
  ) => {
    const query = new URLSearchParams();
    query.set('scope', scope);
    const hasSplitContribution =
      typeof options?.amountBrl === 'number' || typeof options?.amountUsd === 'number';
    if (hasSplitContribution) {
      if (typeof options?.amountBrl === 'number' && Number.isFinite(options.amountBrl)) {
        query.set('amountBrl', String(options.amountBrl));
      }
      if (typeof options?.amountUsd === 'number' && Number.isFinite(options.amountUsd)) {
        query.set('amountUsd', String(options.amountUsd));
      }
    } else {
      query.set('amount', String(amount));
    }
    return request<RebalanceSuggestionResponse>(
      `/portfolios/${portfolioId}/rebalance/suggestion?${query.toString()}`
    );
  },
  setRebalanceTargets: (portfolioId: string, targets: RebalanceTarget[]) =>
    request<RebalanceTargetsResponse>(`/portfolios/${portfolioId}/rebalance/targets`, { method: 'POST', body: JSON.stringify({ targets }) }),
  getTheses: (portfolioId: string, options?: { includeHistory?: boolean }) => {
    const query = new URLSearchParams();
    if (options?.includeHistory) query.set('includeHistory', 'true');
    const suffix = query.toString() ? `?${query.toString()}` : '';
    return request<ThesisListResponse>(`/portfolios/${encodeURIComponent(portfolioId)}/theses${suffix}`);
  },
  getThesis: (portfolioId: string, scopeKey: string) =>
    request<ThesisDetailResponse>(
      `/portfolios/${encodeURIComponent(portfolioId)}/theses/${encodeURIComponent(scopeKey)}`
    ),
  upsertThesis: (portfolioId: string, data: ThesisUpsertPayload) =>
    request<{ portfolioId: string; thesis: ThesisRecord; previous: ThesisRecord | null }>(
      `/portfolios/${encodeURIComponent(portfolioId)}/theses`,
      { method: 'POST', body: JSON.stringify(data) }
    ),
  archiveThesis: (portfolioId: string, scopeKey: string) =>
    request<{ portfolioId: string; scopeKey: string; thesis: ThesisRecord }>(
      `/portfolios/${encodeURIComponent(portfolioId)}/theses/${encodeURIComponent(scopeKey)}`,
      { method: 'DELETE' }
    ),
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
  combineReports: (reportIds: string[], locale?: string) =>
    request<ReportContentResponse>('/reports/combine', {
      method: 'POST',
      body: JSON.stringify({
        reportIds,
        locale: locale || null,
      }),
    }),
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
