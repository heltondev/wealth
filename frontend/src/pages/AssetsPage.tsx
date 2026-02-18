import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
import FormModal from '../components/FormModal';
import ExpandableText from '../components/ExpandableText';
import {
  api,
  type Asset,
  type DividendsResponse,
  type Transaction,
  type DropdownConfigMap,
} from '../services/api';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import { useToast } from '../context/ToastContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './AssetsPage.scss';

type AssetRow = Asset & { quantity: number; source: string | null; investedAmount: number };
type AssetTradeHistoryRow = {
  transId: string;
  date: string;
  type: 'buy' | 'sell';
  quantity: number;
  price: number;
  amount: number;
  currency: string;
  source: string | null;
};
type AssetTradeHistoryPoint = AssetTradeHistoryRow & {
  x: number;
  y: number;
  index: number;
};
type AssetProventEvent = {
  id: string;
  ticker: string;
  eventDate: string;
  exDate: string | null;
  paymentDate: string | null;
  eventType: string | null;
  amountPerUnit: number | null;
  status: 'paid' | 'provisioned';
  source: string | null;
  sourceUrl: string | null;
};
type AssetProventSummary = {
  events: AssetProventEvent[];
  nextPaymentDate: string | null;
  nextExDate: string | null;
  nextAmountPerUnit: number | null;
  lastPaymentDate: string | null;
  lastAmountPerUnit: number | null;
  paidCount12m: number;
  paidPerUnit12m: number;
  sources: string[];
};
type AssetInsightsSnapshot = {
  status: 'loading' | 'ready' | 'error';
  source: string | null;
  fetchedAt: string | null;
  sector: string | null;
  industry: string | null;
  marketCap: number | null;
  averageVolume: number | null;
  currentPrice: number | null;
  fairPrice: number | null;
  marginOfSafetyPct: number | null;
  pe: number | null;
  pb: number | null;
  roe: number | null;
  payout: number | null;
  evEbitda: number | null;
  netMargin: number | null;
  statusInvestUrl: string | null;
  b3Url: string | null;
  clubeFiiUrl: string | null;
  fiisUrl: string | null;
  errorMessage: string | null;
};

const COUNTRY_FLAG_MAP: Record<string, string> = {
  BR: '🇧🇷',
  US: '🇺🇸',
  CA: '🇨🇦',
};
const COUNTRY_NAME_MAP: Record<string, string> = {
  BR: 'Brazil',
  US: 'United States',
  CA: 'Canada',
};
const DECIMAL_PRECISION = 2;
const DECIMAL_FACTOR = 10 ** DECIMAL_PRECISION;
const DEFAULT_ITEMS_PER_PAGE = 10;
const HISTORY_CHART_WIDTH = 860;
const HISTORY_CHART_HEIGHT = 220;
const PERCENT_DISPLAY_PRECISION = 2;

const toPageSizeOptions = (options: { value: string }[]): number[] => {
  const values = new Set<number>();

  for (const option of options) {
    const numeric = Number(option.value);
    if (!Number.isFinite(numeric) || numeric <= 0) continue;
    values.add(Math.round(numeric));
  }

  return Array.from(values).sort((left, right) => left - right);
};

const ensureSelectedValue = (current: string, options: { value: string }[]): string => {
  if (options.some((option) => option.value === current)) return current;
  return options[0]?.value || '';
};

const normalizeText = (value: unknown): string =>
  (value || '')
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();

const normalizeAssetClassKey = (value: unknown): string => {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized || 'unknown';
};

const summarizeSourceValue = (value: unknown): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (normalized.includes('NUBANK') || normalized.includes('NU INVEST') || normalized.includes('NU BANK')) return 'NU BANK';
  if (normalized.includes('XP')) return 'XP';
  if (normalized.includes('ITAU')) return 'ITAU';
  if (normalized.includes('B3')) return 'B3';
  return null;
};

const getLocalIsoDate = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const normalizeIsoDate = (value: unknown): string | null => {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const toNumericValue = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toObjectRecord = (value: unknown): Record<string, unknown> => (
  value && typeof value === 'object' ? value as Record<string, unknown> : {}
);

const toNonEmptyString = (value: unknown): string | null => {
  const text = String(value || '').trim();
  return text || null;
};

const firstFiniteNumber = (...values: unknown[]): number | null => {
  for (const candidate of values) {
    const numeric = toNumericValue(candidate);
    if (numeric !== null) return numeric;
  }
  return null;
};

const toTickerSlug = (ticker: string): string => (
  String(ticker || '')
    .toLowerCase()
    .replace(/\.sa$/i, '')
    .replace(/[^a-z0-9]/g, '')
);

const buildAssetExternalLinks = (ticker: string, assetClass: string) => {
  const normalizedTicker = String(ticker || '').toUpperCase();
  const slug = toTickerSlug(normalizedTicker);
  const isFii = String(assetClass || '').toLowerCase() === 'fii';

  return {
    statusInvestUrl: slug
      ? `https://statusinvest.com.br/${isFii ? 'fundos-imobiliarios' : 'acoes'}/${slug}`
      : null,
    b3Url: isFii
      ? 'https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/renda-variavel/fundos-de-investimentos/fii/fiis-listados/'
      : null,
    clubeFiiUrl: isFii && slug ? `https://www.clubefii.com.br/fii/${slug}` : null,
    fiisUrl: isFii && slug ? `https://fiis.com.br/${slug}/` : null,
  };
};

const createEmptyInsightsSnapshot = (
  status: AssetInsightsSnapshot['status'],
  ticker: string,
  assetClass: string,
): AssetInsightsSnapshot => ({
  status,
  source: null,
  fetchedAt: null,
  sector: null,
  industry: null,
  marketCap: null,
  averageVolume: null,
  currentPrice: null,
  fairPrice: null,
  marginOfSafetyPct: null,
  pe: null,
  pb: null,
  roe: null,
  payout: null,
  evEbitda: null,
  netMargin: null,
  ...buildAssetExternalLinks(ticker, assetClass),
  errorMessage: null,
});

const dateMonthsBack = (months: number) => {
  const date = new Date();
  date.setDate(1);
  date.setMonth(date.getMonth() - months + 1);
  return date.toISOString().slice(0, 10);
};

const AssetsPage = () => {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const portfolioIdFromQuery = searchParams.get('portfolioId')?.trim() || '';
  const tickerFromQuery = searchParams.get('ticker')?.trim().toUpperCase() || '';
  const { showToast } = useToast();
  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    assets: contextAssets,
    transactions,
    loading,
    metrics,
  } = usePortfolioData();
  const [localAssetAdds, setLocalAssetAdds] = useState<Asset[]>([]);
  const [localAssetDeletes, setLocalAssetDeletes] = useState<Set<string>>(new Set());
  const assets = useMemo(() => {
    const filtered = contextAssets.filter((a) => !localAssetDeletes.has(a.assetId));
    return [...filtered, ...localAssetAdds];
  }, [contextAssets, localAssetAdds, localAssetDeletes]);

  // Reset local mutations when context assets update (fresh data from server).
  useEffect(() => {
    setLocalAssetAdds([]);
    setLocalAssetDeletes(new Set());
  }, [contextAssets]);

  const [showModal, setShowModal] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [itemsPerPage, setItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);
  const [selectedAsset, setSelectedAsset] = useState<AssetRow | null>(null);
  const currentQuotesByAssetId = useMemo(() => metrics?.currentQuotes || {}, [metrics]);
  const averageCostByAssetId = useMemo(() => metrics?.averageCosts || {}, [metrics]);
  const portfolioMarketValueByAssetId = useMemo(() => metrics?.marketValues || {}, [metrics]);
  const [selectedHistoryPoint, setSelectedHistoryPoint] = useState<AssetTradeHistoryPoint | null>(null);
  const [assetInsightsByTicker, setAssetInsightsByTicker] = useState<Record<string, AssetInsightsSnapshot>>({});
  const assetInsightsByTickerRef = useRef<Record<string, AssetInsightsSnapshot>>({});
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [dividendsPayload, setDividendsPayload] = useState<DividendsResponse | null>(null);
  const [form, setForm] = useState<{
    ticker: string;
    name: string;
    assetClass: string;
    country: string;
    currency: string;
  }>({
    ticker: '',
    name: '',
    assetClass: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.assetClass')[0]?.value || 'stock',
    country: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.country')[0]?.value || 'BR',
    currency: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.currency')[0]?.value || 'BRL',
  });

  // Sync portfolio selection from URL query if present.
  useEffect(() => {
    if (portfolioIdFromQuery && portfolios.length > 0) {
      const match = portfolios.find((item) => item.portfolioId === portfolioIdFromQuery);
      if (match && match.portfolioId !== selectedPortfolio) {
        setSelectedPortfolio(match.portfolioId);
      }
    }
  }, [portfolioIdFromQuery, portfolios, selectedPortfolio, setSelectedPortfolio]);

  useEffect(() => {
    if (!tickerFromQuery) return;
    setSearchTerm((current) => (current === tickerFromQuery ? current : tickerFromQuery));
    setStatusFilter('active');
  }, [tickerFromQuery]);

  useEffect(() => {
    api.getDropdownSettings()
      .then((dropdownSettings) => {
        setDropdownConfig(normalizeDropdownConfig(dropdownSettings.dropdowns));
      })
      .catch(() => {
        setDropdownConfig(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      });
  }, []);

  useEffect(() => {
    setSelectedHistoryPoint(null);
  }, [selectedPortfolio]);

  useEffect(() => {
    assetInsightsByTickerRef.current = assetInsightsByTicker;
  }, [assetInsightsByTicker]);

  useEffect(() => {
    setAssetInsightsByTicker({});
    assetInsightsByTickerRef.current = {};
  }, [selectedPortfolio]);

  useEffect(() => {
    if (!selectedPortfolio) {
      setDividendsPayload(null);
      return;
    }

    let cancelled = false;
    api.getDividends(selectedPortfolio, { periodMonths: 24 })
      .then((response) => {
        if (cancelled) return;
        setDividendsPayload(response);
      })
      .catch(() => {
        if (cancelled) return;
        setDividendsPayload(null);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedPortfolio]);

  const buildInsightsSnapshot = useCallback((asset: AssetRow, detailsPayload: unknown, fairPayload: unknown): AssetInsightsSnapshot => {
    const details = toObjectRecord(detailsPayload);
    const fair = toObjectRecord(fairPayload);
    const detail = toObjectRecord(details.detail);
    const quote = toObjectRecord(detail.quote);
    const fundamentals = toObjectRecord(detail.fundamentals);
    const raw = toObjectRecord(detail.raw);
    const finalPayload = toObjectRecord(raw.final_payload);
    const primaryPayload = toObjectRecord(raw.primary_payload);
    const finalInfo = toObjectRecord(finalPayload.info);
    const primaryInfo = toObjectRecord(primaryPayload.info);
    const latestPrice = toObjectRecord(details.latest_price);
    const fairFundamentals = toObjectRecord(fair.fundamentals);

    return {
      ...createEmptyInsightsSnapshot('ready', asset.ticker, asset.assetClass),
      status: 'ready',
      source: toNonEmptyString(detail.data_source) || toNonEmptyString(latestPrice.source),
      fetchedAt: toNonEmptyString(details.fetched_at) || toNonEmptyString(detail.fetched_at) || toNonEmptyString(fair.fetched_at),
      sector: toNonEmptyString(finalInfo.sector) || toNonEmptyString(primaryInfo.sector) || toNonEmptyString(fundamentals.sector),
      industry: toNonEmptyString(finalInfo.industry)
        || toNonEmptyString(finalInfo.segment)
        || toNonEmptyString(primaryInfo.industry)
        || toNonEmptyString(primaryInfo.segment)
        || toNonEmptyString(fundamentals.industry),
      marketCap: firstFiniteNumber(finalInfo.marketCap, primaryInfo.marketCap, quote.marketCap, latestPrice.marketCap),
      averageVolume: firstFiniteNumber(
        finalInfo.averageVolume,
        finalInfo.averageDailyVolume10Day,
        primaryInfo.averageVolume,
        primaryInfo.averageDailyVolume10Day,
        quote.volume,
        latestPrice.volume,
      ),
      currentPrice: firstFiniteNumber(fair.current_price, latestPrice.close, quote.currentPrice, asset.currentPrice),
      fairPrice: firstFiniteNumber(fair.fair_price),
      marginOfSafetyPct: firstFiniteNumber(fair.margin_of_safety_pct),
      pe: firstFiniteNumber(
        fairFundamentals.pe,
        finalInfo.trailingPE,
        finalInfo.pe,
        primaryInfo.trailingPE,
        primaryInfo.pe,
        fundamentals.pe,
      ),
      pb: firstFiniteNumber(
        fairFundamentals.pb,
        finalInfo.priceToBook,
        finalInfo.pvp,
        primaryInfo.priceToBook,
        primaryInfo.pvp,
        fundamentals.pb,
      ),
      roe: firstFiniteNumber(
        fairFundamentals.roe,
        finalInfo.returnOnEquity,
        finalInfo.roe,
        primaryInfo.returnOnEquity,
        primaryInfo.roe,
        fundamentals.roe,
      ),
      payout: firstFiniteNumber(
        fairFundamentals.payout,
        finalInfo.payoutRatio,
        finalInfo.payout,
        primaryInfo.payoutRatio,
        primaryInfo.payout,
        fundamentals.payout,
      ),
      evEbitda: firstFiniteNumber(
        fairFundamentals.evEbitda,
        finalInfo.enterpriseToEbitda,
        primaryInfo.enterpriseToEbitda,
        fundamentals.evEbitda,
      ),
      netMargin: firstFiniteNumber(
        fairFundamentals.netMargin,
        finalInfo.profitMargins,
        finalInfo.netMargin,
        primaryInfo.profitMargins,
        primaryInfo.netMargin,
        fundamentals.netMargin,
      ),
      errorMessage: null,
    };
  }, []);

  useEffect(() => {
    if (!selectedAsset || !selectedPortfolio) return;
    const tickerKey = String(selectedAsset.ticker || '').trim().toUpperCase();
    if (!tickerKey) return;

    const cached = assetInsightsByTickerRef.current[tickerKey];
    if (cached && (cached.status === 'loading' || cached.status === 'ready')) return;

    let cancelled = false;
    setAssetInsightsByTicker((previous) => ({
      ...previous,
      [tickerKey]: {
        ...(previous[tickerKey] || createEmptyInsightsSnapshot('loading', selectedAsset.ticker, selectedAsset.assetClass)),
        status: 'loading',
        errorMessage: null,
      },
    }));

    const loadPayloads = async () => {
      const [details, fair] = await Promise.all([
        api.getAssetDetails(tickerKey, selectedPortfolio),
        api.getAssetFairPrice(tickerKey, selectedPortfolio),
      ]);
      return { details, fair };
    };

    loadPayloads()
      .then(async (initialPayloads) => {
        let detailsPayload = initialPayloads.details;
        let fairPayload = initialPayloads.fair;
        const detailsRecord = toObjectRecord(detailsPayload);

        if (detailsRecord.detail == null) {
          await api.refreshMarketData(selectedPortfolio, selectedAsset.assetId).catch(() => null);
          try {
            const refreshedPayloads = await loadPayloads();
            detailsPayload = refreshedPayloads.details;
            fairPayload = refreshedPayloads.fair;
          } catch {
            // Keep the initial payload when the refresh fetch fails.
          }
        }

        if (cancelled) return;
        setAssetInsightsByTicker((previous) => ({
          ...previous,
          [tickerKey]: buildInsightsSnapshot(selectedAsset, detailsPayload, fairPayload),
        }));
      })
      .catch((error) => {
        if (cancelled) return;
        const fallback = createEmptyInsightsSnapshot('error', selectedAsset.ticker, selectedAsset.assetClass);
        fallback.errorMessage = error instanceof Error ? error.message : null;
        setAssetInsightsByTicker((previous) => ({
          ...previous,
          [tickerKey]: fallback,
        }));
      });

    return () => {
      cancelled = true;
    };
  }, [
    buildInsightsSnapshot,
    selectedAsset,
    selectedPortfolio,
  ]);

  const assetClassOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.form.assetClass');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.assetClass');
  }, [dropdownConfig]);

  const countryOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.form.country');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.country');
  }, [dropdownConfig]);

  const currencyOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.form.currency');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.currency');
  }, [dropdownConfig]);

  const statusFilterOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.filters.status');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.filters.status');
  }, [dropdownConfig]);

  const pageSizeOptions = useMemo(() => {
    const configuredOptions = toPageSizeOptions(
      getDropdownOptions(dropdownConfig, 'tables.pagination.itemsPerPage')
    );
    if (configuredOptions.length > 0) return configuredOptions;
    return toPageSizeOptions(
      getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'tables.pagination.itemsPerPage')
    );
  }, [dropdownConfig]);

  useEffect(() => {
    setForm((previous) => {
      const next = {
        ...previous,
        assetClass: ensureSelectedValue(previous.assetClass, assetClassOptions),
        country: ensureSelectedValue(previous.country, countryOptions),
        currency: ensureSelectedValue(previous.currency, currencyOptions),
      };
      if (
        next.assetClass === previous.assetClass
        && next.country === previous.country
        && next.currency === previous.currency
      ) {
        return previous;
      }
      return next;
    });
  }, [assetClassOptions, countryOptions, currencyOptions]);

  useEffect(() => {
    if (statusFilterOptions.some((option) => option.value === statusFilter)) return;
    setStatusFilter(statusFilterOptions[0]?.value || 'all');
  }, [statusFilter, statusFilterOptions]);

  useEffect(() => {
    if (pageSizeOptions.includes(itemsPerPage)) return;
    setItemsPerPage(pageSizeOptions[0] || DEFAULT_ITEMS_PER_PAGE);
  }, [itemsPerPage, pageSizeOptions]);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!selectedPortfolio) return;

    try {
      const newAsset = await api.createAsset(selectedPortfolio, form);
      setLocalAssetAdds((previous) => [...previous, newAsset]);
      setShowModal(false);
      setForm({
        ticker: '',
        name: '',
        assetClass: assetClassOptions[0]?.value || '',
        country: countryOptions[0]?.value || '',
        currency: currencyOptions[0]?.value || '',
      });
      showToast('Asset added', 'success');
    } catch {
      showToast('Failed to add asset', 'error');
    }
  };

  const handleDelete = async (assetId: string) => {
    if (!selectedPortfolio) return;
    try {
      await api.deleteAsset(selectedPortfolio, assetId);
      setLocalAssetDeletes((previous) => new Set([...previous, assetId]));
      setLocalAssetAdds((previous) => previous.filter((asset) => asset.assetId !== assetId));
      showToast('Asset deleted', 'success');
    } catch {
      showToast('Failed to delete asset', 'error');
    }
  };

  const openAssetDetails = useCallback((asset: AssetRow) => {
    const params = new URLSearchParams();
    const effectivePortfolioId = selectedPortfolio || asset.portfolioId || '';
    if (effectivePortfolioId) params.set('portfolioId', effectivePortfolioId);
    const queryString = params.toString();
    const path = queryString
      ? `/assets/${asset.assetId}?${queryString}`
      : `/assets/${asset.assetId}`;
    navigate(path);
  }, [navigate, selectedPortfolio]);

  const formatCountryFlag = useCallback((country: string) =>
    COUNTRY_FLAG_MAP[country] || '🏳️', []);

  const formatCountryDetail = useCallback((country: string) =>
    `${formatCountryFlag(country)} ${COUNTRY_NAME_MAP[country] || country}`, [formatCountryFlag]);

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('assets.modal.noValue');
    return String(value);
  }, [t]);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const formatAssetQuantity = useCallback((value: unknown) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return formatDetailValue(value);

    const hasFraction = Math.abs(numeric % 1) > Number.EPSILON;
    return numeric.toLocaleString(numberLocale, {
      minimumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
      maximumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
    });
  }, [formatDetailValue, numberLocale]);

  const formatSignedCurrency = useCallback((value: number, currency: string) => {
    const absolute = formatCurrency(Math.abs(value), currency, numberLocale);
    if (Math.abs(value) <= Number.EPSILON) return absolute;
    return `${value > 0 ? '+' : '-'}${absolute}`;
  }, [numberLocale]);

  const hasPrimaryTradeByAssetId = useMemo(() => {
    const set = new Set<string>();

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      if (normalizedType !== 'buy' && normalizedType !== 'sell' && normalizedType !== 'subscription') {
        continue;
      }

      const sourceTag = normalizeText(transaction.sourceDocId);
      if (!sourceTag.includes('B3-NEGOCIACAO')) continue;
      set.add(transaction.assetId);
    }

    return set;
  }, [transactions]);

  const shouldIgnoreConsolidatedTrade = useCallback((transaction: Transaction) => {
    const normalizedType = transaction.type?.toLowerCase() || '';
    if (normalizedType !== 'buy' && normalizedType !== 'sell' && normalizedType !== 'subscription') {
      return false;
    }

    const sourceTag = normalizeText(transaction.sourceDocId);
    if (!sourceTag.includes('B3-RELATORIO')) return false;

    return hasPrimaryTradeByAssetId.has(transaction.assetId);
  }, [hasPrimaryTradeByAssetId]);

  const assetQuantitiesById = useMemo(() => {
    const quantities: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;
      if (shouldIgnoreConsolidatedTrade(transaction)) continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      const normalizedQuantity = Math.round(Number(transaction.quantity || 0) * DECIMAL_FACTOR) / DECIMAL_FACTOR;

      if (!Number.isFinite(normalizedQuantity)) continue;

      if (normalizedType === 'buy' || normalizedType === 'subscription') {
        quantities[transaction.assetId] = (quantities[transaction.assetId] || 0) + normalizedQuantity;
        continue;
      }

      if (normalizedType === 'sell') {
        quantities[transaction.assetId] = (quantities[transaction.assetId] || 0) - normalizedQuantity;
      }
    }

    return quantities;
  }, [shouldIgnoreConsolidatedTrade, transactions]);

  const assetInvestedAmountById = useMemo(() => {
    const investedById: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;
      if (shouldIgnoreConsolidatedTrade(transaction)) continue;

      const amount = Number(transaction.amount || 0);
      if (!Number.isFinite(amount)) continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      if (normalizedType === 'buy' || normalizedType === 'subscription') {
        investedById[transaction.assetId] = (investedById[transaction.assetId] || 0) + amount;
        continue;
      }

      if (normalizedType === 'sell') {
        investedById[transaction.assetId] = (investedById[transaction.assetId] || 0) - amount;
      }
    }

    return investedById;
  }, [shouldIgnoreConsolidatedTrade, transactions]);

  const assetSourcesById = useMemo(() => {
    const sources: Record<string, string[]> = {};

    for (const transaction of transactions) {
      const sourceDocId = transaction.sourceDocId?.toString().trim();
      const institution = transaction.institution?.toString().trim();

      if (sourceDocId) {
        sources[transaction.assetId] = [...(sources[transaction.assetId] || []), sourceDocId];
      }

      if (institution) {
        sources[transaction.assetId] = [...(sources[transaction.assetId] || []), institution];
      }
    }

    return sources;
  }, [transactions]);

  const assetRows = useMemo<AssetRow[]>(() => {
    return assets.map((asset) => ({
      ...asset,
      quantity: Number.isFinite(Number(asset.quantity))
        ? Number(asset.quantity)
        : (assetQuantitiesById[asset.assetId] || 0),
      source: (() => {
        const labels = new Set<string>();
        const assetSource = summarizeSourceValue(asset.source);
        if (assetSource) labels.add(assetSource);
        for (const candidate of (assetSourcesById[asset.assetId] || [])) {
          const label = summarizeSourceValue(candidate);
          if (label) labels.add(label);
        }
        if (labels.size > 0) return Array.from(labels).join(', ');
        return null;
      })(),
      investedAmount: assetInvestedAmountById[asset.assetId] || 0,
    }));
  }, [assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

  const proventSummaryByTicker = useMemo(() => {
    const summaries = new Map<string, AssetProventSummary>();
    const calendar = Array.isArray(dividendsPayload?.calendar) ? dividendsPayload.calendar : [];
    if (calendar.length === 0) return summaries;

    const todayIso = getLocalIsoDate();
    const trailing12mStart = dateMonthsBack(12);
    const eventsByTicker = new Map<string, AssetProventEvent[]>();

    calendar.forEach((rawEvent, index) => {
      const eventRecord = (rawEvent && typeof rawEvent === 'object')
        ? (rawEvent as Record<string, unknown>)
        : {};
      const details = (eventRecord.details && typeof eventRecord.details === 'object')
        ? (eventRecord.details as Record<string, unknown>)
        : {};
      const ticker = String(eventRecord.ticker || details.ticker || '').trim().toUpperCase();
      if (!ticker) return;

      const eventDate = normalizeIsoDate(
        eventRecord.eventDate
        || eventRecord.date
        || details.paymentDate
        || details.exDate
      );
      if (!eventDate) return;

      const exDate = normalizeIsoDate(details.exDate || details.recordDate || details.dataCom);
      const paymentDate = normalizeIsoDate(details.paymentDate) || eventDate;
      const amountPerUnit = toNumericValue(details.value);
      const eventType = String(eventRecord.eventType || details.rawType || details.type || '').trim() || null;
      const source = String(eventRecord.data_source || details.value_source || '').trim() || null;
      const sourceUrl = String(details.url || '').trim() || null;
      const eventId = String(eventRecord.eventId || `${ticker}-${eventDate}-${eventType || index}`);
      const status: 'paid' | 'provisioned' = eventDate < todayIso ? 'paid' : 'provisioned';

      const normalizedEvent: AssetProventEvent = {
        id: eventId,
        ticker,
        eventDate,
        exDate,
        paymentDate,
        eventType,
        amountPerUnit,
        status,
        source,
        sourceUrl,
      };
      const bucket = eventsByTicker.get(ticker) || [];
      bucket.push(normalizedEvent);
      eventsByTicker.set(ticker, bucket);
    });

    for (const [ticker, events] of eventsByTicker.entries()) {
      const ascendingEvents = [...events].sort((left, right) => (
        left.eventDate.localeCompare(right.eventDate)
      ));
      const paidEvents = ascendingEvents.filter((event) => event.status === 'paid');
      const paidLast12m = paidEvents.filter((event) => event.eventDate >= trailing12mStart);
      const upcomingEvents = ascendingEvents.filter((event) => event.status === 'provisioned');
      const nextPayment = upcomingEvents[0] || null;
      const lastPayment = paidEvents.length > 0 ? paidEvents[paidEvents.length - 1] : null;
      const paidPerUnit12m = paidLast12m.reduce((sum, event) => (
        sum + (event.amountPerUnit !== null ? event.amountPerUnit : 0)
      ), 0);
      const sources = Array.from(
        new Set(
          ascendingEvents
            .map((event) => event.source)
            .filter((value): value is string => Boolean(value))
        )
      );

      summaries.set(ticker, {
        events: [...ascendingEvents].sort((left, right) => (
          right.eventDate.localeCompare(left.eventDate)
        )),
        nextPaymentDate: nextPayment?.eventDate || null,
        nextExDate: nextPayment?.exDate || null,
        nextAmountPerUnit: nextPayment?.amountPerUnit ?? null,
        lastPaymentDate: lastPayment?.eventDate || null,
        lastAmountPerUnit: lastPayment?.amountPerUnit ?? null,
        paidCount12m: paidLast12m.length,
        paidPerUnit12m,
        sources,
      });
    }

    return summaries;
  }, [dividendsPayload?.calendar]);

  const formatPercent = useCallback((ratio: number) => (
    `${(ratio * 100).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}%`
  ), [numberLocale]);

  const formatSignedPercent = useCallback((ratio: number) => {
    const absolute = Math.abs(ratio * 100).toLocaleString(numberLocale, {
      minimumFractionDigits: PERCENT_DISPLAY_PRECISION,
      maximumFractionDigits: PERCENT_DISPLAY_PRECISION,
    });
    if (Math.abs(ratio) <= Number.EPSILON) return `${absolute}%`;
    return `${ratio > 0 ? '+' : '-'}${absolute}%`;
  }, [numberLocale]);

  const resolveAssetCurrentPrice = useCallback((asset: AssetRow): number | null => {
    const quantity = Number(asset.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const cachedCurrentPrice = currentQuotesByAssetId[asset.assetId];
    const directCurrentPrice = Number(asset.currentPrice);

    if (
      typeof cachedCurrentPrice === 'number'
      && Number.isFinite(cachedCurrentPrice)
      && (!hasOpenPosition || Math.abs(cachedCurrentPrice) > Number.EPSILON)
    ) {
      return cachedCurrentPrice;
    }

    if (
      Number.isFinite(directCurrentPrice)
      && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON)
    ) {
      return directCurrentPrice;
    }

    const directCurrentValue = Number(asset.currentValue);
    if (
      Number.isFinite(directCurrentValue)
      && Number.isFinite(quantity)
      && Math.abs(quantity) > Number.EPSILON
      && (!hasOpenPosition || Math.abs(directCurrentValue) > Number.EPSILON)
    ) {
      return directCurrentValue / quantity;
    }

    return null;
  }, [currentQuotesByAssetId]);

  const resolveAssetCurrentValue = useCallback((asset: AssetRow): number | null => {
    const metricCurrentValue = portfolioMarketValueByAssetId[asset.assetId];
    if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
      return metricCurrentValue;
    }

    const quantity = Number(asset.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const cachedCurrentPrice = currentQuotesByAssetId[asset.assetId];
    const directCurrentPrice = Number(asset.currentPrice);
    const resolvedCurrentPrice =
      (typeof cachedCurrentPrice === 'number'
        && Number.isFinite(cachedCurrentPrice)
        && (!hasOpenPosition || Math.abs(cachedCurrentPrice) > Number.EPSILON))
        ? cachedCurrentPrice
        : (Number.isFinite(directCurrentPrice)
          && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON))
          ? directCurrentPrice
          : null;

    if (resolvedCurrentPrice !== null && Number.isFinite(quantity)) {
      return resolvedCurrentPrice * quantity;
    }

    const directCurrentValue = Number(asset.currentValue);
    if (
      Number.isFinite(directCurrentValue)
      && (!hasOpenPosition || Math.abs(directCurrentValue) > Number.EPSILON)
    ) {
      return directCurrentValue;
    }

    return null;
  }, [currentQuotesByAssetId, portfolioMarketValueByAssetId]);

  const resolveAssetAverageCost = useCallback((asset: AssetRow): number | null => {
    const cachedAverageCost = averageCostByAssetId[asset.assetId];
    if (typeof cachedAverageCost === 'number' && Number.isFinite(cachedAverageCost)) {
      return cachedAverageCost;
    }

    const quantity = Number(asset.quantity);
    const investedAmount = Number(asset.investedAmount);
    if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return investedAmount / quantity;
  }, [averageCostByAssetId]);

  const currentValueByAssetId = useMemo(() => {
    const values: Record<string, number | null> = {};
    for (const row of assetRows) {
      const currentValue = resolveAssetCurrentValue(row);
      values[row.assetId] = Number.isFinite(currentValue) ? Number(currentValue) : null;
    }
    return values;
  }, [assetRows, resolveAssetCurrentValue]);

  const priceVariationByAssetId = useMemo(() => {
    const variations: Record<string, { ratio: number | null; trend: 'positive' | 'negative' | 'neutral' | 'unknown' }> = {};

    for (const row of assetRows) {
      const averageCost = resolveAssetAverageCost(row);
      const currentPrice = resolveAssetCurrentPrice(row);

      if (
        averageCost === null
        || currentPrice === null
        || !Number.isFinite(averageCost)
        || !Number.isFinite(currentPrice)
        || Math.abs(averageCost) <= Number.EPSILON
      ) {
        variations[row.assetId] = { ratio: null, trend: 'unknown' };
        continue;
      }

      const ratio = (currentPrice - averageCost) / Math.abs(averageCost);
      const trend = Math.abs(ratio) <= Number.EPSILON
        ? 'neutral'
        : ratio > 0
          ? 'positive'
          : 'negative';

      variations[row.assetId] = { ratio, trend };
    }

    return variations;
  }, [assetRows, resolveAssetAverageCost, resolveAssetCurrentPrice]);

  const portfolioCurrentTotal = useMemo<number>(() => (
    Object.values(currentValueByAssetId).reduce<number>((sum, value) => (
      typeof value === 'number' && Number.isFinite(value) ? sum + value : sum
    ), 0)
  ), [currentValueByAssetId]);

  const portfolioInvestedTotal = useMemo<number>(() => (
    assetRows.reduce((sum, row) => {
      const investedAmount = Number(row.investedAmount);
      return Number.isFinite(investedAmount) ? sum + investedAmount : sum;
    }, 0)
  ), [assetRows]);

  const columns: DataTableColumn<AssetRow>[] = [
    {
      key: 'ticker',
      label: t('assets.ticker'),
      sortable: true,
      sortValue: (asset) => asset.ticker,
      cellClassName: 'assets-page__cell--ticker',
      render: (asset) => {
        const trend = priceVariationByAssetId[asset.assetId]?.trend || 'unknown';
        return (
          <span className={`assets-page__ticker assets-page__ticker--${trend}`}>
            {asset.ticker}
          </span>
        );
      },
    },
    {
      key: 'name',
      label: t('assets.name'),
      sortable: true,
      sortValue: (asset) => asset.name,
      cellClassName: 'assets-page__cell--name',
      render: (asset) => (
        <span className="assets-page__name-ellipsis" title={asset.name}>
          {asset.name}
        </span>
      ),
    },
    {
      key: 'quantity',
      label: t('assets.quantity'),
      sortable: true,
      sortValue: (asset) => asset.quantity,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => formatAssetQuantity(asset.quantity),
    },
    {
      key: 'averagePrice',
      label: t('assets.averagePrice'),
      sortable: true,
      sortValue: (asset) => resolveAssetAverageCost(asset) ?? Number.NEGATIVE_INFINITY,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => {
        const averageCost = resolveAssetAverageCost(asset);
        if (averageCost === null) return t('assets.modal.noValue');
        return formatCurrency(averageCost, asset.currency || 'BRL', numberLocale);
      },
    },
    {
      key: 'priceVsAveragePct',
      label: t('assets.modal.priceHistory.changePct', { defaultValue: 'Change %' }),
      sortable: true,
      sortValue: (asset) => priceVariationByAssetId[asset.assetId]?.ratio ?? Number.NEGATIVE_INFINITY,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => {
        const variation = priceVariationByAssetId[asset.assetId];
        if (!variation || variation.ratio === null) return t('assets.modal.noValue');

        return (
          <span className={`assets-page__delta assets-page__delta--${variation.trend}`}>
            {formatSignedPercent(variation.ratio)}
          </span>
        );
      },
    },
    {
      key: 'balance',
      label: t('assets.balance'),
      sortable: true,
      sortValue: (asset) => currentValueByAssetId[asset.assetId] ?? Number.NEGATIVE_INFINITY,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => {
        const currentValue = currentValueByAssetId[asset.assetId];
        if (typeof currentValue !== 'number' || !Number.isFinite(currentValue)) return t('assets.modal.noValue');
        return formatCurrency(currentValue, asset.currency || 'BRL', numberLocale);
      },
    },
    {
      key: 'provents',
      label: t('assets.provents.column', { defaultValue: 'Provents' }),
      sortable: true,
      sortValue: (asset) => {
        const summary = proventSummaryByTicker.get(String(asset.ticker || '').toUpperCase());
        return summary?.nextPaymentDate || '';
      },
      render: (asset) => {
        const summary = proventSummaryByTicker.get(String(asset.ticker || '').toUpperCase());
        if (!summary) return t('assets.modal.noValue');

        const quantity = Number(asset.quantity || 0);
        const expectedNextCash = (
          Number.isFinite(quantity)
          && quantity > 0
          && summary.nextAmountPerUnit !== null
        )
          ? summary.nextAmountPerUnit * quantity
          : null;
        const expected12mCash = (
          Number.isFinite(quantity)
          && quantity > 0
          && summary.paidPerUnit12m > 0
        )
          ? summary.paidPerUnit12m * quantity
          : null;

        return (
          <div className="assets-page__provents-cell">
            <strong className="assets-page__provents-main">
              {summary.nextPaymentDate
                ? t('assets.provents.nextWithDate', {
                  date: formatDate(summary.nextPaymentDate, numberLocale),
                  defaultValue: `Next: ${formatDate(summary.nextPaymentDate, numberLocale)}`,
                })
                : t('assets.provents.noneUpcoming', { defaultValue: 'No upcoming' })}
            </strong>
            <span className="assets-page__provents-sub">
              {summary.nextAmountPerUnit !== null
                ? `${formatCurrency(summary.nextAmountPerUnit, asset.currency || 'BRL', numberLocale)}${expectedNextCash !== null ? ` • ${formatCurrency(expectedNextCash, asset.currency || 'BRL', numberLocale)}` : ''}`
                : t('assets.modal.noValue')}
            </span>
            <span className="assets-page__provents-sub">
              {t('assets.provents.last12m', {
                count: summary.paidCount12m,
                defaultValue: `${summary.paidCount12m} events (12M)`,
              })}
              {expected12mCash !== null ? ` • ${formatCurrency(expected12mCash, asset.currency || 'BRL', numberLocale)}` : ''}
            </span>
          </div>
        );
      },
    },
    {
      key: 'portfolioWeight',
      label: t('assets.portfolioWeight'),
      sortable: true,
      sortValue: (asset) => {
        const currentValue = currentValueByAssetId[asset.assetId];
        if (typeof currentValue !== 'number' || !Number.isFinite(currentValue)) return 0;
        return portfolioCurrentTotal > 0 ? currentValue / portfolioCurrentTotal : 0;
      },
      cellClassName: 'assets-page__cell--numeric assets-page__cell--weight',
      render: (asset) => {
        const currentValue = currentValueByAssetId[asset.assetId];
        if (typeof currentValue !== 'number' || !Number.isFinite(currentValue)) return t('assets.modal.noValue');
        if (portfolioCurrentTotal <= 0) return t('assets.modal.noValue');
        const ratio = currentValue / portfolioCurrentTotal;
        return formatPercent(ratio);
      },
    },
    {
      key: 'assetClass',
      label: t('assets.class'),
      sortable: true,
      sortValue: (asset) => normalizeAssetClassKey(asset.assetClass),
      render: (asset) => {
        const classKey = normalizeAssetClassKey(asset.assetClass);
        return (
          <span className={`badge badge--${classKey}`}>
            {t(`assets.classes.${classKey}`, { defaultValue: asset.assetClass || classKey.toUpperCase() })}
          </span>
        );
      },
    },
    {
      key: 'country',
      label: t('assets.country'),
      sortable: true,
      sortValue: (asset) => asset.country,
      render: (asset) => formatCountryFlag(asset.country),
    },
    {
      key: 'status',
      label: t('assets.status'),
      sortable: true,
      sortValue: (asset) => asset.status,
      render: (asset) =>
        t(`assets.statuses.${asset.status?.toLowerCase() || 'unknown'}`, {
          defaultValue: asset.status || t('assets.statuses.unknown'),
        }),
    },
    {
      key: 'actions',
      label: t('assets.actions'),
      render: (asset) => (
        <button
          type="button"
          className="assets-page__delete"
          onClick={(event) => {
            event.stopPropagation();
            handleDelete(asset.assetId);
          }}
        >
          {t('common.delete')}
        </button>
      ),
    },
  ];

  const filters: DataTableFilter<AssetRow>[] = [
    {
      key: 'status',
      label: t('assets.filters.status.label'),
      value: statusFilter,
      options: statusFilterOptions.map((option) => ({
        value: option.value,
        label: t(`assets.filters.status.${option.value}`, { defaultValue: option.label }),
      })),
      onChange: setStatusFilter,
      matches: (asset, filterValue) =>
        filterValue === 'all' || (asset.status?.toLowerCase() || '') === filterValue,
    },
  ];

  const assetDetailsSections = useMemo<RecordDetailsSection[]>(() => {
    if (!selectedAsset) return [];

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    const cachedCurrentPrice = currentQuotesByAssetId[selectedAsset.assetId];
    const derivedCurrentPrice = (() => {
      const currentValue = Number(selectedAsset.currentValue);
      const quantity = Number(selectedAsset.quantity);
      if (!Number.isFinite(currentValue) || !Number.isFinite(quantity)) return null;
      if (Math.abs(quantity) <= Number.EPSILON) return null;
      return currentValue / quantity;
    })();
    const resolvedCurrentPrice =
      Number.isFinite(directCurrentPrice)
        ? directCurrentPrice
        : (typeof cachedCurrentPrice === 'number' && Number.isFinite(cachedCurrentPrice))
          ? cachedCurrentPrice
          : derivedCurrentPrice;
    const resolvedCurrentValue = (() => {
      const metricCurrentValue = portfolioMarketValueByAssetId[selectedAsset.assetId];
      if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
        return metricCurrentValue;
      }

      const quantity = Number(selectedAsset.quantity);
      if (resolvedCurrentPrice !== null && Number.isFinite(quantity)) {
        return quantity * resolvedCurrentPrice;
      }

      const directCurrentValue = Number(selectedAsset.currentValue);
      return Number.isFinite(directCurrentValue) ? directCurrentValue : null;
    })();
    const resolvedAverageCost = (() => {
      const cachedAverageCost = averageCostByAssetId[selectedAsset.assetId];
      if (typeof cachedAverageCost === 'number' && Number.isFinite(cachedAverageCost)) {
        return cachedAverageCost;
      }

      const quantity = Number(selectedAsset.quantity);
      const investedAmount = Number(selectedAsset.investedAmount);
      if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
      if (Math.abs(quantity) <= Number.EPSILON) return null;
      return investedAmount / quantity;
    })();
    const quoteVsAverage = (() => {
      if (resolvedCurrentPrice === null || resolvedAverageCost === null) return null;
      return resolvedCurrentPrice - resolvedAverageCost;
    })();
    const balanceMinusInvested = (() => {
      if (resolvedCurrentValue === null) return null;
      const investedAmount = Number(selectedAsset.investedAmount);
      if (!Number.isFinite(investedAmount)) return null;
      return resolvedCurrentValue - investedAmount;
    })();
    const positionStatus = (() => {
      if (balanceMinusInvested === null) return null;
      if (Math.abs(balanceMinusInvested) <= Number.EPSILON) return 'neutral';
      return balanceMinusInvested > 0 ? 'positive' : 'negative';
    })();
    const selectedProvents = proventSummaryByTicker.get(String(selectedAsset.ticker || '').toUpperCase()) || null;
    const estimatedNextProventsCash = (() => {
      if (!selectedProvents || selectedProvents.nextAmountPerUnit === null) return null;
      const quantity = Number(selectedAsset.quantity || 0);
      if (!Number.isFinite(quantity) || quantity <= 0) return null;
      return selectedProvents.nextAmountPerUnit * quantity;
    })();
    const estimated12mProventsCash = (() => {
      if (!selectedProvents || selectedProvents.paidPerUnit12m <= 0) return null;
      const quantity = Number(selectedAsset.quantity || 0);
      if (!Number.isFinite(quantity) || quantity <= 0) return null;
      return selectedProvents.paidPerUnit12m * quantity;
    })();

    return [
      {
        key: 'overview',
        title: t('assets.modal.sections.overview'),
        columns: 2,
        fields: [
          { key: 'ticker', label: t('assets.modal.fields.ticker'), value: formatDetailValue(selectedAsset.ticker) },
          {
            key: 'name',
            label: t('assets.modal.fields.name'),
            value: selectedAsset.name
              ? (
                <ExpandableText
                  text={selectedAsset.name}
                  maxLines={2}
                  expandLabel={t('assets.modal.fields.nameExpandHint')}
                  collapseLabel={t('assets.modal.fields.nameCollapseHint')}
                />
              )
              : formatDetailValue(selectedAsset.name),
          },
          {
            key: 'quantity',
            label: t('assets.modal.fields.quantity'),
            value: formatAssetQuantity(selectedAsset.quantity),
          },
          {
            key: 'investedAmount',
            label: t('assets.modal.fields.investedAmount'),
            value: formatCurrency(selectedAsset.investedAmount, selectedAsset.currency || 'BRL', numberLocale),
          },
          {
            key: 'averagePrice',
            label: t('assets.modal.fields.averagePrice'),
            value: resolvedAverageCost !== null
              ? formatCurrency(resolvedAverageCost, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(resolvedAverageCost),
          },
          {
            key: 'currentPrice',
            label: t('assets.modal.fields.currentPrice'),
            value: resolvedCurrentPrice !== null
              ? formatCurrency(resolvedCurrentPrice, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(selectedAsset.currentPrice),
          },
          {
            key: 'currentValue',
            label: t('assets.modal.fields.currentValue'),
            value: resolvedCurrentValue !== null
              ? formatCurrency(resolvedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(selectedAsset.currentValue),
          },
        ],
      },
      {
        key: 'market',
        title: t('assets.modal.sections.market'),
        columns: 2,
        fields: [
          {
            key: 'assetClass',
            label: t('assets.modal.fields.class'),
            value: t(`assets.classes.${normalizeAssetClassKey(selectedAsset.assetClass)}`, { defaultValue: selectedAsset.assetClass }),
          },
          {
            key: 'status',
            label: t('assets.modal.fields.status'),
            value: t(`assets.statuses.${selectedAsset.status?.toLowerCase() || 'unknown'}`, {
              defaultValue: selectedAsset.status || t('assets.statuses.unknown'),
            }),
          },
          {
            key: 'country',
            label: t('assets.modal.fields.country'),
            value: formatCountryDetail(selectedAsset.country),
          },
          {
            key: 'currency',
            label: t('assets.modal.fields.currency'),
            value: formatDetailValue(selectedAsset.currency),
          },
          {
            key: 'quoteVsAverage',
            label: t('assets.modal.fields.quoteVsAverage'),
            value:
              quoteVsAverage !== null
                ? formatSignedCurrency(quoteVsAverage, selectedAsset.currency || 'BRL')
                : formatDetailValue(quoteVsAverage),
          },
          {
            key: 'investedMinusCurrent',
            label: t('assets.modal.fields.investedMinusCurrent'),
            value:
              balanceMinusInvested !== null
                ? formatSignedCurrency(balanceMinusInvested, selectedAsset.currency || 'BRL')
                : formatDetailValue(balanceMinusInvested),
          },
          {
            key: 'positionStatus',
            label: t('assets.modal.fields.positionStatus'),
            value:
              positionStatus
                ? (
                  <span className={`assets-page__position assets-page__position--${positionStatus}`}>
                    {t(`assets.modal.position.${positionStatus}`)}
                  </span>
                )
                : formatDetailValue(positionStatus),
          },
        ],
      },
      {
        key: 'provents',
        title: t('assets.modal.sections.provents', { defaultValue: 'Provents & Calendar' }),
        columns: 2,
        fields: [
          {
            key: 'nextPaymentDate',
            label: t('assets.modal.provents.nextPaymentDate', { defaultValue: 'Next Payment' }),
            value: selectedProvents?.nextPaymentDate
              ? formatDate(selectedProvents.nextPaymentDate, numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'nextExDate',
            label: t('assets.modal.provents.nextExDate', { defaultValue: 'Next Ex-Date' }),
            value: selectedProvents?.nextExDate
              ? formatDate(selectedProvents.nextExDate, numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'nextPerUnit',
            label: t('assets.modal.provents.nextPerUnit', { defaultValue: 'Next Per Unit' }),
            value: selectedProvents?.nextAmountPerUnit !== null && selectedProvents?.nextAmountPerUnit !== undefined
              ? formatCurrency(selectedProvents.nextAmountPerUnit, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'nextExpected',
            label: t('assets.modal.provents.nextExpected', { defaultValue: 'Next Expected Cash' }),
            value: estimatedNextProventsCash !== null
              ? formatCurrency(estimatedNextProventsCash, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'lastPaymentDate',
            label: t('assets.modal.provents.lastPaymentDate', { defaultValue: 'Last Payment' }),
            value: selectedProvents?.lastPaymentDate
              ? formatDate(selectedProvents.lastPaymentDate, numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'lastPerUnit',
            label: t('assets.modal.provents.lastPerUnit', { defaultValue: 'Last Per Unit' }),
            value: selectedProvents?.lastAmountPerUnit !== null && selectedProvents?.lastAmountPerUnit !== undefined
              ? formatCurrency(selectedProvents.lastAmountPerUnit, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'paidEvents12m',
            label: t('assets.modal.provents.paidEvents12m', { defaultValue: 'Paid Events (12M)' }),
            value: selectedProvents?.paidCount12m !== undefined
              ? selectedProvents.paidCount12m.toLocaleString(numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'paidPerUnit12m',
            label: t('assets.modal.provents.paidPerUnit12m', { defaultValue: 'Paid Per Unit (12M)' }),
            value: selectedProvents && selectedProvents.paidPerUnit12m > 0
              ? formatCurrency(selectedProvents.paidPerUnit12m, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'estimatedCash12m',
            label: t('assets.modal.provents.estimatedCash12m', { defaultValue: 'Estimated Cash (12M)' }),
            value: estimated12mProventsCash !== null
              ? formatCurrency(estimated12mProventsCash, selectedAsset.currency || 'BRL', numberLocale)
              : formatDetailValue(null),
          },
          {
            key: 'sources',
            label: t('assets.modal.provents.sources', { defaultValue: 'Sources' }),
            value: selectedProvents && selectedProvents.sources.length > 0
              ? selectedProvents.sources.join(', ')
              : formatDetailValue(null),
          },
        ],
      },
    ];
  }, [
    averageCostByAssetId,
    currentQuotesByAssetId,
    formatAssetQuantity,
    formatCountryDetail,
    formatDetailValue,
    formatSignedCurrency,
    numberLocale,
    portfolioMarketValueByAssetId,
    proventSummaryByTicker,
    selectedAsset,
    t,
  ]);

  const selectedAssetWeightMetrics = useMemo(() => {
    if (!selectedAsset) return null;

    const selectedCurrentValue = currentValueByAssetId[selectedAsset.assetId];
    const normalizedSelectedValue = (typeof selectedCurrentValue === 'number' && Number.isFinite(selectedCurrentValue)) ? selectedCurrentValue : 0;
    const portfolioWeight = portfolioCurrentTotal > 0 ? normalizedSelectedValue / portfolioCurrentTotal : 0;
    const selectedAssetClass = normalizeAssetClassKey(selectedAsset.assetClass);
    const classTotal = assetRows
      .filter((row) => normalizeAssetClassKey(row.assetClass) === selectedAssetClass)
      .reduce((sum, row) => {
        const value = currentValueByAssetId[row.assetId];
        return (typeof value === 'number' && Number.isFinite(value)) ? sum + value : sum;
      }, 0);
    const classWeight = classTotal > 0 ? normalizedSelectedValue / classTotal : 0;

    return {
      selectedValue: normalizedSelectedValue,
      portfolioTotal: portfolioCurrentTotal,
      portfolioWeight,
      classTotal,
      classWeight,
    };
  }, [assetRows, currentValueByAssetId, portfolioCurrentTotal, selectedAsset]);

  useEffect(() => {
    setSelectedHistoryPoint(null);
  }, [selectedAsset?.assetId]);

  const assetTradeHistoryRows = useMemo<AssetTradeHistoryRow[]>(() => {
    if (!selectedAsset) return [];

    return transactions
      .filter((transaction) => {
        if (transaction.assetId !== selectedAsset.assetId) return false;
        const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
        if (normalizedStatus !== 'confirmed') return false;
        if (shouldIgnoreConsolidatedTrade(transaction)) return false;
        const normalizedType = transaction.type?.toLowerCase() || '';
        return normalizedType === 'buy' || normalizedType === 'sell';
      })
      .map((transaction) => ({
        transId: transaction.transId,
        date: transaction.date || transaction.createdAt?.slice(0, 10) || '',
        type: transaction.type.toLowerCase() as 'buy' | 'sell',
        quantity: Number(transaction.quantity || 0),
        price: Number(transaction.price || 0),
        amount: Number(transaction.amount || 0),
        currency: transaction.currency || selectedAsset.currency || 'BRL',
        source: summarizeSourceValue(transaction.sourceDocId || transaction.institution) || null,
      }))
      .filter((row) => row.date && Number.isFinite(row.price))
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
  }, [selectedAsset, shouldIgnoreConsolidatedTrade, transactions]);

  const assetTradeHistoryChart = useMemo(() => {
    const rows = assetTradeHistoryRows.filter((row) => Number.isFinite(row.price));
    if (!rows.length) return null;

    const chartPadding = { top: 16, right: 20, bottom: 28, left: 20 };
    const chartWidth = HISTORY_CHART_WIDTH - chartPadding.left - chartPadding.right;
    const chartHeight = HISTORY_CHART_HEIGHT - chartPadding.top - chartPadding.bottom;
    const prices = rows.map((point) => point.price);
    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    const spread = Math.max(maxPrice - minPrice, 1);
    const paddedMin = minPrice - spread * 0.08;
    const paddedMax = maxPrice + spread * 0.08;
    const yBase = chartPadding.top + chartHeight;

    const xFor = (index: number) =>
      rows.length === 1
        ? chartPadding.left + chartWidth / 2
        : chartPadding.left + (index / (rows.length - 1)) * chartWidth;
    const yFor = (price: number) =>
      chartPadding.top + (1 - (price - paddedMin) / (paddedMax - paddedMin)) * chartHeight;

    const points: AssetTradeHistoryPoint[] = rows.map((point, index) => ({
      ...point,
      x: xFor(index),
      y: yFor(point.price),
      index,
    }));

    const polyline = points
      .map((point, index) => {
        const x = point.x.toFixed(2);
        const y = point.y.toFixed(2);
        return `${index === 0 ? 'M' : 'L'} ${x} ${y}`;
      })
      .join(' ');
    const areaPath = `${polyline} L ${points[points.length - 1].x.toFixed(2)} ${yBase.toFixed(2)} L ${points[0].x.toFixed(2)} ${yBase.toFixed(2)} Z`;

    return {
      points,
      polyline,
      areaPath,
      firstDate: rows[0].date,
      lastDate: rows[rows.length - 1].date,
      firstPrice: rows[0].price,
      lastPrice: rows[rows.length - 1].price,
      midPrice: (paddedMin + paddedMax) / 2,
      minPrice: paddedMin,
      maxPrice: paddedMax,
      padding: chartPadding,
      yBase,
    };
  }, [assetTradeHistoryRows]);

  const assetTradeHistoryStats = useMemo(() => {
    const buys = assetTradeHistoryRows.filter((row) => row.type === 'buy');
    const sells = assetTradeHistoryRows.filter((row) => row.type === 'sell');

    const weightedAveragePrice = (rows: AssetTradeHistoryRow[]) => {
      const totalQuantity = rows.reduce((sum, row) => sum + row.quantity, 0);
      if (Math.abs(totalQuantity) <= Number.EPSILON) return null;
      const totalAmount = rows.reduce((sum, row) => sum + (row.price * row.quantity), 0);
      return totalAmount / totalQuantity;
    };

    return {
      trades: assetTradeHistoryRows.length,
      buys: buys.length,
      sells: sells.length,
      avgBuyPrice: weightedAveragePrice(buys),
      avgSellPrice: weightedAveragePrice(sells),
    };
  }, [assetTradeHistoryRows]);

  const assetDetailsExtraContent = useMemo(() => {
    if (!selectedAsset) return null;
    const weightMetrics = selectedAssetWeightMetrics;
    const selectedProvents = proventSummaryByTicker.get(String(selectedAsset.ticker || '').toUpperCase()) || null;
    const proventTimeline = selectedProvents?.events || [];
    const ringRadius = 44;
    const ringCircumference = 2 * Math.PI * ringRadius;
    const ringOffsetFor = (ratio: number) => ringCircumference * (1 - Math.max(0, Math.min(1, ratio)));

    const chartGradientId = `asset-history-gradient-${selectedAsset.assetId.replace(/[^a-zA-Z0-9_-]/g, '')}`;
    const tooltipStyle = (() => {
      if (!selectedHistoryPoint) return null;
      const isRightSide = selectedHistoryPoint.x > HISTORY_CHART_WIDTH * 0.68;
      const isNearTop = selectedHistoryPoint.y < HISTORY_CHART_HEIGHT * 0.25;

      return {
        left: `${(selectedHistoryPoint.x / HISTORY_CHART_WIDTH) * 100}%`,
        top: `${(selectedHistoryPoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
        transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
      };
    })();

    return (
      <div className="assets-page__history">
        {weightMetrics ? (
          <div className="assets-page__weights">
            <h3>{t('assets.modal.weights.title')}</h3>
            <div className="assets-page__weights-grid">
              <article className="assets-page__weight-card">
                <h4>{t('assets.modal.weights.portfolio')}</h4>
                <div className="assets-page__weight-chart">
                  <svg viewBox="0 0 120 120" aria-hidden="true">
                    <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r={ringRadius} />
                    <circle
                      className="assets-page__weight-ring assets-page__weight-ring--portfolio"
                      cx="60"
                      cy="60"
                      r={ringRadius}
                      strokeDasharray={`${ringCircumference} ${ringCircumference}`}
                      strokeDashoffset={ringOffsetFor(weightMetrics.portfolioWeight)}
                    />
                  </svg>
                  <div className="assets-page__weight-chart-center">
                    <strong>{formatPercent(weightMetrics.portfolioWeight)}</strong>
                  </div>
                </div>
                <div className="assets-page__weight-meta">
                  <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(weightMetrics.selectedValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                  <span>{t('assets.modal.weights.portfolioTotal')}: <strong>{formatCurrency(weightMetrics.portfolioTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                </div>
              </article>

              <article className="assets-page__weight-card">
                <h4>{t('assets.modal.weights.class', { className: t(`assets.classes.${normalizeAssetClassKey(selectedAsset.assetClass)}`, { defaultValue: selectedAsset.assetClass }) })}</h4>
                <div className="assets-page__weight-chart">
                  <svg viewBox="0 0 120 120" aria-hidden="true">
                    <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r={ringRadius} />
                    <circle
                      className="assets-page__weight-ring assets-page__weight-ring--class"
                      cx="60"
                      cy="60"
                      r={ringRadius}
                      strokeDasharray={`${ringCircumference} ${ringCircumference}`}
                      strokeDashoffset={ringOffsetFor(weightMetrics.classWeight)}
                    />
                  </svg>
                  <div className="assets-page__weight-chart-center">
                    <strong>{formatPercent(weightMetrics.classWeight)}</strong>
                  </div>
                </div>
                <div className="assets-page__weight-meta">
                  <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(weightMetrics.selectedValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                  <span>{t('assets.modal.weights.classTotal')}: <strong>{formatCurrency(weightMetrics.classTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                </div>
              </article>
            </div>
          </div>
        ) : null}

        <div className="assets-page__provents-history">
          <h3>{t('assets.modal.provents.title', { defaultValue: 'Provents & Calendar' })}</h3>
          {proventTimeline.length === 0 ? (
            <p className="assets-page__history-empty">
              {t('assets.modal.provents.empty', { defaultValue: 'No provent events found for this asset.' })}
            </p>
          ) : (
            <div className="assets-page__provents-table-wrap">
              <table className="assets-page__provents-table">
                <thead>
                  <tr>
                    <th>{t('assets.modal.provents.timeline.date', { defaultValue: 'Date' })}</th>
                    <th>{t('assets.modal.provents.timeline.status', { defaultValue: 'Status' })}</th>
                    <th>{t('assets.modal.provents.timeline.type', { defaultValue: 'Type' })}</th>
                    <th>{t('assets.modal.provents.timeline.perUnit', { defaultValue: 'Per Unit' })}</th>
                    <th>{t('assets.modal.provents.timeline.exDate', { defaultValue: 'Ex-Date' })}</th>
                    <th>{t('assets.modal.provents.timeline.source', { defaultValue: 'Source' })}</th>
                  </tr>
                </thead>
                <tbody>
                  {proventTimeline.slice(0, 24).map((event) => (
                    <tr key={`${event.id}-${event.eventDate}`}>
                      <td>{formatDate(event.eventDate, numberLocale)}</td>
                      <td>
                        <span className={`assets-page__provent-status assets-page__provent-status--${event.status}`}>
                          {event.status === 'paid'
                            ? t('assets.modal.provents.statuses.paid', { defaultValue: 'Paid' })
                            : t('assets.modal.provents.statuses.provisioned', { defaultValue: 'Provisioned' })}
                        </span>
                      </td>
                      <td>{event.eventType || '-'}</td>
                      <td>
                        {event.amountPerUnit !== null
                          ? formatCurrency(event.amountPerUnit, selectedAsset.currency || 'BRL', numberLocale)
                          : '-'}
                      </td>
                      <td>{event.exDate ? formatDate(event.exDate, numberLocale) : '-'}</td>
                      <td>
                        {event.sourceUrl ? (
                          <a
                            href={event.sourceUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="assets-page__provents-source-link"
                          >
                            {event.source || '-'}
                          </a>
                        ) : (event.source || '-')}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <h3>{t('assets.modal.history.title')}</h3>

        {assetTradeHistoryRows.length === 0 ? (
          <p className="assets-page__history-empty">{t('assets.modal.history.empty')}</p>
        ) : (
          <>
            <div className="assets-page__history-stats">
              <span>{t('assets.modal.history.totalTrades')}: <strong>{assetTradeHistoryStats.trades}</strong></span>
              <span>{t('assets.modal.history.buys')}: <strong>{assetTradeHistoryStats.buys}</strong></span>
              <span>{t('assets.modal.history.sells')}: <strong>{assetTradeHistoryStats.sells}</strong></span>
              <span>
                {t('assets.modal.history.avgBuyPrice')}: <strong>
                  {assetTradeHistoryStats.avgBuyPrice !== null
                    ? formatCurrency(assetTradeHistoryStats.avgBuyPrice, selectedAsset.currency || 'BRL', numberLocale)
                    : '-'}
                </strong>
              </span>
              <span>
                {t('assets.modal.history.avgSellPrice')}: <strong>
                  {assetTradeHistoryStats.avgSellPrice !== null
                    ? formatCurrency(assetTradeHistoryStats.avgSellPrice, selectedAsset.currency || 'BRL', numberLocale)
                    : '-'}
                </strong>
              </span>
            </div>

            <div className="assets-page__history-chart">
              <svg
                viewBox={`0 0 ${HISTORY_CHART_WIDTH} ${HISTORY_CHART_HEIGHT}`}
                role="img"
                aria-label={t('assets.modal.history.chart')}
                onClick={() => setSelectedHistoryPoint(null)}
              >
                <defs>
                  <linearGradient id={chartGradientId} x1="0%" y1="0%" x2="0%" y2="100%">
                    <stop offset="0%" stopColor="rgba(99, 102, 241, 0.42)" />
                    <stop offset="100%" stopColor="rgba(99, 102, 241, 0.02)" />
                  </linearGradient>
                </defs>
                {assetTradeHistoryChart ? (
                  <>
                    <line
                      className="assets-page__history-grid"
                      x1={assetTradeHistoryChart.padding.left}
                      x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                      y1={assetTradeHistoryChart.padding.top}
                      y2={assetTradeHistoryChart.padding.top}
                    />
                    <line
                      className="assets-page__history-grid"
                      x1={assetTradeHistoryChart.padding.left}
                      x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                      y1={assetTradeHistoryChart.padding.top + ((assetTradeHistoryChart.yBase - assetTradeHistoryChart.padding.top) / 2)}
                      y2={assetTradeHistoryChart.padding.top + ((assetTradeHistoryChart.yBase - assetTradeHistoryChart.padding.top) / 2)}
                    />
                    <line
                      className="assets-page__history-grid"
                      x1={assetTradeHistoryChart.padding.left}
                      x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                      y1={assetTradeHistoryChart.yBase}
                      y2={assetTradeHistoryChart.yBase}
                    />
                    <path className="assets-page__history-area" d={assetTradeHistoryChart.areaPath} fill={`url(#${chartGradientId})`} />
                    <path className="assets-page__history-line" d={assetTradeHistoryChart.polyline} />
                    {assetTradeHistoryChart.points.map((point) => (
                      <circle
                        key={`${point.transId}-${point.date}-${point.price}-${point.index}`}
                        className={`assets-page__history-point assets-page__history-point--${point.type}`}
                        cx={point.x}
                        cy={point.y}
                        r={selectedHistoryPoint?.transId === point.transId && selectedHistoryPoint?.index === point.index ? 6 : 4}
                        role="button"
                        tabIndex={0}
                        onClick={(event) => {
                          event.stopPropagation();
                          setSelectedHistoryPoint(point);
                        }}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            setSelectedHistoryPoint(point);
                          }
                        }}
                      >
                        <title>
                          {`${formatDate(point.date, numberLocale)} | ${t(`transactions.types.${point.type}`, { defaultValue: point.type })} | ${formatCurrency(point.price, point.currency, numberLocale)}`}
                        </title>
                      </circle>
                    ))}
                  </>
                ) : null}
              </svg>
              {selectedHistoryPoint && tooltipStyle ? (
                <div className="assets-page__history-tooltip" style={tooltipStyle}>
                  <div className="assets-page__history-tooltip-header">
                    <span className={`assets-page__history-type assets-page__history-type--${selectedHistoryPoint.type}`}>
                      {t(`transactions.types.${selectedHistoryPoint.type}`, { defaultValue: selectedHistoryPoint.type })}
                    </span>
                    <strong>{formatDate(selectedHistoryPoint.date, numberLocale)}</strong>
                  </div>
                  <div className="assets-page__history-tooltip-grid">
                    <span>{t('assets.modal.history.quantity')}</span>
                    <strong>{formatAssetQuantity(selectedHistoryPoint.quantity)}</strong>
                    <span>{t('assets.modal.history.price')}</span>
                    <strong>{formatCurrency(selectedHistoryPoint.price, selectedHistoryPoint.currency, numberLocale)}</strong>
                    <span>{t('assets.modal.history.amount')}</span>
                    <strong>{formatCurrency(selectedHistoryPoint.amount, selectedHistoryPoint.currency, numberLocale)}</strong>
                    <span>{t('assets.modal.history.source')}</span>
                    <strong>{selectedHistoryPoint.source || '-'}</strong>
                  </div>
                </div>
              ) : null}
              {assetTradeHistoryChart ? (
                <div className="assets-page__history-scale">
                  <span>{formatDate(assetTradeHistoryChart.firstDate, numberLocale)}</span>
                  <span>{formatCurrency(assetTradeHistoryChart.minPrice, selectedAsset.currency || 'BRL', numberLocale)}</span>
                  <span>{formatCurrency(assetTradeHistoryChart.maxPrice, selectedAsset.currency || 'BRL', numberLocale)}</span>
                  <span>{formatDate(assetTradeHistoryChart.lastDate, numberLocale)}</span>
                </div>
              ) : null}
              {assetTradeHistoryChart ? (
                <div className="assets-page__history-meta">
                  <span>{t('assets.modal.history.lastPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.lastPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                  <span>{t('assets.modal.history.minPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.minPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                  <span>{t('assets.modal.history.maxPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.maxPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                </div>
              ) : null}
            </div>

            <div className="assets-page__history-table-wrap">
              <table className="assets-page__history-table">
                <thead>
                  <tr>
                    <th>{t('assets.modal.history.date')}</th>
                    <th>{t('assets.modal.history.type')}</th>
                    <th>{t('assets.modal.history.quantity')}</th>
                    <th>{t('assets.modal.history.price')}</th>
                    <th>{t('assets.modal.history.amount')}</th>
                    <th>{t('assets.modal.history.source')}</th>
                  </tr>
                </thead>
                <tbody>
                  {[...assetTradeHistoryRows].reverse().map((row) => (
                    <tr key={`${row.transId}-${row.date}-${row.type}`}>
                      <td>{formatDate(row.date, numberLocale)}</td>
                      <td>
                        <span className={`assets-page__history-type assets-page__history-type--${row.type}`}>
                          {t(`transactions.types.${row.type}`, { defaultValue: row.type })}
                        </span>
                      </td>
                      <td>{formatAssetQuantity(row.quantity)}</td>
                      <td>{formatCurrency(row.price, row.currency, numberLocale)}</td>
                      <td>{formatCurrency(row.amount, row.currency, numberLocale)}</td>
                      <td>{row.source || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    );
  }, [assetTradeHistoryChart, assetTradeHistoryRows, assetTradeHistoryStats, formatAssetQuantity, formatPercent, numberLocale, proventSummaryByTicker, selectedAsset, selectedAssetWeightMetrics, selectedHistoryPoint, t]);

  return (
    <Layout>
      <div className="assets-page">
        <div className="assets-page__header">
          <h1 className="assets-page__title">{t('assets.title')}</h1>
          <div className="assets-page__controls">
            {portfolios.length > 0 && (
              <select
                className="assets-page__select"
                value={selectedPortfolio}
                onChange={(event) => setSelectedPortfolio(event.target.value)}
              >
                {portfolios.map((portfolio) => (
                  <option key={portfolio.portfolioId} value={portfolio.portfolioId}>
                    {portfolio.name}
                  </option>
                ))}
              </select>
            )}
            <button className="assets-page__add-btn" onClick={() => setShowModal(true)}>
              {t('assets.addAsset')}
            </button>
          </div>
        </div>

        {loading && <p className="assets-page__loading">{t('common.loading')}</p>}

        {!loading && assetRows.length === 0 && (
          <div className="assets-page__empty">
            <p>{t('assets.empty')}</p>
          </div>
        )}

        {!loading && assetRows.length > 0 && (
          <DataTable
            rows={assetRows}
            rowKey={(asset) => asset.assetId}
            columns={columns}
            searchLabel={t('assets.filters.search')}
            searchPlaceholder={t('assets.filters.searchPlaceholder')}
            searchTerm={searchTerm}
            onSearchTermChange={setSearchTerm}
            matchesSearch={(asset, normalizedSearch) => {
              const proventSummary = proventSummaryByTicker.get(String(asset.ticker || '').toUpperCase());
              return [
                asset.ticker,
                asset.name,
                asset.quantity,
                resolveAssetAverageCost(asset),
                currentValueByAssetId[asset.assetId] || 0,
                portfolioInvestedTotal > 0 ? `${((Number(asset.investedAmount) || 0) / portfolioInvestedTotal) * 100}%` : '0%',
                asset.assetClass,
                asset.country,
                asset.currency,
                asset.status,
                asset.source,
                proventSummary?.nextPaymentDate || '',
                proventSummary?.lastPaymentDate || '',
                proventSummary?.sources.join(' ') || '',
              ]
                .join(' ')
                .toLowerCase()
                .includes(normalizedSearch);
            }}
            filters={filters}
            itemsPerPage={itemsPerPage}
            onItemsPerPageChange={setItemsPerPage}
            pageSizeOptions={pageSizeOptions}
            emptyLabel={t('assets.emptyFiltered')}
            labels={{
              itemsPerPage: t('assets.pagination.itemsPerPage'),
              prev: t('assets.pagination.prev'),
              next: t('assets.pagination.next'),
              page: (page, total) => t('assets.pagination.page', { page, total }),
              showing: (start, end, total) => t('assets.pagination.showing', { start, end, total }),
            }}
            defaultSort={{ key: 'ticker', direction: 'asc' }}
            onRowClick={openAssetDetails}
            rowAriaLabel={(asset) => t('assets.modal.openDetails', { ticker: asset.ticker })}
          />
        )}

        <RecordDetailsModal
          open={Boolean(selectedAsset)}
          title={selectedAsset ? `${selectedAsset.ticker} • ${selectedAsset.name}` : t('assets.modal.title')}
          subtitle={t('assets.modal.subtitle')}
          closeLabel={t('assets.modal.close')}
          headerActions={selectedAsset ? (
            <button
              type="button"
              className="record-modal__action"
              onClick={() => {
                const params = new URLSearchParams();
                if (selectedPortfolio) params.set('portfolioId', selectedPortfolio);
                const queryString = params.toString();
                const path = queryString
                  ? `/assets/${selectedAsset.assetId}?${queryString}`
                  : `/assets/${selectedAsset.assetId}`;
                setSelectedAsset(null);
                navigate(path);
              }}
            >
              {t('assets.modal.openPage', { defaultValue: 'Open asset page' })}
            </button>
          ) : null}
          sections={assetDetailsSections}
          extraContent={assetDetailsExtraContent}
          onClose={() => setSelectedAsset(null)}
        />

        <FormModal
          open={showModal}
          title={t('assets.addAsset')}
          closeLabel={t('assets.form.cancel')}
          cancelLabel={t('assets.form.cancel')}
          submitLabel={t('assets.form.submit')}
          onClose={() => setShowModal(false)}
          onSubmit={handleSubmit}
        >
          <div className="form-modal__field">
            <label>{t('assets.form.ticker')}</label>
            <input
              type="text"
              value={form.ticker}
              onChange={(event) => setForm({ ...form, ticker: event.target.value })}
              required
            />
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.name')}</label>
            <input
              type="text"
              value={form.name}
              onChange={(event) => setForm({ ...form, name: event.target.value })}
              required
            />
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.class')}</label>
            <select
              value={form.assetClass}
              onChange={(event) => setForm({ ...form, assetClass: event.target.value })}
            >
              {assetClassOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {t(`assets.classes.${normalizeAssetClassKey(option.value)}`, { defaultValue: option.label })}
                </option>
              ))}
            </select>
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.country')}</label>
            <select
              value={form.country}
              onChange={(event) => setForm({ ...form, country: event.target.value })}
            >
              {countryOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.currency')}</label>
            <select
              value={form.currency}
              onChange={(event) => setForm({ ...form, currency: event.target.value })}
            >
              {currencyOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        </FormModal>
      </div>
    </Layout>
  );
};

export default AssetsPage;
