import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router';
import Layout from '../components/Layout';
import ExpandableText from '../components/ExpandableText';
import { api, type Asset, type Transaction } from '../services/api';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './AssetsPage.scss';
import './AssetDetailsPage.scss';

type AssetRow = Asset & {
  quantity: number;
  source: string | null;
  investedAmount: number;
};

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

type AssetPriceSeriesPoint = {
  date: string;
  display_date: string | undefined;
  close: number | null;
};

type ChartPeriodPreset = 'MAX' | '5A' | '2A' | '1A' | '6M' | '3M' | '1M' | 'CUSTOM';
type AssetInsightsSnapshot = {
  status: 'loading' | 'ready' | 'error';
  source: string | null;
  fetchedAt: string | null;
  sector: string | null;
  industry: string | null;
  marketCap: number | null;
  averageVolume: number | null;
  currentPrice: number | null;
  graham: number | null;
  bazin: number | null;
  fairPrice: number | null;
  marginOfSafetyPct: number | null;
  pe: number | null;
  pb: number | null;
  roe: number | null;
  roa: number | null;
  roic: number | null;
  payout: number | null;
  evEbitda: number | null;
  netDebtEbitda: number | null;
  lpa: number | null;
  vpa: number | null;
  netMargin: number | null;
  ebitMargin: number | null;
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
const HISTORY_CHART_WIDTH = 860;
const HISTORY_CHART_HEIGHT = 220;
const CHART_PERIOD_DAYS: Partial<Record<ChartPeriodPreset, number>> = {
  '1M': 30,
  '3M': 90,
  '6M': 180,
  '1A': 365,
  '2A': 730,
  '5A': 1825,
};

const toIsoDate = (value: string) => {
  const parsed = new Date(`${value}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) return null;
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

const normalizeRatioMetric = (value: number | null): number | null => {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.abs(value) > 1.5 ? value / 100 : value;
};

const normalizePercentValueToRatio = (value: number | null): number | null => {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.abs(value) > 1 ? value / 100 : value;
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
  graham: null,
  bazin: null,
  fairPrice: null,
  marginOfSafetyPct: null,
  pe: null,
  pb: null,
  roe: null,
  roa: null,
  roic: null,
  payout: null,
  evEbitda: null,
  netDebtEbitda: null,
  lpa: null,
  vpa: null,
  netMargin: null,
  ebitMargin: null,
  ...buildAssetExternalLinks(ticker, assetClass),
  errorMessage: null,
});

const addDaysToIsoDate = (date: string, days: number): string => {
  const parsed = new Date(`${date}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) return date;
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
};

const normalizeText = (value: unknown): string =>
  (value || '')
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();

const summarizeSourceValue = (value: unknown): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (normalized.includes('NUBANK') || normalized.includes('NU INVEST') || normalized.includes('NU BANK')) return 'NU BANK';
  if (normalized.includes('XP')) return 'XP';
  if (normalized.includes('ITAU')) return 'ITAU';
  if (normalized.includes('B3')) return 'B3';
  return null;
};

const AssetDetailsPage = () => {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const { assetId = '' } = useParams();
  const [searchParams] = useSearchParams();
  const portfolioIdFromQuery = searchParams.get('portfolioId')?.trim() || '';

  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    assets,
    transactions,
    loading,
    metrics,
  } = usePortfolioData();
  const portfolioId = selectedPortfolio;
  const portfolioMarketValueByAssetId = useMemo(() => metrics?.marketValues || {}, [metrics]);
  const [currentQuote, setCurrentQuote] = useState<number | null>(null);
  const [averageCost, setAverageCost] = useState<number | null>(null);
  const [marketSeries, setMarketSeries] = useState<AssetPriceSeriesPoint[]>([]);
  const [marketSeriesLoading, setMarketSeriesLoading] = useState(false);
  const [hoveredMarketPointIndex, setHoveredMarketPointIndex] = useState<number | null>(null);
  const [selectedTradePoint, setSelectedTradePoint] = useState<AssetTradeHistoryPoint | null>(null);
  const [chartPeriod, setChartPeriod] = useState<ChartPeriodPreset>('MAX');
  const [customRangeStart, setCustomRangeStart] = useState('');
  const [customRangeEnd, setCustomRangeEnd] = useState('');
  const [assetInsights, setAssetInsights] = useState<AssetInsightsSnapshot | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('assets.modal.noValue');
    return String(value);
  }, [t]);

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

  const formatSignedPercent = useCallback((value: number) => {
    const absolute = Math.abs(value).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    return `${value >= 0 ? '+' : '-'}${absolute}%`;
  }, [numberLocale]);

  const formatPercent = useCallback((ratio: number) => (
    `${(ratio * 100).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}%`
  ), [numberLocale]);

  const formatCompactNumber = useCallback((value: number) => (
    value.toLocaleString(numberLocale, {
      maximumFractionDigits: 2,
      notation: 'compact',
      compactDisplay: 'short',
    })
  ), [numberLocale]);

  const formatCountryDetail = useCallback((country: string) =>
    `${COUNTRY_FLAG_MAP[country] || '🏳️'} ${COUNTRY_NAME_MAP[country] || country}`, []);

  // Sync portfolio selection from URL query if present.
  useEffect(() => {
    if (portfolioIdFromQuery && portfolios.length > 0) {
      const match = portfolios.find((item) => item.portfolioId === portfolioIdFromQuery);
      if (match && match.portfolioId !== selectedPortfolio) {
        setSelectedPortfolio(match.portfolioId);
      }
    }
  }, [portfolioIdFromQuery, portfolios, selectedPortfolio, setSelectedPortfolio]);

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

  const selectedAsset = useMemo<AssetRow | null>(() => {
    const baseAsset = assets.find((entry) => entry.assetId === assetId);
    if (!baseAsset) return null;

    const labels = new Set<string>();
    const assetSource = summarizeSourceValue(baseAsset.source);
    if (assetSource) labels.add(assetSource);

    for (const candidate of (assetSourcesById[baseAsset.assetId] || [])) {
      const label = summarizeSourceValue(candidate);
      if (label) labels.add(label);
    }

    return {
      ...baseAsset,
      quantity: Number.isFinite(Number(baseAsset.quantity))
        ? Number(baseAsset.quantity)
        : (assetQuantitiesById[baseAsset.assetId] || 0),
      source: labels.size > 0 ? Array.from(labels).join(', ') : null,
      investedAmount: assetInvestedAmountById[baseAsset.assetId] || 0,
    };
  }, [assetId, assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

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
      graham: firstFiniteNumber(fair.graham),
      bazin: firstFiniteNumber(fair.bazin),
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
      roa: firstFiniteNumber(
        fairFundamentals.roa,
        finalInfo.returnOnAssets,
        finalInfo.roa,
        primaryInfo.returnOnAssets,
        primaryInfo.roa,
        fundamentals.roa,
      ),
      roic: firstFiniteNumber(
        fairFundamentals.roic,
        finalInfo.returnOnInvestedCapital,
        finalInfo.roic,
        primaryInfo.returnOnInvestedCapital,
        primaryInfo.roic,
        fundamentals.roic,
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
      netDebtEbitda: firstFiniteNumber(
        fairFundamentals.netDebtEbitda,
        finalInfo.netDebtToEbitda,
        finalInfo.netDebtEbitda,
        primaryInfo.netDebtToEbitda,
        primaryInfo.netDebtEbitda,
        fundamentals.netDebtEbitda,
      ),
      lpa: firstFiniteNumber(
        fairFundamentals.lpa,
        finalInfo.trailingEps,
        finalInfo.epsTrailingTwelveMonths,
        finalInfo.lpa,
        primaryInfo.trailingEps,
        primaryInfo.epsTrailingTwelveMonths,
        primaryInfo.lpa,
        fundamentals.lpa,
      ),
      vpa: firstFiniteNumber(
        fairFundamentals.vpa,
        finalInfo.bookValue,
        finalInfo.vpa,
        primaryInfo.bookValue,
        primaryInfo.vpa,
        fundamentals.vpa,
      ),
      netMargin: firstFiniteNumber(
        fairFundamentals.netMargin,
        finalInfo.profitMargins,
        finalInfo.netMargin,
        primaryInfo.profitMargins,
        primaryInfo.netMargin,
        fundamentals.netMargin,
      ),
      ebitMargin: firstFiniteNumber(
        fairFundamentals.ebitMargin,
        finalInfo.operatingMargins,
        finalInfo.ebitMargin,
        primaryInfo.operatingMargins,
        primaryInfo.ebitMargin,
        fundamentals.ebitMargin,
      ),
      errorMessage: null,
    };
  }, []);

  const assetRows = useMemo<AssetRow[]>(() => {
    return assets.map((asset) => {
      const labels = new Set<string>();
      const assetSource = summarizeSourceValue(asset.source);
      if (assetSource) labels.add(assetSource);
      for (const candidate of (assetSourcesById[asset.assetId] || [])) {
        const label = summarizeSourceValue(candidate);
        if (label) labels.add(label);
      }

      return {
        ...asset,
        quantity: Number.isFinite(Number(asset.quantity))
          ? Number(asset.quantity)
          : (assetQuantitiesById[asset.assetId] || 0),
        source: labels.size > 0 ? Array.from(labels).join(', ') : null,
        investedAmount: assetInvestedAmountById[asset.assetId] || 0,
      };
    });
  }, [assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

  const resolveRowCurrentValue = useCallback((row: AssetRow): number | null => {
    const metricCurrentValue = portfolioMarketValueByAssetId[row.assetId];
    if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
      return metricCurrentValue;
    }

    const quantity = Number(row.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const directCurrentPrice = Number(row.currentPrice);
    if (
      Number.isFinite(directCurrentPrice)
      && Number.isFinite(quantity)
      && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON)
    ) {
      return directCurrentPrice * quantity;
    }

    const directCurrentValue = Number(row.currentValue);
    if (
      Number.isFinite(directCurrentValue)
      && (!hasOpenPosition || Math.abs(directCurrentValue) > Number.EPSILON)
    ) {
      return directCurrentValue;
    }

    return null;
  }, [portfolioMarketValueByAssetId]);

  const currentValueByAssetId = useMemo(() => {
    const values: Record<string, number | null> = {};
    for (const row of assetRows) {
      values[row.assetId] = resolveRowCurrentValue(row);
    }
    return values;
  }, [assetRows, resolveRowCurrentValue]);

  const portfolioCurrentTotal = useMemo<number>(() => (
    Object.values(currentValueByAssetId).reduce<number>((sum, value) => (
      typeof value === 'number' && Number.isFinite(value) ? sum + value : sum
    ), 0)
  ), [currentValueByAssetId]);

  useEffect(() => {
    setCurrentQuote(null);
    setAverageCost(null);
    setMarketSeries([]);
    setHoveredMarketPointIndex(null);
    setSelectedTradePoint(null);
    setChartPeriod('MAX');
    setCustomRangeStart('');
    setCustomRangeEnd('');
    if (selectedAsset) {
      setAssetInsights(createEmptyInsightsSnapshot('loading', selectedAsset.ticker, selectedAsset.assetClass));
    } else {
      setAssetInsights(null);
    }
  }, [selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;

    const loadPayloads = async () => {
      const [details, fair] = await Promise.all([
        api.getAssetDetails(selectedAsset.ticker, portfolioId),
        api.getAssetFairPrice(selectedAsset.ticker, portfolioId),
      ]);
      return { details, fair };
    };

    loadPayloads()
      .then(async (initialPayloads) => {
        let detailsPayload = initialPayloads.details;
        let fairPayload = initialPayloads.fair;
        const detailsRecord = toObjectRecord(detailsPayload);

        if (detailsRecord.detail == null) {
          await api.refreshMarketData(portfolioId, selectedAsset.assetId).catch(() => null);
          try {
            const refreshedPayloads = await loadPayloads();
            detailsPayload = refreshedPayloads.details;
            fairPayload = refreshedPayloads.fair;
          } catch {
            // Keep initial payload when refresh fetch fails.
          }
        }

        if (cancelled) return;
        setAssetInsights(buildInsightsSnapshot(selectedAsset, detailsPayload, fairPayload));
      })
      .catch((error) => {
        if (cancelled) return;
        const fallback = createEmptyInsightsSnapshot('error', selectedAsset.ticker, selectedAsset.assetClass);
        fallback.errorMessage = error instanceof Error ? error.message : null;
        setAssetInsights(fallback);
      });

    return () => {
      cancelled = true;
    };
  }, [buildInsightsSnapshot, portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    const quantity = Number(selectedAsset.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    if (Number.isFinite(directCurrentPrice) && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON)) {
      setCurrentQuote(directCurrentPrice);
    } else {
      api.getPriceAtDate(portfolioId, selectedAsset.ticker, new Date().toISOString().slice(0, 10))
        .then((payload) => {
          if (cancelled) return;
          const close = Number((payload as { close?: unknown }).close);
          setCurrentQuote(Number.isFinite(close) ? close : null);
        })
        .catch(() => {
          if (cancelled) return;
          setCurrentQuote(null);
        });
    }

    api.getAverageCost(portfolioId, selectedAsset.ticker)
      .then((payload) => {
        if (cancelled) return;
        const parsedAverageCost = Number((payload as { average_cost?: unknown }).average_cost);
        setAverageCost(Number.isFinite(parsedAverageCost) ? parsedAverageCost : null);
      })
      .catch(() => {
        if (cancelled) return;
        setAverageCost(null);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;
    setMarketSeriesLoading(true);

    api.getPriceChart(portfolioId, selectedAsset.ticker, 'price_history', 'MAX')
      .then((payload) => {
        if (cancelled) return;

        const normalizedSeries = Array.isArray((payload as { series?: unknown[] }).series)
          ? ((payload as { series: unknown[] }).series
            .map((item) => {
              const point = item as Record<string, unknown>;
              const close = Number(point.close);
              return {
                date: String(point.date || ''),
                display_date: point.display_date ? String(point.display_date) : undefined,
                close: Number.isFinite(close) ? close : null,
              } satisfies AssetPriceSeriesPoint;
            })
            .filter((point) => point.date))
          : [];

        setMarketSeries(normalizedSeries);
      })
      .catch(() => {
        if (cancelled) return;
        setMarketSeries([]);
      })
      .finally(() => {
        if (cancelled) return;
        setMarketSeriesLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  const fallbackAverageCost = useMemo(() => {
    if (!selectedAsset) return null;
    const quantity = Number(selectedAsset.quantity);
    const investedAmount = Number(selectedAsset.investedAmount);
    if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return investedAmount / quantity;
  }, [selectedAsset]);

  const resolvedAverageCost = useMemo(() => {
    if (typeof averageCost === 'number' && Number.isFinite(averageCost)) return averageCost;
    if (selectedAsset) {
      const cached = metrics?.averageCosts?.[selectedAsset.assetId];
      if (typeof cached === 'number' && Number.isFinite(cached)) return cached;
    }
    return fallbackAverageCost;
  }, [averageCost, fallbackAverageCost, metrics, selectedAsset]);

  const resolvedCurrentPrice = useMemo(() => {
    if (!selectedAsset) return null;
    if (typeof currentQuote === 'number' && Number.isFinite(currentQuote)) return currentQuote;

    const quantity = Number(selectedAsset.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const cachedQuote = metrics?.currentQuotes?.[selectedAsset.assetId];
    if (
      typeof cachedQuote === 'number'
      && Number.isFinite(cachedQuote)
      && (!hasOpenPosition || Math.abs(cachedQuote) > Number.EPSILON)
    ) {
      return cachedQuote;
    }

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    if (Number.isFinite(directCurrentPrice) && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON)) {
      return directCurrentPrice;
    }

    const directCurrentValue = Number(selectedAsset.currentValue);
    if (!Number.isFinite(directCurrentValue) || !Number.isFinite(quantity)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return directCurrentValue / quantity;
  }, [currentQuote, metrics, selectedAsset]);

  const resolvedCurrentValue = useMemo(() => {
    if (!selectedAsset) return null;
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
  }, [portfolioMarketValueByAssetId, resolvedCurrentPrice, selectedAsset]);

  const quoteVsAverage = useMemo(() => {
    if (resolvedCurrentPrice === null || resolvedAverageCost === null) return null;
    return resolvedCurrentPrice - resolvedAverageCost;
  }, [resolvedAverageCost, resolvedCurrentPrice]);

  const balanceMinusInvested = useMemo(() => {
    if (!selectedAsset || resolvedCurrentValue === null) return null;
    const investedAmount = Number(selectedAsset.investedAmount);
    if (!Number.isFinite(investedAmount)) return null;
    return resolvedCurrentValue - investedAmount;
  }, [resolvedCurrentValue, selectedAsset]);

  const positionStatus = useMemo(() => {
    if (balanceMinusInvested === null) return null;
    if (Math.abs(balanceMinusInvested) <= Number.EPSILON) return 'neutral';
    return balanceMinusInvested > 0 ? 'positive' : 'negative';
  }, [balanceMinusInvested]);

  const selectedAssetWeightMetrics = useMemo(() => {
    if (!selectedAsset) return null;

    const storedSelectedCurrentValue = currentValueByAssetId[selectedAsset.assetId] ?? 0;
    const selectedCurrentValue = resolvedCurrentValue ?? storedSelectedCurrentValue;
    const adjustedPortfolioTotal = portfolioCurrentTotal - storedSelectedCurrentValue + selectedCurrentValue;
    const portfolioWeight = adjustedPortfolioTotal > 0 ? selectedCurrentValue / adjustedPortfolioTotal : 0;

    const storedClassTotal = assetRows
      .filter((row) => row.assetClass === selectedAsset.assetClass)
      .reduce((sum, row) => {
        const value = currentValueByAssetId[row.assetId];
        return typeof value === 'number' && Number.isFinite(value) ? sum + value : sum;
      }, 0);
    const adjustedClassTotal = storedClassTotal - storedSelectedCurrentValue + selectedCurrentValue;
    const classWeight = adjustedClassTotal > 0 ? selectedCurrentValue / adjustedClassTotal : 0;

    return {
      selectedCurrentValue,
      portfolioTotal: adjustedPortfolioTotal,
      portfolioWeight,
      classTotal: adjustedClassTotal,
      classWeight,
    };
  }, [assetRows, currentValueByAssetId, portfolioCurrentTotal, resolvedCurrentValue, selectedAsset]);

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

  const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), []);
  const firstTradeDate = useMemo(() => (
    assetTradeHistoryRows[0]?.date || null
  ), [assetTradeHistoryRows]);
  const firstSeriesDate = useMemo(() => {
    const sorted = [...marketSeries]
      .filter((row) => row.date)
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
    return sorted[0]?.date || null;
  }, [marketSeries]);
  const minSelectableStartDate = firstTradeDate || firstSeriesDate;

  useEffect(() => {
    if (!minSelectableStartDate) {
      setCustomRangeStart('');
      setCustomRangeEnd(todayIso);
      return;
    }

    setCustomRangeStart((previous) => {
      const normalizedPrevious = toIsoDate(previous);
      if (!normalizedPrevious) return minSelectableStartDate;
      if (normalizedPrevious < minSelectableStartDate) return minSelectableStartDate;
      if (normalizedPrevious > todayIso) return todayIso;
      return normalizedPrevious;
    });

    setCustomRangeEnd((previous) => {
      const normalizedPrevious = toIsoDate(previous);
      if (!normalizedPrevious) return todayIso;
      if (normalizedPrevious < minSelectableStartDate) return minSelectableStartDate;
      if (normalizedPrevious > todayIso) return todayIso;
      return normalizedPrevious;
    });
  }, [minSelectableStartDate, todayIso]);

  const effectiveChartRange = useMemo(() => {
    const minStart = minSelectableStartDate ? toIsoDate(minSelectableStartDate) : null;
    const maxEnd = toIsoDate(todayIso) || todayIso;

    if (chartPeriod === 'CUSTOM') {
      let start = toIsoDate(customRangeStart) || minStart;
      let end = toIsoDate(customRangeEnd) || maxEnd;

      if (minStart && start && start < minStart) start = minStart;
      if (end > maxEnd) end = maxEnd;
      if (start && start > end) start = end;

      return { start, end };
    }

    if (chartPeriod === 'MAX') {
      return { start: null, end: maxEnd };
    }

    const days = CHART_PERIOD_DAYS[chartPeriod];
    if (!Number.isFinite(days)) {
      return { start: null, end: maxEnd };
    }

    let start = addDaysToIsoDate(maxEnd, -Number(days));
    if (minStart && start < minStart) {
      start = minStart;
    }

    return { start, end: maxEnd };
  }, [chartPeriod, customRangeEnd, customRangeStart, minSelectableStartDate, todayIso]);

  const marketSeriesInRange = useMemo(() => (
    marketSeries
      .filter((row) => {
        if (!row.date) return false;
        if (effectiveChartRange.start && row.date < effectiveChartRange.start) return false;
        if (effectiveChartRange.end && row.date > effectiveChartRange.end) return false;
        return true;
      })
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime())
  ), [effectiveChartRange.end, effectiveChartRange.start, marketSeries]);

  const marketPriceChart = useMemo(() => {
    const pointsInput = marketSeriesInRange
      .filter((row) => row.date && Number.isFinite(row.close))
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
    if (!pointsInput.length) return null;

    const chartPadding = { top: 16, right: 20, bottom: 28, left: 20 };
    const chartWidth = HISTORY_CHART_WIDTH - chartPadding.left - chartPadding.right;
    const chartHeight = HISTORY_CHART_HEIGHT - chartPadding.top - chartPadding.bottom;
    const closes = pointsInput.map((row) => Number(row.close));
    const minClose = Math.min(...closes);
    const maxClose = Math.max(...closes);
    const spread = Math.max(maxClose - minClose, 0.01);
    const paddedMin = minClose - spread * 0.08;
    const paddedMax = maxClose + spread * 0.08;
    const yBase = chartPadding.top + chartHeight;

    const xFor = (index: number) => (
      pointsInput.length === 1
        ? chartPadding.left + chartWidth / 2
        : chartPadding.left + (index / (pointsInput.length - 1)) * chartWidth
    );
    const yFor = (close: number) => (
      chartPadding.top + (1 - (close - paddedMin) / (paddedMax - paddedMin)) * chartHeight
    );

    const points = pointsInput.map((row, index) => {
      const close = Number(row.close);
      const previousClose = index > 0 ? Number(pointsInput[index - 1].close) : null;
      const change = previousClose !== null ? close - previousClose : null;
      const changePct =
        previousClose !== null && Math.abs(previousClose) > Number.EPSILON
          ? (change! / previousClose) * 100
          : null;
      return {
        date: row.date,
        displayDate: row.display_date || row.date,
        close,
        change,
        changePct,
        index,
        x: xFor(index),
        y: yFor(close),
      };
    });

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
      firstDate: points[0].date,
      lastDate: points[points.length - 1].date,
      lastClose: points[points.length - 1].close,
      minClose: paddedMin,
      maxClose: paddedMax,
      padding: chartPadding,
      yBase,
    };
  }, [marketSeriesInRange]);

  const hoveredMarketPoint = useMemo(() => {
    if (!marketPriceChart || hoveredMarketPointIndex === null) return null;
    return marketPriceChart.points[hoveredMarketPointIndex] || null;
  }, [hoveredMarketPointIndex, marketPriceChart]);

  const hoveredMarketTooltipStyle = useMemo(() => {
    if (!hoveredMarketPoint) return null;
    const isRightSide = hoveredMarketPoint.x > HISTORY_CHART_WIDTH * 0.68;
    const isNearTop = hoveredMarketPoint.y < HISTORY_CHART_HEIGHT * 0.25;
    return {
      left: `${(hoveredMarketPoint.x / HISTORY_CHART_WIDTH) * 100}%`,
      top: `${(hoveredMarketPoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
      transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
    };
  }, [hoveredMarketPoint]);

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
      lastPrice: rows[rows.length - 1].price,
      minPrice: paddedMin,
      maxPrice: paddedMax,
      padding: chartPadding,
      yBase,
    };
  }, [assetTradeHistoryRows]);

  const selectedTradeTooltipStyle = useMemo(() => {
    if (!selectedTradePoint) return null;
    const isRightSide = selectedTradePoint.x > HISTORY_CHART_WIDTH * 0.68;
    const isNearTop = selectedTradePoint.y < HISTORY_CHART_HEIGHT * 0.25;
    return {
      left: `${(selectedTradePoint.x / HISTORY_CHART_WIDTH) * 100}%`,
      top: `${(selectedTradePoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
      transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
    };
  }, [selectedTradePoint]);

  const overviewFields = useMemo(() => {
    if (!selectedAsset) return [];

    return [
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
      { key: 'quantity', label: t('assets.modal.fields.quantity'), value: formatAssetQuantity(selectedAsset.quantity) },
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
    ];
  }, [formatAssetQuantity, formatDetailValue, numberLocale, resolvedAverageCost, resolvedCurrentPrice, resolvedCurrentValue, selectedAsset, t]);

  const marketFields = useMemo(() => {
    if (!selectedAsset) return [];

    return [
      {
        key: 'assetClass',
        label: t('assets.modal.fields.class'),
        value: t(`assets.classes.${selectedAsset.assetClass}`, { defaultValue: selectedAsset.assetClass }),
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
    ];
  }, [balanceMinusInvested, formatCountryDetail, formatDetailValue, formatSignedCurrency, positionStatus, quoteVsAverage, selectedAsset, t]);

  const insightsLinks = useMemo(() => {
    if (!assetInsights || assetInsights.status !== 'ready') return [];

    const links = [
      {
        key: 'status-invest',
        label: t('assets.modal.insights.links.statusInvest', { defaultValue: 'Status Invest' }),
        href: assetInsights.statusInvestUrl,
      },
      {
        key: 'b3',
        label: t('assets.modal.insights.links.b3', { defaultValue: 'B3' }),
        href: assetInsights.b3Url,
      },
      {
        key: 'clube-fii',
        label: t('assets.modal.insights.links.clubeFii', { defaultValue: 'Clube FII' }),
        href: assetInsights.clubeFiiUrl,
      },
      {
        key: 'fiis',
        label: t('assets.modal.insights.links.fiis', { defaultValue: 'FIIs.com.br' }),
        href: assetInsights.fiisUrl,
      },
    ];

    return links.filter((entry): entry is { key: string; label: string; href: string } => Boolean(entry.href));
  }, [assetInsights, t]);

  const insightsGroups = useMemo(() => {
    if (!selectedAsset) return [];

    const renderInsightsValue = (value: React.ReactNode) => {
      if (!assetInsights || assetInsights.status === 'loading') return t('common.loading');
      if (assetInsights.status === 'error') {
        return t('assets.modal.insights.unavailable', { defaultValue: 'Unavailable' });
      }
      return value;
    };

    const formatRatioAsPercent = (value: number | null, signed = false) => {
      const normalized = normalizeRatioMetric(value);
      if (normalized === null) return formatDetailValue(null);
      return signed
        ? formatSignedPercent(normalized * 100)
        : formatPercent(normalized);
    };

    const marginOfSafetyValue = (() => {
      if (!assetInsights || assetInsights.status !== 'ready') return renderInsightsValue(formatDetailValue(null));
      const ratio = normalizePercentValueToRatio(assetInsights.marginOfSafetyPct);
      if (ratio === null) return formatDetailValue(null);
      const trend = Math.abs(ratio) <= Number.EPSILON
        ? 'neutral'
        : ratio > 0
          ? 'positive'
          : 'negative';
      return (
        <span className={`assets-page__delta assets-page__delta--${trend}`}>
          {formatSignedPercent(ratio * 100)}
        </span>
      );
    })();

    return [
      {
        key: 'valuation',
        title: t('assets.modal.insights.groups.valuation', { defaultValue: 'Valuation' }),
        fields: [
          {
            key: 'currentPrice',
            label: t('assets.modal.insights.currentPrice', { defaultValue: 'Current Price' }),
            value: renderInsightsValue(
              assetInsights?.currentPrice != null
                ? formatCurrency(assetInsights.currentPrice, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'graham',
            label: t('assets.modal.insights.graham', { defaultValue: 'Graham Price' }),
            value: renderInsightsValue(
              assetInsights?.graham != null
                ? formatCurrency(assetInsights.graham, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'bazin',
            label: t('assets.modal.insights.bazin', { defaultValue: 'Bazin Price' }),
            value: renderInsightsValue(
              assetInsights?.bazin != null
                ? formatCurrency(assetInsights.bazin, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'fairPrice',
            label: t('assets.modal.insights.fairPrice', { defaultValue: 'Fair Price' }),
            value: renderInsightsValue(
              assetInsights?.fairPrice != null
                ? formatCurrency(assetInsights.fairPrice, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'marginSafety',
            label: t('assets.modal.insights.marginSafety', { defaultValue: 'Margin of Safety' }),
            value: marginOfSafetyValue,
          },
          {
            key: 'pe',
            label: t('assets.modal.insights.pe', { defaultValue: 'P/L' }),
            value: renderInsightsValue(
              assetInsights?.pe != null
                ? assetInsights.pe.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'pb',
            label: t('assets.modal.insights.pb', { defaultValue: 'P/VP' }),
            value: renderInsightsValue(
              assetInsights?.pb != null
                ? assetInsights.pb.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
        ],
      },
      {
        key: 'fundamentals',
        title: t('assets.modal.insights.groups.fundamentals', { defaultValue: 'Fundamentals' }),
        fields: [
          {
            key: 'roe',
            label: t('assets.modal.insights.roe', { defaultValue: 'ROE' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.roe ?? null, true)),
          },
          {
            key: 'roa',
            label: t('assets.modal.insights.roa', { defaultValue: 'ROA' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.roa ?? null, true)),
          },
          {
            key: 'roic',
            label: t('assets.modal.insights.roic', { defaultValue: 'ROIC' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.roic ?? null, true)),
          },
          {
            key: 'lpa',
            label: t('assets.modal.insights.lpa', { defaultValue: 'LPA (EPS)' }),
            value: renderInsightsValue(
              assetInsights?.lpa != null
                ? assetInsights.lpa.toLocaleString(numberLocale, { maximumFractionDigits: 4 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'vpa',
            label: t('assets.modal.insights.vpa', { defaultValue: 'VPA (Book Value)' }),
            value: renderInsightsValue(
              assetInsights?.vpa != null
                ? assetInsights.vpa.toLocaleString(numberLocale, { maximumFractionDigits: 4 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'netMargin',
            label: t('assets.modal.insights.netMargin', { defaultValue: 'Net Margin' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.netMargin ?? null, true)),
          },
          {
            key: 'ebitMargin',
            label: t('assets.modal.insights.ebitMargin', { defaultValue: 'EBIT Margin' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.ebitMargin ?? null, true)),
          },
          {
            key: 'payout',
            label: t('assets.modal.insights.payout', { defaultValue: 'Payout' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.payout ?? null)),
          },
          {
            key: 'evEbitda',
            label: t('assets.modal.insights.evEbitda', { defaultValue: 'EV/EBITDA' }),
            value: renderInsightsValue(
              assetInsights?.evEbitda != null
                ? assetInsights.evEbitda.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'netDebtEbitda',
            label: t('assets.modal.insights.netDebtEbitda', { defaultValue: 'Net Debt / EBITDA' }),
            value: renderInsightsValue(
              assetInsights?.netDebtEbitda != null
                ? assetInsights.netDebtEbitda.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
        ],
      },
      {
        key: 'profile',
        title: t('assets.modal.insights.groups.profile', { defaultValue: 'Company Profile' }),
        fields: [
          {
            key: 'sector',
            label: t('assets.modal.insights.sector', { defaultValue: 'Sector' }),
            value: renderInsightsValue(assetInsights?.sector || formatDetailValue(null)),
          },
          {
            key: 'industry',
            label: t('assets.modal.insights.industry', { defaultValue: 'Industry / Segment' }),
            value: renderInsightsValue(assetInsights?.industry || formatDetailValue(null)),
          },
          {
            key: 'marketCap',
            label: t('assets.modal.insights.marketCap', { defaultValue: 'Market Cap' }),
            value: renderInsightsValue(
              assetInsights?.marketCap != null
                ? formatCurrency(assetInsights.marketCap, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'averageVolume',
            label: t('assets.modal.insights.averageVolume', { defaultValue: 'Avg Volume' }),
            value: renderInsightsValue(
              assetInsights?.averageVolume != null
                ? formatCompactNumber(assetInsights.averageVolume)
                : formatDetailValue(null)
            ),
          },
        ],
      },
      {
        key: 'source',
        title: t('assets.modal.insights.groups.source', { defaultValue: 'Data Quality' }),
        fields: [
          {
            key: 'sourceLabel',
            label: t('assets.modal.insights.source', { defaultValue: 'Data Source' }),
            value: renderInsightsValue(assetInsights?.source || formatDetailValue(null)),
          },
          {
            key: 'fetchedAt',
            label: t('assets.modal.insights.fetchedAt', { defaultValue: 'Last Sync' }),
            value: renderInsightsValue(
              assetInsights?.fetchedAt
                ? formatDate(assetInsights.fetchedAt, numberLocale)
                : formatDetailValue(null)
            ),
          },
        ],
      },
    ];
  }, [assetInsights, formatCompactNumber, formatDetailValue, formatPercent, formatSignedPercent, numberLocale, selectedAsset, t]);

  const insightsLinksContent = useMemo<React.ReactNode>(() => {
    if (!assetInsights || assetInsights.status === 'loading') return t('common.loading');
    if (assetInsights.status === 'error') {
      return t('assets.modal.insights.unavailable', { defaultValue: 'Unavailable' });
    }
    if (insightsLinks.length === 0) return formatDetailValue(null);

    return (
      <span className="assets-page__insights-links">
        {insightsLinks.map((link, index) => (
          <span key={link.key}>
            <a
              href={link.href}
              target="_blank"
              rel="noreferrer"
              className="assets-page__provents-source-link"
            >
              {link.label}
            </a>
            {index < insightsLinks.length - 1 ? <span className="assets-page__insights-separator"> • </span> : null}
          </span>
        ))}
      </span>
    );
  }, [assetInsights, formatDetailValue, insightsLinks, t]);

  return (
    <Layout>
      <div className="asset-details-page">
        <div className="asset-details-page__header">
          <div>
            <h1 className="asset-details-page__title">
              {selectedAsset
                ? `${selectedAsset.ticker} • ${selectedAsset.name}`
                : t('assets.detail.title', { defaultValue: 'Asset Details' })}
            </h1>
            <p className="asset-details-page__subtitle">
              {t('assets.detail.subtitle', { defaultValue: 'Complete detail view for the selected asset.' })}
            </p>
          </div>
          <button
            type="button"
            className="asset-details-page__back"
            onClick={() => {
              const query = portfolioId ? `?portfolioId=${encodeURIComponent(portfolioId)}` : '';
              navigate(`/assets${query}`);
            }}
          >
            {t('assets.detail.backToAssets', { defaultValue: 'Back to assets' })}
          </button>
        </div>

        {loading ? (
          <div className="asset-details-page__state">{t('common.loading')}</div>
        ) : null}

        {!loading && !selectedAsset ? (
          <div className="asset-details-page__state">
            {t('assets.detail.notFound', { defaultValue: 'Asset not found for the selected portfolio.' })}
          </div>
        ) : null}

        {!loading && selectedAsset ? (
          <>
            <div className="asset-details-page__grid">
              <section className="asset-details-page__card asset-details-page__card--two-cols">
                <h2>{t('assets.modal.sections.overview')}</h2>
                <dl>
                  {overviewFields.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>

              <section className="asset-details-page__card asset-details-page__card--two-cols">
                <h2>{t('assets.modal.sections.market')}</h2>
                <dl>
                  {marketFields.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>

            </div>

            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--insights">
              <h2>{t('assets.modal.sections.insights', { defaultValue: 'Fundamentals & Fair Value' })}</h2>
              <div className="asset-details-page__insights-grid">
                {insightsGroups.map((group) => (
                  <article key={group.key} className="asset-details-page__insights-group">
                    <h3>{group.title}</h3>
                    <dl>
                      {group.fields.map((field) => (
                        <div key={`${group.key}-${field.key}`}>
                          <dt>{field.label}</dt>
                          <dd>{field.value}</dd>
                        </div>
                      ))}
                    </dl>
                  </article>
                ))}
                <article className="asset-details-page__insights-group asset-details-page__insights-group--links">
                  <h3>{t('assets.modal.insights.links.label', { defaultValue: 'External Sources' })}</h3>
                  <div className="asset-details-page__insights-links">{insightsLinksContent}</div>
                </article>
              </div>
            </section>

            {selectedAssetWeightMetrics ? (
              <section className="asset-details-page__card asset-details-page__card--full">
                <div className="assets-page__weights">
                  <h3>{t('assets.modal.weights.title')}</h3>
                  <div className="assets-page__weights-grid">
                    <article className="assets-page__weight-card">
                      <h4>{t('assets.modal.weights.portfolio')}</h4>
                      <div className="assets-page__weight-chart">
                        <svg viewBox="0 0 120 120" aria-hidden="true">
                          <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r="44" />
                          <circle
                            className="assets-page__weight-ring assets-page__weight-ring--portfolio"
                            cx="60"
                            cy="60"
                            r="44"
                            strokeDasharray={`${2 * Math.PI * 44} ${2 * Math.PI * 44}`}
                            strokeDashoffset={(2 * Math.PI * 44) * (1 - Math.max(0, Math.min(1, selectedAssetWeightMetrics.portfolioWeight)))}
                          />
                        </svg>
                        <div className="assets-page__weight-chart-center">
                          <strong>{formatPercent(selectedAssetWeightMetrics.portfolioWeight)}</strong>
                        </div>
                      </div>
                      <div className="assets-page__weight-meta">
                        <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(selectedAssetWeightMetrics.selectedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                        <span>{t('assets.modal.weights.portfolioTotal')}: <strong>{formatCurrency(selectedAssetWeightMetrics.portfolioTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                      </div>
                    </article>

                    <article className="assets-page__weight-card">
                      <h4>{t('assets.modal.weights.class', { className: t(`assets.classes.${selectedAsset.assetClass}`, { defaultValue: selectedAsset.assetClass }) })}</h4>
                      <div className="assets-page__weight-chart">
                        <svg viewBox="0 0 120 120" aria-hidden="true">
                          <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r="44" />
                          <circle
                            className="assets-page__weight-ring assets-page__weight-ring--class"
                            cx="60"
                            cy="60"
                            r="44"
                            strokeDasharray={`${2 * Math.PI * 44} ${2 * Math.PI * 44}`}
                            strokeDashoffset={(2 * Math.PI * 44) * (1 - Math.max(0, Math.min(1, selectedAssetWeightMetrics.classWeight)))}
                          />
                        </svg>
                        <div className="assets-page__weight-chart-center">
                          <strong>{formatPercent(selectedAssetWeightMetrics.classWeight)}</strong>
                        </div>
                      </div>
                      <div className="assets-page__weight-meta">
                        <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(selectedAssetWeightMetrics.selectedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                        <span>{t('assets.modal.weights.classTotal')}: <strong>{formatCurrency(selectedAssetWeightMetrics.classTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                      </div>
                    </article>
                  </div>
                </div>
              </section>
            ) : null}

            <section className="asset-details-page__card asset-details-page__card--full">
              <div className="assets-page__history">
                <h3>{t('assets.modal.priceHistory.title')}</h3>
                <div className="asset-details-page__chart-controls">
                  <label htmlFor="asset-period-select">
                    {t('assets.modal.priceHistory.periodLabel', { defaultValue: 'Period' })}
                  </label>
                  <select
                    id="asset-period-select"
                    className="asset-details-page__chart-select"
                    value={chartPeriod}
                    onChange={(event) => {
                      setChartPeriod(event.target.value as ChartPeriodPreset);
                      setHoveredMarketPointIndex(null);
                    }}
                  >
                    <option value="MAX">{t('assets.modal.priceHistory.period.max', { defaultValue: 'MAX' })}</option>
                    <option value="5A">{t('assets.modal.priceHistory.period.5a', { defaultValue: '5Y' })}</option>
                    <option value="2A">{t('assets.modal.priceHistory.period.2a', { defaultValue: '2Y' })}</option>
                    <option value="1A">{t('assets.modal.priceHistory.period.1a', { defaultValue: '1Y' })}</option>
                    <option value="6M">{t('assets.modal.priceHistory.period.6m', { defaultValue: '6M' })}</option>
                    <option value="3M">{t('assets.modal.priceHistory.period.3m', { defaultValue: '3M' })}</option>
                    <option value="1M">{t('assets.modal.priceHistory.period.1m', { defaultValue: '1M' })}</option>
                    <option value="CUSTOM">{t('assets.modal.priceHistory.period.custom', { defaultValue: 'Custom' })}</option>
                  </select>
                  {chartPeriod === 'CUSTOM' ? (
                    <div className="asset-details-page__chart-range">
                      <label htmlFor="asset-range-start">
                        {t('assets.modal.priceHistory.startDate', { defaultValue: 'From' })}
                      </label>
                      <input
                        id="asset-range-start"
                        type="date"
                        value={customRangeStart}
                        min={minSelectableStartDate || undefined}
                        max={todayIso}
                        onChange={(event) => {
                          const normalized = toIsoDate(event.target.value) || '';
                          if (!normalized) {
                            setCustomRangeStart('');
                            return;
                          }
                          let nextStart = normalized;
                          if (minSelectableStartDate && nextStart < minSelectableStartDate) {
                            nextStart = minSelectableStartDate;
                          }
                          if (nextStart > todayIso) nextStart = todayIso;
                          setCustomRangeStart(nextStart);
                          if (customRangeEnd && customRangeEnd < nextStart) {
                            setCustomRangeEnd(nextStart);
                          }
                        }}
                      />

                      <label htmlFor="asset-range-end">
                        {t('assets.modal.priceHistory.endDate', { defaultValue: 'To' })}
                      </label>
                      <input
                        id="asset-range-end"
                        type="date"
                        value={customRangeEnd}
                        min={customRangeStart || minSelectableStartDate || undefined}
                        max={todayIso}
                        onChange={(event) => {
                          const normalized = toIsoDate(event.target.value) || '';
                          if (!normalized) {
                            setCustomRangeEnd('');
                            return;
                          }
                          let nextEnd = normalized;
                          if (nextEnd > todayIso) nextEnd = todayIso;
                          if (customRangeStart && nextEnd < customRangeStart) {
                            nextEnd = customRangeStart;
                          }
                          if (minSelectableStartDate && nextEnd < minSelectableStartDate) {
                            nextEnd = minSelectableStartDate;
                          }
                          setCustomRangeEnd(nextEnd);
                        }}
                      />
                    </div>
                  ) : null}
                </div>

                {marketSeriesLoading ? (
                  <p className="assets-page__history-empty">{t('assets.modal.priceHistory.loading')}</p>
                ) : null}
                {!marketSeriesLoading && !marketPriceChart ? (
                  <p className="assets-page__history-empty">{t('assets.modal.priceHistory.empty')}</p>
                ) : null}
                {!marketSeriesLoading && marketPriceChart ? (
                  <div className="assets-page__market-chart">
                    <svg
                      viewBox={`0 0 ${HISTORY_CHART_WIDTH} ${HISTORY_CHART_HEIGHT}`}
                      role="img"
                      aria-label={t('assets.modal.priceHistory.chart')}
                      onMouseMove={(event) => {
                        const bounds = event.currentTarget.getBoundingClientRect();
                        const relativeX = ((event.clientX - bounds.left) / bounds.width) * HISTORY_CHART_WIDTH;

                        let nearestIndex = 0;
                        let nearestDistance = Number.POSITIVE_INFINITY;
                        marketPriceChart.points.forEach((point, index) => {
                          const distance = Math.abs(point.x - relativeX);
                          if (distance < nearestDistance) {
                            nearestDistance = distance;
                            nearestIndex = index;
                          }
                        });

                        setHoveredMarketPointIndex(nearestIndex);
                      }}
                      onMouseLeave={() => setHoveredMarketPointIndex(null)}
                    >
                      <defs>
                        <linearGradient id="asset-market-history-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                          <stop offset="0%" stopColor="rgba(34, 211, 238, 0.34)" />
                          <stop offset="100%" stopColor="rgba(34, 211, 238, 0.02)" />
                        </linearGradient>
                      </defs>
                      <line
                        className="assets-page__history-grid"
                        x1={marketPriceChart.padding.left}
                        x2={HISTORY_CHART_WIDTH - marketPriceChart.padding.right}
                        y1={marketPriceChart.padding.top}
                        y2={marketPriceChart.padding.top}
                      />
                      <line
                        className="assets-page__history-grid"
                        x1={marketPriceChart.padding.left}
                        x2={HISTORY_CHART_WIDTH - marketPriceChart.padding.right}
                        y1={marketPriceChart.padding.top + ((marketPriceChart.yBase - marketPriceChart.padding.top) / 2)}
                        y2={marketPriceChart.padding.top + ((marketPriceChart.yBase - marketPriceChart.padding.top) / 2)}
                      />
                      <line
                        className="assets-page__history-grid"
                        x1={marketPriceChart.padding.left}
                        x2={HISTORY_CHART_WIDTH - marketPriceChart.padding.right}
                        y1={marketPriceChart.yBase}
                        y2={marketPriceChart.yBase}
                      />
                      <path className="assets-page__market-area" d={marketPriceChart.areaPath} fill="url(#asset-market-history-gradient)" />
                      <path className="assets-page__market-line" d={marketPriceChart.polyline} />
                      {hoveredMarketPoint ? (
                        <circle
                          className="assets-page__market-hover-point"
                          cx={hoveredMarketPoint.x}
                          cy={hoveredMarketPoint.y}
                          r={5}
                        />
                      ) : null}
                    </svg>
                    {hoveredMarketPoint && hoveredMarketTooltipStyle ? (
                      <div className="assets-page__history-tooltip" style={hoveredMarketTooltipStyle}>
                        <div className="assets-page__history-tooltip-header">
                          <strong>{formatDate(hoveredMarketPoint.displayDate, numberLocale)}</strong>
                        </div>
                        <div className="assets-page__history-tooltip-grid">
                          <span>{t('assets.modal.priceHistory.close')}</span>
                          <strong>{formatCurrency(hoveredMarketPoint.close, selectedAsset.currency || 'BRL', numberLocale)}</strong>
                          <span>{t('assets.modal.priceHistory.change')}</span>
                          <strong>
                            {hoveredMarketPoint.change === null
                              ? '-'
                              : formatSignedCurrency(hoveredMarketPoint.change, selectedAsset.currency || 'BRL')}
                          </strong>
                          <span>{t('assets.modal.priceHistory.changePct')}</span>
                          <strong>
                            {hoveredMarketPoint.changePct === null
                              ? '-'
                              : formatSignedPercent(hoveredMarketPoint.changePct)}
                          </strong>
                        </div>
                      </div>
                    ) : null}
                    <div className="assets-page__history-scale">
                      <span>{formatDate(marketPriceChart.firstDate, numberLocale)}</span>
                      <span>{formatCurrency(marketPriceChart.minClose, selectedAsset.currency || 'BRL', numberLocale)}</span>
                      <span>{formatCurrency(marketPriceChart.maxClose, selectedAsset.currency || 'BRL', numberLocale)}</span>
                      <span>{formatDate(marketPriceChart.lastDate, numberLocale)}</span>
                    </div>
                    <div className="assets-page__history-meta">
                      <span>{t('assets.modal.priceHistory.lastClose')}: <strong>{formatCurrency(marketPriceChart.lastClose, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                    </div>
                  </div>
                ) : null}

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
                        onClick={() => setSelectedTradePoint(null)}
                      >
                        <defs>
                          <linearGradient id="asset-trade-history-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
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
                            <path className="assets-page__history-area" d={assetTradeHistoryChart.areaPath} fill="url(#asset-trade-history-gradient)" />
                            <path className="assets-page__history-line" d={assetTradeHistoryChart.polyline} />
                            {assetTradeHistoryChart.points.map((point) => (
                              <circle
                                key={`${point.transId}-${point.date}-${point.price}-${point.index}`}
                                className={`assets-page__history-point assets-page__history-point--${point.type}`}
                                cx={point.x}
                                cy={point.y}
                                r={selectedTradePoint?.transId === point.transId && selectedTradePoint?.index === point.index ? 6 : 4}
                                role="button"
                                tabIndex={0}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setSelectedTradePoint(point);
                                }}
                                onKeyDown={(event) => {
                                  if (event.key === 'Enter' || event.key === ' ') {
                                    event.preventDefault();
                                    setSelectedTradePoint(point);
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
                      {selectedTradePoint && selectedTradeTooltipStyle ? (
                        <div className="assets-page__history-tooltip" style={selectedTradeTooltipStyle}>
                          <div className="assets-page__history-tooltip-header">
                            <span className={`assets-page__history-type assets-page__history-type--${selectedTradePoint.type}`}>
                              {t(`transactions.types.${selectedTradePoint.type}`, { defaultValue: selectedTradePoint.type })}
                            </span>
                            <strong>{formatDate(selectedTradePoint.date, numberLocale)}</strong>
                          </div>
                          <div className="assets-page__history-tooltip-grid">
                            <span>{t('assets.modal.history.quantity')}</span>
                            <strong>{formatAssetQuantity(selectedTradePoint.quantity)}</strong>
                            <span>{t('assets.modal.history.price')}</span>
                            <strong>{formatCurrency(selectedTradePoint.price, selectedTradePoint.currency, numberLocale)}</strong>
                            <span>{t('assets.modal.history.amount')}</span>
                            <strong>{formatCurrency(selectedTradePoint.amount, selectedTradePoint.currency, numberLocale)}</strong>
                            <span>{t('assets.modal.history.source')}</span>
                            <strong>{selectedTradePoint.source || '-'}</strong>
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
            </section>
          </>
        ) : null}
      </div>
    </Layout>
  );
};

export default AssetDetailsPage;
