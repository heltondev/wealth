import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router';
import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type Asset,
  type CompareAssetRow,
  type CompareAssetsResponse,
} from '../services/api';
import { formatCurrency } from '../utils/formatters';
import './CompareAssetsPage.scss';

const MIN_ASSETS_TO_COMPARE = 2;
const MAX_ASSETS_TO_COMPARE = 6;

type FiiTopCompositionItem = {
  label: string;
  allocationPct: number | null;
};

type FiiComparisonSnapshot = {
  classification: string | null;
  segment: string | null;
  administrator: string | null;
  managerName: string | null;
  quotaCount: number | null;
  dy12m: string | null;
  dySector: string | null;
  dyCategory: string | null;
  dyMarket: string | null;
  lastDividendPerUnit: string | null;
  dividend12mPerUnit: string | null;
  yield12mOnQuote: string | null;
  topComposition: FiiTopCompositionItem[];
};

type ComparisonMetricRow = {
  key: string;
  label: string;
  value: (entry: CompareAssetRow) => string;
  tone?: (entry: CompareAssetRow) => 'positive' | 'negative' | null;
};

type FiiMetricRow = {
  key: string;
  label: string;
  value: (entry: FiiComparisonSnapshot | null) => string;
};

const toObjectRecord = (value: unknown): Record<string, unknown> => (
  value && typeof value === 'object' ? value as Record<string, unknown> : {}
);

const toText = (value: unknown): string | null => {
  const text = String(value || '').trim();
  return text || null;
};

const toUpperTicker = (value: unknown): string => (
  String(value || '').trim().toUpperCase()
);

const toNumber = (value: unknown): number | null => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseLocalizedNumber = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;

  let text = String(value).trim();
  if (!text) return null;
  text = text.replace(/[^\d,.-]/g, '');
  if (!text) return null;

  const hasComma = text.includes(',');
  const hasDot = text.includes('.');
  if (hasComma && hasDot) {
    if (text.lastIndexOf(',') > text.lastIndexOf('.')) {
      text = text.replace(/\./g, '').replace(',', '.');
    } else {
      text = text.replace(/,/g, '');
    }
  } else if (hasComma) {
    text = text.replace(/\./g, '').replace(',', '.');
  }

  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
};

const toStringArray = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => toText(entry))
    .filter((entry): entry is string => Boolean(entry));
};

const normalizeAssetClass = (value: unknown): string => (
  String(value || '').trim().toLowerCase()
);

const formatAssetClassLabel = (value: unknown): string => {
  const normalized = normalizeAssetClass(value);
  const map: Record<string, string> = {
    fii: 'FII',
    fiagro: 'FIAGRO',
    etf: 'ETF',
    bdr: 'BDR',
    reit: 'REIT',
    stock: 'Stock',
    bond: 'Bond',
    crypto: 'Crypto',
    derivative: 'Derivative',
    cash: 'Cash',
    fund: 'Fund',
    fixed_income: 'Fixed Income',
  };
  if (map[normalized]) return map[normalized];
  if (!normalized) return '—';
  return normalized.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
};

const parseDyComparatorValue = (rawFundInfo: Record<string, unknown>, kind: string): string | null => {
  const comparator = toObjectRecord(
    rawFundInfo.dividend_yield_comparator
    ?? rawFundInfo.dividendYieldComparator
  );
  const items = Array.isArray(comparator.items) ? comparator.items : [];
  for (const item of items) {
    const normalized = String(toObjectRecord(item).kind || '').trim().toLowerCase();
    if (normalized !== kind) continue;
    return toText(toObjectRecord(item).value);
  }
  return null;
};

const parseDividendsResumeCell = (
  rawFundInfo: Record<string, unknown>,
  column: 'returnByUnit' | 'relativeToQuote',
  periodMatch: (period: string) => boolean
): string | null => {
  const resume = toObjectRecord(rawFundInfo.dividends_resume ?? rawFundInfo.dividendsResume);
  const table = toObjectRecord(resume.table);
  const periods = toStringArray(table.periods ?? table.periodos);
  const returnByUnit = toStringArray(table.return_by_unit ?? table.returnByUnit);
  const relativeToQuote = toStringArray(table.relative_to_quote ?? table.relativeToQuote);
  const selectedColumn = column === 'returnByUnit' ? returnByUnit : relativeToQuote;
  if (periods.length === 0 || selectedColumn.length === 0) return null;

  const index = periods.findIndex(periodMatch);
  const fallbackIndex = Math.min(selectedColumn.length - 1, 0);
  const finalIndex = index >= 0 ? index : fallbackIndex;
  return toText(selectedColumn[finalIndex] || null);
};

const parseFiiComparisonSnapshot = (payload: unknown): FiiComparisonSnapshot | null => {
  const root = toObjectRecord(payload);
  const fundInfo = toObjectRecord(root.fund_info ?? root.fundInfo);
  if (Object.keys(fundInfo).length === 0) return null;

  const fundPortfolioRows = Array.isArray(root.fund_portfolio) ? root.fund_portfolio : [];
  const topComposition = fundPortfolioRows
    .map((row) => {
      const entry = toObjectRecord(row);
      const label = toText(entry.label ?? entry.name ?? entry.title);
      const allocationPct = parseLocalizedNumber(entry.allocation_pct ?? entry.allocationPct);
      if (!label) return null;
      return { label, allocationPct };
    })
    .filter((entry): entry is FiiTopCompositionItem => Boolean(entry))
    .sort((left, right) => (right.allocationPct || 0) - (left.allocationPct || 0))
    .slice(0, 3);

  const result: FiiComparisonSnapshot = {
    classification: toText(fundInfo.classification),
    segment: toText(fundInfo.segment),
    administrator: toText(fundInfo.administrator),
    managerName: toText(fundInfo.manager_name ?? fundInfo.managerName),
    quotaCount: parseLocalizedNumber(fundInfo.quota_count ?? fundInfo.quotaCount),
    dy12m: parseDyComparatorValue(fundInfo, 'principal'),
    dySector: parseDyComparatorValue(fundInfo, 'sector'),
    dyCategory: parseDyComparatorValue(fundInfo, 'category'),
    dyMarket: parseDyComparatorValue(fundInfo, 'market'),
    lastDividendPerUnit: parseDividendsResumeCell(
      fundInfo,
      'returnByUnit',
      (period) => /último|last/i.test(period)
    ),
    dividend12mPerUnit: parseDividendsResumeCell(
      fundInfo,
      'returnByUnit',
      (period) => /\b12\b/i.test(period)
    ),
    yield12mOnQuote: parseDividendsResumeCell(
      fundInfo,
      'relativeToQuote',
      (period) => /\b12\b/i.test(period)
    ),
    topComposition,
  };

  const hasAnyValue = Object.entries(result).some(([key, value]) => {
    if (key === 'topComposition') return Array.isArray(value) && value.length > 0;
    if (value === null || value === undefined) return false;
    return String(value).trim() !== '';
  });
  return hasAnyValue ? result : null;
};

const CompareAssetsPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio, assets } = usePortfolioData();

  const [tickerPickerValue, setTickerPickerValue] = useState('');
  const [selectedTickers, setSelectedTickers] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [fiiLoading, setFiiLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<CompareAssetsResponse | null>(null);
  const [fiiSnapshotsByTicker, setFiiSnapshotsByTicker] = useState<Record<string, FiiComparisonSnapshot>>({});

  const requestIdRef = useRef(0);
  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const formatNumber = useCallback((value: number | null, digits = 2) => {
    if (value === null || !Number.isFinite(value)) return t('assets.modal.noValue');
    return value.toLocaleString(numberLocale, {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }, [numberLocale, t]);

  const formatPercent = useCallback((value: number | null, options?: { signed?: boolean; ratio?: boolean }) => {
    if (value === null || !Number.isFinite(value)) return t('assets.modal.noValue');
    const normalized = options?.ratio && Math.abs(value) <= 1 ? value * 100 : value;
    const prefix = options?.signed && normalized > 0 ? '+' : '';
    return `${prefix}${normalized.toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}%`;
  }, [numberLocale, t]);

  const formatAssetCurrency = useCallback((value: number | null, currency: string) => {
    if (value === null || !Number.isFinite(value)) return t('assets.modal.noValue');
    return formatCurrency(value, currency || 'BRL', numberLocale);
  }, [numberLocale, t]);

  const activeAssets = useMemo(() => (
    assets
      .filter((asset) => String(asset.status || 'active').toLowerCase() === 'active' && String(asset.ticker || '').trim() !== '')
      .sort((left, right) => String(left.ticker || '').localeCompare(String(right.ticker || ''), numberLocale))
  ), [assets, numberLocale]);

  const activeAssetByTicker = useMemo(() => {
    const map = new Map<string, Asset>();
    for (const asset of activeAssets) {
      map.set(toUpperTicker(asset.ticker), asset);
    }
    return map;
  }, [activeAssets]);

  const portfolioOptions = useMemo(() => (
    portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name }))
  ), [portfolios]);

  const tickerOptions = useMemo(() => (
    activeAssets.map((asset) => ({
      value: toUpperTicker(asset.ticker),
      label: `${toUpperTicker(asset.ticker)} • ${asset.name}`,
    }))
  ), [activeAssets]);

  const selectedTickerSet = useMemo(() => new Set(selectedTickers.map((ticker) => toUpperTicker(ticker))), [selectedTickers]);

  const availableTickerOptions = useMemo(() => (
    tickerOptions.filter((option) => !selectedTickerSet.has(toUpperTicker(option.value)))
  ), [selectedTickerSet, tickerOptions]);

  const tickerPickerOptions = useMemo(() => (
    [
      { value: '', label: t('compare.selectTicker') },
      ...availableTickerOptions,
    ]
  ), [availableTickerOptions, t]);

  useEffect(() => {
    if (!selectedPortfolio) {
      setSelectedTickers([]);
      setTickerPickerValue('');
      setPayload(null);
      setFiiSnapshotsByTicker({});
      setError(null);
      return;
    }

    const availableTickers = tickerOptions.map((option) => toUpperTicker(option.value));
    const availableSet = new Set(availableTickers);

    setSelectedTickers((previous) => {
      const normalizedPrevious = previous.map((ticker) => toUpperTicker(ticker));
      const deduped = normalizedPrevious
        .map((ticker) => toUpperTicker(ticker))
        .filter((ticker, index, list) => list.indexOf(ticker) === index && availableSet.has(ticker));
      const next = [...deduped];
      if (next.length < MIN_ASSETS_TO_COMPARE) {
        for (const ticker of availableTickers) {
          if (next.length >= MIN_ASSETS_TO_COMPARE) break;
          if (!next.includes(ticker)) next.push(ticker);
        }
      }

      if (
        next.length === normalizedPrevious.length &&
        next.every((ticker, index) => ticker === normalizedPrevious[index])
      ) {
        return previous;
      }
      return next;
    });
  }, [selectedPortfolio, tickerOptions]);

  useEffect(() => {
    if (!tickerPickerValue && availableTickerOptions.length > 0) {
      setTickerPickerValue(toUpperTicker(availableTickerOptions[0].value));
      return;
    }
    if (tickerPickerValue && availableTickerOptions.some((option) => toUpperTicker(option.value) === toUpperTicker(tickerPickerValue))) {
      return;
    }
    setTickerPickerValue('');
  }, [availableTickerOptions, tickerPickerValue]);

  const orderedComparison = useMemo<CompareAssetRow[]>(() => {
    if (!payload || !Array.isArray(payload.comparison)) return [];
    const map = new Map(payload.comparison.map((row) => [toUpperTicker(row.ticker), row]));
    return selectedTickers
      .map((ticker) => map.get(toUpperTicker(ticker)))
      .filter((row): row is CompareAssetRow => Boolean(row));
  }, [payload, selectedTickers]);

  const runComparison = useCallback(async () => {
    if (!selectedPortfolio || selectedTickers.length < MIN_ASSETS_TO_COMPARE) {
      setPayload(null);
      setFiiSnapshotsByTicker({});
      setError(null);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    setLoading(true);
    setFiiLoading(false);
    setError(null);

    try {
      const response = await api.compareAssets(selectedTickers, selectedPortfolio);
      if (requestId !== requestIdRef.current) return;
      setPayload(response);

      const fiiTickers = (response.comparison || [])
        .filter((entry) => normalizeAssetClass(entry.assetClass) === 'fii')
        .map((entry) => toUpperTicker(entry.ticker));

      if (fiiTickers.length >= MIN_ASSETS_TO_COMPARE) {
        setFiiLoading(true);
        const entries = await Promise.all(fiiTickers.map(async (ticker) => {
          try {
            const financials = await api.getAssetFinancials(ticker, selectedPortfolio);
            const snapshot = parseFiiComparisonSnapshot(financials);
            return [ticker, snapshot] as const;
          } catch {
            return [ticker, null] as const;
          }
        }));

        if (requestId !== requestIdRef.current) return;

        const nextSnapshots = entries.reduce<Record<string, FiiComparisonSnapshot>>((accumulator, [ticker, snapshot]) => {
          if (snapshot) accumulator[ticker] = snapshot;
          return accumulator;
        }, {});
        setFiiSnapshotsByTicker(nextSnapshots);
      } else {
        setFiiSnapshotsByTicker({});
      }
    } catch (reason) {
      if (requestId !== requestIdRef.current) return;
      setPayload(null);
      setFiiSnapshotsByTicker({});
      setError(reason instanceof Error ? reason.message : t('compare.loadError'));
    } finally {
      if (requestId === requestIdRef.current) {
        setLoading(false);
        setFiiLoading(false);
      }
    }
  }, [selectedPortfolio, selectedTickers, t]);

  useEffect(() => {
    if (!selectedPortfolio || selectedTickers.length < MIN_ASSETS_TO_COMPARE) {
      setPayload(null);
      setFiiSnapshotsByTicker({});
      setError(null);
      return;
    }
    void runComparison();
  }, [runComparison, selectedPortfolio, selectedTickers]);

  const addSelectedTicker = useCallback(() => {
    const ticker = toUpperTicker(tickerPickerValue);
    if (!ticker) return;
    setSelectedTickers((previous) => {
      if (previous.includes(ticker) || previous.length >= MAX_ASSETS_TO_COMPARE) return previous;
      return [...previous, ticker];
    });
  }, [tickerPickerValue]);

  const removeSelectedTicker = useCallback((ticker: string) => {
    const normalized = toUpperTicker(ticker);
    setSelectedTickers((previous) => previous.filter((item) => toUpperTicker(item) !== normalized));
  }, []);

  const resetSelectedTickers = useCallback(() => {
    const fallback = tickerOptions.slice(0, MIN_ASSETS_TO_COMPARE).map((option) => toUpperTicker(option.value));
    setSelectedTickers(fallback);
  }, [tickerOptions]);

  const comparisonMetrics = useMemo<ComparisonMetricRow[]>(() => ([
    {
      key: 'assetClass',
      label: t('compare.metrics.assetClass'),
      value: (entry) => formatAssetClassLabel(entry.assetClass),
    },
    {
      key: 'currency',
      label: t('compare.metrics.currency'),
      value: (entry) => toUpperTicker(entry.currency || '') || t('assets.modal.noValue'),
    },
    {
      key: 'currentPrice',
      label: t('compare.metrics.currentPrice'),
      value: (entry) => formatAssetCurrency(toNumber(entry.current_price), entry.currency || 'BRL'),
    },
    {
      key: 'fairPrice',
      label: t('compare.metrics.fairPrice'),
      value: (entry) => formatAssetCurrency(toNumber(entry.fair_price), entry.currency || 'BRL'),
    },
    {
      key: 'marginOfSafety',
      label: t('compare.metrics.marginOfSafety'),
      value: (entry) => formatPercent(toNumber(entry.margin_of_safety_pct), { signed: true }),
      tone: (entry) => {
        const value = toNumber(entry.margin_of_safety_pct);
        if (value === null) return null;
        if (value > 0) return 'positive';
        if (value < 0) return 'negative';
        return null;
      },
    },
    {
      key: 'volatility',
      label: t('compare.metrics.volatility'),
      value: (entry) => formatPercent(toNumber(entry.risk?.volatility)),
    },
    {
      key: 'drawdown',
      label: t('compare.metrics.drawdown'),
      value: (entry) => formatPercent(toNumber(entry.risk?.drawdown)),
      tone: (entry) => {
        const value = toNumber(entry.risk?.drawdown);
        if (value === null) return null;
        return value < 0 ? 'negative' : 'positive';
      },
    },
    {
      key: 'pe',
      label: t('compare.metrics.pe'),
      value: (entry) => formatNumber(toNumber(entry.fundamentals?.pe)),
    },
    {
      key: 'pb',
      label: t('compare.metrics.pb'),
      value: (entry) => formatNumber(toNumber(entry.fundamentals?.pb)),
    },
    {
      key: 'roe',
      label: t('compare.metrics.roe'),
      value: (entry) => formatPercent(toNumber(entry.fundamentals?.roe), { ratio: true }),
    },
    {
      key: 'payout',
      label: t('compare.metrics.payout'),
      value: (entry) => formatPercent(toNumber(entry.fundamentals?.payout), { ratio: true }),
    },
    {
      key: 'evEbitda',
      label: t('compare.metrics.evEbitda'),
      value: (entry) => formatNumber(toNumber(entry.fundamentals?.evEbitda)),
    },
    {
      key: 'netDebtEbitda',
      label: t('compare.metrics.netDebtEbitda'),
      value: (entry) => formatNumber(toNumber(entry.fundamentals?.netDebtEbitda)),
    },
  ]), [formatAssetCurrency, formatNumber, formatPercent, t]);

  const fiiRows = useMemo(() => (
    orderedComparison.filter((entry) => normalizeAssetClass(entry.assetClass) === 'fii')
  ), [orderedComparison]);

  const hasFiiSpecificComparison = fiiRows.length >= MIN_ASSETS_TO_COMPARE;

  const formatQuotaCount = useCallback((value: number | null) => {
    if (value === null || !Number.isFinite(value)) return t('assets.modal.noValue');
    return value.toLocaleString(numberLocale, { maximumFractionDigits: 0 });
  }, [numberLocale, t]);

  const formatTopComposition = useCallback((items: FiiTopCompositionItem[]) => {
    if (!Array.isArray(items) || items.length === 0) return t('assets.modal.noValue');
    return items
      .map((entry) => {
        if (entry.allocationPct === null || !Number.isFinite(entry.allocationPct)) return entry.label;
        return `${entry.label} (${entry.allocationPct.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%)`;
      })
      .join(' • ');
  }, [numberLocale, t]);

  const fiiMetrics = useMemo<FiiMetricRow[]>(() => ([
    {
      key: 'classification',
      label: t('compare.fiiMetrics.classification'),
      value: (entry) => entry?.classification || t('assets.modal.noValue'),
    },
    {
      key: 'segment',
      label: t('compare.fiiMetrics.segment'),
      value: (entry) => entry?.segment || t('assets.modal.noValue'),
    },
    {
      key: 'administrator',
      label: t('compare.fiiMetrics.administrator'),
      value: (entry) => entry?.administrator || t('assets.modal.noValue'),
    },
    {
      key: 'manager',
      label: t('compare.fiiMetrics.manager'),
      value: (entry) => entry?.managerName || t('assets.modal.noValue'),
    },
    {
      key: 'quotaCount',
      label: t('compare.fiiMetrics.quotaCount'),
      value: (entry) => formatQuotaCount(entry?.quotaCount ?? null),
    },
    {
      key: 'dy12m',
      label: t('compare.fiiMetrics.dy12m'),
      value: (entry) => entry?.dy12m || t('assets.modal.noValue'),
    },
    {
      key: 'dySector',
      label: t('compare.fiiMetrics.dySector'),
      value: (entry) => entry?.dySector || t('assets.modal.noValue'),
    },
    {
      key: 'dyCategory',
      label: t('compare.fiiMetrics.dyCategory'),
      value: (entry) => entry?.dyCategory || t('assets.modal.noValue'),
    },
    {
      key: 'dyMarket',
      label: t('compare.fiiMetrics.dyMarket'),
      value: (entry) => entry?.dyMarket || t('assets.modal.noValue'),
    },
    {
      key: 'lastDividendPerUnit',
      label: t('compare.fiiMetrics.lastDividendPerUnit'),
      value: (entry) => entry?.lastDividendPerUnit || t('assets.modal.noValue'),
    },
    {
      key: 'dividend12mPerUnit',
      label: t('compare.fiiMetrics.dividend12mPerUnit'),
      value: (entry) => entry?.dividend12mPerUnit || t('assets.modal.noValue'),
    },
    {
      key: 'yield12mOnQuote',
      label: t('compare.fiiMetrics.yield12mOnQuote'),
      value: (entry) => entry?.yield12mOnQuote || t('assets.modal.noValue'),
    },
    {
      key: 'topComposition',
      label: t('compare.fiiMetrics.topComposition'),
      value: (entry) => formatTopComposition(entry?.topComposition || []),
    },
  ]), [formatQuotaCount, formatTopComposition, t]);

  const comparedClassesCount = useMemo(() => (
    new Set(orderedComparison.map((entry) => normalizeAssetClass(entry.assetClass))).size
  ), [orderedComparison]);

  const lastUpdatedAtText = useMemo(() => {
    if (!payload?.fetched_at) return t('assets.modal.noValue');
    const parsed = new Date(payload.fetched_at);
    if (Number.isNaN(parsed.getTime())) return t('assets.modal.noValue');
    return parsed.toLocaleString(numberLocale, {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  }, [numberLocale, payload?.fetched_at, t]);

  return (
    <Layout>
      <div className="compare-page">
        <div className="compare-page__header">
          <h1 className="compare-page__title">{t('compare.title')}</h1>
          <div className="compare-page__filters">
            {portfolioOptions.length > 0 ? (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('compare.selectPortfolio')}
                className="compare-page__dropdown compare-page__dropdown--portfolio"
                size="sm"
              />
            ) : null}
            <SharedDropdown
              value={tickerPickerValue}
              options={tickerPickerOptions}
              onChange={setTickerPickerValue}
              ariaLabel={t('compare.selectTicker')}
              className="compare-page__dropdown compare-page__dropdown--ticker"
              size="sm"
              disabled={availableTickerOptions.length === 0 || selectedTickers.length >= MAX_ASSETS_TO_COMPARE}
            />
            <button
              type="button"
              className="compare-page__action"
              onClick={addSelectedTicker}
              disabled={!tickerPickerValue || selectedTickers.length >= MAX_ASSETS_TO_COMPARE}
            >
              {t('compare.addTicker')}
            </button>
            <button
              type="button"
              className="compare-page__action compare-page__action--ghost"
              onClick={resetSelectedTickers}
              disabled={tickerOptions.length < MIN_ASSETS_TO_COMPARE}
            >
              {t('compare.reset')}
            </button>
          </div>
        </div>

        <p className="compare-page__subtitle">{t('compare.subtitle', { min: MIN_ASSETS_TO_COMPARE, max: MAX_ASSETS_TO_COMPARE })}</p>

        <div className="compare-page__selected">
          {selectedTickers.map((ticker) => (
            <button
              key={`selected-${ticker}`}
              type="button"
              className="compare-page__chip"
              onClick={() => removeSelectedTicker(ticker)}
              aria-label={t('compare.removeTicker', { ticker })}
            >
              <span>{ticker}</span>
              <span aria-hidden="true">×</span>
            </button>
          ))}
        </div>

        {selectedTickers.length < MIN_ASSETS_TO_COMPARE ? (
          <div className="compare-page__state">{t('compare.needAtLeastTwo')}</div>
        ) : null}

        {loading ? <div className="compare-page__state">{t('common.loading')}</div> : null}

        {!loading && error ? (
          <div className="compare-page__state compare-page__state--error">
            <p>{t('compare.loadError')}</p>
            <code>{error}</code>
          </div>
        ) : null}

        {!loading && !error && selectedTickers.length >= MIN_ASSETS_TO_COMPARE && orderedComparison.length === 0 ? (
          <div className="compare-page__state">{t('compare.noData')}</div>
        ) : null}

        {!loading && !error && orderedComparison.length > 0 ? (
          <div className="compare-page__content">
            <div className="compare-kpis">
              <article className="compare-kpi">
                <span className="compare-kpi__label">{t('compare.kpis.assets')}</span>
                <strong className="compare-kpi__value">{orderedComparison.length}</strong>
              </article>
              <article className="compare-kpi">
                <span className="compare-kpi__label">{t('compare.kpis.classes')}</span>
                <strong className="compare-kpi__value">{comparedClassesCount}</strong>
              </article>
              <article className="compare-kpi">
                <span className="compare-kpi__label">{t('compare.kpis.fiis')}</span>
                <strong className="compare-kpi__value">{fiiRows.length}</strong>
              </article>
              <article className="compare-kpi">
                <span className="compare-kpi__label">{t('compare.kpis.updatedAt')}</span>
                <strong className="compare-kpi__value">{lastUpdatedAtText}</strong>
              </article>
            </div>

            <section className="compare-card">
              <header className="compare-card__header">
                <h2>{t('compare.sections.sideBySide')}</h2>
              </header>
              <div className="compare-table-wrapper">
                <table className="compare-table">
                  <thead>
                    <tr>
                      <th>{t('compare.table.metric')}</th>
                      {orderedComparison.map((entry) => {
                        const ticker = toUpperTicker(entry.ticker);
                        const asset = activeAssetByTicker.get(ticker);
                        const detailsPath = asset
                          ? `/assets/${encodeURIComponent(asset.assetId)}?portfolioId=${encodeURIComponent(selectedPortfolio)}`
                          : null;
                        return (
                          <th key={`head-${ticker}`}>
                            <div className="compare-table__asset-head">
                              <strong>{ticker}</strong>
                              <span>{entry.name}</span>
                              {detailsPath ? (
                                <Link to={detailsPath} className="compare-table__asset-link">
                                  {t('compare.openAsset')}
                                </Link>
                              ) : null}
                            </div>
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {comparisonMetrics.map((metric) => (
                      <tr key={`metric-${metric.key}`}>
                        <th>{metric.label}</th>
                        {orderedComparison.map((entry) => {
                          const tone = metric.tone?.(entry) || null;
                          return (
                            <td
                              key={`${metric.key}-${toUpperTicker(entry.ticker)}`}
                              className={tone ? `compare-table__value--${tone}` : ''}
                            >
                              {metric.value(entry)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="compare-card">
              <header className="compare-card__header">
                <h2>{t('compare.sections.fii')}</h2>
                {fiiLoading ? (
                  <span className="compare-card__spinner" role="status" aria-label={t('common.loading')} />
                ) : null}
              </header>
              {!hasFiiSpecificComparison ? (
                <p className="compare-card__empty">{t('compare.fiiNeedAtLeastTwo')}</p>
              ) : (
                <div className="compare-table-wrapper">
                  <table className="compare-table compare-table--fii">
                    <thead>
                      <tr>
                        <th>{t('compare.table.metric')}</th>
                        {fiiRows.map((entry) => (
                          <th key={`fii-head-${toUpperTicker(entry.ticker)}`}>{toUpperTicker(entry.ticker)}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {fiiMetrics.map((metric) => (
                        <tr key={`fii-metric-${metric.key}`}>
                          <th>{metric.label}</th>
                          {fiiRows.map((entry) => (
                            <td key={`fii-${metric.key}-${toUpperTicker(entry.ticker)}`}>
                              {metric.value(fiiSnapshotsByTicker[toUpperTicker(entry.ticker)] || null)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </section>
          </div>
        ) : null}
      </div>
    </Layout>
  );
};

export default CompareAssetsPage;
