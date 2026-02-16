import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { api, type RiskResponse } from '../services/api';
import { formatCurrency } from '../utils/formatters';
import './RiskPage.scss';

const THRESHOLD_OPTIONS = [10, 15, 20, 25, 30];
const PERIOD_OPTIONS = ['1M', '3M', '6M', '1Y', 'MAX'] as const;
const RETURN_FILTERS = ['all', 'positive', 'negative', 'neutral'] as const;
const RETURN_ZERO_EPSILON = 0.001;
const RISK_TABS = [
  'concentration',
  'fxExposure',
  'drawdownByAsset',
  'correlation',
  'riskReturn',
  'purchasingPower',
  'sensitivity',
] as const;
const PERIOD_DAYS: Record<(typeof PERIOD_OPTIONS)[number], number> = {
  '1M': 30,
  '3M': 90,
  '6M': 180,
  '1Y': 365,
  MAX: Number.POSITIVE_INFINITY,
};
const FX_COLORS = ['#22d3ee', '#818cf8', '#34d399', '#f59e0b', '#fb7185', '#f97316', '#a78bfa'];

const toNumber = (value: unknown): number => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
};

const toTitleCase = (value: string): string =>
  String(value || '')
    .replace(/_/g, ' ')
    .split(' ')
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const formatAssetClassLabel = (value: string): string => {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) return 'Other';
  const map: Record<string, string> = {
    fii: 'FII',
    fiagro: 'FIAGRO',
    etf: 'ETF',
    bdr: 'BDR',
    reit: 'REIT',
    rsu: 'RSU',
    stock: 'Stock',
    bond: 'Bond',
    crypto: 'Crypto',
    derivative: 'Derivative',
    cash: 'Cash',
    fund: 'Fund',
    fixed_income: 'Fixed Income',
  };
  return map[normalized] || toTitleCase(normalized);
};

const parseIsoDateUtc = (value: string): Date => {
  const [yearRaw, monthRaw, dayRaw] = String(value || '').split('-');
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  const day = Number(dayRaw);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return new Date(Number.NaN);
  }
  return new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
};

const clamp = (value: number, min: number, max: number): number =>
  Math.min(Math.max(value, min), max);

const isPositiveReturn = (value: number): boolean => value > RETURN_ZERO_EPSILON;
const isNegativeReturn = (value: number): boolean => value < -RETURN_ZERO_EPSILON;
const isNeutralReturn = (value: number): boolean => !isPositiveReturn(value) && !isNegativeReturn(value);

const roundScatterValue = (value: number): number => Math.round(toNumber(value) * 10000) / 10000;

const correlationCellColor = (value: number | null): string => {
  if (value === null || !Number.isFinite(value)) return 'rgba(148, 163, 184, 0.08)';
  const normalized = clamp(value, -1, 1);
  const intensity = 0.12 + (Math.abs(normalized) * 0.34);
  if (normalized > 0) return `rgba(52, 211, 153, ${intensity.toFixed(3)})`;
  if (normalized < 0) return `rgba(248, 113, 113, ${intensity.toFixed(3)})`;
  return 'rgba(148, 163, 184, 0.18)';
};

const RiskPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio, assets } = usePortfolioData();
  const [thresholdPct, setThresholdPct] = useState('15');
  const [historyPeriod, setHistoryPeriod] = useState<(typeof PERIOD_OPTIONS)[number]>('1Y');
  const [scatterFilter, setScatterFilter] = useState<(typeof RETURN_FILTERS)[number]>('all');
  const [activeTab, setActiveTab] = useState<(typeof RISK_TABS)[number]>('concentration');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [risk, setRisk] = useState<RiskResponse | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const formatBrl = (value: number) => formatCurrency(value, 'BRL', numberLocale);
  const formatSignedPercent = (value: number, fractionDigits = 2): string =>
    `${value > 0 ? '+' : ''}${toNumber(value).toLocaleString(numberLocale, {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits,
    })}%`;

  const thresholdValue = toNumber(thresholdPct) > 0 ? toNumber(thresholdPct) : 15;

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const activeAssetIds = useMemo(() => (
    new Set(
      assets
        .filter((asset) => String(asset.status || 'active').toLowerCase() === 'active')
        .map((asset) => String(asset.assetId || ''))
        .filter(Boolean)
    )
  ), [assets]);

  const activeTickers = useMemo(() => (
    new Set(
      assets
        .filter((asset) => String(asset.status || 'active').toLowerCase() === 'active')
        .map((asset) => String(asset.ticker || '').toUpperCase())
        .filter(Boolean)
    )
  ), [assets]);

  const activeAssetMetaByKey = useMemo(() => {
    const map = new Map<string, { assetClass: string; currency: string }>();
    for (const asset of assets) {
      if (String(asset.status || 'active').toLowerCase() !== 'active') continue;
      const assetId = String(asset.assetId || '');
      const ticker = String(asset.ticker || '').toUpperCase();
      const assetClass = String(asset.assetClass || 'other').toLowerCase();
      const currency = String(asset.currency || 'BRL').toUpperCase();
      if (assetId) map.set(`id:${assetId}`, { assetClass, currency });
      if (ticker) map.set(`ticker:${ticker}`, { assetClass, currency });
    }
    return map;
  }, [assets]);

  const thresholdOptions = useMemo(
    () =>
      THRESHOLD_OPTIONS.map((value) => ({
        value: String(value),
        label: t('risk.thresholdOption', { value }),
      })),
    [t]
  );

  const periodOptions = useMemo(
    () =>
      PERIOD_OPTIONS.map((value) => ({
        value,
        label: t(`risk.periods.${value}`),
      })),
    [t]
  );

  const tabOptions = useMemo(
    () => RISK_TABS.map((value) => ({
      value,
      label: t(`risk.sections.${value}`),
    })),
    [t]
  );

  useEffect(() => {
    if (!selectedPortfolio) {
      setRisk(null);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api.getRisk(selectedPortfolio, { concentrationThreshold: thresholdValue })
      .then((payload) => {
        if (cancelled) return;
        setRisk(payload);
      })
      .catch((reason: unknown) => {
        if (cancelled) return;
        setRisk(null);
        setError(reason instanceof Error ? reason.message : 'Failed to load risk data');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [selectedPortfolio, thresholdValue]);

  const concentrationRows = useMemo(() => (
    (risk?.concentration || [])
      .filter((row) => {
        const assetId = String(row.assetId || '');
        const ticker = String(row.ticker || '').toUpperCase();
        const hasActiveSnapshot = activeAssetIds.size > 0 || activeTickers.size > 0;
        if (!hasActiveSnapshot) return true;
        return activeAssetIds.has(assetId) || activeTickers.has(ticker);
      })
      .map((row) => ({
        assetId: String(row.assetId || ''),
        ticker: String(row.ticker || '').toUpperCase(),
        marketValue: toNumber(row.market_value),
        weightPct: toNumber(row.weight_pct),
      }))
      .sort((left, right) => right.weightPct - left.weightPct)
  ), [activeAssetIds, activeTickers, risk?.concentration]);

  const concentrationAlerts = useMemo(() => (
    concentrationRows.filter((row) => row.weightPct > thresholdValue)
  ), [concentrationRows, thresholdValue]);

  const largestPosition = concentrationRows[0] || null;

  const correlationMap = useMemo(() => {
    const hasActiveSnapshot = activeTickers.size > 0;
    const map = new Map<string, number>();
    for (const pair of risk?.correlation_matrix || []) {
      const left = String(pair.left || '').toUpperCase();
      const right = String(pair.right || '').toUpperCase();
      if (!left || !right) continue;
      if (hasActiveSnapshot && (!activeTickers.has(left) || !activeTickers.has(right))) continue;
      const correlation = toNumber(pair.correlation);
      map.set(`${left}|${right}`, correlation);
      map.set(`${right}|${left}`, correlation);
    }
    return map;
  }, [activeTickers, risk?.correlation_matrix]);

  const correlationTickers = useMemo(() => {
    const ordered = concentrationRows
      .map((row) => row.ticker)
      .filter(Boolean);
    const seen = new Set(ordered);
    const hasActiveSnapshot = activeTickers.size > 0;
    for (const pair of risk?.correlation_matrix || []) {
      const left = String(pair.left || '').toUpperCase();
      const right = String(pair.right || '').toUpperCase();
      if (hasActiveSnapshot && (!activeTickers.has(left) || !activeTickers.has(right))) continue;
      if (left && !seen.has(left)) {
        ordered.push(left);
        seen.add(left);
      }
      if (right && !seen.has(right)) {
        ordered.push(right);
        seen.add(right);
      }
    }
    return ordered;
  }, [activeTickers, concentrationRows, risk?.correlation_matrix]);

  const resolveCorrelation = (left: string, right: string): number | null => {
    if (left === right) return 1;
    const key = `${left}|${right}`;
    if (!correlationMap.has(key)) return null;
    return correlationMap.get(key) ?? null;
  };

  const fxRows = useMemo(() => {
    const rawRows = Object.entries(risk?.fx_exposure || {})
      .map(([currency, row]) => ({
        currency: String(currency || '').toUpperCase(),
        value: toNumber(row?.value),
        weightPct: toNumber(row?.weight_pct),
      }))
      .sort((left, right) => right.weightPct - left.weightPct);

    if (rawRows.some((row) => row.value > 0)) {
      return rawRows.filter((row) => row.value > 0);
    }
    return rawRows;
  }, [risk?.fx_exposure]);

  const fxTotalValue = useMemo(
    () => fxRows.reduce((sum, row) => sum + row.value, 0),
    [fxRows]
  );

  const fxBrlValue = useMemo(
    () => fxRows.find((row) => row.currency === 'BRL')?.value || 0,
    [fxRows]
  );

  const fxForeignValue = useMemo(
    () => Math.max(fxTotalValue - fxBrlValue, 0),
    [fxBrlValue, fxTotalValue]
  );

  const fxForeignWeight = useMemo(
    () => (fxTotalValue > 0 ? (fxForeignValue / fxTotalValue) * 100 : 0),
    [fxForeignValue, fxTotalValue]
  );

  const fxDominant = useMemo(
    () => fxRows.find((row) => row.value > 0) || fxRows[0] || null,
    [fxRows]
  );

  const fxLocalVsForeign = useMemo(
    () => [
      { bucket: t('risk.fx.localLabel'), value: fxBrlValue },
      { bucket: t('risk.fx.foreignLabel'), value: fxForeignValue },
    ],
    [fxBrlValue, fxForeignValue, t]
  );

  const fxByClass = useMemo(() => {
    const buckets = new Map<string, { assetClass: string; total: number; byCurrency: Record<string, number> }>();
    const currencies = new Set<string>();

    for (const row of concentrationRows) {
      const meta = activeAssetMetaByKey.get(`id:${row.assetId}`) || activeAssetMetaByKey.get(`ticker:${row.ticker}`);
      const currency = String(meta?.currency || 'BRL').toUpperCase();
      const assetClass = formatAssetClassLabel(String(meta?.assetClass || 'other'));
      currencies.add(currency);

      if (!buckets.has(assetClass)) {
        buckets.set(assetClass, { assetClass, total: 0, byCurrency: {} });
      }
      const current = buckets.get(assetClass)!;
      current.total += row.marketValue;
      current.byCurrency[currency] = toNumber(current.byCurrency[currency]) + row.marketValue;
    }

    const orderedCurrencies = Array.from(currencies)
      .sort((left, right) => {
        if (left === 'BRL') return -1;
        if (right === 'BRL') return 1;
        return left.localeCompare(right);
      });

    const rows = Array.from(buckets.values())
      .map((bucket) => {
        const item: Record<string, string | number> = {
          assetClass: bucket.assetClass,
          total: bucket.total,
        };
        for (const currency of orderedCurrencies) {
          item[currency] = toNumber(bucket.byCurrency[currency]);
        }
        return item;
      })
      .sort((left, right) => toNumber(right.total) - toNumber(left.total));

    const normalizedRows = rows.map((row) => {
      const total = toNumber(row.total);
      const normalized: Record<string, string | number> = {
        assetClass: String(row.assetClass || ''),
        total: 100,
      };
      for (const currency of orderedCurrencies) {
        normalized[currency] = total > 0 ? (toNumber(row[currency]) / total) * 100 : 0;
      }
      return normalized;
    });

    return { rows, normalizedRows, currencies: orderedCurrencies };
  }, [activeAssetMetaByKey, concentrationRows]);

  const fxCurrencyColors = useMemo(() => {
    const map = new Map<string, string>();
    const ordered = fxByClass.currencies.length > 0
      ? fxByClass.currencies
      : fxRows.map((row) => row.currency);
    for (const [index, currency] of ordered.entries()) {
      map.set(currency, FX_COLORS[index % FX_COLORS.length]);
    }
    return map;
  }, [fxByClass.currencies, fxRows]);

  const fxTopContributors = useMemo(() => {
    const rows = concentrationRows
      .map((row) => {
        const meta = activeAssetMetaByKey.get(`id:${row.assetId}`) || activeAssetMetaByKey.get(`ticker:${row.ticker}`);
        const currency = String(meta?.currency || 'BRL').toUpperCase();
        if (currency === 'BRL') return null;
        return {
          ticker: row.ticker,
          assetClass: formatAssetClassLabel(String(meta?.assetClass || 'other')),
          currency,
          value: row.marketValue,
          shareOfFx: fxForeignValue > 0 ? (row.marketValue / fxForeignValue) * 100 : 0,
          shareOfPortfolio: row.weightPct,
        };
      })
      .filter((row): row is {
        ticker: string;
        assetClass: string;
        currency: string;
        value: number;
        shareOfFx: number;
        shareOfPortfolio: number;
      } => row !== null)
      .sort((left, right) => right.value - left.value);

    return rows;
  }, [activeAssetMetaByKey, concentrationRows, fxForeignValue]);

  const fxTopContributorsChart = useMemo(
    () => fxTopContributors.slice(0, 10).slice().reverse(),
    [fxTopContributors]
  );

  const fxShockRows = useMemo(() => (
    fxRows
      .filter((row) => row.currency !== 'BRL' && row.value > 0)
      .map((row) => ({
        currency: row.currency,
        up5: row.value * 0.05,
        down5: row.value * -0.05,
      }))
  ), [fxRows]);

  const fxEstimatedImpactPlus5 = useMemo(
    () => fxShockRows.reduce((sum, row) => sum + row.up5, 0),
    [fxShockRows]
  );

  const drawdownRows = useMemo(() => {
    const hasActiveSnapshot = activeTickers.size > 0;
    const volatilityByAsset = risk?.volatility_by_asset || {};
    return Object.entries(risk?.drawdown_by_asset || {})
      .map(([ticker, drawdown]) => ({
        ticker: String(ticker || '').toUpperCase(),
        drawdown: toNumber(drawdown),
        volatility: toNumber(volatilityByAsset[ticker] ?? volatilityByAsset[String(ticker || '').toUpperCase()]),
      }))
      .filter((row) => !hasActiveSnapshot || activeTickers.has(row.ticker))
      .sort((left, right) => left.drawdown - right.drawdown);
  }, [activeTickers, risk?.drawdown_by_asset, risk?.volatility_by_asset]);

  const drawdownChartRows = useMemo(
    () => drawdownRows.slice(0, 12).sort((left, right) => left.drawdown - right.drawdown),
    [drawdownRows]
  );

  const scatterRows = useMemo(() => (
    (risk?.risk_return_scatter || [])
      .map((row) => ({
        ticker: String(row.ticker || '').toUpperCase(),
        volatility: toNumber(row.volatility),
        returnPct: toNumber(row.return_pct),
      }))
      .filter((row) => activeTickers.size === 0 || activeTickers.has(row.ticker))
      .sort((left, right) => right.volatility - left.volatility)
  ), [activeTickers, risk?.risk_return_scatter]);

  const scatterRowsFiltered = useMemo(() => {
    if (scatterFilter === 'positive') return scatterRows.filter((row) => isPositiveReturn(row.returnPct));
    if (scatterFilter === 'negative') return scatterRows.filter((row) => isNegativeReturn(row.returnPct));
    if (scatterFilter === 'neutral') return scatterRows.filter((row) => isNeutralReturn(row.returnPct));
    return scatterRows;
  }, [scatterFilter, scatterRows]);

  const scatterRowsPlotted = useMemo(() => {
    const grouped = new Map<string, Array<(typeof scatterRowsFiltered)[number]>>();
    for (const row of scatterRowsFiltered) {
      const key = `${roundScatterValue(row.volatility)}|${roundScatterValue(row.returnPct)}`;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key)!.push(row);
    }

    const plotted = [];
    for (const rows of grouped.values()) {
      if (rows.length === 1) {
        const row = rows[0];
        plotted.push({
          ...row,
          volatilityPlot: row.volatility,
          returnPctPlot: row.returnPct,
        });
        continue;
      }

      // Spread exact-overlap points in small concentric rings so each ticker stays hoverable.
      for (let index = 0; index < rows.length; index += 1) {
        const row = rows[index];
        const ring = Math.floor(index / 8);
        const angle = ((index % 8) / 8) * Math.PI * 2;
        const radiusX = 0.35 + (ring * 0.22);
        const radiusY = 0.24 + (ring * 0.16);
        const volatilityPlot = Math.max(0, row.volatility + (Math.cos(angle) * radiusX));
        const returnPctPlot = row.returnPct + (Math.sin(angle) * radiusY);
        plotted.push({
          ...row,
          volatilityPlot,
          returnPctPlot,
        });
      }
    }

    return plotted;
  }, [scatterRowsFiltered]);

  const scatterPositive = scatterRowsPlotted.filter((row) => isPositiveReturn(row.returnPct));
  const scatterNegative = scatterRowsPlotted.filter((row) => isNegativeReturn(row.returnPct));
  const scatterNeutral = scatterRowsPlotted.filter((row) => isNeutralReturn(row.returnPct));

  const scatterFilterOptions = useMemo(
    () => RETURN_FILTERS.map((value) => ({
      value,
      label: t(`risk.returnFilter.${value}`),
    })),
    [t]
  );

  const renderScatterTooltip = ({
    active,
    payload,
  }: {
    active?: boolean;
    payload?: ReadonlyArray<{
      name?: string;
      payload?: { ticker?: string; volatility?: number; returnPct?: number };
    }>;
  }) => {
    if (!active || !payload || payload.length === 0) return null;
    const point = payload.find((entry) => entry?.payload)?.payload ?? payload[0]?.payload;
    if (!point) return null;
    const tickerLabel = String(point.ticker || payload[0]?.name || '-').toUpperCase();

    return (
      <div className="risk-tooltip">
        <p className="risk-tooltip__title">{tickerLabel}</p>
        <p className="risk-tooltip__row">
          <span>{t('risk.table.returnPct')}</span>
          <strong>{formatSignedPercent(toNumber(point.returnPct))}</strong>
        </p>
        <p className="risk-tooltip__row">
          <span>{t('risk.table.volatility')}</span>
          <strong>{formatSignedPercent(toNumber(point.volatility))}</strong>
        </p>
      </div>
    );
  };

  const inflationSeriesRaw = useMemo(() => (
    (risk?.inflation_adjusted_value || [])
      .map((point) => ({
        date: String(point.date || ''),
        nominalValue: toNumber(point.nominal_value),
        realValue: toNumber(point.real_value),
      }))
      .filter((point) => point.date)
      .sort((left, right) => left.date.localeCompare(right.date))
  ), [risk?.inflation_adjusted_value]);

  const inflationSeries = useMemo(() => {
    if (inflationSeriesRaw.length === 0) return [];
    if (historyPeriod === 'MAX') return inflationSeriesRaw;
    const latest = parseIsoDateUtc(inflationSeriesRaw[inflationSeriesRaw.length - 1].date);
    if (Number.isNaN(latest.getTime())) return inflationSeriesRaw;
    const days = PERIOD_DAYS[historyPeriod] || Number.POSITIVE_INFINITY;
    const cutoffMs = latest.getTime() - ((days - 1) * 24 * 60 * 60 * 1000);
    return inflationSeriesRaw.filter((point) => parseIsoDateUtc(point.date).getTime() >= cutoffMs);
  }, [historyPeriod, inflationSeriesRaw]);

  const formatTickDate = (date: string): string => {
    const parsed = parseIsoDateUtc(date);
    if (Number.isNaN(parsed.getTime())) return date;
    if (historyPeriod === '1M') {
      return parsed.toLocaleDateString(numberLocale, { day: '2-digit', month: 'short', timeZone: 'UTC' });
    }
    return parsed.toLocaleDateString(numberLocale, { month: 'short', year: '2-digit', timeZone: 'UTC' });
  };

  return (
    <Layout>
      <div className="risk-page">
        <div className="risk-page__header">
          <h1 className="risk-page__title">{t('risk.title')}</h1>
          <div className="risk-page__filters">
            {portfolioOptions.length > 0 && (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('risk.selectPortfolio')}
                className="risk-page__dropdown risk-page__dropdown--portfolio"
                size="sm"
              />
            )}
            <SharedDropdown
              value={thresholdPct}
              options={thresholdOptions}
              onChange={setThresholdPct}
              ariaLabel={t('risk.concentrationThreshold')}
              className="risk-page__dropdown"
              size="sm"
            />
            <SharedDropdown
              value={historyPeriod}
              options={periodOptions}
              onChange={(value) => {
                if (PERIOD_OPTIONS.includes(value as (typeof PERIOD_OPTIONS)[number])) {
                  setHistoryPeriod(value as (typeof PERIOD_OPTIONS)[number]);
                }
              }}
              ariaLabel={t('risk.chartPeriod')}
              className="risk-page__dropdown"
              size="sm"
            />
          </div>
        </div>

        {loading && <div className="risk-page__state">{t('common.loading')}</div>}

        {!loading && portfolios.length === 0 && (
          <div className="risk-page__state">{t('dashboard.noData')}</div>
        )}

        {!loading && error && (
          <div className="risk-page__state risk-page__state--error">
            <p>{t('risk.loadError')}</p>
            <code>{error}</code>
          </div>
        )}

        {!loading && !error && risk && (
          <>
            <div className="risk-page__kpis">
              <article className="risk-kpi">
                <span className="risk-kpi__label">{t('risk.kpis.portfolioVolatility')}</span>
                <span className="risk-kpi__value">
                  {formatSignedPercent(toNumber(risk.portfolio_volatility))}
                </span>
              </article>
              <article className="risk-kpi">
                <span className="risk-kpi__label">{t('risk.kpis.maxDrawdown')}</span>
                <span className="risk-kpi__value risk-kpi__value--negative">
                  {formatSignedPercent(toNumber(risk.portfolio_drawdown))}
                </span>
              </article>
              <article className="risk-kpi">
                <span className="risk-kpi__label">{t('risk.kpis.concentrationAlerts')}</span>
                <span className={`risk-kpi__value ${concentrationAlerts.length > 0 ? 'risk-kpi__value--warning' : ''}`}>
                  {concentrationAlerts.length}
                </span>
              </article>
              <article className="risk-kpi">
                <span className="risk-kpi__label">{t('risk.kpis.largestPosition')}</span>
                <span className="risk-kpi__value">
                  {largestPosition
                    ? `${largestPosition.ticker} • ${formatSignedPercent(largestPosition.weightPct)}`
                    : '-'}
                </span>
              </article>
            </div>

            <div className="risk-page__tabs" role="tablist" aria-label={t('risk.tabs.ariaLabel')}>
              {tabOptions.map((tab) => (
                <button
                  key={tab.value}
                  type="button"
                  role="tab"
                  aria-selected={activeTab === tab.value}
                  className={`risk-page__tab ${activeTab === tab.value ? 'risk-page__tab--active' : ''}`}
                  onClick={() => setActiveTab(tab.value)}
                >
                  {tab.label}
                </button>
              ))}
            </div>

            <div className="risk-page__panel">
              {activeTab === 'concentration' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.concentration')}</h2>
                </header>
                {concentrationRows.length === 0 ? (
                  <p className="risk-card__empty">{t('risk.noSeries')}</p>
                ) : (
                  <div className="risk-table-wrapper">
                    <table className="risk-table">
                      <thead>
                        <tr>
                          <th>{t('risk.table.ticker')}</th>
                          <th>{t('risk.table.marketValue')}</th>
                          <th>{t('risk.table.weight')}</th>
                          <th>{t('risk.table.alert')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {concentrationRows.map((row) => (
                          <tr key={row.assetId || row.ticker}>
                            <td>{row.ticker}</td>
                            <td>{formatBrl(row.marketValue)}</td>
                            <td>
                              <div className="risk-weight">
                                <div className="risk-weight__track">
                                  <span
                                    className={`risk-weight__fill ${row.weightPct > thresholdValue ? 'risk-weight__fill--alert' : ''}`}
                                    style={{ width: `${clamp(row.weightPct, 0, 100)}%` }}
                                  />
                                </div>
                                <span>{formatSignedPercent(row.weightPct)}</span>
                              </div>
                            </td>
                            <td>
                              {row.weightPct > thresholdValue ? (
                                <span className="risk-badge risk-badge--warning">
                                  {t('risk.alerts.overLimit', { limit: thresholdValue })}
                                </span>
                              ) : (
                                <span className="risk-badge risk-badge--ok">{t('risk.alerts.withinLimit')}</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                </section>
              )}

              {activeTab === 'fxExposure' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.fxExposure')}</h2>
                </header>
                {fxRows.length === 0 ? (
                  <p className="risk-card__empty">{t('risk.noSeries')}</p>
                ) : (
                  <>
                    <div className="risk-fx-summary">
                      <article className="risk-fx-kpi">
                        <span className="risk-fx-kpi__label">{t('risk.fx.totalBrlExposure')}</span>
                        <span className="risk-fx-kpi__value">{formatBrl(fxTotalValue)}</span>
                      </article>
                      <article className="risk-fx-kpi">
                        <span className="risk-fx-kpi__label">{t('risk.fx.foreignExposure')}</span>
                        <span className="risk-fx-kpi__value">{formatBrl(fxForeignValue)}</span>
                      </article>
                      <article className="risk-fx-kpi">
                        <span className="risk-fx-kpi__label">{t('risk.fx.foreignShare')}</span>
                        <span className="risk-fx-kpi__value">{formatSignedPercent(fxForeignWeight)}</span>
                      </article>
                      <article className="risk-fx-kpi">
                        <span className="risk-fx-kpi__label">{t('risk.fx.dominantCurrency')}</span>
                        <span className="risk-fx-kpi__value">
                          {fxDominant
                            ? `${fxDominant.currency} • ${formatSignedPercent(fxDominant.weightPct)}`
                            : '-'}
                        </span>
                      </article>
                      <article className="risk-fx-kpi">
                        <span className="risk-fx-kpi__label">{t('risk.fx.estimatedImpactPlus5')}</span>
                        <span className="risk-fx-kpi__value">{formatBrl(fxEstimatedImpactPlus5)}</span>
                      </article>
                    </div>

                    <div className="risk-fx-charts">
                      <div className="risk-fx-chart">
                        <h3 className="risk-fx-chart__title">{t('risk.fx.byCurrencyChart')}</h3>
                        <ResponsiveContainer width="100%" height={250}>
                          <PieChart>
                            <Pie
                              data={fxRows}
                              dataKey="value"
                              nameKey="currency"
                              innerRadius={62}
                              outerRadius={94}
                              isAnimationActive={false}
                            >
                              {fxRows.map((entry, index) => (
                                <Cell
                                  key={entry.currency}
                                  fill={fxCurrencyColors.get(entry.currency) || FX_COLORS[index % FX_COLORS.length]}
                                />
                              ))}
                            </Pie>
                            <Tooltip formatter={(value: number | string | undefined) => formatBrl(toNumber(value))} />
                            <Legend />
                          </PieChart>
                        </ResponsiveContainer>
                      </div>

                      <div className="risk-fx-chart">
                        <h3 className="risk-fx-chart__title">{t('risk.fx.localVsForeign')}</h3>
                        <ResponsiveContainer width="100%" height={250}>
                          <BarChart data={fxLocalVsForeign} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                            <XAxis dataKey="bucket" stroke="var(--text-secondary)" />
                            <YAxis
                              stroke="var(--text-secondary)"
                              tickFormatter={(value) => formatBrl(toNumber(value))}
                              width={110}
                            />
                            <Tooltip formatter={(value: number | string | undefined) => formatBrl(toNumber(value))} />
                            <Bar dataKey="value" isAnimationActive={false}>
                              {fxLocalVsForeign.map((row, index) => (
                                <Cell key={`${row.bucket}-${index}`} fill={index === 0 ? '#22d3ee' : '#f59e0b'} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>

                      <div className="risk-fx-chart">
                        <h3 className="risk-fx-chart__title">{t('risk.fx.byClassChart')}</h3>
                        {fxByClass.rows.length === 0 ? (
                          <p className="risk-card__empty risk-card__empty--left">{t('risk.noSeries')}</p>
                        ) : (
                          <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={fxByClass.rows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                              <XAxis dataKey="assetClass" stroke="var(--text-secondary)" />
                              <YAxis
                                stroke="var(--text-secondary)"
                                tickFormatter={(value) => formatBrl(toNumber(value))}
                                width={110}
                              />
                              <Tooltip formatter={(value: number | string | undefined) => formatBrl(toNumber(value))} />
                              <Legend />
                              {fxByClass.currencies.map((currency) => (
                                <Bar
                                  key={`fx-class-${currency}`}
                                  dataKey={currency}
                                  stackId="fx-class"
                                  name={currency}
                                  fill={fxCurrencyColors.get(currency) || '#22d3ee'}
                                  isAnimationActive={false}
                                />
                              ))}
                            </BarChart>
                          </ResponsiveContainer>
                        )}
                      </div>

                      <div className="risk-fx-chart">
                        <h3 className="risk-fx-chart__title">{t('risk.fx.byClassShareChart')}</h3>
                        {fxByClass.normalizedRows.length === 0 ? (
                          <p className="risk-card__empty risk-card__empty--left">{t('risk.noSeries')}</p>
                        ) : (
                          <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={fxByClass.normalizedRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                              <XAxis dataKey="assetClass" stroke="var(--text-secondary)" />
                              <YAxis
                                stroke="var(--text-secondary)"
                                domain={[0, 100]}
                                tickFormatter={(value) => `${toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}%`}
                                width={56}
                              />
                              <Tooltip formatter={(value: number | string | undefined) => formatSignedPercent(toNumber(value))} />
                              <Legend />
                              {fxByClass.currencies.map((currency) => (
                                <Bar
                                  key={`fx-class-share-${currency}`}
                                  dataKey={currency}
                                  stackId="fx-class-share"
                                  name={currency}
                                  fill={fxCurrencyColors.get(currency) || '#22d3ee'}
                                  isAnimationActive={false}
                                />
                              ))}
                            </BarChart>
                          </ResponsiveContainer>
                        )}
                      </div>

                      <div className="risk-fx-chart">
                        <h3 className="risk-fx-chart__title">{t('risk.fx.shockImpact')}</h3>
                        {fxShockRows.length === 0 ? (
                          <p className="risk-card__empty risk-card__empty--left">{t('risk.fx.noForeignExposure')}</p>
                        ) : (
                          <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={fxShockRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                              <XAxis dataKey="currency" stroke="var(--text-secondary)" />
                              <YAxis
                                stroke="var(--text-secondary)"
                                tickFormatter={(value) => formatBrl(toNumber(value))}
                                width={110}
                              />
                              <Tooltip formatter={(value: number | string | undefined) => formatBrl(toNumber(value))} />
                              <Legend />
                              <ReferenceLine y={0} stroke="rgba(148, 163, 184, 0.42)" />
                              <Bar
                                dataKey="up5"
                                name={t('risk.fx.shockPlus5')}
                                fill="#34d399"
                                isAnimationActive={false}
                              />
                              <Bar
                                dataKey="down5"
                                name={t('risk.fx.shockMinus5')}
                                fill="#f87171"
                                isAnimationActive={false}
                              />
                            </BarChart>
                          </ResponsiveContainer>
                        )}
                      </div>

                      <div className="risk-fx-chart">
                        <h3 className="risk-fx-chart__title">{t('risk.fx.topContributors')}</h3>
                        {fxTopContributorsChart.length === 0 ? (
                          <p className="risk-card__empty risk-card__empty--left">{t('risk.fx.noForeignExposure')}</p>
                        ) : (
                          <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={fxTopContributorsChart} layout="vertical" margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
                              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                              <XAxis
                                type="number"
                                stroke="var(--text-secondary)"
                                tickFormatter={(value) => formatBrl(toNumber(value))}
                              />
                              <YAxis type="category" dataKey="ticker" stroke="var(--text-secondary)" width={74} />
                              <Tooltip formatter={(value: number | string | undefined) => formatBrl(toNumber(value))} />
                              <Bar dataKey="value" name={t('risk.table.marketValue')} fill="#f59e0b" isAnimationActive={false} />
                            </BarChart>
                          </ResponsiveContainer>
                        )}
                      </div>
                    </div>

                    <div className="risk-fx-lists">
                      <ul className="risk-fx">
                        {fxRows.map((row) => (
                          <li key={row.currency} className="risk-fx__item">
                            <span className="risk-fx__currency">{row.currency}</span>
                            <span className="risk-fx__value">{formatBrl(row.value)}</span>
                            <span className="risk-fx__weight">{formatSignedPercent(row.weightPct)}</span>
                          </li>
                        ))}
                      </ul>

                      <div className="risk-table-wrapper">
                        <table className="risk-table risk-table--compact">
                          <thead>
                            <tr>
                              <th>{t('risk.table.ticker')}</th>
                              <th>{t('risk.fx.assetClass')}</th>
                              <th>{t('risk.fx.currency')}</th>
                              <th>{t('risk.table.marketValue')}</th>
                              <th>{t('risk.fx.shareOfForeign')}</th>
                              <th>{t('risk.fx.shareOfPortfolio')}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {fxTopContributors.length === 0 ? (
                              <tr>
                                <td colSpan={6}>{t('risk.fx.noForeignExposure')}</td>
                              </tr>
                            ) : (
                              fxTopContributors.slice(0, 12).map((row) => (
                                <tr key={`fx-top-${row.ticker}`}>
                                  <td>{row.ticker}</td>
                                  <td>{row.assetClass}</td>
                                  <td>{row.currency}</td>
                                  <td>{formatBrl(row.value)}</td>
                                  <td>{formatSignedPercent(row.shareOfFx)}</td>
                                  <td>{formatSignedPercent(row.shareOfPortfolio)}</td>
                                </tr>
                              ))
                            )}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </>
                )}
                </section>
              )}

              {activeTab === 'drawdownByAsset' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.drawdownByAsset')}</h2>
                </header>
                {drawdownRows.length === 0 ? (
                  <p className="risk-card__empty">{t('risk.noSeries')}</p>
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={280}>
                      <BarChart data={drawdownChartRows} layout="vertical" margin={{ top: 4, right: 12, bottom: 4, left: 8 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                        <XAxis
                          type="number"
                          stroke="var(--text-secondary)"
                          tickFormatter={(value) => `${toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}%`}
                        />
                        <YAxis type="category" dataKey="ticker" stroke="var(--text-secondary)" width={72} />
                        <Tooltip formatter={(value: number | string | undefined) => formatSignedPercent(toNumber(value))} />
                        <Bar dataKey="drawdown" name={t('risk.table.drawdown')} isAnimationActive={false}>
                          {drawdownChartRows.map((row) => (
                            <Cell key={row.ticker} fill={row.drawdown < 0 ? '#f87171' : '#34d399'} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                    <div className="risk-table-wrapper">
                      <table className="risk-table risk-table--compact">
                        <thead>
                          <tr>
                            <th>{t('risk.table.ticker')}</th>
                            <th>{t('risk.table.drawdown')}</th>
                            <th>{t('risk.table.volatility')}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {drawdownRows.map((row) => (
                            <tr key={`drawdown-${row.ticker}`}>
                              <td>{row.ticker}</td>
                              <td className={row.drawdown < 0 ? 'risk-table__value risk-table__value--negative' : 'risk-table__value risk-table__value--positive'}>
                                {formatSignedPercent(row.drawdown)}
                              </td>
                              <td>{formatSignedPercent(row.volatility)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
                </section>
              )}

              {activeTab === 'correlation' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.correlation')}</h2>
                </header>
                {correlationTickers.length < 2 ? (
                  <p className="risk-card__empty">{t('risk.noSeries')}</p>
                ) : (
                  <div className="risk-correlation-wrapper">
                    <table className="risk-correlation">
                      <thead>
                        <tr>
                          <th>{t('risk.table.correlation')}</th>
                          {correlationTickers.map((ticker) => (
                            <th key={`col-${ticker}`}>{ticker}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {correlationTickers.map((rowTicker) => (
                          <tr key={`row-${rowTicker}`}>
                            <th>{rowTicker}</th>
                            {correlationTickers.map((colTicker) => {
                              const correlation = resolveCorrelation(rowTicker, colTicker);
                              return (
                                <td
                                  key={`${rowTicker}-${colTicker}`}
                                  style={{ backgroundColor: correlationCellColor(correlation) }}
                                >
                                  {correlation === null
                                    ? '—'
                                    : toNumber(correlation).toLocaleString(numberLocale, {
                                      minimumFractionDigits: 2,
                                      maximumFractionDigits: 2,
                                    })}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                </section>
              )}

              {activeTab === 'riskReturn' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.riskReturn')}</h2>
                  <div className="risk-return-filters" role="group" aria-label={t('risk.returnFilter.ariaLabel')}>
                    {scatterFilterOptions.map((option) => (
                      <button
                        key={option.value}
                        type="button"
                        className={`risk-return-filters__btn ${scatterFilter === option.value ? 'risk-return-filters__btn--active' : ''}`}
                        onClick={() => setScatterFilter(option.value)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </header>
                {scatterRowsFiltered.length === 0 ? (
                  <p className="risk-card__empty">{t('risk.noSeries')}</p>
                ) : (
                  <>
                    <ResponsiveContainer width="100%" height={280}>
                      <ScatterChart margin={{ top: 8, right: 12, bottom: 8, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                        <XAxis
                          type="number"
                          dataKey="volatilityPlot"
                          name={t('risk.table.volatility')}
                          stroke="var(--text-secondary)"
                          tickFormatter={(value) => `${toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}%`}
                        />
                        <YAxis
                          type="number"
                          dataKey="returnPctPlot"
                          name={t('risk.table.returnPct')}
                          stroke="var(--text-secondary)"
                          tickFormatter={(value) => `${toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}%`}
                        />
                        <Tooltip content={renderScatterTooltip} />
                        <Legend />
                        <ReferenceLine y={0} stroke="rgba(148, 163, 184, 0.42)" />
                        <ReferenceLine x={toNumber(risk.portfolio_volatility)} stroke="rgba(34, 211, 238, 0.4)" />
                        {(scatterFilter === 'all' || scatterFilter === 'positive') && (
                          <Scatter name={t('risk.legend.positive')} data={scatterPositive} fill="#34d399" />
                        )}
                        {(scatterFilter === 'all' || scatterFilter === 'negative') && (
                          <Scatter name={t('risk.legend.negative')} data={scatterNegative} fill="#f87171" />
                        )}
                        {(scatterFilter === 'all' || scatterFilter === 'neutral') && (
                          <Scatter name={t('risk.legend.neutral')} data={scatterNeutral} fill="#94a3b8" />
                        )}
                      </ScatterChart>
                    </ResponsiveContainer>
                    <div className="risk-table-wrapper">
                      <table className="risk-table risk-table--compact">
                        <thead>
                          <tr>
                            <th>{t('risk.table.ticker')}</th>
                            <th>{t('risk.table.volatility')}</th>
                            <th>{t('risk.table.returnPct')}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {scatterRowsFiltered.map((row) => (
                            <tr key={`scatter-${row.ticker}`}>
                              <td>{row.ticker}</td>
                              <td>{formatSignedPercent(row.volatility)}</td>
                              <td className={row.returnPct < 0 ? 'risk-table__value risk-table__value--negative' : row.returnPct > 0 ? 'risk-table__value risk-table__value--positive' : 'risk-table__value'}>
                                {formatSignedPercent(row.returnPct)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
                </section>
              )}

              {activeTab === 'purchasingPower' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.purchasingPower')}</h2>
                </header>
                {inflationSeries.length === 0 ? (
                  <p className="risk-card__empty">{t('risk.noSeries')}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart data={inflationSeries} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                      <XAxis dataKey="date" tickFormatter={formatTickDate} stroke="var(--text-secondary)" />
                      <YAxis
                        stroke="var(--text-secondary)"
                        tickFormatter={(value) => formatBrl(toNumber(value))}
                        width={110}
                      />
                      <Tooltip
                        labelFormatter={(value) => toTitleCase(formatTickDate(String(value || '')))}
                        formatter={(value: number | string | undefined, name?: string) => [
                          formatBrl(toNumber(value)),
                          String(name || ''),
                        ]}
                      />
                      <Legend />
                      <Line
                        type="monotone"
                        dataKey="nominalValue"
                        name={t('risk.inflation.nominalValue')}
                        stroke="#22d3ee"
                        strokeWidth={2.4}
                        dot={false}
                        isAnimationActive={false}
                      />
                      <Line
                        type="monotone"
                        dataKey="realValue"
                        name={t('risk.inflation.realValue')}
                        stroke="#f59e0b"
                        strokeWidth={2.4}
                        dot={false}
                        isAnimationActive={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                )}
                </section>
              )}

              {activeTab === 'sensitivity' && (
                <section className="risk-card">
                <header className="risk-card__header">
                  <h2>{t('risk.sections.sensitivity')}</h2>
                </header>
                <p className="risk-card__empty risk-card__empty--left">{t('risk.sensitivityUnavailable')}</p>
                </section>
              )}
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default RiskPage;
