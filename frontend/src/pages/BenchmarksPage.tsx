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
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { api, type BenchmarksResponse } from '../services/api';
import {
  buildAnalyticsCacheKey,
  getOrFetchCachedAnalytics,
} from '../services/analyticsCache';
import {
  normalizeBenchmarkCode,
  reconcileBenchmarkSelectionState,
  type BenchmarkSelectionState,
  resolveDefaultBenchmarkSelection,
} from './benchmarksPage.state.js';
import './BenchmarksPage.scss';

const BENCHMARK_OPTIONS = [
  'IBOV',
  'IBRA',
  'IBRX100',
  'IBRX50',
  'IDIV',
  'SMLL',
  'MLCX',
  'ICON',
  'IFNC',
  'IMAT',
  'IMOB',
  'INDX',
  'UTIL',
  'IEEX',
  'IGCT',
  'ITAG',
  'IVBX2',
  'IFIX',
  'CDI',
  'IPCA',
  'SELIC',
  'POUPANCA',
  'SNP500',
  'NASDAQ',
  'DOWJONES',
  'RUSSELL2000',
  'FTSE100',
  'DAX',
  'CAC40',
  'NIKKEI225',
  'HANGSENG',
  'TSX',
] as const;
const PERIOD_OPTIONS = ['1M', '3M', '6M', '1Y', '2Y', '5Y', 'MAX'] as const;
const SERIES_COLORS = [
  '#22d3ee',
  '#818cf8',
  '#34d399',
  '#f59e0b',
  '#fb7185',
  '#f87171',
  '#38bdf8',
  '#a78bfa',
  '#2dd4bf',
  '#f97316',
  '#84cc16',
  '#f43f5e',
  '#14b8a6',
  '#facc15',
  '#60a5fa',
  '#fb923c',
  '#10b981',
  '#c084fc',
  '#0ea5e9',
  '#ef4444',
  '#06b6d4',
  '#eab308',
  '#8b5cf6',
  '#22c55e',
];

type BenchmarkGroup = {
  id: string;
  labelKey: string;
  codes: string[];
};
const normalizeCode = normalizeBenchmarkCode;

const BENCHMARK_GROUPS: BenchmarkGroup[] = [
  {
    id: 'fii',
    labelKey: 'benchmarks.groups.fii',
    codes: ['IFIX'],
  },
  {
    id: 'macro',
    labelKey: 'benchmarks.groups.macro',
    codes: ['CDI', 'SELIC', 'IPCA', 'POUPANCA'],
  },
  {
    id: 'brazilBroad',
    labelKey: 'benchmarks.groups.brazilBroad',
    codes: ['IBOV', 'IBRA', 'IBRX100', 'IBRX50', 'IVBX2'],
  },
  {
    id: 'international',
    labelKey: 'benchmarks.groups.international',
    codes: ['SNP500', 'NASDAQ', 'DOWJONES', 'RUSSELL2000', 'FTSE100', 'DAX', 'CAC40', 'NIKKEI225', 'HANGSENG', 'TSX'],
  },
  {
    id: 'brazilSectors',
    labelKey: 'benchmarks.groups.brazilSectors',
    codes: ['IDIV', 'SMLL', 'MLCX', 'ICON', 'IFNC', 'IMAT', 'IMOB', 'INDX', 'UTIL', 'IEEX', 'IGCT', 'ITAG'],
  },
];

const BENCHMARK_GROUP_BY_CODE = new Map<string, string>(
  BENCHMARK_GROUPS.flatMap((group) => group.codes.map((code) => [normalizeCode(code), group.id] as const))
);

type NormalizedChartRow = { date: string } & Record<string, string | number | null | undefined>;

type ReturnRow = {
  key: string;
  label: string;
  returnPct: number;
  alphaVsPortfolio: number;
  isPortfolio: boolean;
  isSelected: boolean;
  hasSeries: boolean;
};

const toNumber = (value: unknown): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
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

const BenchmarksPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio } = usePortfolioData();
  const [selectedPeriod, setSelectedPeriod] = useState<(typeof PERIOD_OPTIONS)[number]>('1Y');
  const [selectionState, setSelectionState] = useState<BenchmarkSelectionState>({
    selectedBenchmarks: [],
    selectionInitialized: false,
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<BenchmarksResponse | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const formatSignedPercent = (value: number, fractionDigits = 2): string =>
    `${value > 0 ? '+' : ''}${toNumber(value).toLocaleString(numberLocale, {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits,
    })}%`;
  const formatPoints = (value: number | null): string => {
    if (value === null || value === undefined || !Number.isFinite(value)) return '—';
    return toNumber(value).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const benchmarkOptions = useMemo(
    () => BENCHMARK_OPTIONS.map((benchmark) => ({
      value: benchmark,
      label: t(`benchmarks.options.${benchmark}`, { defaultValue: benchmark }),
    })),
    [t]
  );

  const benchmarkLabelByCode = useMemo(
    () => new Map<string, string>(benchmarkOptions.map((item) => [normalizeCode(item.value), item.label])),
    [benchmarkOptions]
  );

  const periodOptions = useMemo(
    () => PERIOD_OPTIONS.map((period) => ({
      value: period,
      label: t(`benchmarks.periods.${period}`),
    })),
    [t]
  );

  const cacheKey = useMemo(() => {
    if (!selectedPortfolio) return '';
    return buildAnalyticsCacheKey('benchmarks', [selectedPortfolio, selectedPeriod]);
  }, [selectedPeriod, selectedPortfolio]);

  useEffect(() => {
    if (!selectedPortfolio || !cacheKey) {
      setPayload(null);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    getOrFetchCachedAnalytics(
      cacheKey,
      () => api.getBenchmarks(selectedPortfolio, 'IBOV', selectedPeriod),
      { ttlMs: 3 * 60 * 1000 }
    )
      .then((response) => {
        if (cancelled) return;
        setPayload(response);
      })
      .catch((reason: unknown) => {
        if (cancelled) return;
        setPayload(null);
        setError(reason instanceof Error ? reason.message : 'Failed to load benchmark comparison');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [cacheKey, selectedPeriod, selectedPortfolio]);

  useEffect(() => {
    setSelectionState({
      selectedBenchmarks: [],
      selectionInitialized: false,
    });
  }, [selectedPortfolio]);

  const availableBenchmarkCodes = useMemo(() => {
    if (!payload) return [];

    const rawCodes = Array.isArray(payload.available_benchmarks) && payload.available_benchmarks.length > 0
      ? payload.available_benchmarks
      : payload.benchmarks.map((item) => item.benchmark);

    const uniqueCodes = rawCodes
      .map((item) => normalizeCode(item))
      .filter(Boolean)
      .filter((item, index, values) => values.indexOf(item) === index);

    const known = BENCHMARK_OPTIONS.filter((option) => uniqueCodes.includes(option));
    const extras = uniqueCodes.filter((code) => !BENCHMARK_OPTIONS.includes(code as (typeof BENCHMARK_OPTIONS)[number]));
    return [...known, ...extras];
  }, [payload]);

  useEffect(() => {
    setSelectionState((previous) => {
      const next = reconcileBenchmarkSelectionState(previous, availableBenchmarkCodes);
      if (!next.changed) return previous;
      return {
        selectedBenchmarks: next.selectedBenchmarks,
        selectionInitialized: next.selectionInitialized,
      };
    });
  }, [availableBenchmarkCodes]);

  const selectedBenchmarks = selectionState.selectedBenchmarks;

  const selectedBenchmarkSet = useMemo(
    () => new Set(selectedBenchmarks.map((code) => normalizeCode(code))),
    [selectedBenchmarks]
  );

  const benchmarkDataByCode = useMemo(() => {
    const map = new Map<string, BenchmarksResponse['benchmarks'][number]>();
    (payload?.benchmarks || []).forEach((item) => {
      const key = normalizeCode(item.benchmark);
      if (!key) return;
      map.set(key, item);
    });
    return map;
  }, [payload]);

  const normalizedBenchmarkEntries = useMemo(() => {
    if (!payload) return [] as Array<[string, Array<{ date: string; value: number }>]>;

    const byBenchmark = payload.normalized_series?.benchmarks || {};
    return Object.entries(byBenchmark)
      .map(([code, series]) => [normalizeCode(code), series] as [string, Array<{ date: string; value: number }>])
      .filter(([code, series]) => Boolean(code) && Array.isArray(series) && series.length > 1)
      .sort((left, right) => {
        const leftIndex = availableBenchmarkCodes.indexOf(left[0]);
        const rightIndex = availableBenchmarkCodes.indexOf(right[0]);
        const leftRank = leftIndex === -1 ? Number.POSITIVE_INFINITY : leftIndex;
        const rightRank = rightIndex === -1 ? Number.POSITIVE_INFINITY : rightIndex;
        return leftRank - rightRank;
      });
  }, [availableBenchmarkCodes, payload]);

  const selectedNormalizedBenchmarkEntries = useMemo(
    () => normalizedBenchmarkEntries.filter(([benchmark]) => selectedBenchmarkSet.has(benchmark)),
    [normalizedBenchmarkEntries, selectedBenchmarkSet]
  );

  const selectionRows = useMemo(() => availableBenchmarkCodes.map((code) => {
    const item = benchmarkDataByCode.get(code);
    const hasSeries = Boolean(item?.has_series) || normalizedBenchmarkEntries.some(([benchmark]) => benchmark === code);
    const returnPct = hasSeries ? toNumber(item?.return_pct) : null;
    const alphaVsPortfolio = hasSeries && payload
      ? toNumber(payload.portfolio_return_pct) - toNumber(item?.return_pct)
      : null;

    return {
      code,
      label: benchmarkLabelByCode.get(code) || code,
      symbol: item?.symbol || null,
      selected: selectedBenchmarkSet.has(code),
      hasSeries,
      returnPct,
      alphaVsPortfolio,
      currentPoints: item?.current_points ?? null,
      monthMin: item?.month_min ?? null,
      monthMax: item?.month_max ?? null,
      week52Min: item?.week52_min ?? null,
      week52Max: item?.week52_max ?? null,
    };
  }), [availableBenchmarkCodes, benchmarkDataByCode, benchmarkLabelByCode, normalizedBenchmarkEntries, payload, selectedBenchmarkSet]);

  const visibleSelectionRows = useMemo(() => (
    selectionRows.filter((row) => (
      row.hasSeries
      || row.currentPoints !== null
      || row.monthMin !== null
      || row.monthMax !== null
      || row.week52Min !== null
      || row.week52Max !== null
    ))
  ), [selectionRows]);

  const visibleBenchmarkCodes = useMemo(
    () => visibleSelectionRows.map((row) => row.code),
    [visibleSelectionRows]
  );

  const groupedSelectionRows = useMemo(() => {
    const groupedRows = BENCHMARK_GROUPS.map((group) => ({
      id: group.id,
      label: t(group.labelKey),
      rows: [] as typeof selectionRows,
    }));
    const groupById = new Map(groupedRows.map((group) => [group.id, group]));
    const otherGroup = {
      id: 'other',
      label: t('benchmarks.groups.other'),
      rows: [] as typeof selectionRows,
    };

    for (const row of visibleSelectionRows) {
      const groupId = BENCHMARK_GROUP_BY_CODE.get(normalizeCode(row.code));
      if (!groupId) {
        otherGroup.rows.push(row);
        continue;
      }
      const group = groupById.get(groupId);
      if (!group) {
        otherGroup.rows.push(row);
        continue;
      }
      group.rows.push(row);
    }

    return [
      ...groupedRows.filter((group) => group.rows.length > 0),
      ...(otherGroup.rows.length > 0 ? [otherGroup] : []),
    ];
  }, [visibleSelectionRows, t]);

  const returnRows = useMemo<ReturnRow[]>(() => {
    if (!payload) return [];

    return [
      {
        key: 'PORTFOLIO',
        label: t('benchmarks.portfolioSeries'),
        returnPct: toNumber(payload.portfolio_return_pct),
        alphaVsPortfolio: 0,
        isPortfolio: true,
        isSelected: false,
        hasSeries: true,
      },
      ...visibleSelectionRows
        .filter((row) => row.selected)
        .map((row) => ({
          key: row.code,
          label: row.label,
          returnPct: row.returnPct === null ? 0 : row.returnPct,
          alphaVsPortfolio: row.alphaVsPortfolio === null ? 0 : row.alphaVsPortfolio,
          isPortfolio: false,
          isSelected: row.selected,
          hasSeries: row.hasSeries,
        })),
    ];
  }, [payload, visibleSelectionRows, t]);

  const normalizedChartRows = useMemo<NormalizedChartRow[]>(() => {
    if (!payload) return [];
    const byDate = new Map<string, NormalizedChartRow>();

    for (const point of payload.normalized_series?.portfolio || []) {
      const date = String(point.date || '');
      if (!date) continue;
      if (!byDate.has(date)) byDate.set(date, { date });
      byDate.get(date)!.portfolio = toNumber(point.value);
    }

    for (const [benchmark, series] of selectedNormalizedBenchmarkEntries) {
      for (const point of series || []) {
        const date = String(point.date || '');
        if (!date) continue;
        if (!byDate.has(date)) byDate.set(date, { date });
        byDate.get(date)![benchmark] = toNumber(point.value);
      }
    }

    return Array.from(byDate.values())
      .sort((left, right) => String(left.date).localeCompare(String(right.date)));
  }, [payload, selectedNormalizedBenchmarkEntries]);

  const normalizedSeriesMissing = useMemo(() => {
    const available = new Set(selectedNormalizedBenchmarkEntries.map(([benchmark]) => normalizeCode(benchmark)));
    return selectionRows
      .filter((row) => row.selected)
      .filter((row) => !available.has(row.code))
      .map((row) => row.label);
  }, [selectedNormalizedBenchmarkEntries, selectionRows]);

  const colorBySeriesKey = useMemo(() => {
    const map = new Map<string, string>();
    map.set('portfolio', '#22d3ee');
    availableBenchmarkCodes.forEach((benchmark, index) => {
      map.set(benchmark, SERIES_COLORS[(index + 1) % SERIES_COLORS.length]);
    });
    return map;
  }, [availableBenchmarkCodes]);

  const formatTickDate = (date: string): string => {
    const parsed = parseIsoDateUtc(date);
    if (Number.isNaN(parsed.getTime())) return date;
    return parsed.toLocaleDateString(numberLocale, {
      month: 'short',
      year: '2-digit',
      timeZone: 'UTC',
    });
  };

  const selectedBenchmarkReturn = toNumber(payload?.selected_benchmark?.return_pct);
  const alphaValue = payload?.alpha === null || payload?.alpha === undefined
    ? null
    : toNumber(payload.alpha);

  const selectedCount = visibleSelectionRows.filter((row) => row.selected).length;
  const hasSameCodes = (left: string[], right: string[]) => (
    left.length === right.length && left.every((value, index) => value === right[index])
  );

  const toggleBenchmark = (code: string, checked: boolean) => {
    setSelectionState((previous) => {
      const next = new Set(previous.selectedBenchmarks.map((item) => normalizeCode(item)));
      if (checked) {
        next.add(code);
      } else {
        next.delete(code);
      }
      const nextSelectedBenchmarks = availableBenchmarkCodes.filter((item) => next.has(item));
      if (previous.selectionInitialized && hasSameCodes(previous.selectedBenchmarks, nextSelectedBenchmarks)) {
        return previous;
      }
      return {
        selectedBenchmarks: nextSelectedBenchmarks,
        selectionInitialized: true,
      };
    });
  };

  const selectAllBenchmarks = () => {
    setSelectionState((previous) => {
      const nextSelectedBenchmarks = [...visibleBenchmarkCodes];
      if (previous.selectionInitialized && hasSameCodes(previous.selectedBenchmarks, nextSelectedBenchmarks)) {
        return previous;
      }
      return {
        selectedBenchmarks: nextSelectedBenchmarks,
        selectionInitialized: true,
      };
    });
  };

  const clearAllBenchmarks = () => {
    setSelectionState((previous) => {
      if (previous.selectionInitialized && previous.selectedBenchmarks.length === 0) {
        return previous;
      }
      return {
        selectedBenchmarks: [],
        selectionInitialized: true,
      };
    });
  };

  const resetToDefaultBenchmarks = () => {
    setSelectionState((previous) => {
      const nextSelectedBenchmarks = resolveDefaultBenchmarkSelection(visibleBenchmarkCodes);
      if (previous.selectionInitialized && hasSameCodes(previous.selectedBenchmarks, nextSelectedBenchmarks)) {
        return previous;
      }
      return {
        selectedBenchmarks: nextSelectedBenchmarks,
        selectionInitialized: true,
      };
    });
  };

  return (
    <Layout>
      <div className="benchmarks-page">
        <div className="benchmarks-page__header">
          <h1 className="benchmarks-page__title">{t('benchmarks.title')}</h1>
          <div className="benchmarks-page__filters">
            {portfolioOptions.length > 0 && (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('benchmarks.selectPortfolio')}
                className="benchmarks-page__dropdown benchmarks-page__dropdown--portfolio"
                size="sm"
              />
            )}
            <SharedDropdown
              value={selectedPeriod}
              options={periodOptions}
              onChange={(value) => setSelectedPeriod(value as (typeof PERIOD_OPTIONS)[number])}
              ariaLabel={t('benchmarks.selectPeriod')}
              className="benchmarks-page__dropdown"
              size="sm"
            />
          </div>
        </div>

        {loading && <div className="benchmarks-page__state">{t('common.loading')}</div>}

        {!loading && !error && !payload && (
          <div className="benchmarks-page__state">{t('dashboard.noData')}</div>
        )}

        {!loading && error && (
          <div className="benchmarks-page__state benchmarks-page__state--error">
            <p>{t('benchmarks.loadError')}</p>
            <code>{error}</code>
          </div>
        )}

        {!loading && !error && payload && (
          <div className="benchmarks-page__content">
            <div className="benchmarks-kpis">
              <article className="benchmarks-kpi">
                <span className="benchmarks-kpi__label">{t('benchmarks.kpis.portfolioReturn')}</span>
                <span className={`benchmarks-kpi__value ${toNumber(payload.portfolio_return_pct) >= 0 ? 'benchmarks-kpi__value--positive' : 'benchmarks-kpi__value--negative'}`}>
                  {formatSignedPercent(toNumber(payload.portfolio_return_pct))}
                </span>
              </article>
              <article className="benchmarks-kpi">
                <span className="benchmarks-kpi__label">{t('benchmarks.kpis.selectedBenchmarkReturn')}</span>
                <span className={`benchmarks-kpi__value ${selectedBenchmarkReturn >= 0 ? 'benchmarks-kpi__value--positive' : 'benchmarks-kpi__value--negative'}`}>
                  {formatSignedPercent(selectedBenchmarkReturn)}
                </span>
              </article>
              <article className="benchmarks-kpi">
                <span className="benchmarks-kpi__label">{t('benchmarks.kpis.alpha')}</span>
                <span className={`benchmarks-kpi__value ${toNumber(alphaValue) >= 0 ? 'benchmarks-kpi__value--positive' : 'benchmarks-kpi__value--negative'}`}>
                  {alphaValue === null ? '—' : formatSignedPercent(alphaValue)}
                </span>
              </article>
              <article className="benchmarks-kpi">
                <span className="benchmarks-kpi__label">{t('benchmarks.kpis.range')}</span>
                <span className="benchmarks-kpi__value">
                  {`${formatTickDate(payload.from)} - ${formatTickDate(payload.to)}`}
                </span>
              </article>
            </div>

            <section className="benchmarks-card">
              <header className="benchmarks-card__header">
                <h2>{t('benchmarks.sections.normalized')}</h2>
                <div className="benchmarks-selection__actions">
                  <span className="benchmarks-selection__summary">
                    {t('benchmarks.selection.selectedSummary', {
                      selected: selectedCount,
                      total: visibleBenchmarkCodes.length,
                    })}
                  </span>
                  <button
                    type="button"
                    className="benchmarks-selection__btn"
                    onClick={selectAllBenchmarks}
                    disabled={selectedCount === visibleBenchmarkCodes.length}
                  >
                    {t('benchmarks.selection.selectAll')}
                  </button>
                  <button
                    type="button"
                    className="benchmarks-selection__btn"
                    onClick={clearAllBenchmarks}
                    disabled={selectedCount === 0}
                  >
                    {t('benchmarks.selection.clearAll')}
                  </button>
                  <button
                    type="button"
                    className="benchmarks-selection__btn"
                    onClick={resetToDefaultBenchmarks}
                    disabled={visibleBenchmarkCodes.length === 0}
                  >
                    {t('benchmarks.selection.resetDefault')}
                  </button>
                </div>
              </header>
              {normalizedChartRows.length === 0 ? (
                <p className="benchmarks-card__empty">{t('benchmarks.noNormalizedSeries')}</p>
              ) : (
                <>
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart data={normalizedChartRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                      <XAxis dataKey="date" tickFormatter={formatTickDate} stroke="var(--text-secondary)" />
                      <YAxis
                        stroke="var(--text-secondary)"
                        tickFormatter={(value) => toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}
                        width={64}
                      />
                      <Tooltip
                        labelFormatter={(value) => formatTickDate(String(value || ''))}
                        formatter={(value: number | string | undefined, name?: string) => [
                          toNumber(value).toLocaleString(numberLocale, {
                            minimumFractionDigits: 2,
                            maximumFractionDigits: 2,
                          }),
                          String(name || ''),
                        ]}
                      />
                      <Legend />
                      <ReferenceLine y={100} stroke="rgba(148, 163, 184, 0.4)" />
                      <Line
                        type="monotone"
                        dataKey="portfolio"
                        name={t('benchmarks.portfolioSeries')}
                        stroke={colorBySeriesKey.get('portfolio') || '#22d3ee'}
                        strokeWidth={2.5}
                        dot={false}
                        isAnimationActive={false}
                      />
                      {selectedNormalizedBenchmarkEntries.map(([benchmark]) => (
                        <Line
                          key={`line-${benchmark}`}
                          type="monotone"
                          dataKey={benchmark}
                          name={benchmarkLabelByCode.get(benchmark) || benchmark}
                          stroke={colorBySeriesKey.get(benchmark) || '#818cf8'}
                          strokeWidth={2}
                          dot={false}
                          connectNulls
                          isAnimationActive={false}
                        />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                  {normalizedSeriesMissing.length > 0 && (
                    <p className="benchmarks-card__hint">
                      {t('benchmarks.missingSeries', { list: normalizedSeriesMissing.join(', ') })}
                    </p>
                  )}
                </>
              )}
            </section>

            <section className="benchmarks-card">
              <header className="benchmarks-card__header">
                <h2>{t('benchmarks.sections.returns')}</h2>
              </header>
              {returnRows.length === 0 ? (
                <p className="benchmarks-card__empty">{t('benchmarks.noSeries')}</p>
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={returnRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                    <XAxis dataKey="label" stroke="var(--text-secondary)" interval={0} angle={-16} textAnchor="end" height={74} />
                    <YAxis
                      stroke="var(--text-secondary)"
                      tickFormatter={(value) => `${toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}%`}
                      width={64}
                    />
                    <Tooltip formatter={(value: number | string | undefined) => formatSignedPercent(toNumber(value))} />
                    <ReferenceLine y={0} stroke="rgba(148, 163, 184, 0.4)" />
                    <Bar dataKey="returnPct" isAnimationActive={false}>
                      {returnRows.map((row) => {
                        if (row.isPortfolio) return <Cell key={`return-${row.key}`} fill="#22d3ee" />;
                        if (!row.hasSeries) return <Cell key={`return-${row.key}`} fill="rgba(148, 163, 184, 0.35)" />;
                        if (row.isSelected) return <Cell key={`return-${row.key}`} fill="#818cf8" />;
                        return <Cell key={`return-${row.key}`} fill={row.returnPct >= 0 ? '#34d399' : '#f87171'} />;
                      })}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </section>

            <section className="benchmarks-card">
              <header className="benchmarks-card__header">
                <h2>{t('benchmarks.sections.details')}</h2>
              </header>
              {visibleSelectionRows.length === 0 ? (
                <p className="benchmarks-card__empty">{t('benchmarks.noSeries')}</p>
              ) : (
                <div className="benchmarks-groups">
                  {groupedSelectionRows.map((group) => (
                    <article key={group.id} className="benchmarks-group">
                      <header className="benchmarks-group__header">
                        <h3>{group.label}</h3>
                        <span>{t('benchmarks.groups.count', { count: group.rows.length })}</span>
                      </header>
                      <div className="benchmarks-table-wrapper">
                        <table className="benchmarks-table">
                          <thead>
                            <tr>
                              <th className="benchmarks-table__checkbox-col">{t('benchmarks.table.visible')}</th>
                              <th>{t('benchmarks.table.benchmark')}</th>
                              <th>{t('benchmarks.table.returnPct')}</th>
                              <th>{t('benchmarks.table.alphaVsPortfolio')}</th>
                              <th>{t('benchmarks.table.points')}</th>
                              <th>{t('benchmarks.table.monthMin')}</th>
                              <th>{t('benchmarks.table.monthMax')}</th>
                              <th>{t('benchmarks.table.week52Min')}</th>
                              <th>{t('benchmarks.table.week52Max')}</th>
                              <th>{t('benchmarks.table.series')}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {group.rows.map((row) => (
                              <tr key={`detail-${group.id}-${row.code}`} className={row.selected ? 'benchmarks-table__row--selected' : ''}>
                                <td className="benchmarks-table__checkbox-col">
                                  <input
                                    type="checkbox"
                                    className="benchmarks-table__checkbox"
                                    checked={row.selected}
                                    onChange={(event) => toggleBenchmark(row.code, event.target.checked)}
                                    aria-label={t('benchmarks.selection.toggleOne', { benchmark: row.label })}
                                  />
                                </td>
                                <td>
                                  <div className="benchmarks-table__benchmark-cell">
                                    <span>{row.label}</span>
                                    {row.symbol ? <small>{row.symbol}</small> : null}
                                  </div>
                                </td>
                                <td className={
                                  row.returnPct === null
                                    ? 'benchmarks-table__value'
                                    : row.returnPct >= 0
                                      ? 'benchmarks-table__value benchmarks-table__value--positive'
                                      : 'benchmarks-table__value benchmarks-table__value--negative'
                                }
                                >
                                  {row.returnPct === null ? '—' : formatSignedPercent(row.returnPct)}
                                </td>
                                <td className={
                                  row.alphaVsPortfolio === null
                                    ? 'benchmarks-table__value'
                                    : row.alphaVsPortfolio >= 0
                                      ? 'benchmarks-table__value benchmarks-table__value--positive'
                                      : 'benchmarks-table__value benchmarks-table__value--negative'
                                }
                                >
                                  {row.alphaVsPortfolio === null ? '—' : formatSignedPercent(row.alphaVsPortfolio)}
                                </td>
                                <td className="benchmarks-table__value">
                                  {formatPoints(row.currentPoints)}
                                </td>
                                <td className="benchmarks-table__value">
                                  {formatPoints(row.monthMin)}
                                </td>
                                <td className="benchmarks-table__value">
                                  {formatPoints(row.monthMax)}
                                </td>
                                <td className="benchmarks-table__value">
                                  {formatPoints(row.week52Min)}
                                </td>
                                <td className="benchmarks-table__value">
                                  {formatPoints(row.week52Max)}
                                </td>
                                <td>
                                  {row.hasSeries
                                    ? t('benchmarks.selection.seriesAvailable')
                                    : t('benchmarks.selection.seriesUnavailable')}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </article>
                  ))}
                </div>
              )}
            </section>
          </div>
        )}
      </div>
    </Layout>
  );
};

export default BenchmarksPage;
