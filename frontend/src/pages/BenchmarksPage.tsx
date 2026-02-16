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
import './BenchmarksPage.scss';

const BENCHMARK_OPTIONS = ['IBOV', 'CDI', 'IPCA', 'SNP500', 'IFIX', 'POUPANCA', 'TSX'] as const;
const PERIOD_OPTIONS = ['1M', '3M', '6M', '1Y', '2Y', '5Y', 'MAX'] as const;
const SERIES_COLORS = ['#22d3ee', '#818cf8', '#34d399', '#f59e0b', '#fb7185', '#f87171', '#38bdf8'];

type NormalizedChartRow = { date: string } & Record<string, string | number | null | undefined>;

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
  const [selectedBenchmark, setSelectedBenchmark] = useState<(typeof BENCHMARK_OPTIONS)[number]>('IBOV');
  const [selectedPeriod, setSelectedPeriod] = useState<(typeof PERIOD_OPTIONS)[number]>('1Y');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<BenchmarksResponse | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const formatSignedPercent = (value: number, fractionDigits = 2): string =>
    `${value > 0 ? '+' : ''}${toNumber(value).toLocaleString(numberLocale, {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits,
    })}%`;

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const benchmarkOptions = useMemo(
    () => BENCHMARK_OPTIONS.map((benchmark) => ({
      value: benchmark,
      label: t(`benchmarks.options.${benchmark}`),
    })),
    [t]
  );

  const benchmarkLabelByCode = useMemo(
    () => new Map<string, string>(benchmarkOptions.map((item) => [String(item.value), item.label])),
    [benchmarkOptions]
  );

  const periodOptions = useMemo(
    () => PERIOD_OPTIONS.map((period) => ({
      value: period,
      label: t(`benchmarks.periods.${period}`),
    })),
    [t]
  );

  useEffect(() => {
    if (!selectedPortfolio) {
      setPayload(null);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api.getBenchmarks(selectedPortfolio, selectedBenchmark, selectedPeriod)
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
  }, [selectedBenchmark, selectedPeriod, selectedPortfolio]);

  const selectedBenchmarkReturn = toNumber(payload?.selected_benchmark?.return_pct);
  const alphaValue = payload?.alpha === null || payload?.alpha === undefined
    ? null
    : toNumber(payload.alpha);

  const returnRows = useMemo(() => {
    if (!payload) return [];

    return [
      {
        key: 'PORTFOLIO',
        label: t('benchmarks.portfolioSeries'),
        returnPct: toNumber(payload.portfolio_return_pct),
        alphaVsPortfolio: 0,
        isPortfolio: true,
        isSelected: false,
      },
      ...payload.benchmarks.map((item) => ({
        key: item.benchmark,
        label: benchmarkLabelByCode.get(item.benchmark) || item.benchmark,
        returnPct: toNumber(item.return_pct),
        alphaVsPortfolio: toNumber(payload.portfolio_return_pct) - toNumber(item.return_pct),
        isPortfolio: false,
        isSelected: String(payload.selected_benchmark?.benchmark || '').toUpperCase() === String(item.benchmark || '').toUpperCase(),
      })),
    ];
  }, [benchmarkLabelByCode, payload, t]);

  const normalizedBenchmarkEntries = useMemo(() => {
    if (!payload) return [];
    const byBenchmark = payload.normalized_series?.benchmarks || {};
    return Object.entries(byBenchmark)
      .filter(([, series]) => Array.isArray(series) && series.length > 1)
      .sort((left, right) => {
        const leftIndex = BENCHMARK_OPTIONS.indexOf(left[0] as (typeof BENCHMARK_OPTIONS)[number]);
        const rightIndex = BENCHMARK_OPTIONS.indexOf(right[0] as (typeof BENCHMARK_OPTIONS)[number]);
        const leftRank = leftIndex === -1 ? Number.POSITIVE_INFINITY : leftIndex;
        const rightRank = rightIndex === -1 ? Number.POSITIVE_INFINITY : rightIndex;
        return leftRank - rightRank;
      });
  }, [payload]);

  const normalizedChartRows = useMemo<NormalizedChartRow[]>(() => {
    if (!payload) return [];
    const byDate = new Map<string, NormalizedChartRow>();

    for (const point of payload.normalized_series?.portfolio || []) {
      const date = String(point.date || '');
      if (!date) continue;
      if (!byDate.has(date)) byDate.set(date, { date });
      byDate.get(date)!.portfolio = toNumber(point.value);
    }

    // Merge every benchmark series by date so each line can render independently.
    for (const [benchmark, series] of normalizedBenchmarkEntries) {
      for (const point of series || []) {
        const date = String(point.date || '');
        if (!date) continue;
        if (!byDate.has(date)) byDate.set(date, { date });
        byDate.get(date)![benchmark] = toNumber(point.value);
      }
    }

    return Array.from(byDate.values())
      .sort((left, right) => String(left.date).localeCompare(String(right.date)));
  }, [normalizedBenchmarkEntries, payload]);

  const normalizedSeriesMissing = useMemo(() => {
    if (!payload) return [];
    const available = new Set(normalizedBenchmarkEntries.map(([benchmark]) => String(benchmark || '').toUpperCase()));
    return payload.benchmarks
      .map((item) => String(item.benchmark || '').toUpperCase())
      .filter((benchmark, index, values) => values.indexOf(benchmark) === index)
      .filter((benchmark) => !available.has(benchmark))
      .map((benchmark) => benchmarkLabelByCode.get(benchmark) || benchmark);
  }, [benchmarkLabelByCode, normalizedBenchmarkEntries, payload]);

  const colorBySeriesKey = useMemo(() => {
    const map = new Map<string, string>();
    map.set('portfolio', '#22d3ee');
    normalizedBenchmarkEntries.forEach(([benchmark], index) => {
      map.set(benchmark, SERIES_COLORS[(index + 1) % SERIES_COLORS.length]);
    });
    return map;
  }, [normalizedBenchmarkEntries]);

  const formatTickDate = (date: string): string => {
    const parsed = parseIsoDateUtc(date);
    if (Number.isNaN(parsed.getTime())) return date;
    return parsed.toLocaleDateString(numberLocale, {
      month: 'short',
      year: '2-digit',
      timeZone: 'UTC',
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
              value={selectedBenchmark}
              options={benchmarkOptions}
              onChange={(value) => setSelectedBenchmark(value as (typeof BENCHMARK_OPTIONS)[number])}
              ariaLabel={t('benchmarks.selectBenchmark')}
              className="benchmarks-page__dropdown"
              size="sm"
            />
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
                      {normalizedBenchmarkEntries.map(([benchmark]) => (
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

            <section className="benchmarks-card benchmarks-card--half">
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
                        if (row.isSelected) return <Cell key={`return-${row.key}`} fill="#818cf8" />;
                        return <Cell key={`return-${row.key}`} fill={row.returnPct >= 0 ? '#34d399' : '#f87171'} />;
                      })}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </section>

            <section className="benchmarks-card benchmarks-card--half">
              <header className="benchmarks-card__header">
                <h2>{t('benchmarks.sections.details')}</h2>
              </header>
              {returnRows.length === 0 ? (
                <p className="benchmarks-card__empty">{t('benchmarks.noSeries')}</p>
              ) : (
                <div className="benchmarks-table-wrapper">
                  <table className="benchmarks-table">
                    <thead>
                      <tr>
                        <th>{t('benchmarks.table.benchmark')}</th>
                        <th>{t('benchmarks.table.returnPct')}</th>
                        <th>{t('benchmarks.table.alphaVsPortfolio')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {returnRows.map((row) => (
                        <tr key={`detail-${row.key}`} className={row.isSelected ? 'benchmarks-table__row--selected' : ''}>
                          <td>{row.label}</td>
                          <td className={row.returnPct >= 0 ? 'benchmarks-table__value benchmarks-table__value--positive' : 'benchmarks-table__value benchmarks-table__value--negative'}>
                            {formatSignedPercent(row.returnPct)}
                          </td>
                          <td className={row.alphaVsPortfolio >= 0 ? 'benchmarks-table__value benchmarks-table__value--positive' : 'benchmarks-table__value benchmarks-table__value--negative'}>
                            {row.isPortfolio ? '—' : formatSignedPercent(row.alphaVsPortfolio)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
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
