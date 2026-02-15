import { useTranslation } from 'react-i18next';
import { useEffect, useMemo, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import Layout from '../components/Layout';
import { api, type DashboardAllocationItem, type DashboardResponse } from '../services/api';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './DashboardPage.scss';

const CHART_COLORS = ['#22d3ee', '#818cf8', '#34d399', '#f59e0b', '#fb7185', '#38bdf8', '#f97316', '#a78bfa'];
const EVOLUTION_STROKE = '#22d3ee';
const EVOLUTION_FILL = 'rgba(34, 211, 238, 0.26)';

type Trend = 'positive' | 'negative' | 'neutral';

interface AllocationChartDatum {
  key: string;
  label: string;
  value: number;
  weightPct: number;
}

const normalizeAllocation = (
  rows: DashboardAllocationItem[] | undefined,
  labelResolver: (key: string) => string
): AllocationChartDatum[] => {
  if (!Array.isArray(rows)) return [];

  return rows
    .map((row) => ({
      key: String(row.key || 'unknown'),
      label: labelResolver(String(row.key || 'unknown')),
      value: Number(row.value || 0),
      weightPct: Number(row.weight_pct || 0),
    }))
    .filter((row) => Number.isFinite(row.value) && row.value > 0);
};

const resolveTrend = (value: number): Trend => {
  if (!Number.isFinite(value) || Math.abs(value) <= Number.EPSILON) return 'neutral';
  return value > 0 ? 'positive' : 'negative';
};

const DashboardPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio } = usePortfolioData();
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedPortfolio) {
      setDashboard(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api.getDashboard(selectedPortfolio)
      .then((payload) => {
        if (cancelled) return;
        setDashboard(payload);
      })
      .catch((reason: unknown) => {
        if (cancelled) return;
        setDashboard(null);
        setError(reason instanceof Error ? reason.message : 'Failed to load dashboard');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [selectedPortfolio]);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const absoluteReturn = Number(dashboard?.return_absolute || 0);
  const percentReturn = Number(dashboard?.return_percent || 0);
  const absoluteTrend = resolveTrend(absoluteReturn);
  const percentTrend = resolveTrend(percentReturn);

  const classAllocation = useMemo(() => normalizeAllocation(
    dashboard?.allocation_by_class,
    (key) => t(`assets.classes.${key}`, { defaultValue: key.replace(/_/g, ' ') })
  ), [dashboard?.allocation_by_class, t]);

  const currencyAllocation = useMemo(() => normalizeAllocation(
    dashboard?.allocation_by_currency,
    (key) => key.toUpperCase()
  ), [dashboard?.allocation_by_currency]);

  const sectorAllocation = useMemo(() => normalizeAllocation(
    dashboard?.allocation_by_sector,
    (key) => key.replace(/_/g, ' ')
  ), [dashboard?.allocation_by_sector]);

  const evolutionData = useMemo(() => {
    if (!Array.isArray(dashboard?.evolution)) return [];

    return dashboard.evolution
      .map((point) => ({
        date: String(point.date || ''),
        value: Number(point.value || 0),
      }))
      .filter((point) => point.date && Number.isFinite(point.value));
  }, [dashboard?.evolution]);

  return (
    <Layout>
      <div className="dashboard">
        <div className="dashboard__header">
          <h1 className="dashboard__title">{t('dashboard.title')}</h1>
          {portfolios.length > 0 && (
            <select
              className="dashboard__select"
              value={selectedPortfolio}
              onChange={(event) => setSelectedPortfolio(event.target.value)}
              aria-label={t('dashboard.selectPortfolio', { defaultValue: 'Select portfolio' })}
            >
              {portfolios.map((portfolio) => (
                <option key={portfolio.portfolioId} value={portfolio.portfolioId}>
                  {portfolio.name}
                </option>
              ))}
            </select>
          )}
        </div>

        {loading && <p className="dashboard__loading">{t('common.loading')}</p>}

        {!loading && portfolios.length === 0 && (
          <div className="dashboard__empty">
            <p>{t('dashboard.noData')}</p>
          </div>
        )}

        {!loading && error && (
          <div className="dashboard__error">
            <p>{t('dashboard.loadError', { defaultValue: 'Failed to load dashboard data.' })}</p>
            <code>{error}</code>
          </div>
        )}

        {!loading && !error && dashboard && (
          <>
            <div className="dashboard__kpi-grid">
              <article className="kpi-card">
                <span className="kpi-card__label">{t('dashboard.totalValue')}</span>
                <span className="kpi-card__value">
                  {formatCurrency(Number(dashboard.total_value_brl || 0), 'BRL', numberLocale)}
                </span>
              </article>

              <article className="kpi-card">
                <span className="kpi-card__label">{t('dashboard.totalGain')}</span>
                <span className={`kpi-card__value kpi-card__value--${absoluteTrend}`}>
                  {formatCurrency(Math.abs(absoluteReturn), 'BRL', numberLocale).replace(/^/, absoluteReturn > 0 ? '+' : absoluteReturn < 0 ? '-' : '')}
                </span>
              </article>

              <article className="kpi-card">
                <span className="kpi-card__label">{t('dashboard.returnPercent', { defaultValue: 'Return %' })}</span>
                <span className={`kpi-card__value kpi-card__value--${percentTrend}`}>
                  {`${percentReturn > 0 ? '+' : ''}${percentReturn.toLocaleString(numberLocale, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                  })}%`}
                </span>
              </article>

              <article className="kpi-card">
                <span className="kpi-card__label">{t('dashboard.assetsCount', { defaultValue: 'Asset Classes' })}</span>
                <span className="kpi-card__value">{classAllocation.length}</span>
              </article>
            </div>

            <div className="dashboard__charts-grid">
              <section className="dashboard-card">
                <header className="dashboard-card__header">
                  <h2>{t('dashboard.allocationByClass', { defaultValue: 'Allocation by Class' })}</h2>
                </header>
                {classAllocation.length === 0 ? (
                  <p className="dashboard-card__empty">{t('dashboard.noSeries', { defaultValue: 'No data available.' })}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={280}>
                    <PieChart>
                      <Pie data={classAllocation} dataKey="value" nameKey="label" innerRadius={64} outerRadius={96}>
                        {classAllocation.map((entry, index) => (
                          <Cell key={`class-${entry.key}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip
                        formatter={(value: number | string | undefined) =>
                          formatCurrency(Number(value || 0), 'BRL', numberLocale)
                        }
                      />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                )}
              </section>

              <section className="dashboard-card">
                <header className="dashboard-card__header">
                  <h2>{t('dashboard.allocationByCurrency', { defaultValue: 'Allocation by Currency' })}</h2>
                </header>
                {currencyAllocation.length === 0 ? (
                  <p className="dashboard-card__empty">{t('dashboard.noSeries', { defaultValue: 'No data available.' })}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={280}>
                    <PieChart>
                      <Pie data={currencyAllocation} dataKey="value" nameKey="label" innerRadius={64} outerRadius={96}>
                        {currencyAllocation.map((entry, index) => (
                          <Cell key={`currency-${entry.key}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip
                        formatter={(value: number | string | undefined) =>
                          formatCurrency(Number(value || 0), 'BRL', numberLocale)
                        }
                      />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                )}
              </section>

              <section className="dashboard-card">
                <header className="dashboard-card__header">
                  <h2>{t('dashboard.allocationBySector', { defaultValue: 'Allocation by Sector' })}</h2>
                </header>
                {sectorAllocation.length === 0 ? (
                  <p className="dashboard-card__empty">{t('dashboard.noSeries', { defaultValue: 'No data available.' })}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={280}>
                    <PieChart>
                      <Pie data={sectorAllocation} dataKey="value" nameKey="label" innerRadius={64} outerRadius={96}>
                        {sectorAllocation.map((entry, index) => (
                          <Cell key={`sector-${entry.key}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip
                        formatter={(value: number | string | undefined) =>
                          formatCurrency(Number(value || 0), 'BRL', numberLocale)
                        }
                      />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                )}
              </section>

              <section className="dashboard-card dashboard-card--wide">
                <header className="dashboard-card__header">
                  <h2>{t('dashboard.evolution', { defaultValue: 'Portfolio Evolution' })}</h2>
                </header>
                {evolutionData.length === 0 ? (
                  <p className="dashboard-card__empty">{t('dashboard.noSeries', { defaultValue: 'No data available.' })}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={320}>
                    <AreaChart data={evolutionData} margin={{ top: 8, right: 20, left: 0, bottom: 0 }}>
                      <defs>
                        <linearGradient id="dashboard-evolution-fill" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor={EVOLUTION_STROKE} stopOpacity={0.55} />
                          <stop offset="95%" stopColor={EVOLUTION_STROKE} stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.25)" />
                      <XAxis
                        dataKey="date"
                        tickFormatter={(value) => formatDate(String(value), numberLocale)}
                        minTickGap={40}
                        stroke="var(--text-secondary)"
                      />
                      <YAxis
                        tickFormatter={(value) => formatCurrency(Number(value || 0), 'BRL', numberLocale)}
                        stroke="var(--text-secondary)"
                        width={110}
                      />
                      <Tooltip
                        formatter={(value: number | string | undefined) =>
                          formatCurrency(Number(value || 0), 'BRL', numberLocale)
                        }
                        labelFormatter={(value) => formatDate(String(value), numberLocale)}
                      />
                      <Area
                        type="monotone"
                        dataKey="value"
                        stroke={EVOLUTION_STROKE}
                        fill={EVOLUTION_FILL}
                        fillOpacity={1}
                        strokeWidth={2.4}
                        activeDot={{ r: 4 }}
                        isAnimationActive={false}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                )}
              </section>
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default DashboardPage;
