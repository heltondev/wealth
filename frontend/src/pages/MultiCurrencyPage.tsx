import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router';
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
import { api, type MultiCurrencyResponse } from '../services/api';
import { formatCurrency } from '../utils/formatters';
import './MultiCurrencyPage.scss';

const PERIOD_OPTIONS = ['1M', '3M', '6M', '1Y', '2Y', '5Y', 'MAX'] as const;

const toNumber = (value: unknown): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const toTitleCase = (value: string): string =>
  String(value || '')
    .replace(/_/g, ' ')
    .split(' ')
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

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

const MultiCurrencyPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio } = usePortfolioData();
  const [selectedPeriod, setSelectedPeriod] = useState<(typeof PERIOD_OPTIONS)[number]>('1Y');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<MultiCurrencyResponse | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const formatSignedPercent = (value: number, fractionDigits = 2): string =>
    `${value > 0 ? '+' : ''}${toNumber(value).toLocaleString(numberLocale, {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits,
    })}%`;

  const formatSignedCurrency = (value: number, currency = 'BRL'): string => {
    const absolute = formatCurrency(Math.abs(toNumber(value)), currency, numberLocale);
    if (Math.abs(toNumber(value)) <= Number.EPSILON) return absolute;
    return `${value > 0 ? '+' : '-'}${absolute}`;
  };

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const periodOptions = useMemo(
    () => PERIOD_OPTIONS.map((period) => ({
      value: period,
      label: t(`multiCurrency.periods.${period}`),
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

    api.getMultiCurrency(selectedPortfolio, selectedPeriod)
      .then((response) => {
        if (cancelled) return;
        setPayload(response);
      })
      .catch((reason: unknown) => {
        if (cancelled) return;
        setPayload(null);
        setError(reason instanceof Error ? reason.message : 'Failed to load multi-currency analytics');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [selectedPeriod, selectedPortfolio]);

  const evolutionRows = useMemo(
    () => (payload?.evolution || []).map((row) => ({
      date: String(row.date || ''),
      valueBrl: toNumber(row.value_brl),
      valueOriginal: toNumber(row.value_original_brl),
      fxImpactBrl: toNumber(row.fx_impact_brl),
    })),
    [payload?.evolution]
  );

  const byCurrencyRows = useMemo(
    () => (payload?.by_currency || [])
      .map((row) => ({
        currency: String(row.currency || '').toUpperCase(),
        endValueBrl: toNumber(row.end_value_brl),
        returnBrlPct: toNumber(row.return_brl_pct),
        returnOriginalPct: toNumber(row.return_original_pct),
        fxImpactBrl: toNumber(row.fx_impact_brl),
        fxImpactPct: toNumber(row.fx_impact_pct),
        weightPct: toNumber(row.weight_pct),
      }))
      .sort((left, right) => right.endValueBrl - left.endValueBrl),
    [payload?.by_currency]
  );

  const byAssetRows = useMemo(
    () => (payload?.by_asset || [])
      .map((row) => ({
        assetId: String(row.assetId || ''),
        ticker: String(row.ticker || '').toUpperCase(),
        name: String(row.name || '').trim(),
        assetClass: toTitleCase(String(row.asset_class || 'unknown')),
        currency: String(row.currency || 'BRL').toUpperCase(),
        endValueNative: toNumber(row.end_value_native),
        endValueBrl: toNumber(row.end_value_brl),
        returnBrlPct: toNumber(row.return_brl_pct),
        returnOriginalPct: toNumber(row.return_original_pct),
        fxImpactBrl: toNumber(row.fx_impact_brl),
        fxImpactPct: toNumber(row.fx_impact_pct),
      }))
      .sort((left, right) => Math.abs(right.fxImpactBrl) - Math.abs(left.fxImpactBrl)),
    [payload?.by_asset]
  );

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
      <div className="multi-currency-page">
        <div className="multi-currency-page__header">
          <h1 className="multi-currency-page__title">{t('multiCurrency.title')}</h1>
          <div className="multi-currency-page__filters">
            {portfolioOptions.length > 0 && (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('multiCurrency.selectPortfolio')}
                className="multi-currency-page__dropdown multi-currency-page__dropdown--portfolio"
                size="sm"
              />
            )}
            <SharedDropdown
              value={selectedPeriod}
              options={periodOptions}
              onChange={(value) => {
                if (PERIOD_OPTIONS.includes(value as (typeof PERIOD_OPTIONS)[number])) {
                  setSelectedPeriod(value as (typeof PERIOD_OPTIONS)[number]);
                }
              }}
              ariaLabel={t('multiCurrency.selectPeriod')}
              className="multi-currency-page__dropdown"
              size="sm"
            />
          </div>
        </div>

        {loading && <div className="multi-currency-page__state">{t('common.loading')}</div>}

        {!loading && !error && !payload && (
          <div className="multi-currency-page__state">{t('dashboard.noData')}</div>
        )}

        {!loading && error && (
          <div className="multi-currency-page__state multi-currency-page__state--error">
            <p>{t('multiCurrency.loadError')}</p>
            <code>{error}</code>
          </div>
        )}

        {!loading && !error && payload && (
          <div className="multi-currency-page__content">
            <div className="multi-currency-kpis">
              <article className="multi-currency-kpi">
                <span className="multi-currency-kpi__label">{t('multiCurrency.kpis.returnBrl')}</span>
                <span className={`multi-currency-kpi__value ${toNumber(payload.portfolio.return_brl_pct) >= 0 ? 'multi-currency-kpi__value--positive' : 'multi-currency-kpi__value--negative'}`}>
                  {formatSignedPercent(payload.portfolio.return_brl_pct)}
                </span>
              </article>
              <article className="multi-currency-kpi">
                <span className="multi-currency-kpi__label">{t('multiCurrency.kpis.returnOriginal')}</span>
                <span className={`multi-currency-kpi__value ${toNumber(payload.portfolio.return_original_pct) >= 0 ? 'multi-currency-kpi__value--positive' : 'multi-currency-kpi__value--negative'}`}>
                  {formatSignedPercent(payload.portfolio.return_original_pct)}
                </span>
              </article>
              <article className="multi-currency-kpi">
                <span className="multi-currency-kpi__label">{t('multiCurrency.kpis.fxImpact')}</span>
                <span className={`multi-currency-kpi__value ${toNumber(payload.portfolio.fx_impact_brl) >= 0 ? 'multi-currency-kpi__value--positive' : 'multi-currency-kpi__value--negative'}`}>
                  {`${formatSignedCurrency(payload.portfolio.fx_impact_brl, 'BRL')} • ${formatSignedPercent(payload.portfolio.fx_impact_pct)}`}
                </span>
              </article>
              <article className="multi-currency-kpi">
                <span className="multi-currency-kpi__label">{t('multiCurrency.kpis.foreignExposure')}</span>
                <span className="multi-currency-kpi__value">
                  {formatSignedPercent(payload.portfolio.foreign_exposure_pct)}
                </span>
              </article>
              <article className="multi-currency-kpi">
                <span className="multi-currency-kpi__label">{t('multiCurrency.kpis.range')}</span>
                <span className="multi-currency-kpi__value">
                  {`${formatTickDate(payload.from)} - ${formatTickDate(payload.to)}`}
                </span>
              </article>
            </div>

            <section className="multi-currency-card">
              <header className="multi-currency-card__header">
                <h2>{t('multiCurrency.sections.evolution')}</h2>
              </header>
              {evolutionRows.length === 0 ? (
                <p className="multi-currency-card__empty">{t('multiCurrency.noSeries')}</p>
              ) : (
                <ResponsiveContainer width="100%" height={330}>
                  <LineChart data={evolutionRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                    <XAxis dataKey="date" tickFormatter={formatTickDate} stroke="var(--text-secondary)" />
                    <YAxis
                      stroke="var(--text-secondary)"
                      tickFormatter={(value) => formatCurrency(toNumber(value), 'BRL', numberLocale)}
                      width={118}
                    />
                    <Tooltip
                      labelFormatter={(value) => formatTickDate(String(value || ''))}
                      formatter={(value: number | string | undefined, name?: string) => [
                        formatCurrency(toNumber(value), 'BRL', numberLocale),
                        String(name || ''),
                      ]}
                    />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="valueBrl"
                      name={t('multiCurrency.series.valueBrl')}
                      stroke="#22d3ee"
                      strokeWidth={2.5}
                      dot={false}
                      isAnimationActive={false}
                    />
                    <Line
                      type="monotone"
                      dataKey="valueOriginal"
                      name={t('multiCurrency.series.valueOriginal')}
                      stroke="#818cf8"
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </section>

            <section className="multi-currency-card multi-currency-card--half">
              <header className="multi-currency-card__header">
                <h2>{t('multiCurrency.sections.currencyFxImpact')}</h2>
              </header>
              {byCurrencyRows.length === 0 ? (
                <p className="multi-currency-card__empty">{t('multiCurrency.noSeries')}</p>
              ) : (
                <ResponsiveContainer width="100%" height={290}>
                  <BarChart data={byCurrencyRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                    <XAxis dataKey="currency" stroke="var(--text-secondary)" />
                    <YAxis
                      stroke="var(--text-secondary)"
                      tickFormatter={(value) => formatCurrency(toNumber(value), 'BRL', numberLocale)}
                      width={110}
                    />
                    <Tooltip formatter={(value: number | string | undefined) => formatCurrency(toNumber(value), 'BRL', numberLocale)} />
                    <ReferenceLine y={0} stroke="rgba(148, 163, 184, 0.4)" />
                    <Bar dataKey="fxImpactBrl" name={t('multiCurrency.table.fxImpactBrl')} isAnimationActive={false}>
                      {byCurrencyRows.map((row) => (
                        <Cell key={`fx-impact-${row.currency}`} fill={row.fxImpactBrl >= 0 ? '#34d399' : '#f87171'} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </section>

            <section className="multi-currency-card multi-currency-card--half">
              <header className="multi-currency-card__header">
                <h2>{t('multiCurrency.sections.currencyReturns')}</h2>
              </header>
              {byCurrencyRows.length === 0 ? (
                <p className="multi-currency-card__empty">{t('multiCurrency.noSeries')}</p>
              ) : (
                <ResponsiveContainer width="100%" height={290}>
                  <BarChart data={byCurrencyRows} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.22)" />
                    <XAxis dataKey="currency" stroke="var(--text-secondary)" />
                    <YAxis
                      stroke="var(--text-secondary)"
                      tickFormatter={(value) => `${toNumber(value).toLocaleString(numberLocale, { maximumFractionDigits: 0 })}%`}
                      width={64}
                    />
                    <Tooltip formatter={(value: number | string | undefined) => formatSignedPercent(toNumber(value))} />
                    <Legend />
                    <ReferenceLine y={0} stroke="rgba(148, 163, 184, 0.4)" />
                    <Bar dataKey="returnOriginalPct" name={t('multiCurrency.table.returnOriginal')} fill="#818cf8" isAnimationActive={false} />
                    <Bar dataKey="returnBrlPct" name={t('multiCurrency.table.returnBrl')} fill="#22d3ee" isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </section>

            <section className="multi-currency-card">
              <header className="multi-currency-card__header">
                <h2>{t('multiCurrency.sections.assets')}</h2>
              </header>
              {byAssetRows.length === 0 ? (
                <p className="multi-currency-card__empty">{t('multiCurrency.noSeries')}</p>
              ) : (
                <div className="multi-currency-table-wrapper">
                  <table className="multi-currency-table">
                    <thead>
                      <tr>
                        <th>{t('multiCurrency.table.asset')}</th>
                        <th>{t('multiCurrency.table.class')}</th>
                        <th>{t('multiCurrency.table.currency')}</th>
                        <th>{t('multiCurrency.table.endValueNative')}</th>
                        <th>{t('multiCurrency.table.endValueBrl')}</th>
                        <th>{t('multiCurrency.table.returnOriginal')}</th>
                        <th>{t('multiCurrency.table.returnBrl')}</th>
                        <th>{t('multiCurrency.table.fxImpactBrl')}</th>
                        <th>{t('multiCurrency.table.fxImpactPct')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {byAssetRows.map((row) => (
                        <tr key={`multi-currency-asset-${row.assetId || row.ticker}`}>
                          <td>
                            {row.assetId ? (
                              <Link
                                to={`/assets/${encodeURIComponent(row.assetId)}?portfolioId=${encodeURIComponent(selectedPortfolio)}`}
                                className="multi-currency-table__asset-link"
                              >
                                <strong>{row.ticker}</strong>
                              </Link>
                            ) : (
                              <strong>{row.ticker}</strong>
                            )}
                            {row.name ? <small>{row.name}</small> : null}
                          </td>
                          <td>{row.assetClass}</td>
                          <td>{row.currency}</td>
                          <td>{formatCurrency(row.endValueNative, row.currency, numberLocale)}</td>
                          <td>{formatCurrency(row.endValueBrl, 'BRL', numberLocale)}</td>
                          <td className={row.returnOriginalPct >= 0 ? 'multi-currency-table__value multi-currency-table__value--positive' : 'multi-currency-table__value multi-currency-table__value--negative'}>
                            {formatSignedPercent(row.returnOriginalPct)}
                          </td>
                          <td className={row.returnBrlPct >= 0 ? 'multi-currency-table__value multi-currency-table__value--positive' : 'multi-currency-table__value multi-currency-table__value--negative'}>
                            {formatSignedPercent(row.returnBrlPct)}
                          </td>
                          <td className={row.fxImpactBrl >= 0 ? 'multi-currency-table__value multi-currency-table__value--positive' : 'multi-currency-table__value multi-currency-table__value--negative'}>
                            {formatSignedCurrency(row.fxImpactBrl, 'BRL')}
                          </td>
                          <td className={row.fxImpactPct >= 0 ? 'multi-currency-table__value multi-currency-table__value--positive' : 'multi-currency-table__value multi-currency-table__value--negative'}>
                            {formatSignedPercent(row.fxImpactPct)}
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

export default MultiCurrencyPage;
