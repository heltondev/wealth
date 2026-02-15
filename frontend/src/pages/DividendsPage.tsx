import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import Layout from '../components/Layout';
import {
  api,
  type AlertEvent,
  type AlertRule,
  type DividendsResponse,
  type DropdownConfigMap,
} from '../services/api';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { useToast } from '../context/ToastContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './DividendsPage.scss';

const normalizeNumericOptions = (
  options: Array<{ value: string; label: string }>,
  fallbackValue: string
) => {
  const normalized = options
    .map((option) => ({
      value: String(option.value || '').trim(),
      label: String(option.label || option.value || '').trim() || String(option.value || '').trim(),
    }))
    .filter((option) => {
      const parsed = Number(option.value);
      return option.value && Number.isFinite(parsed) && parsed > 0;
    });
  if (normalized.length > 0) return normalized;
  return [{ value: fallbackValue, label: fallbackValue }];
};

const normalizeMethodOptions = (
  options: Array<{ value: string; label: string }>,
  fallbackValue: string
) => {
  const allowed = new Set(['fifo', 'weighted_average']);
  const normalized = options
    .map((option) => ({
      value: String(option.value || '').trim().toLowerCase(),
      label: String(option.label || option.value || '').trim() || String(option.value || '').trim(),
    }))
    .filter((option) => option.value && allowed.has(option.value));
  if (normalized.length > 0) return normalized;
  return [{ value: fallbackValue, label: fallbackValue.toUpperCase() }];
};

const toDateMinusMonths = (monthsBack: number) => {
  const date = new Date();
  date.setDate(1);
  date.setMonth(date.getMonth() - monthsBack + 1);
  return date.toISOString().slice(0, 10);
};

const addDaysToIso = (isoDate: string, days: number) => {
  const date = new Date(`${isoDate}T00:00:00`);
  if (Number.isNaN(date.getTime())) return isoDate;
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
};

const normalizeIsoDate = (value: unknown): string | null => {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const DividendsPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio } = usePortfolioData();
  const { showToast } = useToast();
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [periodMonths, setPeriodMonths] = useState('12');
  const [method, setMethod] = useState('fifo');
  const [calendarLookaheadDays, setCalendarLookaheadDays] = useState('90');
  const [alertLookaheadDays, setAlertLookaheadDays] = useState('30');
  const [alertTicker, setAlertTicker] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<DividendsResponse | null>(null);
  const [alertRules, setAlertRules] = useState<AlertRule[]>([]);
  const [alertEvents, setAlertEvents] = useState<AlertEvent[]>([]);
  const [savingRule, setSavingRule] = useState(false);
  const [evaluatingAlerts, setEvaluatingAlerts] = useState(false);
  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const portfolioOptions = useMemo(
    () =>
      portfolios.map((portfolio) => ({
        value: portfolio.portfolioId,
        label: portfolio.name,
      })),
    [portfolios]
  );

  useEffect(() => {
    api.getDropdownSettings()
      .then((settings) => {
        setDropdownConfig(normalizeDropdownConfig(settings.dropdowns));
      })
      .catch(() => {
        setDropdownConfig(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      });
  }, []);

  const periodOptions = useMemo(() => {
    const configured = getDropdownOptions(dropdownConfig, 'dividends.filters.periodMonths');
    const fallback = getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'dividends.filters.periodMonths');
    const source = configured.length > 0 ? configured : fallback;
    return normalizeNumericOptions(source, '12');
  }, [dropdownConfig]);

  const methodOptions = useMemo(() => {
    const configured = getDropdownOptions(dropdownConfig, 'dividends.filters.method');
    const fallback = getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'dividends.filters.method');
    const source = configured.length > 0 ? configured : fallback;
    return normalizeMethodOptions(source, 'fifo');
  }, [dropdownConfig]);

  const calendarLookaheadOptions = useMemo(() => {
    const configured = getDropdownOptions(dropdownConfig, 'dividends.calendar.lookaheadDays');
    const fallback = getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'dividends.calendar.lookaheadDays');
    const source = configured.length > 0 ? configured : fallback;
    return normalizeNumericOptions(source, '90');
  }, [dropdownConfig]);

  const alertLookaheadOptions = useMemo(() => {
    const configured = getDropdownOptions(dropdownConfig, 'dividends.alerts.lookaheadDays');
    const fallback = getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'dividends.alerts.lookaheadDays');
    const source = configured.length > 0 ? configured : fallback;
    return normalizeNumericOptions(source, '30');
  }, [dropdownConfig]);

  useEffect(() => {
    if (!periodOptions.some((option) => option.value === periodMonths)) {
      setPeriodMonths(periodOptions[0]?.value || '12');
    }
  }, [periodMonths, periodOptions]);

  useEffect(() => {
    if (!methodOptions.some((option) => option.value === method)) {
      setMethod(methodOptions[0]?.value || 'fifo');
    }
  }, [method, methodOptions]);

  useEffect(() => {
    if (!calendarLookaheadOptions.some((option) => option.value === calendarLookaheadDays)) {
      setCalendarLookaheadDays(calendarLookaheadOptions[0]?.value || '90');
    }
  }, [calendarLookaheadDays, calendarLookaheadOptions]);

  useEffect(() => {
    if (!alertLookaheadOptions.some((option) => option.value === alertLookaheadDays)) {
      setAlertLookaheadDays(alertLookaheadOptions[0]?.value || '30');
    }
  }, [alertLookaheadDays, alertLookaheadOptions]);

  const refreshAlerts = useCallback(async () => {
    const data = await api.getAlerts();
    setAlertRules(data.rules || []);
    setAlertEvents(data.events || []);
  }, []);

  useEffect(() => {
    refreshAlerts().catch(() => {});
  }, [refreshAlerts]);

  const fromDate = useMemo(() => {
    const months = Number(periodMonths);
    if (!Number.isFinite(months) || months <= 0) return toDateMinusMonths(12);
    return toDateMinusMonths(Math.round(months));
  }, [periodMonths]);
  const selectedPeriodMonths = useMemo(() => {
    const months = Number(periodMonths);
    if (!Number.isFinite(months) || months <= 0) return 12;
    return Math.round(months);
  }, [periodMonths]);
  const selectedPeriodLabel = useMemo(
    () => periodOptions.find((option) => option.value === periodMonths)?.label || `${selectedPeriodMonths}M`,
    [periodMonths, periodOptions, selectedPeriodMonths]
  );

  useEffect(() => {
    if (!selectedPortfolio) {
      setPayload(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api.getDividends(selectedPortfolio, { fromDate, method, periodMonths: selectedPeriodMonths })
      .then((response) => {
        if (cancelled) return;
        setPayload(response);
      })
      .catch((reason: unknown) => {
        if (cancelled) return;
        setPayload(null);
        setError(reason instanceof Error ? reason.message : 'Failed to load dividends');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [fromDate, method, selectedPeriodMonths, selectedPortfolio]);

  const monthlySeries = useMemo(() => (
    (payload?.monthly_dividends || []).map((item) => ({
      period: item.period,
      amount: Number(item.amount || 0),
    }))
  ), [payload?.monthly_dividends]);

  const upcomingEvents = useMemo(() => {
    const source = (payload?.calendar_upcoming && payload.calendar_upcoming.length > 0)
      ? payload.calendar_upcoming
      : (payload?.calendar || []);
    const lookaheadDays = Number(calendarLookaheadDays);
    const today = new Date().toISOString().slice(0, 10);
    const untilDate = addDaysToIso(today, Number.isFinite(lookaheadDays) ? Math.round(lookaheadDays) : 90);
    return source
      .map((event) => ({
        ...event,
        ticker: String(event.ticker || '').toUpperCase(),
        eventType: String(event.eventType || '').replace(/_/g, ' '),
        eventDate: normalizeIsoDate(event.eventDate || event.date || event.fetched_at),
      }))
      .filter((event) => event.eventDate && event.eventDate >= today && event.eventDate <= untilDate)
      .sort((left, right) => String(left.eventDate || '').localeCompare(String(right.eventDate || '')));
  }, [calendarLookaheadDays, payload?.calendar, payload?.calendar_upcoming]);

  const dividendRules = useMemo(() => alertRules
    .filter((rule) => String(rule.type || '').toLowerCase() === 'dividend_announcement')
    .filter((rule) => !selectedPortfolio || !rule.portfolioId || rule.portfolioId === selectedPortfolio)
  , [alertRules, selectedPortfolio]);

  const dividendEvents = useMemo(() => alertEvents
    .filter((event) => String(event.type || '').toLowerCase() === 'dividend_announcement')
    .slice(0, 10), [alertEvents]);
  const totalInPeriod = Number(payload?.total_in_period ?? payload?.total_last_12_months ?? 0);
  const averageMonthlyIncome = Number(payload?.average_monthly_income ?? payload?.projected_monthly_income ?? 0);
  const annualizedIncome = Number(payload?.annualized_income ?? payload?.projected_annual_income ?? (averageMonthlyIncome * 12));
  const yieldOnCostPeriod = Number(payload?.yield_on_cost_realized ?? 0);
  const currentDividendYieldPeriod = Number(payload?.dividend_yield_current ?? 0);

  const handleCreateRule = async () => {
    if (!selectedPortfolio) return;
    setSavingRule(true);
    try {
      const normalizedTicker = alertTicker.trim().toUpperCase();
      const lookahead = Number(alertLookaheadDays);
      await api.createAlertRule({
        type: 'dividend_announcement',
        enabled: true,
        portfolioId: selectedPortfolio,
        params: {
          ticker: normalizedTicker || null,
          lookaheadDays: Number.isFinite(lookahead) ? Math.round(lookahead) : 30,
        },
        description: normalizedTicker
          ? `Dividend announcement alert for ${normalizedTicker}`
          : 'Dividend announcement alert for all assets',
      });
      setAlertTicker('');
      await refreshAlerts();
      showToast(t('dividends.alerts.created', { defaultValue: 'Alert created.' }), 'success');
    } catch {
      showToast(t('dividends.alerts.createError', { defaultValue: 'Failed to create alert.' }), 'error');
    } finally {
      setSavingRule(false);
    }
  };

  const handleDeleteRule = async (ruleId: string) => {
    try {
      await api.deleteAlertRule(ruleId);
      await refreshAlerts();
      showToast(t('dividends.alerts.deleted', { defaultValue: 'Alert removed.' }), 'success');
    } catch {
      showToast(t('dividends.alerts.deleteError', { defaultValue: 'Failed to remove alert.' }), 'error');
    }
  };

  const handleEvaluateAlerts = async () => {
    if (!selectedPortfolio) return;
    setEvaluatingAlerts(true);
    try {
      await api.evaluateAlerts(selectedPortfolio);
      await refreshAlerts();
      showToast(t('dividends.alerts.evaluated', { defaultValue: 'Alerts evaluated.' }), 'success');
    } catch {
      showToast(t('dividends.alerts.evaluateError', { defaultValue: 'Failed to evaluate alerts.' }), 'error');
    } finally {
      setEvaluatingAlerts(false);
    }
  };

  return (
    <Layout>
      <div className="dividends-page">
        <div className="dividends-page__header">
          <h1 className="dividends-page__title">{t('dividends.title', { defaultValue: 'Dividends' })}</h1>
          <div className="dividends-page__filters">
            {portfolioOptions.length > 0 && (
              <SharedDropdown
                className="dividends-page__dropdown dividends-page__dropdown--portfolio"
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('dividends.selectPortfolio', { defaultValue: 'Select portfolio' })}
              />
            )}
            <SharedDropdown
              className="dividends-page__dropdown"
              value={periodMonths}
              onChange={setPeriodMonths}
              options={periodOptions}
              ariaLabel={t('dividends.period', { defaultValue: 'Period' })}
            />
            <SharedDropdown
              className="dividends-page__dropdown"
              value={method}
              onChange={setMethod}
              options={methodOptions}
              ariaLabel={t('dividends.method', { defaultValue: 'Method' })}
            />
          </div>
        </div>

        {loading && (
          <div className="dividends-page__state">
            {t('common.loading')}
          </div>
        )}

        {!loading && error && (
          <div className="dividends-page__state dividends-page__state--error">
            <p>{t('dividends.loadError', { defaultValue: 'Failed to load dividends data.' })}</p>
            <code>{error}</code>
          </div>
        )}

        {!loading && !error && payload && (
          <>
            <div className="dividends-page__kpis">
              <article className="dividends-kpi">
                <span className="dividends-kpi__label">
                  {t('dividends.totalInPeriod', { period: selectedPeriodLabel, defaultValue: `Income in ${selectedPeriodLabel}` })}
                </span>
                <span className="dividends-kpi__value">
                  {formatCurrency(totalInPeriod, 'BRL', numberLocale)}
                </span>
              </article>
              <article className="dividends-kpi">
                <span className="dividends-kpi__label">
                  {t('dividends.averageMonthlyPeriod', { period: selectedPeriodLabel, defaultValue: `Average Monthly (${selectedPeriodLabel})` })}
                </span>
                <span className="dividends-kpi__value">
                  {formatCurrency(averageMonthlyIncome, 'BRL', numberLocale)}
                </span>
              </article>
              <article className="dividends-kpi">
                <span className="dividends-kpi__label">{t('dividends.projectedAnnual', { defaultValue: 'Projected Annual Income' })}</span>
                <span className="dividends-kpi__value">
                  {formatCurrency(annualizedIncome, 'BRL', numberLocale)}
                </span>
              </article>
              <article className="dividends-kpi">
                <span className="dividends-kpi__label">
                  {t('dividends.yieldOnCostPeriod', { period: selectedPeriodLabel, defaultValue: `Yield on Cost (${selectedPeriodLabel})` })}
                </span>
                <span className="dividends-kpi__value">
                  {`${yieldOnCostPeriod.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`}
                </span>
              </article>
              <article className="dividends-kpi">
                <span className="dividends-kpi__label">
                  {t('dividends.currentYieldPeriod', { period: selectedPeriodLabel, defaultValue: `Current Dividend Yield (${selectedPeriodLabel})` })}
                </span>
                <span className="dividends-kpi__value">
                  {`${currentDividendYieldPeriod.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`}
                </span>
              </article>
            </div>

            <div className="dividends-page__grid">
              <section className="dividends-card dividends-card--wide">
                <header className="dividends-card__header">
                  <h2>{t('dividends.monthlyHistory', { defaultValue: 'Monthly Dividends' })}</h2>
                </header>
                {monthlySeries.length === 0 ? (
                  <p className="dividends-card__empty">{t('dividends.noSeries', { defaultValue: 'No data available.' })}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={320}>
                    <BarChart data={monthlySeries} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.25)" />
                      <XAxis dataKey="period" stroke="var(--text-secondary)" />
                      <YAxis
                        stroke="var(--text-secondary)"
                        tickFormatter={(value) => formatCurrency(Number(value || 0), 'BRL', numberLocale)}
                        width={110}
                      />
                      <Tooltip
                        formatter={(value: number | string | undefined) =>
                          formatCurrency(Number(value || 0), 'BRL', numberLocale)
                        }
                      />
                      <Bar dataKey="amount" fill="#34d399" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </section>

              <section className="dividends-card">
                <header className="dividends-card__header">
                  <h2>{t('dividends.calendar', { defaultValue: 'Upcoming Payments' })}</h2>
                  <SharedDropdown
                    className="dividends-page__dropdown"
                    size="sm"
                    value={calendarLookaheadDays}
                    onChange={setCalendarLookaheadDays}
                    options={calendarLookaheadOptions}
                    ariaLabel={t('dividends.lookahead', { defaultValue: 'Lookahead' })}
                  />
                </header>
                {upcomingEvents.length === 0 ? (
                  <p className="dividends-card__empty">{t('dividends.noUpcoming', { defaultValue: 'No upcoming dividend events.' })}</p>
                ) : (
                  <div className="dividends-list">
                    {upcomingEvents.slice(0, 30).map((event) => (
                      <article
                        key={`${event.ticker || 'asset'}-${event.eventDate || 'date'}-${event.eventType || 'type'}-${JSON.stringify(event.details || '')}`}
                        className="dividends-list__item"
                      >
                        <div className="dividends-list__row">
                          <span className="dividends-list__ticker">{event.ticker || '-'}</span>
                          <span className="dividends-list__date">
                            {event.eventDate ? formatDate(event.eventDate, numberLocale) : '-'}
                          </span>
                        </div>
                        <div className="dividends-list__type">{String(event.eventType || '-').replace(/_/g, ' ')}</div>
                      </article>
                    ))}
                  </div>
                )}
              </section>

              <section className="dividends-card">
                <header className="dividends-card__header">
                  <h2>{t('dividends.alerts.title', { defaultValue: 'Dividend Alerts' })}</h2>
                </header>

                <div className="dividends-alerts__form">
                  <input
                    className="dividends-page__input"
                    type="text"
                    value={alertTicker}
                    placeholder={t('dividends.alerts.tickerPlaceholder', { defaultValue: 'Ticker (optional)' })}
                    onChange={(event) => setAlertTicker(event.target.value.toUpperCase())}
                  />
                  <SharedDropdown
                    className="dividends-page__dropdown"
                    value={alertLookaheadDays}
                    onChange={setAlertLookaheadDays}
                    options={alertLookaheadOptions}
                    ariaLabel={t('dividends.alerts.lookaheadLabel', { defaultValue: 'Lookahead' })}
                  />
                  <button
                    type="button"
                    className="dividends-page__button"
                    disabled={savingRule || !selectedPortfolio}
                    onClick={handleCreateRule}
                  >
                    {t('dividends.alerts.create', { defaultValue: 'Create Alert' })}
                  </button>
                  <button
                    type="button"
                    className="dividends-page__button dividends-page__button--secondary"
                    disabled={evaluatingAlerts || !selectedPortfolio}
                    onClick={handleEvaluateAlerts}
                  >
                    {t('dividends.alerts.evaluateNow', { defaultValue: 'Evaluate Now' })}
                  </button>
                </div>

                <div className="dividends-alerts__rules">
                  <h3>{t('dividends.alerts.activeRules', { defaultValue: 'Active Rules' })}</h3>
                  {dividendRules.length === 0 ? (
                    <p className="dividends-card__empty">{t('dividends.alerts.noRules', { defaultValue: 'No dividend alert rules.' })}</p>
                  ) : (
                    <ul>
                      {dividendRules.map((rule) => (
                        <li key={rule.ruleId}>
                          <span>{rule.description || rule.ruleId}</span>
                          <button type="button" onClick={() => handleDeleteRule(rule.ruleId)}>
                            {t('common.delete')}
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div className="dividends-alerts__events">
                  <h3>{t('dividends.alerts.recentEvents', { defaultValue: 'Recent Events' })}</h3>
                  {dividendEvents.length === 0 ? (
                    <p className="dividends-card__empty">{t('dividends.alerts.noEvents', { defaultValue: 'No triggered dividend alerts.' })}</p>
                  ) : (
                    <ul>
                      {dividendEvents.map((event) => (
                        <li key={event.eventId}>
                          <span>{event.message}</span>
                          <time>{formatDate(event.eventAt, numberLocale)}</time>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </section>
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default DividendsPage;
