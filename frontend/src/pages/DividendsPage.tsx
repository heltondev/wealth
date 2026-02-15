import { useEffect, useMemo, useState } from 'react';
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

const getLocalIsoDate = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const getLocalMonth = () => getLocalIsoDate().slice(0, 7);

const normalizeIsoDate = (value: unknown): string | null => {
  if (!value) return null;
  const text = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

type ProventStatus = 'paid' | 'provisioned';

type ProventCalendarItem = {
  ticker: string;
  eventType: string;
  eventDate: string;
  amount: number | null;
  status: ProventStatus;
};

const toRecord = (value: unknown): Record<string, unknown> => (
  value && typeof value === 'object' ? (value as Record<string, unknown>) : {}
);

const toAmount = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const formatMonthLabel = (month: string, locale: string) => {
  const [yearRaw, monthRaw] = String(month).split('-');
  const year = Number(yearRaw);
  const monthIndex = Number(monthRaw) - 1;
  if (!Number.isFinite(year) || !Number.isFinite(monthIndex) || monthIndex < 0 || monthIndex > 11) {
    return month;
  }
  const parsed = new Date(Date.UTC(year, monthIndex, 1, 12, 0, 0));
  return parsed.toLocaleDateString(locale, { month: 'long', year: 'numeric', timeZone: 'UTC' });
};

const DividendsPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio } = usePortfolioData();
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [periodMonths, setPeriodMonths] = useState('12');
  const [method, setMethod] = useState('fifo');
  const [calendarLookaheadDays, setCalendarLookaheadDays] = useState('90');
  const [calendarMonth, setCalendarMonth] = useState(getLocalMonth());
  const [visibleStatuses, setVisibleStatuses] = useState<Record<ProventStatus, boolean>>({
    paid: true,
    provisioned: true,
  });
  const [expandedDates, setExpandedDates] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<DividendsResponse | null>(null);
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

  const serverTodayIso = useMemo(
    () => normalizeIsoDate(payload?.period_to || payload?.fetched_at),
    [payload?.fetched_at, payload?.period_to]
  );
  const todayIso = useMemo(() => {
    const local = getLocalIsoDate();
    if (!serverTodayIso) return local;
    return serverTodayIso > local ? serverTodayIso : local;
  }, [serverTodayIso]);

  const upcomingEvents = useMemo(() => {
    const source = payload?.calendar_upcoming || [];
    const lookaheadDays = Number(calendarLookaheadDays);
    const untilDate = addDaysToIso(todayIso, Number.isFinite(lookaheadDays) ? Math.round(lookaheadDays) : 90);
    return source
      .map((event) => ({
        ...event,
        ticker: String(event.ticker || '').toUpperCase(),
        eventType: String(event.eventType || '').replace(/_/g, ' '),
        eventDate: normalizeIsoDate(event.eventDate || event.date || event.fetched_at),
      }))
      .filter((event) => event.eventDate && event.eventDate >= todayIso && event.eventDate <= untilDate)
      .sort((left, right) => String(left.eventDate || '').localeCompare(String(right.eventDate || '')));
  }, [calendarLookaheadDays, payload?.calendar_upcoming, todayIso]);

  const calendarSourceEvents = useMemo(() => {
    const combined = [
      ...(payload?.calendar || []),
      ...(payload?.calendar_upcoming || []),
    ];
    const deduped = new Map<string, typeof combined[number]>();
    for (const event of combined) {
      const eventDate = normalizeIsoDate(event.eventDate || event.date || event.fetched_at);
      if (!eventDate) continue;
      const key = [
        String(event.ticker || '').toUpperCase(),
        String(event.eventType || '').toLowerCase(),
        eventDate,
      ].join('|');
      deduped.set(key, event);
    }
    return Array.from(deduped.values());
  }, [payload?.calendar, payload?.calendar_upcoming]);

  const calendarEvents = useMemo<ProventCalendarItem[]>(() => (
    calendarSourceEvents
      .map((event) => {
        const eventDate = normalizeIsoDate(event.eventDate || event.date || event.fetched_at);
        if (!eventDate) return null;
        const details = toRecord(event.details);
        const amount = toAmount(details.value);
        const status: ProventStatus = eventDate < todayIso ? 'paid' : 'provisioned';
        return {
          ticker: String(event.ticker || '').toUpperCase(),
          eventType: String(event.eventType || '').replace(/_/g, ' '),
          eventDate,
          amount,
          status,
        };
      })
      .filter((event): event is ProventCalendarItem => Boolean(event))
      .sort((left, right) => (
        String(left.eventDate).localeCompare(String(right.eventDate))
        || String(left.ticker).localeCompare(String(right.ticker))
      ))
  ), [calendarSourceEvents, todayIso]);

  const calendarMonthOptions = useMemo(() => {
    const months = new Set(calendarEvents.map((event) => event.eventDate.slice(0, 7)));
    months.add(todayIso.slice(0, 7));
    return Array.from(months)
      .sort()
      .map((month) => ({
        value: month,
        label: formatMonthLabel(month, numberLocale),
      }));
  }, [calendarEvents, numberLocale, todayIso]);

  useEffect(() => {
    setCalendarMonth(todayIso.slice(0, 7));
  }, [selectedPortfolio, todayIso]);

  useEffect(() => {
    if (!calendarMonthOptions.some((option) => option.value === calendarMonth)) {
      const currentMonth = todayIso.slice(0, 7);
      const fallback = calendarMonthOptions.find((option) => option.value === currentMonth)?.value
        || calendarMonthOptions[calendarMonthOptions.length - 1]?.value
        || currentMonth;
      setCalendarMonth(fallback);
    }
  }, [calendarMonth, calendarMonthOptions, todayIso]);

  const calendarMonthEvents = useMemo(() => (
    calendarEvents.filter((event) => (
      event.eventDate.startsWith(`${calendarMonth}-`)
      && Boolean(visibleStatuses[event.status])
    ))
  ), [calendarEvents, calendarMonth, visibleStatuses]);

  useEffect(() => {
    setExpandedDates({});
  }, [calendarMonth, selectedPortfolio]);

  const calendarEventsByDate = useMemo(() => {
    const grouped = new Map<string, ProventCalendarItem[]>();
    for (const event of calendarMonthEvents) {
      if (!grouped.has(event.eventDate)) grouped.set(event.eventDate, []);
      grouped.get(event.eventDate)?.push(event);
    }
    return grouped;
  }, [calendarMonthEvents]);

  const weekdayLabels = useMemo(() => {
    const sunday = new Date(Date.UTC(2023, 0, 1, 12, 0, 0));
    return Array.from({ length: 7 }, (_, index) => {
      const date = new Date(sunday.getTime() + (index * 86400000));
      return date.toLocaleDateString(numberLocale, { weekday: 'short', timeZone: 'UTC' });
    });
  }, [numberLocale]);

  const calendarCells = useMemo(() => {
    const [yearRaw, monthRaw] = calendarMonth.split('-');
    const year = Number(yearRaw);
    const month = Number(monthRaw);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return [];

    const firstDay = new Date(Date.UTC(year, month - 1, 1));
    const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
    const leadDays = firstDay.getUTCDay();
    const cells: Array<{ date: string | null; day: number | null; events: ProventCalendarItem[] }> = [];

    for (let index = 0; index < leadDays; index += 1) {
      cells.push({ date: null, day: null, events: [] });
    }

    for (let day = 1; day <= daysInMonth; day += 1) {
      const date = `${calendarMonth}-${String(day).padStart(2, '0')}`;
      cells.push({
        date,
        day,
        events: calendarEventsByDate.get(date) || [],
      });
    }

    while (cells.length % 7 !== 0) {
      cells.push({ date: null, day: null, events: [] });
    }

    return cells;
  }, [calendarEventsByDate, calendarMonth]);

  const totalInPeriod = Number(payload?.total_in_period ?? payload?.total_last_12_months ?? 0);
  const averageMonthlyIncome = Number(payload?.average_monthly_income ?? payload?.projected_monthly_income ?? 0);
  const annualizedIncome = Number(payload?.annualized_income ?? payload?.projected_annual_income ?? (averageMonthlyIncome * 12));
  const yieldOnCostPeriod = Number(payload?.yield_on_cost_realized ?? 0);
  const currentDividendYieldPeriod = Number(payload?.dividend_yield_current ?? 0);

  const toggleStatus = (status: ProventStatus) => {
    setVisibleStatuses((previous) => ({
      ...previous,
      [status]: !previous[status],
    }));
  };

  const toggleDateExpansion = (date: string) => {
    setExpandedDates((previous) => ({
      ...previous,
      [date]: !previous[date],
    }));
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

              <section className="dividends-card dividends-card--wide">
                <header className="dividends-card__header">
                  <h2>{t('dividends.proventsCalendar', { defaultValue: 'Provents Calendar' })}</h2>
                  <div className="provents-calendar__controls">
                    <SharedDropdown
                      className="dividends-page__dropdown"
                      size="sm"
                      value={calendarMonth}
                      onChange={setCalendarMonth}
                      options={calendarMonthOptions}
                      ariaLabel={t('dividends.calendarMonth', { defaultValue: 'Month' })}
                    />
                  </div>
                </header>

                <div className="provents-calendar__legend">
                  <button
                    type="button"
                    className={`provents-calendar__legend-item provents-calendar__legend-item--paid ${visibleStatuses.paid ? '' : 'provents-calendar__legend-item--inactive'}`.trim()}
                    onClick={() => toggleStatus('paid')}
                  >
                    {t('dividends.calendarStatus.paid', { defaultValue: 'Paid' })}
                  </button>
                  <button
                    type="button"
                    className={`provents-calendar__legend-item provents-calendar__legend-item--provisioned ${visibleStatuses.provisioned ? '' : 'provents-calendar__legend-item--inactive'}`.trim()}
                    onClick={() => toggleStatus('provisioned')}
                  >
                    {t('dividends.calendarStatus.provisioned', { defaultValue: 'Provisioned' })}
                  </button>
                </div>

                {calendarMonthEvents.length === 0 && (
                  <p className="dividends-card__empty">{t('dividends.noCalendarEvents', { defaultValue: 'No provents in selected month.' })}</p>
                )}

                <div className="provents-calendar__weekdays">
                  {weekdayLabels.map((label) => (
                    <span key={label} className="provents-calendar__weekday">{label}</span>
                  ))}
                </div>

                <div className="provents-calendar__grid">
                  {calendarCells.map((cell, index) => (
                    <article
                      key={`${cell.date || 'empty'}-${index}`}
                      className={`provents-calendar__cell ${cell.date ? '' : 'provents-calendar__cell--placeholder'}`.trim()}
                    >
                      {cell.day ? (
                        <>
                          <header className="provents-calendar__cell-header">
                            <span className="provents-calendar__cell-day">{cell.day}</span>
                          </header>
                          <div className="provents-calendar__cell-events">
                            {(cell.date && expandedDates[cell.date] ? cell.events : cell.events.slice(0, 3)).map((entry, eventIndex) => (
                              <div
                                key={`${cell.date}-${entry.ticker}-${entry.eventType}-${eventIndex}`}
                                className={`provents-calendar__event provents-calendar__event--${entry.status}`}
                                title={`${entry.ticker} ${entry.eventType}`}
                              >
                                <span className="provents-calendar__event-ticker">{entry.ticker}</span>
                                {entry.amount !== null && (
                                  <span className="provents-calendar__event-amount">
                                    {formatCurrency(entry.amount, 'BRL', numberLocale)}
                                  </span>
                                )}
                              </div>
                            ))}
                            {cell.events.length > 3 && (
                              <button
                                type="button"
                                className="provents-calendar__more"
                                onClick={() => cell.date && toggleDateExpansion(cell.date)}
                              >
                                {cell.date && expandedDates[cell.date]
                                  ? t('common.showLess', { defaultValue: 'Show less' })
                                  : `+${cell.events.length - 3}`}
                              </button>
                            )}
                          </div>
                        </>
                      ) : null}
                    </article>
                  ))}
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
