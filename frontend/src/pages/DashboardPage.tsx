import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
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
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
import {
  api,
  type DashboardAllocationItem,
  type DashboardResponse,
  type DropdownConfigMap,
  type PortfolioEventNoticeItem,
} from '../services/api';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './DashboardPage.scss';

const CHART_COLORS = ['#22d3ee', '#818cf8', '#34d399', '#f59e0b', '#fb7185', '#38bdf8', '#f97316', '#a78bfa'];
const EVOLUTION_STROKE = '#22d3ee';
const EVOLUTION_FILL = 'rgba(34, 211, 238, 0.26)';
const SUPPORTED_EVOLUTION_PERIODS = new Set(['1M', '3M', '6M', '1Y', '2Y', '5Y', 'MAX']);

type Trend = 'positive' | 'negative' | 'neutral';
type NoticeKind = 'payment' | 'provisioned' | 'informe' | 'event';
type NoticeSeverity = 'low' | 'medium' | 'high';

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

const toTitleLabel = (value: string): string => value
  .replace(/_/g, ' ')
  .split(' ')
  .filter(Boolean)
  .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
  .join(' ');

const normalizeEventTypeLabel = (value: string): string => toTitleLabel(
  String(value || '')
    .toLowerCase()
    .replace(/[_-]+/g, ' ')
);

const toFieldLabel = (value: string): string => toTitleLabel(
  String(value || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[._-]+/g, ' ')
);

const normalizeNoticeKind = (value: unknown): NoticeKind => {
  const text = String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
  if (text === 'payment') return 'payment';
  if (text === 'provisioned') return 'provisioned';
  if (text === 'informe') return 'informe';
  return 'event';
};

const normalizeNoticeSeverity = (value: unknown): NoticeSeverity => {
  const text = String(value || '').toLowerCase().trim();
  if (text === 'high') return 'high';
  if (text === 'medium') return 'medium';
  return 'low';
};

const DashboardPage = () => {
  const { t, i18n } = useTranslation();
  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    eventNotices,
    eventNoticesLoading,
    refreshEventNotices,
    setEventNoticeRead,
    markAllEventNoticesRead,
  } = usePortfolioData();
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [evolutionPeriod, setEvolutionPeriod] = useState<string>('MAX');
  const [selectedEventNotice, setSelectedEventNotice] = useState<PortfolioEventNoticeItem | null>(null);
  const [eventNoticeFilter, setEventNoticeFilter] = useState<'all' | 'unread'>('all');

  useEffect(() => {
    api.getDropdownSettings()
      .then((dropdownSettings) => {
        setDropdownConfig(normalizeDropdownConfig(dropdownSettings.dropdowns));
      })
      .catch(() => {
        setDropdownConfig(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      });
  }, []);

  const evolutionPeriodOptions = useMemo(() => {
    const configured = getDropdownOptions(dropdownConfig, 'dashboard.evolution.period');
    const fallback = getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'dashboard.evolution.period');
    const normalizePeriodOptions = (source: Array<{ value: string; label: string }>) => source
      .map((option) => ({
        value: String(option.value || '').toUpperCase(),
        label: String(option.label || option.value || '').trim(),
      }))
      .filter((option) => option.value && SUPPORTED_EVOLUTION_PERIODS.has(option.value));

    const configuredOptions = normalizePeriodOptions(configured);
    if (configuredOptions.length > 0) return configuredOptions;
    return normalizePeriodOptions(fallback);
  }, [dropdownConfig]);

  useEffect(() => {
    if (evolutionPeriodOptions.length === 0) return;
    if (evolutionPeriodOptions.some((option) => option.value === evolutionPeriod)) return;
    setEvolutionPeriod(evolutionPeriodOptions[0].value);
  }, [evolutionPeriod, evolutionPeriodOptions]);

  useEffect(() => {
    if (!selectedPortfolio) {
      setDashboard(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api.getDashboard(selectedPortfolio, evolutionPeriod)
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
  }, [selectedPortfolio, evolutionPeriod]);

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
    (key) => toTitleLabel(key)
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
  const todayTotalCount = Number(eventNotices?.today_count || 0);
  const weekTotalCount = Number(eventNotices?.week_count || 0);
  const unreadTodayCount = Number(eventNotices?.unread_today_count ?? 0);
  const unreadWeekCount = Number(eventNotices?.unread_week_count ?? 0);
  const todayEventsRaw = useMemo(() => eventNotices?.today_events || [], [eventNotices?.today_events]);
  const weekEventsRaw = useMemo(() => eventNotices?.week_events || [], [eventNotices?.week_events]);
  const todayEvents = useMemo(() => {
    const source = todayEventsRaw;
    if (eventNoticeFilter === 'unread') return source.filter((event) => !event.read);
    return source;
  }, [todayEventsRaw, eventNoticeFilter]);
  const weekEvents = useMemo(() => {
    const source = weekEventsRaw;
    if (eventNoticeFilter === 'unread') return source.filter((event) => !event.read);
    return source;
  }, [weekEventsRaw, eventNoticeFilter]);

  useEffect(() => {
    setSelectedEventNotice(null);
    setEventNoticeFilter('all');
  }, [selectedPortfolio]);

  useEffect(() => {
    if (!selectedEventNotice) return;
    const stillExists = [...todayEventsRaw, ...weekEventsRaw]
      .some((event) => event.id === selectedEventNotice.id);
    if (!stillExists) {
      setSelectedEventNotice(null);
    }
  }, [selectedEventNotice, todayEventsRaw, weekEventsRaw]);

  const handleOpenEventNotice = useCallback((event: PortfolioEventNoticeItem) => {
    setSelectedEventNotice(event);
    if (!event.id || event.read) return;
    void setEventNoticeRead(event.id, true)
      .then(() => {
        setSelectedEventNotice((current) => {
          if (!current || current.id !== event.id) return current;
          return {
            ...current,
            read: true,
            readAt: current.readAt || new Date().toISOString(),
          };
        });
      })
      .catch(() => undefined);
  }, [setEventNoticeRead]);

  const handleToggleSelectedEventRead = useCallback(() => {
    if (!selectedEventNotice?.id) return;
    const nextRead = !selectedEventNotice.read;
    void setEventNoticeRead(selectedEventNotice.id, nextRead)
      .then(() => {
        setSelectedEventNotice((current) => {
          if (!current || current.id !== selectedEventNotice.id) return current;
          return {
            ...current,
            read: nextRead,
            readAt: nextRead ? new Date().toISOString() : null,
          };
        });
      })
      .catch(() => undefined);
  }, [selectedEventNotice, setEventNoticeRead]);

  const handleMarkAllEventNoticesRead = useCallback(() => {
    void markAllEventNoticesRead('all');
  }, [markAllEventNoticesRead]);

  const formatNoticeDateTime = (value: unknown): string => {
    const text = String(value || '').trim();
    if (!text) return '-';
    const parsed = new Date(text);
    if (Number.isNaN(parsed.getTime())) return text;
    return parsed.toLocaleString(numberLocale, {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
  };

  const renderNoticeValue = (key: string, value: unknown): ReactNode => {
    if (value === null || value === undefined || value === '') return '-';

    if (typeof value === 'boolean') {
      return value ? t('common.yes') : t('common.no');
    }

    if (typeof value === 'number') {
      const normalizedKey = key.toLowerCase();
      if (normalizedKey.includes('pct') || normalizedKey.includes('percent') || normalizedKey.includes('yield')) {
        return `${value.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
      }
      if (normalizedKey.includes('value') || normalizedKey.includes('amount') || normalizedKey.includes('price')) {
        return formatCurrency(value, 'BRL', numberLocale);
      }
      return value.toLocaleString(numberLocale);
    }

    if (typeof value === 'string') {
      const text = value.trim();
      if (!text) return '-';

      if (/^https?:\/\//i.test(text)) {
        return (
          <a href={text} target="_blank" rel="noreferrer">
            {text}
          </a>
        );
      }

      const normalizedKey = key.toLowerCase();
      const looksLikeDate = /^\d{4}-\d{2}-\d{2}/.test(text);
      if ((normalizedKey.includes('date') || normalizedKey.includes('at')) && looksLikeDate) {
        if (text.length > 10) return formatNoticeDateTime(text);
        return formatDate(text.slice(0, 10), numberLocale);
      }

      return text;
    }

    if (Array.isArray(value)) {
      if (value.length === 0) return '-';
      return value.map((item, index) => (
        <span key={`${key}-${index}`}>
          {renderNoticeValue(`${key}-${index}`, item)}
          {index < value.length - 1 ? ', ' : ''}
        </span>
      ));
    }

    if (typeof value === 'object') {
      return (
        <code className="dashboard-events__json">
          {JSON.stringify(value, null, 2)}
        </code>
      );
    }

    return String(value);
  };

  const eventNoticeSections: RecordDetailsSection[] = (() => {
    if (!selectedEventNotice) return [];

    const details =
      selectedEventNotice.details && typeof selectedEventNotice.details === 'object'
        ? selectedEventNotice.details
        : {};

    const overviewFields = [
      {
        key: 'ticker',
        label: t('dashboard.eventsNotice.modal.fields.ticker', { defaultValue: 'Ticker' }),
        value: selectedEventNotice.ticker || '-',
      },
      {
        key: 'kind',
        label: t('dashboard.eventsNotice.modal.fields.kind', { defaultValue: 'Category' }),
        value: t(`dashboard.eventsNotice.kinds.${normalizeNoticeKind(selectedEventNotice.notice_kind)}`, {
          defaultValue: normalizeNoticeKind(selectedEventNotice.notice_kind),
        }),
      },
      {
        key: 'severity',
        label: t('dashboard.eventsNotice.modal.fields.severity', { defaultValue: 'Severity' }),
        value: t(`dashboard.eventsNotice.severity.${normalizeNoticeSeverity(selectedEventNotice.severity)}`, {
          defaultValue: normalizeNoticeSeverity(selectedEventNotice.severity),
        }),
      },
      {
        key: 'read',
        label: t('dashboard.eventsNotice.modal.fields.read', { defaultValue: 'Status' }),
        value: selectedEventNotice.read
          ? t('dashboard.eventsNotice.read', { defaultValue: 'Read' })
          : t('dashboard.eventsNotice.unread', { defaultValue: 'Unread' }),
      },
      {
        key: 'eventDate',
        label: t('dashboard.eventsNotice.modal.fields.eventDate', { defaultValue: 'Event Date' }),
        value: formatDate(selectedEventNotice.eventDate, numberLocale),
      },
      {
        key: 'eventType',
        label: t('dashboard.eventsNotice.modal.fields.eventType', { defaultValue: 'Event Type' }),
        value: selectedEventNotice.eventType || '-',
      },
      {
        key: 'eventTitle',
        label: t('dashboard.eventsNotice.modal.fields.eventTitle', { defaultValue: 'Title' }),
        value: selectedEventNotice.eventTitle || '-',
      },
      {
        key: 'source',
        label: t('dashboard.eventsNotice.modal.fields.source', { defaultValue: 'Source' }),
        value: selectedEventNotice.data_source || '-',
      },
      {
        key: 'updatedAt',
        label: t('dashboard.eventsNotice.modal.fields.updatedAt', { defaultValue: 'Updated At' }),
        value: selectedEventNotice.updatedAt ? formatNoticeDateTime(selectedEventNotice.updatedAt) : '-',
      },
      {
        key: 'readAt',
        label: t('dashboard.eventsNotice.modal.fields.readAt', { defaultValue: 'Read At' }),
        value: selectedEventNotice.readAt ? formatNoticeDateTime(selectedEventNotice.readAt) : '-',
      },
      {
        key: 'id',
        label: t('dashboard.eventsNotice.modal.fields.id', { defaultValue: 'ID' }),
        value: selectedEventNotice.id || '-',
      },
    ];

    const detailEntries = Object.entries(details).map(([key, value]) => ({
      key,
      label: toFieldLabel(key),
      value: renderNoticeValue(key, value),
    }));

    const detailFields = detailEntries.length > 0
      ? detailEntries
      : [{
        key: 'empty',
        label: t('dashboard.eventsNotice.modal.fields.details', { defaultValue: 'Details' }),
        value: t('dashboard.eventsNotice.modal.emptyDetails', { defaultValue: 'No additional details.' }),
      }];

    return [
      {
        key: 'overview',
        title: t('dashboard.eventsNotice.modal.sections.overview', { defaultValue: 'Overview' }),
        fields: overviewFields,
        columns: 2,
      },
      {
        key: 'details',
        title: t('dashboard.eventsNotice.modal.sections.details', { defaultValue: 'Event Details' }),
        fields: detailFields,
        columns: 2,
        fullWidth: true,
      },
    ];
  })();

  const eventModalHeaderActions = selectedEventNotice ? (
    <button
      type="button"
      className="dashboard-events__modal-action"
      onClick={handleToggleSelectedEventRead}
    >
      {selectedEventNotice.read
        ? t('dashboard.eventsNotice.markAsUnread', { defaultValue: 'Mark as unread' })
        : t('dashboard.eventsNotice.markAsRead', { defaultValue: 'Mark as read' })}
    </button>
  ) : null;

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

        {!!selectedPortfolio && (
          <section className="dashboard-events">
            <header className="dashboard-events__header">
              <h2>{t('dashboard.eventsNotice.title')}</h2>
              <div className="dashboard-events__actions">
                <div className="dashboard-events__filters">
                  <button
                    type="button"
                    className={`dashboard-events__filter ${eventNoticeFilter === 'all' ? 'dashboard-events__filter--active' : ''}`}
                    onClick={() => setEventNoticeFilter('all')}
                  >
                    {t('dashboard.eventsNotice.filters.all', { defaultValue: 'All' })}
                  </button>
                  <button
                    type="button"
                    className={`dashboard-events__filter ${eventNoticeFilter === 'unread' ? 'dashboard-events__filter--active' : ''}`}
                    onClick={() => setEventNoticeFilter('unread')}
                  >
                    {t('dashboard.eventsNotice.filters.unread', { defaultValue: 'Unread' })}
                  </button>
                </div>
                <button
                  type="button"
                  className="dashboard-events__mark-read"
                  onClick={handleMarkAllEventNoticesRead}
                  disabled={eventNoticesLoading || (Number(eventNotices?.unread_count || 0) <= 0)}
                >
                  {t('dashboard.eventsNotice.markAllRead', { defaultValue: 'Mark all as read' })}
                </button>
                <button
                  type="button"
                  className="dashboard-events__refresh"
                  onClick={refreshEventNotices}
                  disabled={eventNoticesLoading}
                >
                  {t('dashboard.eventsNotice.refresh')}
                </button>
              </div>
            </header>
            <div className="dashboard-events__kpis">
              <span className="dashboard-events__kpi dashboard-events__kpi--today">
                {t('dashboard.eventsNotice.todayCount', { count: todayTotalCount })}
                <small>{t('dashboard.eventsNotice.unreadCount', { count: unreadTodayCount })}</small>
              </span>
              <span className="dashboard-events__kpi dashboard-events__kpi--week">
                {t('dashboard.eventsNotice.weekCount', { count: weekTotalCount })}
                <small>{t('dashboard.eventsNotice.unreadCount', { count: unreadWeekCount })}</small>
              </span>
            </div>
            {eventNoticesLoading ? (
              <p className="dashboard-events__loading">{t('dashboard.eventsNotice.loading')}</p>
            ) : (
              <div className="dashboard-events__grid">
                <div className="dashboard-events__column">
                  <h3>{t('dashboard.eventsNotice.today')}</h3>
                  {todayEvents.length === 0 ? (
                    <p className="dashboard-events__empty">{t('dashboard.eventsNotice.noneToday')}</p>
                  ) : (
                    <ul className="dashboard-events__list">
                      {todayEvents.map((event) => (
                        <li
                          key={`today-${event.id}`}
                          className={event.read ? 'dashboard-events__item dashboard-events__item--read' : 'dashboard-events__item dashboard-events__item--unread'}
                        >
                          <button
                            type="button"
                            className="dashboard-events__event-btn"
                            onClick={() => handleOpenEventNotice(event)}
                            aria-label={t('dashboard.eventsNotice.modal.openDetails', {
                              defaultValue: 'Open details for {{ticker}}',
                              ticker: event.ticker,
                            })}
                          >
                            <strong>
                              <span className={`dashboard-events__state ${event.read ? 'dashboard-events__state--read' : 'dashboard-events__state--unread'}`} />
                              {event.ticker}
                              <em className={`dashboard-events__tag dashboard-events__tag--${normalizeNoticeKind(event.notice_kind)}`}>
                                {t(`dashboard.eventsNotice.kinds.${normalizeNoticeKind(event.notice_kind)}`, {
                                  defaultValue: normalizeNoticeKind(event.notice_kind),
                                })}
                              </em>
                              <em className={`dashboard-events__severity dashboard-events__severity--${normalizeNoticeSeverity(event.severity)}`}>
                                {t(`dashboard.eventsNotice.severity.${normalizeNoticeSeverity(event.severity)}`, {
                                  defaultValue: normalizeNoticeSeverity(event.severity),
                                })}
                              </em>
                            </strong>
                            <span>{event.eventTitle || normalizeEventTypeLabel(event.eventType)}</span>
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
                <div className="dashboard-events__column">
                  <h3>{t('dashboard.eventsNotice.week')}</h3>
                  {weekEvents.length === 0 ? (
                    <p className="dashboard-events__empty">{t('dashboard.eventsNotice.noneWeek')}</p>
                  ) : (
                    <ul className="dashboard-events__list">
                      {weekEvents.map((event) => (
                        <li
                          key={`week-${event.id}`}
                          className={event.read ? 'dashboard-events__item dashboard-events__item--read' : 'dashboard-events__item dashboard-events__item--unread'}
                        >
                          <button
                            type="button"
                            className="dashboard-events__event-btn"
                            onClick={() => handleOpenEventNotice(event)}
                            aria-label={t('dashboard.eventsNotice.modal.openDetails', {
                              defaultValue: 'Open details for {{ticker}}',
                              ticker: event.ticker,
                            })}
                          >
                            <strong>
                              <span className={`dashboard-events__state ${event.read ? 'dashboard-events__state--read' : 'dashboard-events__state--unread'}`} />
                              {event.ticker}
                              <em className={`dashboard-events__tag dashboard-events__tag--${normalizeNoticeKind(event.notice_kind)}`}>
                                {t(`dashboard.eventsNotice.kinds.${normalizeNoticeKind(event.notice_kind)}`, {
                                  defaultValue: normalizeNoticeKind(event.notice_kind),
                                })}
                              </em>
                              <em className={`dashboard-events__severity dashboard-events__severity--${normalizeNoticeSeverity(event.severity)}`}>
                                {t(`dashboard.eventsNotice.severity.${normalizeNoticeSeverity(event.severity)}`, {
                                  defaultValue: normalizeNoticeSeverity(event.severity),
                                })}
                              </em>
                            </strong>
                            <span>
                              {`${formatDate(event.eventDate, numberLocale)} · ${event.eventTitle || normalizeEventTypeLabel(event.eventType)}`}
                            </span>
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            )}
          </section>
        )}

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
                  <div className="dashboard-card__controls">
                    <label htmlFor="dashboard-evolution-period">
                      {t('dashboard.period', { defaultValue: 'Period' })}
                    </label>
                    <select
                      id="dashboard-evolution-period"
                      className="dashboard__select dashboard__select--small"
                      value={evolutionPeriod}
                      onChange={(event) => setEvolutionPeriod(event.target.value)}
                    >
                      {evolutionPeriodOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label || option.value}
                        </option>
                      ))}
                    </select>
                  </div>
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

        <RecordDetailsModal
          open={Boolean(selectedEventNotice)}
          title={t('dashboard.eventsNotice.modal.title', { defaultValue: 'Event Details' })}
          subtitle={
            selectedEventNotice
              ? `${selectedEventNotice.ticker} · ${formatDate(selectedEventNotice.eventDate, numberLocale)}`
              : t('dashboard.eventsNotice.modal.subtitle', { defaultValue: 'Selected event data' })
          }
          closeLabel={t('common.close', { defaultValue: 'Close' })}
          sections={eventNoticeSections}
          headerActions={eventModalHeaderActions}
          onClose={() => setSelectedEventNotice(null)}
        />
      </div>
    </Layout>
  );
};

export default DashboardPage;
