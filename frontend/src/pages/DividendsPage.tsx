import { useEffect, useMemo, useRef, useState } from 'react';
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
import { Link } from 'react-router';
import Layout from '../components/Layout';
import {
  api,
  type DividendCalendarEvent,
  type DividendCalendarMonthResponse,
  type DividendsResponse,
  type DropdownConfigMap,
} from '../services/api';
import {
  buildAnalyticsCacheKey,
  getOrFetchCachedAnalytics,
} from '../services/analyticsCache';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import {
  normalizeMethodOptions,
  normalizeNumericOptions,
  resolveCalendarMonthValue,
  resolveSelectableValue,
} from './dividendsPage.state.js';
import './DividendsPage.scss';

const toDateMinusMonths = (monthsBack: number) => {
  const date = new Date();
  date.setDate(1);
  date.setMonth(date.getMonth() - monthsBack + 1);
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
type ProventCategory = 'dividend' | 'jcp' | 'amortization' | 'rendimento' | 'subscription' | 'other';
type ProventFrequencyKey =
  | 'dividends.frequencyValues.insufficient'
  | 'dividends.frequencyValues.monthly'
  | 'dividends.frequencyValues.quarterly'
  | 'dividends.frequencyValues.semiannual'
  | 'dividends.frequencyValues.annual'
  | 'dividends.frequencyValues.irregular';

type ProventEvent = {
  id: string;
  ticker: string;
  assetId: string | null;
  eventType: string | null;
  category: ProventCategory;
  eventDate: string;
  exDate: string | null;
  recordDate: string | null;
  announcementDate: string | null;
  amountPerUnit: number | null;
  quantity: number | null;
  expectedGross: number | null;
  expectedNet: number | null;
  netIsEstimated: boolean;
  taxHintKey: string | null;
  status: ProventStatus;
  source: string | null;
  sourceUrl: string | null;
  valueSource: string | null;
  hasRevision: boolean;
  revisionNoteKey: string | null;
  yieldCurrentPct: number | null;
  yieldOnCostPct: number | null;
  currency: string;
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

const normalizeProventCategory = (eventTypeValue: unknown, rawTypeValue: unknown): ProventCategory => {
  const text = `${String(eventTypeValue || '')} ${String(rawTypeValue || '')}`.toLowerCase();
  if (!text) return 'other';
  if (text.includes('jcp') || text.includes('juros')) return 'jcp';
  if (text.includes('amort')) return 'amortization';
  if (text.includes('rend')) return 'rendimento';
  if (text.includes('subscr') || text.includes('subscription') || text.includes('preferenc')) return 'subscription';
  if (text.includes('divid') || text.includes('provent')) return 'dividend';
  return 'other';
};

const isIncomeCategory = (category: ProventCategory): boolean => (
  category === 'dividend'
  || category === 'jcp'
  || category === 'amortization'
  || category === 'rendimento'
);

const formatPercent = (value: number | null, locale: string) => (
  value === null
    ? '-'
    : `${value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`
);

const sourcePriority = (source: string | null) => {
  const normalized = String(source || '').toLowerCase();
  if (normalized.includes('statusinvest')) return 3;
  if (normalized.includes('fundsexplorer')) return 2;
  if (normalized) return 1;
  return 0;
};

const dedupeFamily = (category: ProventCategory) => {
  if (category === 'jcp') return 'jcp';
  if (category === 'amortization') return 'amortization';
  if (category === 'subscription') return 'subscription';
  return 'income';
};

const dedupeKeyForEvent = (event: Pick<ProventEvent, 'ticker' | 'eventDate' | 'category'>) => (
  `${event.ticker}|${event.eventDate}|${dedupeFamily(event.category)}`
);

const dedupeScoreForEvent = (event: ProventEvent) => {
  let score = 0;
  if (event.amountPerUnit !== null) score += event.amountPerUnit > 0 ? 200 : 90;
  if (event.expectedGross !== null && event.expectedGross > 0) score += 10;
  if (event.exDate) score += 10;
  if (event.recordDate) score += 6;
  if (event.announcementDate) score += 4;
  if (event.valueSource) score += 15;
  score += sourcePriority(event.source) * 20;
  const type = String(event.eventType || '').toLowerCase();
  if (type.includes('payment') || type.includes('dividend') || type.includes('rend') || type.includes('jcp')) {
    score += 8;
  }
  return score;
};

const frequencyKeyFromDates = (dates: string[]): ProventFrequencyKey => {
  const sorted = Array.from(new Set(dates)).sort();
  if (sorted.length < 3) return 'dividends.frequencyValues.insufficient';
  const intervals: number[] = [];
  for (let index = 1; index < sorted.length; index += 1) {
    const prev = new Date(`${sorted[index - 1]}T00:00:00Z`);
    const next = new Date(`${sorted[index]}T00:00:00Z`);
    const diffMs = next.getTime() - prev.getTime();
    if (Number.isFinite(diffMs) && diffMs > 0) {
      intervals.push(diffMs / 86400000);
    }
  }
  if (intervals.length === 0) return 'dividends.frequencyValues.insufficient';
  const avgDays = intervals.reduce((sum, value) => sum + value, 0) / intervals.length;
  if (avgDays <= 45) return 'dividends.frequencyValues.monthly';
  if (avgDays <= 110) return 'dividends.frequencyValues.quarterly';
  if (avgDays <= 200) return 'dividends.frequencyValues.semiannual';
  if (avgDays <= 400) return 'dividends.frequencyValues.annual';
  return 'dividends.frequencyValues.irregular';
};

const formatSource = (value: string | null) => {
  const normalized = String(value || '').trim();
  if (!normalized) return '-';
  return normalized
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
};

const DividendsPage = () => {
  const { t, i18n } = useTranslation();
  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    assets: portfolioAssets,
    metrics,
  } = usePortfolioData();
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [periodMonths, setPeriodMonths] = useState('12');
  const [method, setMethod] = useState('fifo');
  const [calendarMonth, setCalendarMonth] = useState(getLocalMonth());
  const [visibleStatuses, setVisibleStatuses] = useState<Record<ProventStatus, boolean>>({
    paid: true,
    provisioned: true,
  });
  const [selectedCalendarDate, setSelectedCalendarDate] = useState<string | null>(null);
  const [selectedCalendarEventId, setSelectedCalendarEventId] = useState<string | null>(null);
  const [expandedDates, setExpandedDates] = useState<Record<string, boolean>>({});
  const calendarSectionRef = useRef<HTMLElement | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [payload, setPayload] = useState<DividendsResponse | null>(null);
  const [calendarLoading, setCalendarLoading] = useState(false);
  const calendarCacheRef = useRef(new Map<string, DividendCalendarEvent[]>());
  const [calendarRawEvents, setCalendarRawEvents] = useState<DividendCalendarEvent[]>([]);
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

  useEffect(() => {
    const nextValue = resolveSelectableValue(periodMonths, periodOptions, '12');
    if (nextValue !== periodMonths) setPeriodMonths(nextValue);
  }, [periodMonths, periodOptions]);

  useEffect(() => {
    const nextValue = resolveSelectableValue(method, methodOptions, 'fifo');
    if (nextValue !== method) setMethod(nextValue);
  }, [method, methodOptions]);

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

  const cacheKey = useMemo(() => {
    if (!selectedPortfolio) return '';
    return buildAnalyticsCacheKey('dividends', [
      selectedPortfolio,
      fromDate,
      method,
      selectedPeriodMonths,
    ]);
  }, [fromDate, method, selectedPeriodMonths, selectedPortfolio]);

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
      () => api.getDividends(selectedPortfolio, { fromDate, method, periodMonths: selectedPeriodMonths }),
      { ttlMs: 3 * 60 * 1000 }
    )
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
  }, [cacheKey, fromDate, method, selectedPeriodMonths, selectedPortfolio]);

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

  const quantityByTicker = useMemo(() => {
    const map = new Map<string, number>();
    for (const asset of portfolioAssets) {
      const status = String(asset.status || '').toLowerCase();
      if (status && status !== 'active') continue;
      const ticker = String(asset.ticker || '').toUpperCase();
      if (!ticker) continue;
      const quantity = toAmount(asset.quantity) ?? 0;
      map.set(ticker, (map.get(ticker) ?? 0) + quantity);
    }
    return map;
  }, [portfolioAssets]);

  const assetIdByTicker = useMemo(() => {
    const map = new Map<string, string>();
    for (const asset of portfolioAssets) {
      const status = String(asset.status || '').toLowerCase();
      if (status && status !== 'active') continue;
      const ticker = String(asset.ticker || '').toUpperCase();
      const assetId = String(asset.assetId || '').trim();
      if (!ticker || !assetId || map.has(ticker)) continue;
      map.set(ticker, assetId);
    }
    return map;
  }, [portfolioAssets]);

  const currentPriceByTicker = useMemo(() => {
    const map = new Map<string, number>();
    for (const asset of portfolioAssets) {
      const status = String(asset.status || '').toLowerCase();
      if (status && status !== 'active') continue;
      const ticker = String(asset.ticker || '').toUpperCase();
      if (!ticker) continue;
      const metricQuote = toAmount(metrics?.currentQuotes?.[asset.assetId]);
      const snapshotQuote = toAmount(asset.currentPrice);
      const resolved = metricQuote ?? snapshotQuote;
      if (resolved === null || resolved <= 0 || map.has(ticker)) continue;
      map.set(ticker, resolved);
    }
    return map;
  }, [metrics?.currentQuotes, portfolioAssets]);

  const currencyByTicker = useMemo(() => {
    const map = new Map<string, string>();
    for (const asset of portfolioAssets) {
      const status = String(asset.status || '').toLowerCase();
      if (status && status !== 'active') continue;
      const ticker = String(asset.ticker || '').toUpperCase();
      if (!ticker || map.has(ticker)) continue;
      const cur = String(asset.currency || '').toUpperCase();
      if (cur) map.set(ticker, cur);
    }
    return map;
  }, [portfolioAssets]);

  const averageCostByTicker = useMemo(() => {
    const accum = new Map<string, {
      weightedCost: number;
      quantity: number;
      fallbackCostSum: number;
      fallbackCount: number;
    }>();

    for (const asset of portfolioAssets) {
      const status = String(asset.status || '').toLowerCase();
      if (status && status !== 'active') continue;
      const ticker = String(asset.ticker || '').toUpperCase();
      if (!ticker) continue;
      const avgCost = toAmount(metrics?.averageCosts?.[asset.assetId]);
      if (avgCost === null || avgCost <= 0) continue;
      const quantity = Math.max(toAmount(asset.quantity) ?? 0, 0);
      const current = accum.get(ticker) || {
        weightedCost: 0,
        quantity: 0,
        fallbackCostSum: 0,
        fallbackCount: 0,
      };
      if (quantity > 0) {
        current.weightedCost += avgCost * quantity;
        current.quantity += quantity;
      } else {
        current.fallbackCostSum += avgCost;
        current.fallbackCount += 1;
      }
      accum.set(ticker, current);
    }

    const resolved = new Map<string, number>();
    for (const [ticker, value] of accum.entries()) {
      if (value.quantity > 0) {
        resolved.set(ticker, value.weightedCost / value.quantity);
      } else if (value.fallbackCount > 0) {
        resolved.set(ticker, value.fallbackCostSum / value.fallbackCount);
      }
    }
    return resolved;
  }, [metrics?.averageCosts, portfolioAssets]);

  const normalizedRawEvents = useMemo<ProventEvent[]>(() => {
    const combined = calendarRawEvents;
    const normalized: ProventEvent[] = [];
    combined.forEach((event, index) => {
      const details = toRecord(event.details);
      const ticker = String(event.ticker || details.ticker || '').toUpperCase();
      if (!ticker) return;

      const category = normalizeProventCategory(event.eventType, details.rawType);
      const incomeEvent = isIncomeCategory(category);
      const eventDate = normalizeIsoDate(
        details.paymentDate
        || event.eventDate
        || event.date
        || event.fetched_at
      );
      if (!eventDate) return;

      const exDate = normalizeIsoDate(details.exDate || details.baseDate || details.base_date);
      const recordDate = normalizeIsoDate(details.recordDate || details.comDate || details.dataCom || details.data_com);
      const announcementDate = normalizeIsoDate(
        details.announcementDate
        || details.declarationDate
        || details.approvedDate
        || details.announcement_date
      );
      const amountPerUnit = toAmount(details.value);
      const quantity = quantityByTicker.get(ticker) ?? null;
      const transactionTotalAmount = toAmount(
        details.totalAmount
        ?? details.total_amount
        ?? details.totalGross
        ?? details.total_gross
      );
      const explicitNetAmount = toAmount(
        details.netAmount
        ?? details.net_amount
        ?? details.totalNet
        ?? details.total_net
      );
      const expectedGrossFromUnit = incomeEvent && amountPerUnit !== null && amountPerUnit > 0 && quantity !== null && quantity > 0
        ? amountPerUnit * quantity
        : null;
      const expectedGross = incomeEvent
        ? expectedGrossFromUnit ?? transactionTotalAmount
        : null;
      const expectedNet = !incomeEvent
        ? null
        : explicitNetAmount ?? (
          expectedGross === null
            ? null
            : category === 'jcp'
              ? expectedGross * 0.85
              : expectedGross
        );
      const netIsEstimated =
        incomeEvent
        && explicitNetAmount === null
        && category === 'jcp'
        && expectedGross !== null;
      const taxHintKey = incomeEvent && category === 'jcp' ? 'dividends.hints.jcpWithholding' : null;
      const currentPrice = currentPriceByTicker.get(ticker) ?? null;
      const averageCost = averageCostByTicker.get(ticker) ?? null;
      const yieldCurrentPct =
        incomeEvent && amountPerUnit !== null && currentPrice !== null && currentPrice > 0
          ? (amountPerUnit / currentPrice) * 100
          : null;
      const yieldOnCostPct =
        incomeEvent && amountPerUnit !== null && averageCost !== null && averageCost > 0
          ? (amountPerUnit / averageCost) * 100
          : null;
      const resolvedCurrency = String(details.currency || '').toUpperCase()
        || currencyByTicker.get(ticker)
        || 'BRL';
      const source = String(event.data_source || '').trim() || null;
      const sourceUrl = String(details.url || '').trim() || null;
      const valueSource = String(details.value_source || '').trim() || null;
      const status: ProventStatus = eventDate < todayIso ? 'paid' : 'provisioned';
      const label = String(details.rawType || event.eventTitle || event.eventType || '').trim();

      normalized.push({
        id: String(event.eventId || `${ticker}-${eventDate}-${category}-${index}`),
        ticker,
        assetId: assetIdByTicker.get(ticker) ?? null,
        eventType: label.replace(/_/g, ' '),
        category,
        eventDate,
        exDate,
        recordDate,
        announcementDate,
        amountPerUnit,
        quantity,
        expectedGross,
        expectedNet,
        netIsEstimated,
        taxHintKey,
        status,
        source,
        sourceUrl,
        valueSource,
        hasRevision: Boolean(details.revised),
        revisionNoteKey: details.revised ? 'dividends.hints.mergedSources' : null,
        yieldCurrentPct,
        yieldOnCostPct,
        currency: resolvedCurrency,
      });
    });
    return normalized.sort((left, right) => (
      left.eventDate.localeCompare(right.eventDate)
      || left.ticker.localeCompare(right.ticker)
    ));
  }, [
    calendarRawEvents,
    quantityByTicker,
    assetIdByTicker,
    currentPriceByTicker,
    currencyByTicker,
    averageCostByTicker,
    todayIso,
  ]);

  const revisionFlagsByKey = useMemo(() => {
    const map = new Map<string, { hasRevision: boolean; noteKey: string | null }>();
    const grouped = new Map<string, {
      values: Set<string>;
      sources: Set<string>;
      overriddenBySource: boolean;
    }>();

    for (const event of normalizedRawEvents) {
      const key = dedupeKeyForEvent(event);
      const current = grouped.get(key) || {
        values: new Set<string>(),
        sources: new Set<string>(),
        overriddenBySource: false,
      };
      if (event.amountPerUnit !== null) {
        current.values.add(event.amountPerUnit.toFixed(8));
      }
      if (event.source) current.sources.add(event.source);
      if (event.valueSource && event.source && event.valueSource !== event.source) {
        current.overriddenBySource = true;
      }
      grouped.set(key, current);
    }

    for (const [key, value] of grouped.entries()) {
      const hasRevision = value.values.size > 1 || value.overriddenBySource;
      const noteKey = value.values.size > 1
        ? 'dividends.hints.valueConflict'
        : value.overriddenBySource
          ? 'dividends.hints.sourceOverride'
          : null;
      map.set(key, { hasRevision, noteKey });
    }
    return map;
  }, [normalizedRawEvents]);

  const calendarEvents = useMemo<ProventEvent[]>(() => {
    const deduped = new Map<string, ProventEvent>();
    for (const event of normalizedRawEvents) {
      const key = dedupeKeyForEvent(event);
      const existing = deduped.get(key);
      if (!existing) {
        deduped.set(key, event);
        continue;
      }

      const existingScore = dedupeScoreForEvent(existing);
      const candidateScore = dedupeScoreForEvent(event);
      if (candidateScore > existingScore) {
        deduped.set(key, event);
      }
    }

    return Array.from(deduped.entries())
      .map(([key, event]) => ({
        ...event,
        hasRevision: revisionFlagsByKey.get(key)?.hasRevision ?? false,
        revisionNoteKey: revisionFlagsByKey.get(key)?.noteKey ?? null,
      }))
      .sort((left, right) => (
        left.eventDate.localeCompare(right.eventDate)
        || left.ticker.localeCompare(right.ticker)
      ));
  }, [normalizedRawEvents, revisionFlagsByKey]);

  const calendarMonthOptions = useMemo(() => {
    const base = new Date(`${todayIso}T00:00:00Z`);
    if (Number.isNaN(base.getTime())) return [];
    const months: string[] = [];
    for (let offset = -12; offset <= 2; offset += 1) {
      const d = new Date(base.getTime());
      d.setUTCDate(1);
      d.setUTCMonth(d.getUTCMonth() + offset);
      const y = d.getUTCFullYear();
      const m = String(d.getUTCMonth() + 1).padStart(2, '0');
      months.push(`${y}-${m}`);
    }
    return months.map((month) => ({
      value: month,
      label: formatMonthLabel(month, numberLocale),
    }));
  }, [numberLocale, todayIso]);

  useEffect(() => {
    const currentMonth = todayIso.slice(0, 7);
    setCalendarMonth((previous) => (previous === currentMonth ? previous : currentMonth));
    calendarCacheRef.current.clear();
  }, [selectedPortfolio, todayIso]);

  useEffect(() => {
    const nextMonth = resolveCalendarMonthValue(
      calendarMonth,
      calendarMonthOptions.map((option) => option.value),
      todayIso.slice(0, 7)
    );
    if (nextMonth !== calendarMonth) setCalendarMonth(nextMonth);
  }, [calendarMonth, calendarMonthOptions, todayIso]);

  useEffect(() => {
    if (!selectedPortfolio || !calendarMonth) {
      setCalendarRawEvents([]);
      setCalendarLoading(false);
      return;
    }
    const cacheKeyCalendar = `${selectedPortfolio}::${calendarMonth}`;
    const cached = calendarCacheRef.current.get(cacheKeyCalendar);
    if (cached) {
      setCalendarRawEvents(cached);
      setCalendarLoading(false);
      return;
    }
    let cancelled = false;
    setCalendarLoading(true);
    api.getDividendCalendar(selectedPortfolio, calendarMonth)
      .then((response: DividendCalendarMonthResponse) => {
        if (cancelled) return;
        calendarCacheRef.current.set(cacheKeyCalendar, response.events);
        setCalendarRawEvents(response.events);
      })
      .catch(() => {
        if (cancelled) return;
        setCalendarRawEvents([]);
      })
      .finally(() => {
        if (!cancelled) setCalendarLoading(false);
      });
    return () => { cancelled = true; };
  }, [selectedPortfolio, calendarMonth]);

  const calendarMonthEvents = useMemo(() => (
    calendarEvents.filter((event) => (
      event.eventDate.startsWith(`${calendarMonth}-`)
      && Boolean(visibleStatuses[event.status])
    ))
  ), [calendarEvents, calendarMonth, visibleStatuses]);

  const sortedMonthEvents = useMemo(
    () => [...calendarMonthEvents].sort((left, right) => (
      right.eventDate.localeCompare(left.eventDate) || left.ticker.localeCompare(right.ticker)
    )),
    [calendarMonthEvents]
  );

  const listMonthEvents = useMemo(() => {
    if (selectedCalendarEventId) {
      return sortedMonthEvents.filter((event) => event.id === selectedCalendarEventId);
    }
    if (selectedCalendarDate) {
      return sortedMonthEvents.filter((event) => event.eventDate === selectedCalendarDate);
    }
    return sortedMonthEvents;
  }, [selectedCalendarDate, selectedCalendarEventId, sortedMonthEvents]);

  useEffect(() => {
    setExpandedDates({});
  }, [calendarMonth, selectedPortfolio]);

  useEffect(() => {
    setSelectedCalendarDate(null);
    setSelectedCalendarEventId(null);
  }, [calendarMonth, selectedPortfolio]);

  useEffect(() => {
    if (!selectedCalendarDate && !selectedCalendarEventId) return;

    const hasSelectedEvent = selectedCalendarEventId
      ? calendarMonthEvents.some((event) => event.id === selectedCalendarEventId)
      : false;

    if (selectedCalendarEventId && !hasSelectedEvent) {
      setSelectedCalendarEventId(null);
    }
  }, [calendarMonthEvents, selectedCalendarDate, selectedCalendarEventId]);

  useEffect(() => {
    if (!selectedCalendarDate && !selectedCalendarEventId) return;

    const onDocumentPointerDown = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (calendarSectionRef.current?.contains(target)) return;
      setSelectedCalendarDate(null);
      setSelectedCalendarEventId(null);
    };

    document.addEventListener('mousedown', onDocumentPointerDown);
    return () => {
      document.removeEventListener('mousedown', onDocumentPointerDown);
    };
  }, [selectedCalendarDate, selectedCalendarEventId]);

  const calendarEventsByDate = useMemo(() => {
    const grouped = new Map<string, ProventEvent[]>();
    for (const event of calendarMonthEvents) {
      if (!grouped.has(event.eventDate)) grouped.set(event.eventDate, []);
      grouped.get(event.eventDate)?.push(event);
    }
    return grouped;
  }, [calendarMonthEvents]);

  const rolling12mStart = useMemo(() => {
    const date = new Date(`${todayIso}T00:00:00Z`);
    if (Number.isNaN(date.getTime())) return todayIso;
    date.setUTCDate(1);
    date.setUTCMonth(date.getUTCMonth() - 11);
    return date.toISOString().slice(0, 10);
  }, [todayIso]);

  const tickerInsights = useMemo(() => {
    const grouped = new Map<string, {
      ticker: string;
      assetId: string | null;
      totalGross12m: number;
      totalPerUnit12m: number;
      paidCount12m: number;
      paidDates: string[];
      currentPrice: number | null;
      averageCost: number | null;
      nextPaymentDate: string | null;
      latestAnnouncementDate: string | null;
      hasRevisions: boolean;
    }>();

    for (const event of calendarEvents) {
      const incomeEvent = isIncomeCategory(event.category);
      const current = grouped.get(event.ticker) || {
        ticker: event.ticker,
        assetId: event.assetId,
        totalGross12m: 0,
        totalPerUnit12m: 0,
        paidCount12m: 0,
        paidDates: [],
        currentPrice: event.amountPerUnit !== null ? currentPriceByTicker.get(event.ticker) ?? null : null,
        averageCost: averageCostByTicker.get(event.ticker) ?? null,
        nextPaymentDate: null,
        latestAnnouncementDate: null,
        hasRevisions: false,
      };

      if (!current.assetId && event.assetId) current.assetId = event.assetId;
      if (current.currentPrice === null) current.currentPrice = currentPriceByTicker.get(event.ticker) ?? null;
      if (current.averageCost === null) current.averageCost = averageCostByTicker.get(event.ticker) ?? null;

      if (incomeEvent && event.status === 'paid') {
        current.paidDates.push(event.eventDate);
      }
      if (incomeEvent && event.status === 'paid' && event.eventDate >= rolling12mStart) {
        current.totalGross12m += event.expectedGross ?? 0;
        current.totalPerUnit12m += event.amountPerUnit ?? 0;
        current.paidCount12m += 1;
      }
      if (
        incomeEvent
        && event.status === 'provisioned'
        && event.eventDate >= todayIso
        && (!current.nextPaymentDate || event.eventDate < current.nextPaymentDate)
      ) {
        current.nextPaymentDate = event.eventDate;
      }
      if (
        incomeEvent
        && event.announcementDate
        && (!current.latestAnnouncementDate || event.announcementDate > current.latestAnnouncementDate)
      ) {
        current.latestAnnouncementDate = event.announcementDate;
      }
      if (event.hasRevision) current.hasRevisions = true;

      grouped.set(event.ticker, current);
    }

    return Array.from(grouped.values())
      .map((entry) => {
        const yieldCurrent12mPct =
          entry.currentPrice !== null && entry.currentPrice > 0
            ? (entry.totalPerUnit12m / entry.currentPrice) * 100
            : null;
        const yieldOnCost12mPct =
          entry.averageCost !== null && entry.averageCost > 0
            ? (entry.totalPerUnit12m / entry.averageCost) * 100
            : null;

        return {
          ...entry,
          frequencyKey: frequencyKeyFromDates(entry.paidDates),
          yieldCurrent12mPct,
          yieldOnCost12mPct,
        };
      })
      .sort((left, right) => (
        right.totalGross12m - left.totalGross12m
        || left.ticker.localeCompare(right.ticker)
      ));
  }, [calendarEvents, rolling12mStart, todayIso, currentPriceByTicker, averageCostByTicker]);

  const tickerInsightByTicker = useMemo(
    () => new Map(tickerInsights.map((insight) => [insight.ticker, insight])),
    [tickerInsights]
  );

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
    const cells: Array<{ date: string | null; day: number | null; events: ProventEvent[] }> = [];

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
  const isAllStatusesVisible = visibleStatuses.paid && visibleStatuses.provisioned;

  const toggleStatus = (status: ProventStatus) => {
    setVisibleStatuses((previous) => ({
      ...previous,
      [status]: !previous[status],
    }));
  };

  const enableAllStatuses = () => {
    setVisibleStatuses({ paid: true, provisioned: true });
  };

  const toggleDateExpansion = (date: string) => {
    setExpandedDates((previous) => ({
      ...previous,
      [date]: !previous[date],
    }));
  };

  const selectCalendarDay = (date: string) => {
    if (selectedCalendarDate === date && !selectedCalendarEventId) {
      setSelectedCalendarDate(null);
      setSelectedCalendarEventId(null);
      return;
    }
    setSelectedCalendarDate(date);
    setSelectedCalendarEventId(null);
  };

  const selectCalendarEvent = (date: string, eventId: string) => {
    if (selectedCalendarEventId === eventId) {
      setSelectedCalendarDate(date);
      setSelectedCalendarEventId(null);
      return;
    }
    setSelectedCalendarDate(date);
    setSelectedCalendarEventId(eventId);
  };

  const activeStatusLabel = isAllStatusesVisible
    ? t('common.all', { defaultValue: 'All' })
    : visibleStatuses.paid && !visibleStatuses.provisioned
      ? t('dividends.calendarStatus.paid', { defaultValue: 'Paid' })
      : !visibleStatuses.paid && visibleStatuses.provisioned
        ? t('dividends.calendarStatus.provisioned', { defaultValue: 'Provisioned' })
        : t('dividends.paymentsScope.none', { defaultValue: 'No statuses selected' });

  const paymentsScopeLabel = selectedCalendarEventId
    ? t('dividends.paymentsScope.event', {
      date: selectedCalendarDate ? formatDate(selectedCalendarDate, numberLocale) : '-',
      defaultValue: 'Showing selected event for {{date}}',
    })
    : selectedCalendarDate
      ? t('dividends.paymentsScope.day', {
        date: formatDate(selectedCalendarDate, numberLocale),
        status: activeStatusLabel,
        defaultValue: 'Showing {{status}} events for {{date}}',
      })
      : t('dividends.paymentsScope.month', {
        status: activeStatusLabel,
        defaultValue: 'Showing {{status}} events in selected month',
      });

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

              <section className="dividends-card dividends-card--wide" ref={calendarSectionRef}>
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
                    className={`provents-calendar__legend-item provents-calendar__legend-item--all ${isAllStatusesVisible ? '' : 'provents-calendar__legend-item--inactive'}`.trim()}
                    onClick={enableAllStatuses}
                  >
                    {t('common.all', { defaultValue: 'All' })}
                  </button>
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

                {calendarLoading && (
                  <p className="dividends-card__empty">{t('common.loading')}</p>
                )}

                {!calendarLoading && calendarMonthEvents.length === 0 && (
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
                      className={`provents-calendar__cell ${cell.date ? '' : 'provents-calendar__cell--placeholder'} ${cell.date && selectedCalendarDate === cell.date ? 'provents-calendar__cell--selected' : ''}`.trim()}
                      onClick={() => {
                        if (cell.date) selectCalendarDay(cell.date);
                      }}
                    >
                      {cell.day ? (
                        <>
                          <header className="provents-calendar__cell-header">
                            <button
                              type="button"
                              className={`provents-calendar__cell-day ${cell.date && selectedCalendarDate === cell.date ? 'provents-calendar__cell-day--selected' : ''}`.trim()}
                              onClick={(event) => {
                                event.stopPropagation();
                                if (cell.date) selectCalendarDay(cell.date);
                              }}
                            >
                              {cell.day}
                            </button>
                          </header>
                          <div className="provents-calendar__cell-events">
                            {(cell.date && expandedDates[cell.date] ? cell.events : cell.events.slice(0, 3)).map((entry, eventIndex) => (
                              <button
                                type="button"
                                key={`${cell.date}-${entry.ticker}-${entry.eventType}-${eventIndex}`}
                                className={`provents-calendar__event provents-calendar__event--${entry.status} ${selectedCalendarEventId === entry.id ? 'provents-calendar__event--selected' : ''}`.trim()}
                                title={`${entry.ticker} ${entry.eventType} - ${entry.eventDate}`}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  if (cell.date) selectCalendarEvent(cell.date, entry.id);
                                }}
                              >
                                <span className="provents-calendar__event-ticker">{entry.ticker}</span>
                                {(() => {
                                  const amt = entry.expectedGross ?? entry.amountPerUnit ?? null;
                                  return amt !== null && Math.abs(amt) >= 0.005 ? (
                                    <span className="provents-calendar__event-amount">
                                      {formatCurrency(amt, entry.currency, numberLocale)}
                                    </span>
                                  ) : null;
                                })()}
                              </button>
                            ))}
                            {cell.events.length > 3 && (
                              <button
                                type="button"
                                className="provents-calendar__more"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  if (cell.date) toggleDateExpansion(cell.date);
                                }}
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

              <section className="dividends-card dividends-card--wide">
                <header className="dividends-card__header">
                  <div className="dividends-card__header-copy">
                    <h2>{t('dividends.paymentsListTitle', { defaultValue: 'Payment Details' })}</h2>
                    <p className="dividends-card__subtitle">{paymentsScopeLabel}</p>
                  </div>
                </header>
                {listMonthEvents.length === 0 ? (
                  <p className="dividends-card__empty">{t('dividends.noUpcoming', { defaultValue: 'No dividend events for selected filters.' })}</p>
                ) : (
                  <div className="dividends-list">
                    {listMonthEvents.slice(0, 60).map((event) => {
                      const tickerInsight = tickerInsightByTicker.get(event.ticker);
                      return (
                        <article
                          key={event.id}
                          className="dividends-list__item"
                        >
                          <div className="dividends-list__row">
                            <Link
                              to={event.assetId
                                ? `/assets/${encodeURIComponent(event.assetId)}?portfolioId=${encodeURIComponent(selectedPortfolio)}`
                                : `/assets?portfolioId=${encodeURIComponent(selectedPortfolio)}&ticker=${encodeURIComponent(event.ticker)}`}
                              className="dividends-list__ticker dividends-list__ticker--link"
                            >
                              {event.ticker}
                            </Link>
                            <div className="dividends-list__meta">
                              <span className="dividends-list__date">
                                {formatDate(event.eventDate, numberLocale)}
                              </span>
                              <span className={`dividends-list__status dividends-list__status--${event.status}`}>
                                {event.status === 'paid'
                                  ? t('dividends.calendarStatus.paid')
                                  : t('dividends.calendarStatus.provisioned')}
                              </span>
                              <span className={`dividends-list__category dividends-list__category--${event.category}`}>
                                {t(`dividends.categories.${event.category}`)}
                              </span>
                              {event.hasRevision && (
                                <span className="dividends-list__revision">
                                  {t('dividends.revision')}
                                </span>
                              )}
                            </div>
                          </div>

                          <div className="dividends-list__groups">
                            <section className="dividends-list__group">
                              <h3 className="dividends-list__group-title">{t('dividends.groups.event')}</h3>
                              <div className="dividends-list__group-items">
                                <div className="dividends-list__kv"><span>{t('dividends.paymentDate')}</span><strong>{formatDate(event.eventDate, numberLocale)}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.exDate')}</span><strong>{event.exDate ? formatDate(event.exDate, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.recordDate')}</span><strong>{event.recordDate ? formatDate(event.recordDate, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.announcementDate')}</span><strong>{event.announcementDate ? formatDate(event.announcementDate, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.type')}</span><strong>{event.eventType || t(`dividends.categories.${event.category}`)}</strong></div>
                              </div>
                            </section>

                            <section className="dividends-list__group">
                              <h3 className="dividends-list__group-title">{t('dividends.groups.cashYield')}</h3>
                              <div className="dividends-list__group-items">
                                <div className="dividends-list__kv"><span>{t('dividends.perUnit')}</span><strong>{event.amountPerUnit !== null ? formatCurrency(event.amountPerUnit, event.currency, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.quantity')}</span><strong>{event.quantity !== null ? event.quantity.toLocaleString(numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.grossReceive')}</span><strong>{event.expectedGross !== null ? formatCurrency(event.expectedGross, event.currency, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.netReceive')}</span><strong>{event.expectedNet !== null ? formatCurrency(event.expectedNet, event.currency, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.yieldCurrentEvent')}</span><strong>{formatPercent(event.yieldCurrentPct, numberLocale)}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.yieldOnCostEvent')}</span><strong>{formatPercent(event.yieldOnCostPct, numberLocale)}</strong></div>
                              </div>
                            </section>

                            <section className="dividends-list__group">
                              <h3 className="dividends-list__group-title">{t('dividends.groups.ticker12m')}</h3>
                              <div className="dividends-list__group-items">
                                <div className="dividends-list__kv"><span>{t('dividends.frequency')}</span><strong>{tickerInsight ? t(tickerInsight.frequencyKey) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.gross12m')}</span><strong>{tickerInsight ? formatCurrency(tickerInsight.totalGross12m, event.currency, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.events12m')}</span><strong>{tickerInsight ? tickerInsight.paidCount12m.toLocaleString(numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.yieldCurrent12m')}</span><strong>{tickerInsight ? formatPercent(tickerInsight.yieldCurrent12mPct, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.yieldCost12m')}</span><strong>{tickerInsight ? formatPercent(tickerInsight.yieldOnCost12mPct, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.nextPayment')}</span><strong>{tickerInsight?.nextPaymentDate ? formatDate(tickerInsight.nextPaymentDate, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.latestAnnouncement')}</span><strong>{tickerInsight?.latestAnnouncementDate ? formatDate(tickerInsight.latestAnnouncementDate, numberLocale) : '-'}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.revisions')}</span><strong>{tickerInsight ? (tickerInsight.hasRevisions ? t('common.yes') : t('common.no')) : '-'}</strong></div>
                              </div>
                            </section>

                            <section className="dividends-list__group">
                              <h3 className="dividends-list__group-title">{t('dividends.groups.sourceQuality')}</h3>
                              <div className="dividends-list__group-items">
                                <div className="dividends-list__kv">
                                  <span>{t('dividends.source')}</span>
                                  <strong>
                                    {event.sourceUrl ? (
                                      <a href={event.sourceUrl} target="_blank" rel="noreferrer" className="dividends-list__source-link">
                                        {formatSource(event.source)}
                                      </a>
                                    ) : (
                                      formatSource(event.source)
                                    )}
                                  </strong>
                                </div>
                                <div className="dividends-list__kv"><span>{t('dividends.valueSource')}</span><strong>{formatSource(event.valueSource)}</strong></div>
                                <div className="dividends-list__kv"><span>{t('dividends.revision')}</span><strong>{event.hasRevision ? t('common.yes') : t('common.no')}</strong></div>
                              </div>
                              {event.taxHintKey && (
                                <div className="dividends-list__hint">
                                  {t(event.taxHintKey)}
                                  {event.netIsEstimated ? ` ${t('dividends.estimated')}` : ''}
                                </div>
                              )}
                              {event.hasRevision && event.revisionNoteKey && (
                                <div className="dividends-list__hint">{t(event.revisionNoteKey)}</div>
                              )}
                            </section>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                )}
              </section>
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default DividendsPage;
