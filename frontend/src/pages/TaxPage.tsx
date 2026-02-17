import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type TaxMonthlyClassTrace,
  type TaxMonthlyItem,
  type TaxReportResponse,
} from '../services/api';
import { formatCurrency } from '../utils/formatters';
import './TaxPage.scss';

const TAX_STOCK_EXEMPTION_LIMIT = 20000;
const CHART_COLORS = ['#38bdf8', '#34d399', '#f59e0b', '#f87171', '#a78bfa', '#22d3ee'];

type MonthStyle = 'short' | 'long';

interface MonthlyTotalsRow {
  month: string;
  label: string;
  grossSales: number;
  realizedGain: number;
  taxDue: number;
  dividends: number;
  jcp: number;
  stockGrossSales: number;
  stockExempt: boolean;
  hasData: boolean;
}

interface TaxClassRow {
  month: string;
  monthLabel: string;
  classKey: string;
  grossSales: number;
  realizedGain: number;
  carryIn: number;
  taxDue: number;
  carryLoss: number;
  stockExempt: boolean;
  trace: TaxMonthlyClassTrace | null;
}

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

const humanizeTraceToken = (value: string): string =>
  toTitleCase(String(value || '').replace(/_/g, ' ').trim());

const sumValues = (values: Record<string, number> | undefined): number =>
  Object.values(values || {}).reduce((sum, value) => sum + toNumber(value), 0);

const formatMonthLabel = (month: string, locale: string, style: MonthStyle): string => {
  const [yearRaw, monthRaw] = String(month).split('-');
  const year = Number(yearRaw);
  const monthIndex = Number(monthRaw) - 1;
  if (!Number.isFinite(year) || !Number.isFinite(monthIndex) || monthIndex < 0 || monthIndex > 11) {
    return month;
  }
  return new Date(Date.UTC(year, monthIndex, 1, 12, 0, 0)).toLocaleDateString(locale, {
    month: style,
    year: style === 'long' ? 'numeric' : undefined,
    timeZone: 'UTC',
  });
};

const TaxPage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio, transactions } = usePortfolioData();
  const [selectedYear, setSelectedYear] = useState(() => String(new Date().getFullYear()));
  const [classFilter, setClassFilter] = useState('all');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<TaxReportResponse | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const formatBrl = (value: number) => formatCurrency(value, 'BRL', numberLocale);
  const formatSignedBrl = (value: number) => {
    const abs = formatBrl(Math.abs(value));
    if (value > 0) return `+${abs}`;
    if (value < 0) return `-${abs}`;
    return abs;
  };
  const stockExemptionLimit = toNumber(
    report?.tax_rules_by_class?.stock?.exemption?.limit_brl ?? TAX_STOCK_EXEMPTION_LIMIT
  ) || TAX_STOCK_EXEMPTION_LIMIT;

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const yearOptions = useMemo(() => {
    const years = new Set<number>();
    const currentYear = new Date().getFullYear();

    for (let offset = 0; offset < 8; offset += 1) {
      years.add(currentYear - offset);
    }

    for (const transaction of transactions) {
      const year = Number(String(transaction.date || '').slice(0, 4));
      if (!Number.isFinite(year) || year < 2000) continue;
      years.add(year);
    }

    if (Number.isFinite(report?.year)) {
      years.add(Number(report?.year));
    }

    return Array.from(years)
      .sort((left, right) => right - left)
      .map((year) => ({ value: String(year), label: String(year) }));
  }, [report?.year, transactions]);

  useEffect(() => {
    if (yearOptions.length === 0) return;
    if (yearOptions.some((option) => option.value === selectedYear)) return;
    setSelectedYear(yearOptions[0].value);
  }, [selectedYear, yearOptions]);

  useEffect(() => {
    if (!selectedPortfolio) {
      setReport(null);
      setLoading(false);
      return;
    }

    const yearNumeric = Number(selectedYear);
    if (!Number.isFinite(yearNumeric) || yearNumeric < 2000) {
      setReport(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    api.getTaxReport(selectedPortfolio, yearNumeric)
      .then((payload) => {
        if (cancelled) return;
        setReport(payload);
      })
      .catch((reason: unknown) => {
        if (cancelled) return;
        setReport(null);
        setError(reason instanceof Error ? reason.message : 'Failed to load tax report');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [selectedPortfolio, selectedYear]);

  const yearMonthKeys = useMemo(() => {
    const yearNumeric = Number(selectedYear);
    if (!Number.isFinite(yearNumeric) || yearNumeric < 2000) return [];
    return Array.from({ length: 12 }, (_, index) => `${selectedYear}-${String(index + 1).padStart(2, '0')}`);
  }, [selectedYear]);

  const monthlyByKey = useMemo(() => {
    const map = new Map<string, TaxMonthlyItem>();
    for (const row of report?.monthly || []) {
      if (!row?.month) continue;
      map.set(row.month, row);
    }
    return map;
  }, [report?.monthly]);

  const classKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const row of report?.monthly || []) {
      for (const key of Object.keys(row.gross_sales || {})) keys.add(key);
      for (const key of Object.keys(row.realized_gain || {})) keys.add(key);
      for (const key of Object.keys(row.tax_due || {})) keys.add(key);
      for (const key of Object.keys(row.explain_by_class || {})) keys.add(key);
    }
    for (const key of Object.keys(report?.carry_loss_start_by_class || {})) keys.add(key);
    for (const key of Object.keys(report?.carry_loss_by_class || {})) keys.add(key);
    for (const key of Object.keys(report?.tax_rules_by_class || {})) keys.add(key);
    return Array.from(keys).filter(Boolean).sort((left, right) => left.localeCompare(right));
  }, [
    report?.carry_loss_by_class,
    report?.carry_loss_start_by_class,
    report?.monthly,
    report?.tax_rules_by_class,
  ]);

  useEffect(() => {
    if (classFilter === 'all') return;
    if (classKeys.includes(classFilter)) return;
    setClassFilter('all');
  }, [classFilter, classKeys]);

  const classOptions = useMemo(
    () => [
      { value: 'all', label: t('tax.classFilter.all', { defaultValue: t('common.all') }) },
      ...classKeys.map((classKey) => ({
        value: classKey,
        label: t(`assets.classes.${classKey}`, { defaultValue: toTitleCase(classKey) }),
      })),
    ],
    [classKeys, t]
  );

  const monthlyTotals = useMemo<MonthlyTotalsRow[]>(() => (
    yearMonthKeys.map((month) => {
      const record = monthlyByKey.get(month);
      const grossSales = sumValues(record?.gross_sales);
      const realizedGain = sumValues(record?.realized_gain);
      const taxDue = sumValues(record?.tax_due);
      const dividends = toNumber(record?.dividends);
      const jcp = toNumber(record?.jcp);
      const stockTrace = record?.explain_by_class?.stock;
      const stockGrossSales = toNumber(stockTrace?.gross_sales ?? record?.gross_sales?.stock);
      const stockExempt = stockTrace
        ? Boolean(stockTrace.exemption?.applied)
        : (stockGrossSales > 0 && stockGrossSales <= stockExemptionLimit);
      const hasData = grossSales !== 0 || realizedGain !== 0 || taxDue !== 0 || dividends !== 0 || jcp !== 0;

      return {
        month,
        label: formatMonthLabel(month, numberLocale, 'long'),
        grossSales,
        realizedGain,
        taxDue,
        dividends,
        jcp,
        stockGrossSales,
        stockExempt,
        hasData,
      };
    })
  ), [monthlyByKey, numberLocale, stockExemptionLimit, yearMonthKeys]);

  const classRows = useMemo<TaxClassRow[]>(() => {
    const runningCarry: Record<string, number> = {};
    const rows: TaxClassRow[] = [];

    for (const classKey of classKeys) {
      runningCarry[classKey] = toNumber(report?.carry_loss_start_by_class?.[classKey]);
    }

    for (const month of yearMonthKeys) {
      const record = monthlyByKey.get(month);
      const grossSalesByClass = record?.gross_sales || {};
      const realizedGainByClass = record?.realized_gain || {};
      const taxDueByClass = record?.tax_due || {};
      const explainByClass = record?.explain_by_class || {};

      for (const classKey of classKeys) {
        const trace = explainByClass[classKey] || null;
        const previousCarry = toNumber(runningCarry[classKey]);
        const grossSales = trace ? toNumber(trace.gross_sales) : toNumber(grossSalesByClass[classKey]);
        const realizedGain = trace ? toNumber(trace.realized_gain) : toNumber(realizedGainByClass[classKey]);
        const taxDue = trace ? toNumber(trace.tax_due) : toNumber(taxDueByClass[classKey]);
        const carryIn = trace ? toNumber(trace.carry_in) : previousCarry;
        const stockExempt = trace
          ? Boolean(trace.exemption?.applied)
          : (
            classKey === 'stock' &&
            grossSales > 0 &&
            grossSales <= stockExemptionLimit
          );
        const carryLoss = trace
          ? toNumber(trace.carry_out)
          : (stockExempt ? previousCarry : Math.min(0, previousCarry + realizedGain));
        runningCarry[classKey] = carryLoss;

        const hasMovement =
          grossSales !== 0 ||
          realizedGain !== 0 ||
          taxDue !== 0 ||
          carryIn !== 0 ||
          carryLoss !== 0;

        if (!hasMovement) continue;

        rows.push({
          month,
          monthLabel: formatMonthLabel(month, numberLocale, 'long'),
          classKey,
          grossSales,
          realizedGain,
          carryIn,
          taxDue,
          carryLoss,
          stockExempt,
          trace,
        });
      }
    }

    return rows.sort(
      (left, right) =>
        right.month.localeCompare(left.month) ||
        left.classKey.localeCompare(right.classKey)
    );
  }, [
    classKeys,
    monthlyByKey,
    numberLocale,
    report?.carry_loss_start_by_class,
    stockExemptionLimit,
    yearMonthKeys,
  ]);

  const filteredClassRows = useMemo(
    () => (classFilter === 'all'
      ? classRows
      : classRows.filter((row) => row.classKey === classFilter)),
    [classFilter, classRows]
  );

  const annualRows = useMemo(
    () => monthlyTotals
      .filter((row) => row.hasData)
      .sort((left, right) => right.month.localeCompare(left.month)),
    [monthlyTotals]
  );

  const annualTotals = useMemo(() => annualRows.reduce(
    (accumulator, row) => ({
      grossSales: accumulator.grossSales + row.grossSales,
      realizedGain: accumulator.realizedGain + row.realizedGain,
      taxDue: accumulator.taxDue + row.taxDue,
      dividends: accumulator.dividends + row.dividends,
      jcp: accumulator.jcp + row.jcp,
    }),
    { grossSales: 0, realizedGain: 0, taxDue: 0, dividends: 0, jcp: 0 }
  ), [annualRows]);

  const chartClassKeys = classFilter === 'all'
    ? classKeys
    : classKeys.filter((classKey) => classKey === classFilter);

  const gainChartData = useMemo(
    () =>
      yearMonthKeys.map((month) => {
        const row = monthlyByKey.get(month);
        const values: Record<string, string | number> = {
          month: formatMonthLabel(month, numberLocale, 'short'),
        };
        for (const classKey of chartClassKeys) {
          values[classKey] = toNumber(row?.realized_gain?.[classKey]);
        }
        return values;
      }),
    [chartClassKeys, monthlyByKey, numberLocale, yearMonthKeys]
  );

  const darfChartData = useMemo(
    () =>
      monthlyTotals.map((row) => ({
        month: formatMonthLabel(row.month, numberLocale, 'short'),
        taxDue: row.taxDue,
      })),
    [monthlyTotals, numberLocale]
  );

  const carryLossRows = useMemo(
    () =>
      Object.entries(report?.carry_loss_by_class || {})
        .map(([classKey, value]) => ({ classKey, value: toNumber(value) }))
        .filter((row) => row.value !== 0)
        .sort((left, right) => left.classKey.localeCompare(right.classKey)),
    [report?.carry_loss_by_class]
  );

  const monthsWithDarf = monthlyTotals.filter((row) => row.taxDue > 0).length;
  const monthsWithStockExemption = monthlyTotals.filter((row) => row.stockExempt).length;

  const renderExemptionLabel = (row: TaxClassRow) => {
    const exemption = row.trace?.exemption;
    if (!exemption || exemption.type === null) {
      return {
        text: t('tax.exemption.notApplicable'),
        className: 'tax-badge tax-badge--muted',
      };
    }
    if (exemption.applied) {
      return {
        text: t('tax.exemption.exempt'),
        className: 'tax-badge tax-badge--positive',
      };
    }
    if (exemption.reason === 'no_sales' || row.grossSales <= 0) {
      return {
        text: t('tax.exemption.noSales'),
        className: 'tax-badge tax-badge--muted',
      };
    }
    return {
      text: t('tax.exemption.taxable'),
      className: 'tax-badge tax-badge--warning',
    };
  };

  const renderTraceDecision = (decision: string | undefined) =>
    decision
      ? t(`tax.trace.decisions.${decision}`, { defaultValue: humanizeTraceToken(decision) })
      : t('tax.trace.notAvailable');

  const renderTraceReason = (reason: string | undefined) =>
    reason
      ? t(`tax.trace.reasons.${reason}`, { defaultValue: humanizeTraceToken(reason) })
      : t('tax.trace.notAvailable');

  return (
    <Layout>
      <div className="tax-page">
        <div className="tax-page__header">
          <h1 className="tax-page__title">{t('tax.title')}</h1>
          <div className="tax-page__filters">
            {portfolioOptions.length > 0 && (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('tax.selectPortfolio')}
                className="tax-page__dropdown tax-page__dropdown--portfolio"
                size="sm"
              />
            )}
            <SharedDropdown
              value={selectedYear}
              options={yearOptions}
              onChange={setSelectedYear}
              ariaLabel={t('tax.selectYear')}
              className="tax-page__dropdown"
              size="sm"
              disabled={yearOptions.length === 0}
            />
            <SharedDropdown
              value={classFilter}
              options={classOptions}
              onChange={setClassFilter}
              ariaLabel={t('tax.selectClass')}
              className="tax-page__dropdown"
              size="sm"
              disabled={classOptions.length === 0}
            />
          </div>
        </div>

        {loading && <div className="tax-page__state">{t('common.loading')}</div>}

        {!loading && portfolios.length === 0 && (
          <div className="tax-page__state">{t('dashboard.noData')}</div>
        )}

        {!loading && error && (
          <div className="tax-page__state tax-page__state--error">
            <p>{t('tax.loadError')}</p>
            <code>{error}</code>
          </div>
        )}

        {!loading && !error && report && (
          <>
            <div className="tax-page__kpis">
              <article className="tax-kpi">
                <span className="tax-kpi__label">{t('tax.kpis.annualTaxDue')}</span>
                <span className="tax-kpi__value tax-kpi__value--negative">
                  {formatBrl(toNumber(report.total_tax_due))}
                </span>
              </article>
              <article className="tax-kpi">
                <span className="tax-kpi__label">{t('tax.kpis.taxableJcp')}</span>
                <span className="tax-kpi__value">{formatBrl(toNumber(report.total_jcp_tributavel))}</span>
              </article>
              <article className="tax-kpi">
                <span className="tax-kpi__label">{t('tax.kpis.exemptDividends')}</span>
                <span className="tax-kpi__value">{formatBrl(toNumber(report.total_dividends_isentos))}</span>
              </article>
              <article className="tax-kpi">
                <span className="tax-kpi__label">{t('tax.kpis.monthsWithDarf')}</span>
                <span className="tax-kpi__value">{monthsWithDarf}</span>
              </article>
              <article className="tax-kpi">
                <span className="tax-kpi__label">{t('tax.kpis.stockExemptMonths')}</span>
                <span className="tax-kpi__value">{monthsWithStockExemption}</span>
              </article>
            </div>

            <div className="tax-page__grid">
              <section className="tax-card tax-card--wide">
                <header className="tax-card__header">
                  <div>
                    <h2>{t('tax.sections.gainByClass')}</h2>
                    <p>{t('tax.stockExemptionRule', { limit: formatBrl(stockExemptionLimit) })}</p>
                  </div>
                </header>
                {chartClassKeys.length === 0 ? (
                  <p className="tax-card__empty">{t('tax.noSeries')}</p>
                ) : (
                  <ResponsiveContainer width="100%" height={310}>
                    <BarChart data={gainChartData} margin={{ top: 10, right: 12, left: 0, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.25)" />
                      <XAxis dataKey="month" stroke="var(--text-secondary)" />
                      <YAxis
                        stroke="var(--text-secondary)"
                        tickFormatter={(value) => formatBrl(toNumber(value))}
                        width={120}
                      />
                      <Tooltip formatter={(value) => formatBrl(toNumber(value))} />
                      <Legend />
                      {chartClassKeys.map((classKey, index) => (
                        <Bar
                          key={classKey}
                          dataKey={classKey}
                          name={t(`assets.classes.${classKey}`, { defaultValue: toTitleCase(classKey) })}
                          fill={CHART_COLORS[index % CHART_COLORS.length]}
                          stackId={classFilter === 'all' ? 'gain-by-class' : undefined}
                          isAnimationActive={false}
                        />
                      ))}
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </section>

              <section className="tax-card">
                <header className="tax-card__header">
                  <h2>{t('tax.sections.monthlyDarf')}</h2>
                </header>
                <ResponsiveContainer width="100%" height={260}>
                  <LineChart data={darfChartData} margin={{ top: 10, right: 12, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.25)" />
                    <XAxis dataKey="month" stroke="var(--text-secondary)" />
                    <YAxis
                      stroke="var(--text-secondary)"
                      tickFormatter={(value) => formatBrl(toNumber(value))}
                      width={100}
                    />
                    <Tooltip formatter={(value) => formatBrl(toNumber(value))} />
                    <Line
                      dataKey="taxDue"
                      name={t('tax.table.taxDue')}
                      stroke="#f97316"
                      strokeWidth={2.4}
                      dot={{ r: 2 }}
                      activeDot={{ r: 4 }}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </section>

              <section className="tax-card">
                <header className="tax-card__header">
                  <h2>{t('tax.sections.carryLoss')}</h2>
                </header>
                {carryLossRows.length === 0 ? (
                  <p className="tax-card__empty">{t('tax.carryLoss.none')}</p>
                ) : (
                  <ul className="tax-carry">
                    {carryLossRows.map((row) => (
                      <li key={row.classKey} className="tax-carry__item">
                        <span className="tax-carry__class">
                          {t(`assets.classes.${row.classKey}`, { defaultValue: toTitleCase(row.classKey) })}
                        </span>
                        <span className={row.value < 0 ? 'tax-carry__value tax-carry__value--negative' : 'tax-carry__value'}>
                          {formatSignedBrl(row.value)}
                        </span>
                      </li>
                    ))}
                  </ul>
                )}
              </section>

              <section className="tax-card tax-card--wide">
                <header className="tax-card__header">
                  <h2>{t('tax.sections.monthlyByClass')}</h2>
                </header>
                {filteredClassRows.length === 0 ? (
                  <p className="tax-card__empty">{t('tax.noSeries')}</p>
                ) : (
                  <div className="tax-table-wrapper">
                    <table className="tax-table">
                      <thead>
                        <tr>
                          <th>{t('tax.table.month')}</th>
                          <th>{t('tax.table.class')}</th>
                          <th>{t('tax.table.grossSales')}</th>
                          <th>{t('tax.table.realizedGain')}</th>
                          <th>{t('tax.table.taxDue')}</th>
                          <th>{t('tax.table.carryIn')}</th>
                          <th>{t('tax.table.carryLoss')}</th>
                          <th>{t('tax.table.exemption')}</th>
                          <th>{t('tax.table.trace')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredClassRows.map((row) => {
                          const exemption = renderExemptionLabel(row);
                          const classLabel = t(`assets.classes.${row.classKey}`, { defaultValue: toTitleCase(row.classKey) });
                          return (
                            <tr key={`${row.month}-${row.classKey}`}>
                              <td>{row.monthLabel}</td>
                              <td>{classLabel}</td>
                              <td>{formatBrl(row.grossSales)}</td>
                              <td className={row.realizedGain < 0 ? 'tax-table__value tax-table__value--negative' : row.realizedGain > 0 ? 'tax-table__value tax-table__value--positive' : 'tax-table__value'}>
                                {formatSignedBrl(row.realizedGain)}
                              </td>
                              <td>{formatBrl(row.taxDue)}</td>
                              <td className={row.carryIn < 0 ? 'tax-table__value tax-table__value--negative' : row.carryIn > 0 ? 'tax-table__value tax-table__value--positive' : 'tax-table__value'}>
                                {formatSignedBrl(row.carryIn)}
                              </td>
                              <td className={row.carryLoss < 0 ? 'tax-table__value tax-table__value--negative' : row.carryLoss > 0 ? 'tax-table__value tax-table__value--positive' : 'tax-table__value'}>
                                {formatSignedBrl(row.carryLoss)}
                              </td>
                              <td>
                                <span className={exemption.className}>{exemption.text}</span>
                              </td>
                              <td>
                                {!row.trace ? (
                                  <span className="tax-trace__empty">{t('tax.trace.notAvailable')}</span>
                                ) : (
                                  <details className="tax-trace">
                                    <summary className="tax-trace__summary">
                                      {renderTraceDecision(row.trace.decision)}
                                    </summary>
                                    <dl className="tax-trace__details">
                                      <div>
                                        <dt>{t('tax.trace.rule')}</dt>
                                        <dd>{row.trace.rule_label || row.trace.rule_id}</dd>
                                      </div>
                                      <div>
                                        <dt>{t('tax.trace.formula')}</dt>
                                        <dd>
                                          {`${formatSignedBrl(toNumber(row.trace.carry_in))} + ${formatSignedBrl(toNumber(row.trace.realized_gain))} = ${formatSignedBrl(toNumber(row.trace.adjusted_gain))}`}
                                        </dd>
                                      </div>
                                      <div>
                                        <dt>{t('tax.trace.taxableGain')}</dt>
                                        <dd>{formatSignedBrl(toNumber(row.trace.taxable_gain))}</dd>
                                      </div>
                                      <div>
                                        <dt>{t('tax.trace.rate')}</dt>
                                        <dd>{`${(toNumber(row.trace.tax_rate) * 100).toFixed(2)}%`}</dd>
                                      </div>
                                      <div>
                                        <dt>{t('tax.trace.taxDue')}</dt>
                                        <dd>{formatBrl(toNumber(row.trace.tax_due))}</dd>
                                      </div>
                                      <div>
                                        <dt>{t('tax.trace.exemption')}</dt>
                                        <dd>{renderTraceReason(row.trace.exemption?.reason)}</dd>
                                      </div>
                                      {row.trace.exemption?.limit_brl !== null && row.trace.exemption?.limit_brl !== undefined && (
                                        <div>
                                          <dt>{t('tax.trace.exemptionLimit')}</dt>
                                          <dd>{formatBrl(toNumber(row.trace.exemption.limit_brl))}</dd>
                                        </div>
                                      )}
                                      <div>
                                        <dt>{t('tax.trace.carryOut')}</dt>
                                        <dd>{formatSignedBrl(toNumber(row.trace.carry_out))}</dd>
                                      </div>
                                    </dl>
                                  </details>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </section>

              <section className="tax-card tax-card--wide">
                <header className="tax-card__header">
                  <h2>{t('tax.sections.annualSummary')}</h2>
                </header>
                {annualRows.length === 0 ? (
                  <p className="tax-card__empty">{t('tax.noSeries')}</p>
                ) : (
                  <div className="tax-table-wrapper">
                    <table className="tax-table">
                      <thead>
                        <tr>
                          <th>{t('tax.table.month')}</th>
                          <th>{t('tax.table.grossSales')}</th>
                          <th>{t('tax.table.realizedGain')}</th>
                          <th>{t('tax.table.taxDue')}</th>
                          <th>{t('tax.table.dividends')}</th>
                          <th>{t('tax.table.jcp')}</th>
                          <th>{t('tax.table.stockExemption')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {annualRows.map((row) => (
                          <tr key={row.month}>
                            <td>{row.label}</td>
                            <td>{formatBrl(row.grossSales)}</td>
                            <td className={row.realizedGain < 0 ? 'tax-table__value tax-table__value--negative' : row.realizedGain > 0 ? 'tax-table__value tax-table__value--positive' : 'tax-table__value'}>
                              {formatSignedBrl(row.realizedGain)}
                            </td>
                            <td>{formatBrl(row.taxDue)}</td>
                            <td>{formatBrl(row.dividends)}</td>
                            <td>{formatBrl(row.jcp)}</td>
                            <td>
                              {row.stockGrossSales <= 0 ? (
                                <span className="tax-badge tax-badge--muted">
                                  {t('tax.exemption.noSales')}
                                </span>
                              ) : row.stockExempt ? (
                                <span className="tax-badge tax-badge--positive">
                                  {t('tax.exemption.exempt')}
                                </span>
                              ) : (
                                <span className="tax-badge tax-badge--warning">
                                  {t('tax.exemption.taxable')}
                                </span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr>
                          <th>{t('tax.table.total')}</th>
                          <th>{formatBrl(annualTotals.grossSales)}</th>
                          <th className={annualTotals.realizedGain < 0 ? 'tax-table__value tax-table__value--negative' : annualTotals.realizedGain > 0 ? 'tax-table__value tax-table__value--positive' : 'tax-table__value'}>
                            {formatSignedBrl(annualTotals.realizedGain)}
                          </th>
                          <th>{formatBrl(annualTotals.taxDue)}</th>
                          <th>{formatBrl(annualTotals.dividends)}</th>
                          <th>{formatBrl(annualTotals.jcp)}</th>
                          <th>-</th>
                        </tr>
                      </tfoot>
                    </table>
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

export default TaxPage;
