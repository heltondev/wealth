import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router';
import Layout from '../components/Layout';
import { api, type Asset, type Transaction } from '../services/api';
import { formatCurrency, formatDate } from '../utils/formatters';
import './AssetsPage.scss';
import './AssetDetailsPage.scss';

type AssetRow = Asset & {
  quantity: number;
  source: string | null;
  investedAmount: number;
};

type AssetTradeHistoryRow = {
  transId: string;
  date: string;
  type: 'buy' | 'sell';
  quantity: number;
  price: number;
  amount: number;
  currency: string;
  source: string | null;
};

type AssetTradeHistoryPoint = AssetTradeHistoryRow & {
  x: number;
  y: number;
  index: number;
};

const COUNTRY_FLAG_MAP: Record<string, string> = {
  BR: '🇧🇷',
  US: '🇺🇸',
  CA: '🇨🇦',
};

const COUNTRY_NAME_MAP: Record<string, string> = {
  BR: 'Brazil',
  US: 'United States',
  CA: 'Canada',
};

const DECIMAL_PRECISION = 2;
const DECIMAL_FACTOR = 10 ** DECIMAL_PRECISION;
const HISTORY_CHART_WIDTH = 860;
const HISTORY_CHART_HEIGHT = 220;

const normalizeText = (value: unknown): string =>
  (value || '')
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();

const summarizeSourceValue = (value: unknown): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (normalized.includes('NUBANK') || normalized.includes('NU INVEST') || normalized.includes('NU BANK')) return 'NU BANK';
  if (normalized.includes('XP')) return 'XP';
  if (normalized.includes('ITAU')) return 'ITAU';
  if (normalized.includes('B3')) return 'B3';
  return null;
};

const AssetDetailsPage = () => {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const { assetId = '' } = useParams();
  const [searchParams] = useSearchParams();
  const portfolioIdFromQuery = searchParams.get('portfolioId')?.trim() || '';

  const [loading, setLoading] = useState(true);
  const [portfolioId, setPortfolioId] = useState('');
  const [assets, setAssets] = useState<Asset[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [currentQuote, setCurrentQuote] = useState<number | null>(null);
  const [averageCost, setAverageCost] = useState<number | null>(null);
  const [selectedHistoryPoint, setSelectedHistoryPoint] = useState<AssetTradeHistoryPoint | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('assets.modal.noValue');
    return String(value);
  }, [t]);

  const formatAssetQuantity = useCallback((value: unknown) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return formatDetailValue(value);

    const hasFraction = Math.abs(numeric % 1) > Number.EPSILON;
    return numeric.toLocaleString(numberLocale, {
      minimumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
      maximumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
    });
  }, [formatDetailValue, numberLocale]);

  const formatSignedCurrency = useCallback((value: number, currency: string) => {
    const absolute = formatCurrency(Math.abs(value), currency, numberLocale);
    if (Math.abs(value) <= Number.EPSILON) return absolute;
    return `${value > 0 ? '+' : '-'}${absolute}`;
  }, [numberLocale]);

  const formatCountryDetail = useCallback((country: string) =>
    `${COUNTRY_FLAG_MAP[country] || '🏳️'} ${COUNTRY_NAME_MAP[country] || country}`, []);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setLoading(true);
      try {
        let resolvedPortfolioId = portfolioIdFromQuery;
        if (!resolvedPortfolioId) {
          const portfolioItems = await api.getPortfolios();
          resolvedPortfolioId = portfolioItems[0]?.portfolioId || '';
        }

        if (cancelled) return;

        setPortfolioId(resolvedPortfolioId);

        if (!resolvedPortfolioId || !assetId) {
          setAssets([]);
          setTransactions([]);
          return;
        }

        const [assetItems, transactionItems] = await Promise.all([
          api.getAssets(resolvedPortfolioId),
          api.getTransactions(resolvedPortfolioId),
        ]);

        if (cancelled) return;

        setAssets(assetItems);
        setTransactions(transactionItems);
      } catch {
        if (cancelled) return;
        setAssets([]);
        setTransactions([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();

    return () => {
      cancelled = true;
    };
  }, [assetId, portfolioIdFromQuery]);

  const hasPrimaryTradeByAssetId = useMemo(() => {
    const set = new Set<string>();

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      if (normalizedType !== 'buy' && normalizedType !== 'sell' && normalizedType !== 'subscription') {
        continue;
      }

      const sourceTag = normalizeText(transaction.sourceDocId);
      if (!sourceTag.includes('B3-NEGOCIACAO')) continue;
      set.add(transaction.assetId);
    }

    return set;
  }, [transactions]);

  const shouldIgnoreConsolidatedTrade = useCallback((transaction: Transaction) => {
    const normalizedType = transaction.type?.toLowerCase() || '';
    if (normalizedType !== 'buy' && normalizedType !== 'sell' && normalizedType !== 'subscription') {
      return false;
    }

    const sourceTag = normalizeText(transaction.sourceDocId);
    if (!sourceTag.includes('B3-RELATORIO')) return false;

    return hasPrimaryTradeByAssetId.has(transaction.assetId);
  }, [hasPrimaryTradeByAssetId]);

  const assetQuantitiesById = useMemo(() => {
    const quantities: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;
      if (shouldIgnoreConsolidatedTrade(transaction)) continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      const normalizedQuantity = Math.round(Number(transaction.quantity || 0) * DECIMAL_FACTOR) / DECIMAL_FACTOR;
      if (!Number.isFinite(normalizedQuantity)) continue;

      if (normalizedType === 'buy' || normalizedType === 'subscription') {
        quantities[transaction.assetId] = (quantities[transaction.assetId] || 0) + normalizedQuantity;
        continue;
      }

      if (normalizedType === 'sell') {
        quantities[transaction.assetId] = (quantities[transaction.assetId] || 0) - normalizedQuantity;
      }
    }

    return quantities;
  }, [shouldIgnoreConsolidatedTrade, transactions]);

  const assetInvestedAmountById = useMemo(() => {
    const investedById: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;
      if (shouldIgnoreConsolidatedTrade(transaction)) continue;

      const amount = Number(transaction.amount || 0);
      if (!Number.isFinite(amount)) continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      if (normalizedType === 'buy' || normalizedType === 'subscription') {
        investedById[transaction.assetId] = (investedById[transaction.assetId] || 0) + amount;
        continue;
      }

      if (normalizedType === 'sell') {
        investedById[transaction.assetId] = (investedById[transaction.assetId] || 0) - amount;
      }
    }

    return investedById;
  }, [shouldIgnoreConsolidatedTrade, transactions]);

  const assetSourcesById = useMemo(() => {
    const sources: Record<string, string[]> = {};

    for (const transaction of transactions) {
      const sourceDocId = transaction.sourceDocId?.toString().trim();
      const institution = transaction.institution?.toString().trim();

      if (sourceDocId) {
        sources[transaction.assetId] = [...(sources[transaction.assetId] || []), sourceDocId];
      }

      if (institution) {
        sources[transaction.assetId] = [...(sources[transaction.assetId] || []), institution];
      }
    }

    return sources;
  }, [transactions]);

  const selectedAsset = useMemo<AssetRow | null>(() => {
    const baseAsset = assets.find((entry) => entry.assetId === assetId);
    if (!baseAsset) return null;

    const labels = new Set<string>();
    const assetSource = summarizeSourceValue(baseAsset.source);
    if (assetSource) labels.add(assetSource);

    for (const candidate of (assetSourcesById[baseAsset.assetId] || [])) {
      const label = summarizeSourceValue(candidate);
      if (label) labels.add(label);
    }

    return {
      ...baseAsset,
      quantity: Number.isFinite(Number(baseAsset.quantity))
        ? Number(baseAsset.quantity)
        : (assetQuantitiesById[baseAsset.assetId] || 0),
      source: labels.size > 0 ? Array.from(labels).join(', ') : null,
      investedAmount: assetInvestedAmountById[baseAsset.assetId] || 0,
    };
  }, [assetId, assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

  useEffect(() => {
    setCurrentQuote(null);
    setAverageCost(null);
    setSelectedHistoryPoint(null);
  }, [selectedAsset?.assetId]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    if (Number.isFinite(directCurrentPrice)) {
      setCurrentQuote(directCurrentPrice);
    } else {
      api.getPriceAtDate(portfolioId, selectedAsset.ticker, new Date().toISOString().slice(0, 10))
        .then((payload) => {
          if (cancelled) return;
          const close = Number((payload as { close?: unknown }).close);
          setCurrentQuote(Number.isFinite(close) ? close : null);
        })
        .catch(() => {
          if (cancelled) return;
          setCurrentQuote(null);
        });
    }

    api.getAverageCost(portfolioId, selectedAsset.ticker)
      .then((payload) => {
        if (cancelled) return;
        const parsedAverageCost = Number((payload as { average_cost?: unknown }).average_cost);
        setAverageCost(Number.isFinite(parsedAverageCost) ? parsedAverageCost : null);
      })
      .catch(() => {
        if (cancelled) return;
        setAverageCost(null);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  const fallbackAverageCost = useMemo(() => {
    if (!selectedAsset) return null;
    const quantity = Number(selectedAsset.quantity);
    const investedAmount = Number(selectedAsset.investedAmount);
    if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return investedAmount / quantity;
  }, [selectedAsset]);

  const resolvedAverageCost = useMemo(() => {
    if (typeof averageCost === 'number' && Number.isFinite(averageCost)) return averageCost;
    return fallbackAverageCost;
  }, [averageCost, fallbackAverageCost]);

  const resolvedCurrentPrice = useMemo(() => {
    if (!selectedAsset) return null;
    if (typeof currentQuote === 'number' && Number.isFinite(currentQuote)) return currentQuote;

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    if (Number.isFinite(directCurrentPrice)) return directCurrentPrice;

    const directCurrentValue = Number(selectedAsset.currentValue);
    const quantity = Number(selectedAsset.quantity);
    if (!Number.isFinite(directCurrentValue) || !Number.isFinite(quantity)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return directCurrentValue / quantity;
  }, [currentQuote, selectedAsset]);

  const resolvedCurrentValue = useMemo(() => {
    if (!selectedAsset) return null;
    const quantity = Number(selectedAsset.quantity);
    if (resolvedCurrentPrice !== null && Number.isFinite(quantity)) {
      return quantity * resolvedCurrentPrice;
    }

    const directCurrentValue = Number(selectedAsset.currentValue);
    return Number.isFinite(directCurrentValue) ? directCurrentValue : null;
  }, [resolvedCurrentPrice, selectedAsset]);

  const quoteVsAverage = useMemo(() => {
    if (resolvedCurrentPrice === null || resolvedAverageCost === null) return null;
    return resolvedCurrentPrice - resolvedAverageCost;
  }, [resolvedAverageCost, resolvedCurrentPrice]);

  const balanceMinusInvested = useMemo(() => {
    if (!selectedAsset || resolvedCurrentValue === null) return null;
    const investedAmount = Number(selectedAsset.investedAmount);
    if (!Number.isFinite(investedAmount)) return null;
    return resolvedCurrentValue - investedAmount;
  }, [resolvedCurrentValue, selectedAsset]);

  const positionStatus = useMemo(() => {
    if (balanceMinusInvested === null) return null;
    if (Math.abs(balanceMinusInvested) <= Number.EPSILON) return 'neutral';
    return balanceMinusInvested > 0 ? 'positive' : 'negative';
  }, [balanceMinusInvested]);

  const assetTradeHistoryRows = useMemo<AssetTradeHistoryRow[]>(() => {
    if (!selectedAsset) return [];

    return transactions
      .filter((transaction) => {
        if (transaction.assetId !== selectedAsset.assetId) return false;
        const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
        if (normalizedStatus !== 'confirmed') return false;
        if (shouldIgnoreConsolidatedTrade(transaction)) return false;
        const normalizedType = transaction.type?.toLowerCase() || '';
        return normalizedType === 'buy' || normalizedType === 'sell';
      })
      .map((transaction) => ({
        transId: transaction.transId,
        date: transaction.date || transaction.createdAt?.slice(0, 10) || '',
        type: transaction.type.toLowerCase() as 'buy' | 'sell',
        quantity: Number(transaction.quantity || 0),
        price: Number(transaction.price || 0),
        amount: Number(transaction.amount || 0),
        currency: transaction.currency || selectedAsset.currency || 'BRL',
        source: summarizeSourceValue(transaction.sourceDocId || transaction.institution) || null,
      }))
      .filter((row) => row.date && Number.isFinite(row.price))
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
  }, [selectedAsset, shouldIgnoreConsolidatedTrade, transactions]);

  const assetTradeHistoryChart = useMemo(() => {
    const rows = assetTradeHistoryRows.filter((row) => Number.isFinite(row.price));
    if (!rows.length) return null;

    const chartPadding = { top: 16, right: 20, bottom: 28, left: 20 };
    const chartWidth = HISTORY_CHART_WIDTH - chartPadding.left - chartPadding.right;
    const chartHeight = HISTORY_CHART_HEIGHT - chartPadding.top - chartPadding.bottom;
    const prices = rows.map((point) => point.price);
    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    const spread = Math.max(maxPrice - minPrice, 1);
    const paddedMin = minPrice - spread * 0.08;
    const paddedMax = maxPrice + spread * 0.08;
    const yBase = chartPadding.top + chartHeight;

    const xFor = (index: number) =>
      rows.length === 1
        ? chartPadding.left + chartWidth / 2
        : chartPadding.left + (index / (rows.length - 1)) * chartWidth;
    const yFor = (price: number) =>
      chartPadding.top + (1 - (price - paddedMin) / (paddedMax - paddedMin)) * chartHeight;

    const points: AssetTradeHistoryPoint[] = rows.map((point, index) => ({
      ...point,
      x: xFor(index),
      y: yFor(point.price),
      index,
    }));

    const polyline = points
      .map((point, index) => {
        const x = point.x.toFixed(2);
        const y = point.y.toFixed(2);
        return `${index === 0 ? 'M' : 'L'} ${x} ${y}`;
      })
      .join(' ');
    const areaPath = `${polyline} L ${points[points.length - 1].x.toFixed(2)} ${yBase.toFixed(2)} L ${points[0].x.toFixed(2)} ${yBase.toFixed(2)} Z`;

    return {
      points,
      polyline,
      areaPath,
      firstDate: rows[0].date,
      lastDate: rows[rows.length - 1].date,
      lastPrice: rows[rows.length - 1].price,
      minPrice: paddedMin,
      maxPrice: paddedMax,
      padding: chartPadding,
      yBase,
    };
  }, [assetTradeHistoryRows]);

  const assetTradeHistoryStats = useMemo(() => {
    const buys = assetTradeHistoryRows.filter((row) => row.type === 'buy');
    const sells = assetTradeHistoryRows.filter((row) => row.type === 'sell');

    const weightedAveragePrice = (rows: AssetTradeHistoryRow[]) => {
      const totalQuantity = rows.reduce((sum, row) => sum + row.quantity, 0);
      if (Math.abs(totalQuantity) <= Number.EPSILON) return null;
      const totalAmount = rows.reduce((sum, row) => sum + (row.price * row.quantity), 0);
      return totalAmount / totalQuantity;
    };

    return {
      trades: assetTradeHistoryRows.length,
      buys: buys.length,
      sells: sells.length,
      avgBuyPrice: weightedAveragePrice(buys),
      avgSellPrice: weightedAveragePrice(sells),
    };
  }, [assetTradeHistoryRows]);

  const tooltipStyle = useMemo(() => {
    if (!selectedHistoryPoint) return null;
    const isRightSide = selectedHistoryPoint.x > HISTORY_CHART_WIDTH * 0.68;
    const isNearTop = selectedHistoryPoint.y < HISTORY_CHART_HEIGHT * 0.25;

    return {
      left: `${(selectedHistoryPoint.x / HISTORY_CHART_WIDTH) * 100}%`,
      top: `${(selectedHistoryPoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
      transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
    };
  }, [selectedHistoryPoint]);

  const overviewFields = useMemo(() => {
    if (!selectedAsset) return [];

    return [
      { key: 'ticker', label: t('assets.modal.fields.ticker'), value: formatDetailValue(selectedAsset.ticker) },
      { key: 'name', label: t('assets.modal.fields.name'), value: formatDetailValue(selectedAsset.name) },
      { key: 'quantity', label: t('assets.modal.fields.quantity'), value: formatAssetQuantity(selectedAsset.quantity) },
      {
        key: 'investedAmount',
        label: t('assets.modal.fields.investedAmount'),
        value: formatCurrency(selectedAsset.investedAmount, selectedAsset.currency || 'BRL', numberLocale),
      },
      {
        key: 'averagePrice',
        label: t('assets.modal.fields.averagePrice'),
        value: resolvedAverageCost !== null
          ? formatCurrency(resolvedAverageCost, selectedAsset.currency || 'BRL', numberLocale)
          : formatDetailValue(resolvedAverageCost),
      },
      {
        key: 'currentPrice',
        label: t('assets.modal.fields.currentPrice'),
        value: resolvedCurrentPrice !== null
          ? formatCurrency(resolvedCurrentPrice, selectedAsset.currency || 'BRL', numberLocale)
          : formatDetailValue(selectedAsset.currentPrice),
      },
      {
        key: 'currentValue',
        label: t('assets.modal.fields.currentValue'),
        value: resolvedCurrentValue !== null
          ? formatCurrency(resolvedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)
          : formatDetailValue(selectedAsset.currentValue),
      },
    ];
  }, [formatAssetQuantity, formatDetailValue, numberLocale, resolvedAverageCost, resolvedCurrentPrice, resolvedCurrentValue, selectedAsset, t]);

  const marketFields = useMemo(() => {
    if (!selectedAsset) return [];

    return [
      {
        key: 'assetClass',
        label: t('assets.modal.fields.class'),
        value: t(`assets.classes.${selectedAsset.assetClass}`, { defaultValue: selectedAsset.assetClass }),
      },
      {
        key: 'status',
        label: t('assets.modal.fields.status'),
        value: t(`assets.statuses.${selectedAsset.status?.toLowerCase() || 'unknown'}`, {
          defaultValue: selectedAsset.status || t('assets.statuses.unknown'),
        }),
      },
      {
        key: 'country',
        label: t('assets.modal.fields.country'),
        value: formatCountryDetail(selectedAsset.country),
      },
      {
        key: 'currency',
        label: t('assets.modal.fields.currency'),
        value: formatDetailValue(selectedAsset.currency),
      },
      {
        key: 'quoteVsAverage',
        label: t('assets.modal.fields.quoteVsAverage'),
        value:
          quoteVsAverage !== null
            ? formatSignedCurrency(quoteVsAverage, selectedAsset.currency || 'BRL')
            : formatDetailValue(quoteVsAverage),
      },
      {
        key: 'investedMinusCurrent',
        label: t('assets.modal.fields.investedMinusCurrent'),
        value:
          balanceMinusInvested !== null
            ? formatSignedCurrency(balanceMinusInvested, selectedAsset.currency || 'BRL')
            : formatDetailValue(balanceMinusInvested),
      },
      {
        key: 'positionStatus',
        label: t('assets.modal.fields.positionStatus'),
        value:
          positionStatus
            ? (
              <span className={`assets-page__position assets-page__position--${positionStatus}`}>
                {t(`assets.modal.position.${positionStatus}`)}
              </span>
            )
            : formatDetailValue(positionStatus),
      },
    ];
  }, [balanceMinusInvested, formatCountryDetail, formatDetailValue, formatSignedCurrency, positionStatus, quoteVsAverage, selectedAsset, t]);

  return (
    <Layout>
      <div className="asset-details-page">
        <div className="asset-details-page__header">
          <div>
            <h1 className="asset-details-page__title">
              {t('assets.detail.title', { defaultValue: 'Asset Details' })}
            </h1>
            <p className="asset-details-page__subtitle">
              {selectedAsset
                ? `${selectedAsset.ticker} • ${selectedAsset.name}`
                : t('assets.detail.subtitle', { defaultValue: 'Complete detail view for the selected asset.' })}
            </p>
          </div>
          <button
            type="button"
            className="asset-details-page__back"
            onClick={() => {
              const query = portfolioId ? `?portfolioId=${encodeURIComponent(portfolioId)}` : '';
              navigate(`/assets${query}`);
            }}
          >
            {t('assets.detail.backToAssets', { defaultValue: 'Back to assets' })}
          </button>
        </div>

        {loading ? (
          <div className="asset-details-page__state">{t('common.loading')}</div>
        ) : null}

        {!loading && !selectedAsset ? (
          <div className="asset-details-page__state">
            {t('assets.detail.notFound', { defaultValue: 'Asset not found for the selected portfolio.' })}
          </div>
        ) : null}

        {!loading && selectedAsset ? (
          <>
            <div className="asset-details-page__grid">
              <section className="asset-details-page__card">
                <h2>{t('assets.modal.sections.overview')}</h2>
                <dl>
                  {overviewFields.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>

              <section className="asset-details-page__card">
                <h2>{t('assets.modal.sections.market')}</h2>
                <dl>
                  {marketFields.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>
            </div>

            <section className="asset-details-page__card asset-details-page__card--full">
              <div className="assets-page__history">
                <h3>{t('assets.modal.history.title')}</h3>

                {assetTradeHistoryRows.length === 0 ? (
                  <p className="assets-page__history-empty">{t('assets.modal.history.empty')}</p>
                ) : (
                  <>
                    <div className="assets-page__history-stats">
                      <span>{t('assets.modal.history.totalTrades')}: <strong>{assetTradeHistoryStats.trades}</strong></span>
                      <span>{t('assets.modal.history.buys')}: <strong>{assetTradeHistoryStats.buys}</strong></span>
                      <span>{t('assets.modal.history.sells')}: <strong>{assetTradeHistoryStats.sells}</strong></span>
                      <span>
                        {t('assets.modal.history.avgBuyPrice')}: <strong>
                          {assetTradeHistoryStats.avgBuyPrice !== null
                            ? formatCurrency(assetTradeHistoryStats.avgBuyPrice, selectedAsset.currency || 'BRL', numberLocale)
                            : '-'}
                        </strong>
                      </span>
                      <span>
                        {t('assets.modal.history.avgSellPrice')}: <strong>
                          {assetTradeHistoryStats.avgSellPrice !== null
                            ? formatCurrency(assetTradeHistoryStats.avgSellPrice, selectedAsset.currency || 'BRL', numberLocale)
                            : '-'}
                        </strong>
                      </span>
                    </div>

                    <div className="assets-page__history-chart">
                      <svg
                        viewBox={`0 0 ${HISTORY_CHART_WIDTH} ${HISTORY_CHART_HEIGHT}`}
                        role="img"
                        aria-label={t('assets.modal.history.chart')}
                        onClick={() => setSelectedHistoryPoint(null)}
                      >
                        <defs>
                          <linearGradient id="asset-page-history-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                            <stop offset="0%" stopColor="rgba(99, 102, 241, 0.42)" />
                            <stop offset="100%" stopColor="rgba(99, 102, 241, 0.02)" />
                          </linearGradient>
                        </defs>
                        {assetTradeHistoryChart ? (
                          <>
                            <line
                              className="assets-page__history-grid"
                              x1={assetTradeHistoryChart.padding.left}
                              x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                              y1={assetTradeHistoryChart.padding.top}
                              y2={assetTradeHistoryChart.padding.top}
                            />
                            <line
                              className="assets-page__history-grid"
                              x1={assetTradeHistoryChart.padding.left}
                              x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                              y1={assetTradeHistoryChart.padding.top + ((assetTradeHistoryChart.yBase - assetTradeHistoryChart.padding.top) / 2)}
                              y2={assetTradeHistoryChart.padding.top + ((assetTradeHistoryChart.yBase - assetTradeHistoryChart.padding.top) / 2)}
                            />
                            <line
                              className="assets-page__history-grid"
                              x1={assetTradeHistoryChart.padding.left}
                              x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                              y1={assetTradeHistoryChart.yBase}
                              y2={assetTradeHistoryChart.yBase}
                            />
                            <path className="assets-page__history-area" d={assetTradeHistoryChart.areaPath} fill="url(#asset-page-history-gradient)" />
                            <path className="assets-page__history-line" d={assetTradeHistoryChart.polyline} />
                            {assetTradeHistoryChart.points.map((point) => (
                              <circle
                                key={`${point.transId}-${point.date}-${point.price}-${point.index}`}
                                className={`assets-page__history-point assets-page__history-point--${point.type}`}
                                cx={point.x}
                                cy={point.y}
                                r={selectedHistoryPoint?.transId === point.transId && selectedHistoryPoint?.index === point.index ? 6 : 4}
                                role="button"
                                tabIndex={0}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setSelectedHistoryPoint(point);
                                }}
                                onKeyDown={(event) => {
                                  if (event.key === 'Enter' || event.key === ' ') {
                                    event.preventDefault();
                                    setSelectedHistoryPoint(point);
                                  }
                                }}
                              >
                                <title>
                                  {`${formatDate(point.date, numberLocale)} | ${t(`transactions.types.${point.type}`, { defaultValue: point.type })} | ${formatCurrency(point.price, point.currency, numberLocale)}`}
                                </title>
                              </circle>
                            ))}
                          </>
                        ) : null}
                      </svg>
                      {selectedHistoryPoint && tooltipStyle ? (
                        <div className="assets-page__history-tooltip" style={tooltipStyle}>
                          <div className="assets-page__history-tooltip-header">
                            <span className={`assets-page__history-type assets-page__history-type--${selectedHistoryPoint.type}`}>
                              {t(`transactions.types.${selectedHistoryPoint.type}`, { defaultValue: selectedHistoryPoint.type })}
                            </span>
                            <strong>{formatDate(selectedHistoryPoint.date, numberLocale)}</strong>
                          </div>
                          <div className="assets-page__history-tooltip-grid">
                            <span>{t('assets.modal.history.quantity')}</span>
                            <strong>{formatAssetQuantity(selectedHistoryPoint.quantity)}</strong>
                            <span>{t('assets.modal.history.price')}</span>
                            <strong>{formatCurrency(selectedHistoryPoint.price, selectedHistoryPoint.currency, numberLocale)}</strong>
                            <span>{t('assets.modal.history.amount')}</span>
                            <strong>{formatCurrency(selectedHistoryPoint.amount, selectedHistoryPoint.currency, numberLocale)}</strong>
                            <span>{t('assets.modal.history.source')}</span>
                            <strong>{selectedHistoryPoint.source || '-'}</strong>
                          </div>
                        </div>
                      ) : null}
                      {assetTradeHistoryChart ? (
                        <div className="assets-page__history-scale">
                          <span>{formatDate(assetTradeHistoryChart.firstDate, numberLocale)}</span>
                          <span>{formatCurrency(assetTradeHistoryChart.minPrice, selectedAsset.currency || 'BRL', numberLocale)}</span>
                          <span>{formatCurrency(assetTradeHistoryChart.maxPrice, selectedAsset.currency || 'BRL', numberLocale)}</span>
                          <span>{formatDate(assetTradeHistoryChart.lastDate, numberLocale)}</span>
                        </div>
                      ) : null}
                      {assetTradeHistoryChart ? (
                        <div className="assets-page__history-meta">
                          <span>{t('assets.modal.history.lastPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.lastPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                          <span>{t('assets.modal.history.minPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.minPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                          <span>{t('assets.modal.history.maxPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.maxPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                        </div>
                      ) : null}
                    </div>

                    <div className="assets-page__history-table-wrap">
                      <table className="assets-page__history-table">
                        <thead>
                          <tr>
                            <th>{t('assets.modal.history.date')}</th>
                            <th>{t('assets.modal.history.type')}</th>
                            <th>{t('assets.modal.history.quantity')}</th>
                            <th>{t('assets.modal.history.price')}</th>
                            <th>{t('assets.modal.history.amount')}</th>
                            <th>{t('assets.modal.history.source')}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {[...assetTradeHistoryRows].reverse().map((row) => (
                            <tr key={`${row.transId}-${row.date}-${row.type}`}>
                              <td>{formatDate(row.date, numberLocale)}</td>
                              <td>
                                <span className={`assets-page__history-type assets-page__history-type--${row.type}`}>
                                  {t(`transactions.types.${row.type}`, { defaultValue: row.type })}
                                </span>
                              </td>
                              <td>{formatAssetQuantity(row.quantity)}</td>
                              <td>{formatCurrency(row.price, row.currency, numberLocale)}</td>
                              <td>{formatCurrency(row.amount, row.currency, numberLocale)}</td>
                              <td>{row.source || '-'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </div>
            </section>
          </>
        ) : null}
      </div>
    </Layout>
  );
};

export default AssetDetailsPage;
