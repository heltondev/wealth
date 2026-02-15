import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
import FormModal from '../components/FormModal';
import ExpandableText from '../components/ExpandableText';
import {
  api,
  type Asset,
  type Portfolio,
  type Transaction,
  type DropdownConfigMap,
} from '../services/api';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import { useToast } from '../context/ToastContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './AssetsPage.scss';

type AssetRow = Asset & { quantity: number; source: string | null; investedAmount: number };
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
const DEFAULT_ITEMS_PER_PAGE = 10;
const HISTORY_CHART_WIDTH = 860;
const HISTORY_CHART_HEIGHT = 220;

const toPageSizeOptions = (options: { value: string }[]): number[] => {
  const values = new Set<number>();

  for (const option of options) {
    const numeric = Number(option.value);
    if (!Number.isFinite(numeric) || numeric <= 0) continue;
    values.add(Math.round(numeric));
  }

  return Array.from(values).sort((left, right) => left - right);
};

const ensureSelectedValue = (current: string, options: { value: string }[]): string => {
  if (options.some((option) => option.value === current)) return current;
  return options[0]?.value || '';
};

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

const AssetsPage = () => {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const portfolioIdFromQuery = searchParams.get('portfolioId')?.trim() || '';
  const { showToast } = useToast();
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>('');
  const [assets, setAssets] = useState<Asset[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [itemsPerPage, setItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);
  const [selectedAsset, setSelectedAsset] = useState<AssetRow | null>(null);
  const [currentQuotesByAssetId, setCurrentQuotesByAssetId] = useState<Record<string, number | null>>({});
  const [averageCostByAssetId, setAverageCostByAssetId] = useState<Record<string, number | null>>({});
  const [portfolioMarketValueByAssetId, setPortfolioMarketValueByAssetId] = useState<Record<string, number | null>>({});
  const [selectedHistoryPoint, setSelectedHistoryPoint] = useState<AssetTradeHistoryPoint | null>(null);
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [form, setForm] = useState<{
    ticker: string;
    name: string;
    assetClass: string;
    country: string;
    currency: string;
  }>({
    ticker: '',
    name: '',
    assetClass: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.assetClass')[0]?.value || 'stock',
    country: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.country')[0]?.value || 'BR',
    currency: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.currency')[0]?.value || 'BRL',
  });

  useEffect(() => {
    Promise.all([api.getPortfolios(), api.getDropdownSettings()])
      .then(([portfolioItems, dropdownSettings]) => {
        setPortfolios(portfolioItems);
        if (portfolioItems.length > 0) {
          const requestedPortfolio = portfolioItems.find((item) => item.portfolioId === portfolioIdFromQuery);
          setSelectedPortfolio(requestedPortfolio?.portfolioId || portfolioItems[0].portfolioId);
        }
        setDropdownConfig(normalizeDropdownConfig(dropdownSettings.dropdowns));
      })
      .catch(() => {
        setPortfolios([]);
        setDropdownConfig(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      })
      .finally(() => setLoading(false));
  }, [portfolioIdFromQuery]);

  useEffect(() => {
    if (!selectedPortfolio) return;
    setLoading(true);
    Promise.all([api.getAssets(selectedPortfolio), api.getTransactions(selectedPortfolio)])
      .then(([assetItems, transactionItems]) => {
        setAssets(assetItems);
        setTransactions(transactionItems);
      })
      .catch(() => {
        setAssets([]);
        setTransactions([]);
      })
      .finally(() => setLoading(false));
  }, [selectedPortfolio]);

  useEffect(() => {
    setCurrentQuotesByAssetId({});
    setAverageCostByAssetId({});
    setPortfolioMarketValueByAssetId({});
    setSelectedHistoryPoint(null);
  }, [selectedPortfolio]);

  useEffect(() => {
    if (!selectedPortfolio) return;
    let cancelled = false;

    api.getPortfolioMetrics(selectedPortfolio)
      .then((payload) => {
        if (cancelled) return;
        const metrics = Array.isArray((payload as { assets?: unknown[] }).assets)
          ? (payload as { assets: unknown[] }).assets
          : [];

        const nextMarketValues: Record<string, number | null> = {};
        const nextAverageCosts: Record<string, number | null> = {};
        const nextCurrentQuotes: Record<string, number | null> = {};
        for (const item of metrics) {
          const metric = item as Record<string, unknown>;
          const assetId = String(metric.assetId || '');
          if (!assetId) continue;

          const marketValue = Number(metric.market_value);
          const averageCost = Number(metric.average_cost);
          const currentPrice = Number(metric.current_price);
          const quantityCurrent = Number(metric.quantity_current);
          const resolvedMarketValue =
            Number.isFinite(marketValue)
              ? marketValue
              : (Number.isFinite(currentPrice) && Number.isFinite(quantityCurrent))
                ? currentPrice * quantityCurrent
                : null;

          if (Number.isFinite(resolvedMarketValue)) {
            nextMarketValues[assetId] = resolvedMarketValue;
          }
          if (Number.isFinite(averageCost)) {
            nextAverageCosts[assetId] = averageCost;
          }
          if (Number.isFinite(currentPrice)) {
            nextCurrentQuotes[assetId] = currentPrice;
          }
        }

        setPortfolioMarketValueByAssetId((previous) => ({ ...previous, ...nextMarketValues }));
        setAverageCostByAssetId((previous) => ({ ...previous, ...nextAverageCosts }));
        setCurrentQuotesByAssetId((previous) => ({ ...previous, ...nextCurrentQuotes }));
      })
      .catch(() => {
        if (cancelled) return;
        // Keep current values on fetch errors; avoid replacing with zeros.
      });

    return () => {
      cancelled = true;
    };
  }, [selectedPortfolio, assets.length, transactions.length]);

  const assetClassOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.form.assetClass');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.assetClass');
  }, [dropdownConfig]);

  const countryOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.form.country');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.country');
  }, [dropdownConfig]);

  const currencyOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.form.currency');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.form.currency');
  }, [dropdownConfig]);

  const statusFilterOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'assets.filters.status');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'assets.filters.status');
  }, [dropdownConfig]);

  const pageSizeOptions = useMemo(() => {
    const configuredOptions = toPageSizeOptions(
      getDropdownOptions(dropdownConfig, 'tables.pagination.itemsPerPage')
    );
    if (configuredOptions.length > 0) return configuredOptions;
    return toPageSizeOptions(
      getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'tables.pagination.itemsPerPage')
    );
  }, [dropdownConfig]);

  useEffect(() => {
    setForm((previous) => {
      const next = {
        ...previous,
        assetClass: ensureSelectedValue(previous.assetClass, assetClassOptions),
        country: ensureSelectedValue(previous.country, countryOptions),
        currency: ensureSelectedValue(previous.currency, currencyOptions),
      };
      if (
        next.assetClass === previous.assetClass
        && next.country === previous.country
        && next.currency === previous.currency
      ) {
        return previous;
      }
      return next;
    });
  }, [assetClassOptions, countryOptions, currencyOptions]);

  useEffect(() => {
    if (statusFilterOptions.some((option) => option.value === statusFilter)) return;
    setStatusFilter(statusFilterOptions[0]?.value || 'all');
  }, [statusFilter, statusFilterOptions]);

  useEffect(() => {
    if (pageSizeOptions.includes(itemsPerPage)) return;
    setItemsPerPage(pageSizeOptions[0] || DEFAULT_ITEMS_PER_PAGE);
  }, [itemsPerPage, pageSizeOptions]);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!selectedPortfolio) return;

    try {
      const newAsset = await api.createAsset(selectedPortfolio, form);
      setAssets((previous) => [...previous, newAsset]);
      setShowModal(false);
      setForm({
        ticker: '',
        name: '',
        assetClass: assetClassOptions[0]?.value || '',
        country: countryOptions[0]?.value || '',
        currency: currencyOptions[0]?.value || '',
      });
      showToast('Asset added', 'success');
    } catch {
      showToast('Failed to add asset', 'error');
    }
  };

  const handleDelete = async (assetId: string) => {
    if (!selectedPortfolio) return;
    try {
      await api.deleteAsset(selectedPortfolio, assetId);
      setAssets((previous) => previous.filter((asset) => asset.assetId !== assetId));
      showToast('Asset deleted', 'success');
    } catch {
      showToast('Failed to delete asset', 'error');
    }
  };

  const formatCountryFlag = useCallback((country: string) =>
    COUNTRY_FLAG_MAP[country] || '🏳️', []);

  const formatCountryDetail = useCallback((country: string) =>
    `${formatCountryFlag(country)} ${COUNTRY_NAME_MAP[country] || country}`, [formatCountryFlag]);

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('assets.modal.noValue');
    return String(value);
  }, [t]);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

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

  const assetRows = useMemo<AssetRow[]>(() => {
    return assets.map((asset) => ({
      ...asset,
      quantity: Number.isFinite(Number(asset.quantity))
        ? Number(asset.quantity)
        : (assetQuantitiesById[asset.assetId] || 0),
      source: (() => {
        const labels = new Set<string>();
        const assetSource = summarizeSourceValue(asset.source);
        if (assetSource) labels.add(assetSource);
        for (const candidate of (assetSourcesById[asset.assetId] || [])) {
          const label = summarizeSourceValue(candidate);
          if (label) labels.add(label);
        }
        if (labels.size > 0) return Array.from(labels).join(', ');
        return null;
      })(),
      investedAmount: assetInvestedAmountById[asset.assetId] || 0,
    }));
  }, [assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

  const formatPercent = useCallback((ratio: number) => (
    `${(ratio * 100).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}%`
  ), [numberLocale]);

  const resolveAssetCurrentValue = useCallback((asset: AssetRow): number | null => {
    const metricCurrentValue = portfolioMarketValueByAssetId[asset.assetId];
    if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
      return metricCurrentValue;
    }

    const quantity = Number(asset.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const cachedCurrentPrice = currentQuotesByAssetId[asset.assetId];
    const directCurrentPrice = Number(asset.currentPrice);
    const resolvedCurrentPrice =
      (typeof cachedCurrentPrice === 'number'
        && Number.isFinite(cachedCurrentPrice)
        && (!hasOpenPosition || Math.abs(cachedCurrentPrice) > Number.EPSILON))
        ? cachedCurrentPrice
        : (Number.isFinite(directCurrentPrice)
          && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON))
          ? directCurrentPrice
          : null;

    if (resolvedCurrentPrice !== null && Number.isFinite(quantity)) {
      return resolvedCurrentPrice * quantity;
    }

    const directCurrentValue = Number(asset.currentValue);
    if (
      Number.isFinite(directCurrentValue)
      && (!hasOpenPosition || Math.abs(directCurrentValue) > Number.EPSILON)
    ) {
      return directCurrentValue;
    }

    return null;
  }, [currentQuotesByAssetId, portfolioMarketValueByAssetId]);

  const resolveAssetAverageCost = useCallback((asset: AssetRow): number | null => {
    const cachedAverageCost = averageCostByAssetId[asset.assetId];
    if (typeof cachedAverageCost === 'number' && Number.isFinite(cachedAverageCost)) {
      return cachedAverageCost;
    }

    const quantity = Number(asset.quantity);
    const investedAmount = Number(asset.investedAmount);
    if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return investedAmount / quantity;
  }, [averageCostByAssetId]);

  const currentValueByAssetId = useMemo(() => {
    const values: Record<string, number | null> = {};
    for (const row of assetRows) {
      const currentValue = resolveAssetCurrentValue(row);
      values[row.assetId] = Number.isFinite(currentValue) ? Number(currentValue) : null;
    }
    return values;
  }, [assetRows, resolveAssetCurrentValue]);

  const portfolioCurrentTotal = useMemo<number>(() => (
    Object.values(currentValueByAssetId).reduce<number>((sum, value) => (
      typeof value === 'number' && Number.isFinite(value) ? sum + value : sum
    ), 0)
  ), [currentValueByAssetId]);

  useEffect(() => {
    if (!selectedPortfolio || assetRows.length === 0) return;

    const pendingRows = assetRows.filter(
      (row) => !Object.prototype.hasOwnProperty.call(averageCostByAssetId, row.assetId)
    );
    if (pendingRows.length === 0) return;

    let cancelled = false;
    const run = async () => {
      const nextValues: Record<string, number | null> = {};
      const nextMarketValues: Record<string, number | null> = {};
      const nextQuotes: Record<string, number | null> = {};

      for (const row of pendingRows) {
        try {
          const payload = await api.getAverageCost(selectedPortfolio, row.ticker);
          const averageCost = Number((payload as { average_cost?: unknown }).average_cost);
          const marketValue = Number((payload as { market_value?: unknown }).market_value);
          const currentPrice = Number((payload as { current_price?: unknown }).current_price);
          const quantity = Number(row.quantity);
          const resolvedMarketValue =
            Number.isFinite(marketValue)
              ? marketValue
              : (Number.isFinite(currentPrice) && Number.isFinite(quantity))
                ? currentPrice * quantity
                : null;
          nextValues[row.assetId] = Number.isFinite(averageCost) ? averageCost : null;
          nextMarketValues[row.assetId] = Number.isFinite(resolvedMarketValue) ? resolvedMarketValue : null;
          nextQuotes[row.assetId] = Number.isFinite(currentPrice) ? currentPrice : null;
        } catch {
          nextValues[row.assetId] = null;
          nextMarketValues[row.assetId] = null;
          nextQuotes[row.assetId] = null;
        }

        if (cancelled) return;
      }

      if (cancelled) return;
      setAverageCostByAssetId((previous) => ({ ...previous, ...nextValues }));
      setPortfolioMarketValueByAssetId((previous) => ({ ...previous, ...nextMarketValues }));
      setCurrentQuotesByAssetId((previous) => ({ ...previous, ...nextQuotes }));
    };

    void run();
    return () => {
      cancelled = true;
    };
  }, [assetRows, averageCostByAssetId, selectedPortfolio]);

  const columns: DataTableColumn<AssetRow>[] = [
    {
      key: 'ticker',
      label: t('assets.ticker'),
      sortable: true,
      sortValue: (asset) => asset.ticker,
      cellClassName: 'assets-page__cell--ticker',
      render: (asset) => asset.ticker,
    },
    {
      key: 'name',
      label: t('assets.name'),
      sortable: true,
      sortValue: (asset) => asset.name,
      cellClassName: 'assets-page__cell--name',
      render: (asset) => (
        <span className="assets-page__name-ellipsis" title={asset.name}>
          {asset.name}
        </span>
      ),
    },
    {
      key: 'quantity',
      label: t('assets.quantity'),
      sortable: true,
      sortValue: (asset) => asset.quantity,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => formatAssetQuantity(asset.quantity),
    },
    {
      key: 'averagePrice',
      label: t('assets.averagePrice'),
      sortable: true,
      sortValue: (asset) => resolveAssetAverageCost(asset) ?? Number.NEGATIVE_INFINITY,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => {
        const averageCost = resolveAssetAverageCost(asset);
        if (averageCost === null) return t('assets.modal.noValue');
        return formatCurrency(averageCost, asset.currency || 'BRL', numberLocale);
      },
    },
    {
      key: 'balance',
      label: t('assets.balance'),
      sortable: true,
      sortValue: (asset) => currentValueByAssetId[asset.assetId] ?? Number.NEGATIVE_INFINITY,
      cellClassName: 'assets-page__cell--numeric',
      render: (asset) => {
        const currentValue = currentValueByAssetId[asset.assetId];
        if (typeof currentValue !== 'number' || !Number.isFinite(currentValue)) return t('assets.modal.noValue');
        return formatCurrency(currentValue, asset.currency || 'BRL', numberLocale);
      },
    },
    {
      key: 'portfolioWeight',
      label: t('assets.portfolioWeight'),
      sortable: true,
      sortValue: (asset) => (
        portfolioCurrentTotal > 0
          ? ((currentValueByAssetId[asset.assetId] || 0) / portfolioCurrentTotal)
          : 0
      ),
      cellClassName: 'assets-page__cell--numeric assets-page__cell--weight',
      render: (asset) => {
        const currentValue = currentValueByAssetId[asset.assetId];
        if (typeof currentValue !== 'number' || !Number.isFinite(currentValue)) return t('assets.modal.noValue');
        if (portfolioCurrentTotal <= 0) return t('assets.modal.noValue');
        const ratio = currentValue / portfolioCurrentTotal;
        return formatPercent(ratio);
      },
    },
    {
      key: 'assetClass',
      label: t('assets.class'),
      sortable: true,
      sortValue: (asset) => asset.assetClass,
      render: (asset) => (
        <span className={`badge badge--${asset.assetClass}`}>
          {t(`assets.classes.${asset.assetClass}`)}
        </span>
      ),
    },
    {
      key: 'country',
      label: t('assets.country'),
      sortable: true,
      sortValue: (asset) => asset.country,
      render: (asset) => formatCountryFlag(asset.country),
    },
    {
      key: 'status',
      label: t('assets.status'),
      sortable: true,
      sortValue: (asset) => asset.status,
      render: (asset) =>
        t(`assets.statuses.${asset.status?.toLowerCase() || 'unknown'}`, {
          defaultValue: asset.status || t('assets.statuses.unknown'),
        }),
    },
    {
      key: 'actions',
      label: t('assets.actions'),
      render: (asset) => (
        <button
          type="button"
          className="assets-page__delete"
          onClick={(event) => {
            event.stopPropagation();
            handleDelete(asset.assetId);
          }}
        >
          {t('common.delete')}
        </button>
      ),
    },
  ];

  const filters: DataTableFilter<AssetRow>[] = [
    {
      key: 'status',
      label: t('assets.filters.status.label'),
      value: statusFilter,
      options: statusFilterOptions.map((option) => ({
        value: option.value,
        label: t(`assets.filters.status.${option.value}`, { defaultValue: option.label }),
      })),
      onChange: setStatusFilter,
      matches: (asset, filterValue) =>
        filterValue === 'all' || (asset.status?.toLowerCase() || '') === filterValue,
    },
  ];

  const assetDetailsSections = useMemo<RecordDetailsSection[]>(() => {
    if (!selectedAsset) return [];

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    const cachedCurrentPrice = currentQuotesByAssetId[selectedAsset.assetId];
    const derivedCurrentPrice = (() => {
      const currentValue = Number(selectedAsset.currentValue);
      const quantity = Number(selectedAsset.quantity);
      if (!Number.isFinite(currentValue) || !Number.isFinite(quantity)) return null;
      if (Math.abs(quantity) <= Number.EPSILON) return null;
      return currentValue / quantity;
    })();
    const resolvedCurrentPrice =
      Number.isFinite(directCurrentPrice)
        ? directCurrentPrice
        : (typeof cachedCurrentPrice === 'number' && Number.isFinite(cachedCurrentPrice))
          ? cachedCurrentPrice
          : derivedCurrentPrice;
    const resolvedCurrentValue = (() => {
      const metricCurrentValue = portfolioMarketValueByAssetId[selectedAsset.assetId];
      if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
        return metricCurrentValue;
      }

      const quantity = Number(selectedAsset.quantity);
      if (resolvedCurrentPrice !== null && Number.isFinite(quantity)) {
        return quantity * resolvedCurrentPrice;
      }

      const directCurrentValue = Number(selectedAsset.currentValue);
      return Number.isFinite(directCurrentValue) ? directCurrentValue : null;
    })();
    const resolvedAverageCost = (() => {
      const cachedAverageCost = averageCostByAssetId[selectedAsset.assetId];
      if (typeof cachedAverageCost === 'number' && Number.isFinite(cachedAverageCost)) {
        return cachedAverageCost;
      }

      const quantity = Number(selectedAsset.quantity);
      const investedAmount = Number(selectedAsset.investedAmount);
      if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
      if (Math.abs(quantity) <= Number.EPSILON) return null;
      return investedAmount / quantity;
    })();
    const quoteVsAverage = (() => {
      if (resolvedCurrentPrice === null || resolvedAverageCost === null) return null;
      return resolvedCurrentPrice - resolvedAverageCost;
    })();
    const balanceMinusInvested = (() => {
      if (resolvedCurrentValue === null) return null;
      const investedAmount = Number(selectedAsset.investedAmount);
      if (!Number.isFinite(investedAmount)) return null;
      return resolvedCurrentValue - investedAmount;
    })();
    const positionStatus = (() => {
      if (balanceMinusInvested === null) return null;
      if (Math.abs(balanceMinusInvested) <= Number.EPSILON) return 'neutral';
      return balanceMinusInvested > 0 ? 'positive' : 'negative';
    })();

    return [
      {
        key: 'overview',
        title: t('assets.modal.sections.overview'),
        columns: 2,
        fields: [
          { key: 'ticker', label: t('assets.modal.fields.ticker'), value: formatDetailValue(selectedAsset.ticker) },
          {
            key: 'name',
            label: t('assets.modal.fields.name'),
            value: selectedAsset.name
              ? (
                <ExpandableText
                  text={selectedAsset.name}
                  maxLines={2}
                  expandLabel={t('assets.modal.fields.nameExpandHint')}
                  collapseLabel={t('assets.modal.fields.nameCollapseHint')}
                />
              )
              : formatDetailValue(selectedAsset.name),
          },
          {
            key: 'quantity',
            label: t('assets.modal.fields.quantity'),
            value: formatAssetQuantity(selectedAsset.quantity),
          },
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
        ],
      },
      {
        key: 'market',
        title: t('assets.modal.sections.market'),
        columns: 2,
        fields: [
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
        ],
      },
    ];
  }, [averageCostByAssetId, currentQuotesByAssetId, formatAssetQuantity, formatCountryDetail, formatDetailValue, formatSignedCurrency, numberLocale, portfolioMarketValueByAssetId, selectedAsset, t]);

  const selectedAssetWeightMetrics = useMemo(() => {
    if (!selectedAsset) return null;

    const selectedCurrentValue = currentValueByAssetId[selectedAsset.assetId] || 0;
    const portfolioWeight = portfolioCurrentTotal > 0 ? selectedCurrentValue / portfolioCurrentTotal : 0;
    const classTotal = assetRows
      .filter((row) => row.assetClass === selectedAsset.assetClass)
      .reduce((sum, row) => sum + (currentValueByAssetId[row.assetId] || 0), 0);
    const classWeight = classTotal > 0 ? selectedCurrentValue / classTotal : 0;

    return {
      selectedCurrentValue,
      portfolioTotal: portfolioCurrentTotal,
      portfolioWeight,
      classTotal,
      classWeight,
    };
  }, [assetRows, currentValueByAssetId, portfolioCurrentTotal, selectedAsset]);

  useEffect(() => {
    setSelectedHistoryPoint(null);
  }, [selectedAsset?.assetId]);

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
      firstPrice: rows[0].price,
      lastPrice: rows[rows.length - 1].price,
      midPrice: (paddedMin + paddedMax) / 2,
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

  const assetDetailsExtraContent = useMemo(() => {
    if (!selectedAsset) return null;
    const weightMetrics = selectedAssetWeightMetrics;
    const ringRadius = 44;
    const ringCircumference = 2 * Math.PI * ringRadius;
    const ringOffsetFor = (ratio: number) => ringCircumference * (1 - Math.max(0, Math.min(1, ratio)));

    const chartGradientId = `asset-history-gradient-${selectedAsset.assetId.replace(/[^a-zA-Z0-9_-]/g, '')}`;
    const tooltipStyle = (() => {
      if (!selectedHistoryPoint) return null;
      const isRightSide = selectedHistoryPoint.x > HISTORY_CHART_WIDTH * 0.68;
      const isNearTop = selectedHistoryPoint.y < HISTORY_CHART_HEIGHT * 0.25;

      return {
        left: `${(selectedHistoryPoint.x / HISTORY_CHART_WIDTH) * 100}%`,
        top: `${(selectedHistoryPoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
        transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
      };
    })();

    return (
      <div className="assets-page__history">
        {weightMetrics ? (
          <div className="assets-page__weights">
            <h3>{t('assets.modal.weights.title')}</h3>
            <div className="assets-page__weights-grid">
              <article className="assets-page__weight-card">
                <h4>{t('assets.modal.weights.portfolio')}</h4>
                <div className="assets-page__weight-chart">
                  <svg viewBox="0 0 120 120" aria-hidden="true">
                    <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r={ringRadius} />
                    <circle
                      className="assets-page__weight-ring assets-page__weight-ring--portfolio"
                      cx="60"
                      cy="60"
                      r={ringRadius}
                      strokeDasharray={`${ringCircumference} ${ringCircumference}`}
                      strokeDashoffset={ringOffsetFor(weightMetrics.portfolioWeight)}
                    />
                  </svg>
                  <div className="assets-page__weight-chart-center">
                    <strong>{formatPercent(weightMetrics.portfolioWeight)}</strong>
                  </div>
                </div>
                <div className="assets-page__weight-meta">
                  <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(weightMetrics.selectedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                  <span>{t('assets.modal.weights.portfolioTotal')}: <strong>{formatCurrency(weightMetrics.portfolioTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                </div>
              </article>

              <article className="assets-page__weight-card">
                <h4>{t('assets.modal.weights.class', { className: t(`assets.classes.${selectedAsset.assetClass}`, { defaultValue: selectedAsset.assetClass }) })}</h4>
                <div className="assets-page__weight-chart">
                  <svg viewBox="0 0 120 120" aria-hidden="true">
                    <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r={ringRadius} />
                    <circle
                      className="assets-page__weight-ring assets-page__weight-ring--class"
                      cx="60"
                      cy="60"
                      r={ringRadius}
                      strokeDasharray={`${ringCircumference} ${ringCircumference}`}
                      strokeDashoffset={ringOffsetFor(weightMetrics.classWeight)}
                    />
                  </svg>
                  <div className="assets-page__weight-chart-center">
                    <strong>{formatPercent(weightMetrics.classWeight)}</strong>
                  </div>
                </div>
                <div className="assets-page__weight-meta">
                  <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(weightMetrics.selectedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                  <span>{t('assets.modal.weights.classTotal')}: <strong>{formatCurrency(weightMetrics.classTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                </div>
              </article>
            </div>
          </div>
        ) : null}

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
                  <linearGradient id={chartGradientId} x1="0%" y1="0%" x2="0%" y2="100%">
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
                    <path className="assets-page__history-area" d={assetTradeHistoryChart.areaPath} fill={`url(#${chartGradientId})`} />
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
    );
  }, [assetRows, assetTradeHistoryChart, assetTradeHistoryRows, assetTradeHistoryStats, currentValueByAssetId, formatAssetQuantity, formatPercent, numberLocale, selectedAsset, selectedAssetWeightMetrics, selectedHistoryPoint, t]);

  useEffect(() => {
    if (!selectedAsset || !selectedPortfolio) return;

    if (Object.prototype.hasOwnProperty.call(averageCostByAssetId, selectedAsset.assetId)) return;

    let cancelled = false;
    api.getAverageCost(selectedPortfolio, selectedAsset.ticker)
      .then((payload) => {
        if (cancelled) return;
        const averageCost = Number((payload as { average_cost?: unknown }).average_cost);
        setAverageCostByAssetId((previous) => ({
          ...previous,
          [selectedAsset.assetId]: Number.isFinite(averageCost) ? averageCost : null,
        }));
      })
      .catch(() => {
        if (cancelled) return;
        setAverageCostByAssetId((previous) => ({
          ...previous,
          [selectedAsset.assetId]: null,
        }));
      });

    return () => {
      cancelled = true;
    };
  }, [averageCostByAssetId, selectedAsset, selectedPortfolio]);

  useEffect(() => {
    if (!selectedAsset || !selectedPortfolio) return;

    const directCurrentPrice = Number(selectedAsset.currentPrice);
    if (Number.isFinite(directCurrentPrice)) return;

    if (Object.prototype.hasOwnProperty.call(currentQuotesByAssetId, selectedAsset.assetId)) return;

    let cancelled = false;
    api.getPriceAtDate(selectedPortfolio, selectedAsset.ticker, new Date().toISOString().slice(0, 10))
      .then((payload) => {
        if (cancelled) return;
        const close = Number((payload as { close?: unknown }).close);
        setCurrentQuotesByAssetId((previous) => ({
          ...previous,
          [selectedAsset.assetId]: Number.isFinite(close) ? close : null,
        }));
      })
      .catch(() => {
        if (cancelled) return;
        setCurrentQuotesByAssetId((previous) => ({
          ...previous,
          [selectedAsset.assetId]: null,
        }));
      });

    return () => {
      cancelled = true;
    };
  }, [currentQuotesByAssetId, selectedAsset, selectedPortfolio]);

  return (
    <Layout>
      <div className="assets-page">
        <div className="assets-page__header">
          <h1 className="assets-page__title">{t('assets.title')}</h1>
          <div className="assets-page__controls">
            {portfolios.length > 0 && (
              <select
                className="assets-page__select"
                value={selectedPortfolio}
                onChange={(event) => setSelectedPortfolio(event.target.value)}
              >
                {portfolios.map((portfolio) => (
                  <option key={portfolio.portfolioId} value={portfolio.portfolioId}>
                    {portfolio.name}
                  </option>
                ))}
              </select>
            )}
            <button className="assets-page__add-btn" onClick={() => setShowModal(true)}>
              {t('assets.addAsset')}
            </button>
          </div>
        </div>

        {loading && <p className="assets-page__loading">{t('common.loading')}</p>}

        {!loading && assetRows.length === 0 && (
          <div className="assets-page__empty">
            <p>{t('assets.empty')}</p>
          </div>
        )}

        {!loading && assetRows.length > 0 && (
          <DataTable
            rows={assetRows}
            rowKey={(asset) => asset.assetId}
            columns={columns}
            searchLabel={t('assets.filters.search')}
            searchPlaceholder={t('assets.filters.searchPlaceholder')}
            searchTerm={searchTerm}
            onSearchTermChange={setSearchTerm}
            matchesSearch={(asset, normalizedSearch) =>
              [
                asset.ticker,
                asset.name,
                asset.quantity,
                resolveAssetAverageCost(asset),
                currentValueByAssetId[asset.assetId] || 0,
                portfolioCurrentTotal > 0 ? `${((currentValueByAssetId[asset.assetId] || 0) / portfolioCurrentTotal) * 100}%` : '0%',
                asset.assetClass,
                asset.country,
                asset.currency,
                asset.status,
                asset.source,
              ]
                .join(' ')
                .toLowerCase()
                .includes(normalizedSearch)
            }
            filters={filters}
            itemsPerPage={itemsPerPage}
            onItemsPerPageChange={setItemsPerPage}
            pageSizeOptions={pageSizeOptions}
            emptyLabel={t('assets.emptyFiltered')}
            labels={{
              itemsPerPage: t('assets.pagination.itemsPerPage'),
              prev: t('assets.pagination.prev'),
              next: t('assets.pagination.next'),
              page: (page, total) => t('assets.pagination.page', { page, total }),
              showing: (start, end, total) => t('assets.pagination.showing', { start, end, total }),
            }}
            defaultSort={{ key: 'ticker', direction: 'asc' }}
            onRowClick={setSelectedAsset}
            rowAriaLabel={(asset) => t('assets.modal.openDetails', { ticker: asset.ticker })}
          />
        )}

        <RecordDetailsModal
          open={Boolean(selectedAsset)}
          title={selectedAsset ? `${selectedAsset.ticker} • ${selectedAsset.name}` : t('assets.modal.title')}
          subtitle={t('assets.modal.subtitle')}
          closeLabel={t('assets.modal.close')}
          headerActions={selectedAsset ? (
            <button
              type="button"
              className="record-modal__action"
              onClick={() => {
                const params = new URLSearchParams();
                if (selectedPortfolio) params.set('portfolioId', selectedPortfolio);
                const queryString = params.toString();
                const path = queryString
                  ? `/assets/${selectedAsset.assetId}?${queryString}`
                  : `/assets/${selectedAsset.assetId}`;
                setSelectedAsset(null);
                navigate(path);
              }}
            >
              {t('assets.modal.openPage', { defaultValue: 'Open asset page' })}
            </button>
          ) : null}
          sections={assetDetailsSections}
          extraContent={assetDetailsExtraContent}
          onClose={() => setSelectedAsset(null)}
        />

        <FormModal
          open={showModal}
          title={t('assets.addAsset')}
          closeLabel={t('assets.form.cancel')}
          cancelLabel={t('assets.form.cancel')}
          submitLabel={t('assets.form.submit')}
          onClose={() => setShowModal(false)}
          onSubmit={handleSubmit}
        >
          <div className="form-modal__field">
            <label>{t('assets.form.ticker')}</label>
            <input
              type="text"
              value={form.ticker}
              onChange={(event) => setForm({ ...form, ticker: event.target.value })}
              required
            />
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.name')}</label>
            <input
              type="text"
              value={form.name}
              onChange={(event) => setForm({ ...form, name: event.target.value })}
              required
            />
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.class')}</label>
            <select
              value={form.assetClass}
              onChange={(event) => setForm({ ...form, assetClass: event.target.value })}
            >
              {assetClassOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {t(`assets.classes.${option.value}`, { defaultValue: option.label })}
                </option>
              ))}
            </select>
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.country')}</label>
            <select
              value={form.country}
              onChange={(event) => setForm({ ...form, country: event.target.value })}
            >
              {countryOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
          <div className="form-modal__field">
            <label>{t('assets.form.currency')}</label>
            <select
              value={form.currency}
              onChange={(event) => setForm({ ...form, currency: event.target.value })}
            >
              {currencyOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        </FormModal>
      </div>
    </Layout>
  );
};

export default AssetsPage;
