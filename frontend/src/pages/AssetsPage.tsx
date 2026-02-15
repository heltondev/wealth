import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
import FormModal from '../components/FormModal';
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
import { formatCurrency } from '../utils/formatters';
import './AssetsPage.scss';

type AssetRow = Asset & { quantity: number; source: string | null; investedAmount: number };

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
        if (portfolioItems.length > 0) setSelectedPortfolio(portfolioItems[0].portfolioId);
        setDropdownConfig(normalizeDropdownConfig(dropdownSettings.dropdowns));
      })
      .catch(() => {
        setPortfolios([]);
        setDropdownConfig(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      })
      .finally(() => setLoading(false));
  }, []);

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

  const assetQuantitiesById = useMemo(() => {
    const quantities: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;

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
  }, [transactions]);

  const assetInvestedAmountById = useMemo(() => {
    const investedById: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;

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
  }, [transactions]);

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
      render: (asset) => formatAssetQuantity(asset.quantity),
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
    return [
      {
        key: 'overview',
        title: t('assets.modal.sections.overview'),
        fields: [
          { key: 'ticker', label: t('assets.modal.fields.ticker'), value: formatDetailValue(selectedAsset.ticker) },
          { key: 'name', label: t('assets.modal.fields.name'), value: formatDetailValue(selectedAsset.name) },
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
            key: 'source',
            label: t('assets.modal.fields.source'),
            value: formatDetailValue(selectedAsset.source),
          },
        ],
      },
      {
        key: 'market',
        title: t('assets.modal.sections.market'),
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
        ],
      },
    ];
  }, [formatAssetQuantity, formatCountryDetail, formatDetailValue, numberLocale, selectedAsset, t]);

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
          title={t('assets.modal.title')}
          subtitle={t('assets.modal.subtitle')}
          closeLabel={t('assets.modal.close')}
          sections={assetDetailsSections}
          rawTitle={t('assets.modal.sections.raw')}
          rawData={selectedAsset}
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
