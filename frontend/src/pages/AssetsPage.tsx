import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
import { api, type Asset, type Portfolio, type Transaction } from '../services/api';
import { useToast } from '../context/ToastContext';
import { formatNumber } from '../utils/formatters';
import './AssetsPage.scss';

type StatusFilter = 'active' | 'inactive' | 'all';
type AssetRow = Asset & { quantity: number; source: string | null };

const PAGE_SIZE_OPTIONS = [5, 10, 25, 50];
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
const AssetsPage = () => {
  const { t } = useTranslation();
  const { showToast } = useToast();
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>('');
  const [assets, setAssets] = useState<Asset[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('active');
  const [itemsPerPage, setItemsPerPage] = useState(10);
  const [selectedAsset, setSelectedAsset] = useState<AssetRow | null>(null);
  const [form, setForm] = useState<{
    ticker: string;
    name: string;
    assetClass: string;
    country: string;
    currency: string;
  }>({
    ticker: '',
    name: '',
    assetClass: 'stock',
    country: 'BR',
    currency: 'BRL',
  });

  useEffect(() => {
    api.getPortfolios()
      .then((items) => {
        setPortfolios(items);
        if (items.length > 0) setSelectedPortfolio(items[0].portfolioId);
      })
      .catch(() => setPortfolios([]))
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

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!selectedPortfolio) return;

    try {
      const newAsset = await api.createAsset(selectedPortfolio, form);
      setAssets((previous) => [...previous, newAsset]);
      setShowModal(false);
      setForm({ ticker: '', name: '', assetClass: 'stock', country: 'BR', currency: 'BRL' });
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

  const assetQuantitiesById = useMemo(() => {
    const quantities: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      const normalizedQuantity = Math.trunc(Number(transaction.quantity || 0));

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

  const assetSourcesById = useMemo(() => {
    const sources: Record<string, string> = {};

    for (const transaction of transactions) {
      const source = transaction.sourceDocId?.toString().trim();
      if (!source) continue;
      if (!sources[transaction.assetId]) sources[transaction.assetId] = source;
    }

    return sources;
  }, [transactions]);

  const assetRows = useMemo<AssetRow[]>(() => {
    return assets.map((asset) => ({
      ...asset,
      quantity: assetQuantitiesById[asset.assetId] || 0,
      source: asset.source || assetSourcesById[asset.assetId] || null,
    }));
  }, [assetQuantitiesById, assetSourcesById, assets]);

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
      render: (asset) => formatNumber(Math.trunc(Number(asset.quantity || 0)), 0),
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
      options: [
        { value: 'active', label: t('assets.filters.status.active') },
        { value: 'inactive', label: t('assets.filters.status.inactive') },
        { value: 'all', label: t('assets.filters.status.all') },
      ],
      onChange: (value) => setStatusFilter(value as StatusFilter),
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
            value: formatNumber(Math.trunc(Number(selectedAsset.quantity || 0)), 0),
          },
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
  }, [formatCountryDetail, formatDetailValue, selectedAsset, t]);

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
            pageSizeOptions={PAGE_SIZE_OPTIONS}
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

        {showModal && (
          <div className="modal-overlay" onClick={() => setShowModal(false)}>
            <div className="modal" onClick={(event) => event.stopPropagation()}>
              <h2>{t('assets.addAsset')}</h2>
              <form onSubmit={handleSubmit}>
                <div className="modal__field">
                  <label>{t('assets.form.ticker')}</label>
                  <input
                    type="text"
                    value={form.ticker}
                    onChange={(event) => setForm({ ...form, ticker: event.target.value })}
                    required
                  />
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.name')}</label>
                  <input
                    type="text"
                    value={form.name}
                    onChange={(event) => setForm({ ...form, name: event.target.value })}
                    required
                  />
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.class')}</label>
                  <select
                    value={form.assetClass}
                    onChange={(event) => setForm({ ...form, assetClass: event.target.value })}
                  >
                    <option value="stock">{t('assets.classes.stock')}</option>
                    <option value="fii">{t('assets.classes.fii')}</option>
                    <option value="bond">{t('assets.classes.bond')}</option>
                    <option value="crypto">{t('assets.classes.crypto')}</option>
                    <option value="rsu">{t('assets.classes.rsu')}</option>
                  </select>
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.country')}</label>
                  <select
                    value={form.country}
                    onChange={(event) => setForm({ ...form, country: event.target.value })}
                  >
                    <option value="BR">Brazil</option>
                    <option value="US">United States</option>
                    <option value="CA">Canada</option>
                  </select>
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.currency')}</label>
                  <select
                    value={form.currency}
                    onChange={(event) => setForm({ ...form, currency: event.target.value })}
                  >
                    <option value="BRL">BRL</option>
                    <option value="USD">USD</option>
                    <option value="CAD">CAD</option>
                  </select>
                </div>
                <div className="modal__actions">
                  <button
                    type="button"
                    className="modal__btn modal__btn--cancel"
                    onClick={() => setShowModal(false)}
                  >
                    {t('assets.form.cancel')}
                  </button>
                  <button type="submit" className="modal__btn modal__btn--submit">
                    {t('assets.form.submit')}
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
};

export default AssetsPage;
