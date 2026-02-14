import { useTranslation } from 'react-i18next';
import { useEffect, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import { api, type Asset, type Portfolio } from '../services/api';
import { useToast } from '../context/ToastContext';
import './AssetsPage.scss';

type StatusFilter = 'active' | 'inactive' | 'all';

const PAGE_SIZE_OPTIONS = [5, 10, 25, 50];

const AssetsPage = () => {
  const { t } = useTranslation();
  const { showToast } = useToast();
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>('');
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('active');
  const [itemsPerPage, setItemsPerPage] = useState(10);
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
    api.getAssets(selectedPortfolio)
      .then(setAssets)
      .catch(() => setAssets([]))
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

  const columns: DataTableColumn<Asset>[] = [
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
      render: (asset) => asset.name,
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
      render: (asset) => asset.country,
    },
    {
      key: 'currency',
      label: t('assets.currency'),
      sortable: true,
      sortValue: (asset) => asset.currency,
      render: (asset) => asset.currency,
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
        <button type="button" className="assets-page__delete" onClick={() => handleDelete(asset.assetId)}>
          {t('common.delete')}
        </button>
      ),
    },
  ];

  const filters: DataTableFilter<Asset>[] = [
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

        {!loading && assets.length === 0 && (
          <div className="assets-page__empty">
            <p>{t('assets.empty')}</p>
          </div>
        )}

        {!loading && assets.length > 0 && (
          <DataTable
            rows={assets}
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
                asset.assetClass,
                asset.country,
                asset.currency,
                asset.status,
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
          />
        )}

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
