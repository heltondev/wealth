import { useTranslation } from 'react-i18next';
import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import { api, type Asset, type Portfolio } from '../services/api';
import { useToast } from '../context/ToastContext';
import './AssetsPage.scss';

type StatusFilter = 'active' | 'inactive' | 'all';
type SortKey = 'ticker' | 'name' | 'assetClass' | 'country' | 'currency' | 'status';
type SortDirection = 'asc' | 'desc';

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
  const [currentPage, setCurrentPage] = useState(1);
  const [sortKey, setSortKey] = useState<SortKey>('ticker');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');
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
      .then((p) => {
        setPortfolios(p);
        if (p.length > 0) setSelectedPortfolio(p[0].portfolioId);
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

  useEffect(() => {
    setCurrentPage(1);
  }, [selectedPortfolio, searchTerm, statusFilter, itemsPerPage]);

  const processedAssets = useMemo(() => {
    const normalizedSearch = searchTerm.trim().toLowerCase();
    const filtered = assets.filter((asset) => {
      const normalizedStatus = asset.status?.toLowerCase() || '';
      const matchesStatus = statusFilter === 'all' || normalizedStatus === statusFilter;
      if (!matchesStatus) return false;
      if (!normalizedSearch) return true;

      const searchable = [
        asset.ticker,
        asset.name,
        asset.assetClass,
        asset.country,
        asset.currency,
        asset.status,
      ]
        .join(' ')
        .toLowerCase();

      return searchable.includes(normalizedSearch);
    });

    return filtered.sort((a, b) => {
      const left = String(a[sortKey] || '').toLowerCase();
      const right = String(b[sortKey] || '').toLowerCase();
      const result = left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' });
      return sortDirection === 'asc' ? result : -result;
    });
  }, [assets, searchTerm, sortDirection, sortKey, statusFilter]);

  const totalPages = Math.max(1, Math.ceil(processedAssets.length / itemsPerPage));

  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages);
  }, [currentPage, totalPages]);

  const paginatedAssets = useMemo(() => {
    const start = (currentPage - 1) * itemsPerPage;
    return processedAssets.slice(start, start + itemsPerPage);
  }, [currentPage, itemsPerPage, processedAssets]);

  const pageStart = processedAssets.length === 0 ? 0 : (currentPage - 1) * itemsPerPage + 1;
  const pageEnd = Math.min(currentPage * itemsPerPage, processedAssets.length);

  const handleSort = (field: SortKey) => {
    setCurrentPage(1);
    if (sortKey === field) {
      setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(field);
    setSortDirection('asc');
  };

  const getSortIndicator = (field: SortKey) => {
    if (sortKey !== field) return '<>';
    return sortDirection === 'asc' ? '^' : 'v';
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedPortfolio) return;

    try {
      const newAsset = await api.createAsset(selectedPortfolio, form);
      setAssets((prev) => [...prev, newAsset]);
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
      setAssets((prev) => prev.filter((a) => a.assetId !== assetId));
      showToast('Asset deleted', 'success');
    } catch {
      showToast('Failed to delete asset', 'error');
    }
  };

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
                onChange={(e) => setSelectedPortfolio(e.target.value)}
              >
                {portfolios.map((p) => (
                  <option key={p.portfolioId} value={p.portfolioId}>
                    {p.name}
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
          <>
            <div className="assets-page__table-controls">
              <div className="assets-page__filter-group">
                <label htmlFor="assets-search">{t('assets.filters.search')}</label>
                <input
                  id="assets-search"
                  type="search"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder={t('assets.filters.searchPlaceholder')}
                />
              </div>
              <div className="assets-page__filter-group">
                <label htmlFor="assets-status-filter">{t('assets.filters.status.label')}</label>
                <select
                  id="assets-status-filter"
                  value={statusFilter}
                  onChange={(e) => setStatusFilter(e.target.value as StatusFilter)}
                >
                  <option value="active">{t('assets.filters.status.active')}</option>
                  <option value="inactive">{t('assets.filters.status.inactive')}</option>
                  <option value="all">{t('assets.filters.status.all')}</option>
                </select>
              </div>
              <div className="assets-page__filter-group">
                <label htmlFor="assets-page-size">{t('assets.pagination.itemsPerPage')}</label>
                <select
                  id="assets-page-size"
                  value={itemsPerPage}
                  onChange={(e) => setItemsPerPage(Number(e.target.value))}
                >
                  {PAGE_SIZE_OPTIONS.map((size) => (
                    <option key={size} value={size}>
                      {size}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {processedAssets.length === 0 ? (
              <div className="assets-page__empty">
                <p>{t('assets.emptyFiltered')}</p>
              </div>
            ) : (
              <>
                <div className="assets-table-wrapper">
                  <table className="assets-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="assets-table__sort-btn" onClick={() => handleSort('ticker')}>
                            {t('assets.ticker')}
                            <span>{getSortIndicator('ticker')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="assets-table__sort-btn" onClick={() => handleSort('name')}>
                            {t('assets.name')}
                            <span>{getSortIndicator('name')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="assets-table__sort-btn" onClick={() => handleSort('assetClass')}>
                            {t('assets.class')}
                            <span>{getSortIndicator('assetClass')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="assets-table__sort-btn" onClick={() => handleSort('country')}>
                            {t('assets.country')}
                            <span>{getSortIndicator('country')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="assets-table__sort-btn" onClick={() => handleSort('currency')}>
                            {t('assets.currency')}
                            <span>{getSortIndicator('currency')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="assets-table__sort-btn" onClick={() => handleSort('status')}>
                            {t('assets.status')}
                            <span>{getSortIndicator('status')}</span>
                          </button>
                        </th>
                        <th>{t('assets.actions')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {paginatedAssets.map((asset) => (
                        <tr key={asset.assetId}>
                          <td className="assets-table__ticker">{asset.ticker}</td>
                          <td>{asset.name}</td>
                          <td>
                            <span className={`badge badge--${asset.assetClass}`}>
                              {t(`assets.classes.${asset.assetClass}`)}
                            </span>
                          </td>
                          <td>{asset.country}</td>
                          <td>{asset.currency}</td>
                          <td>
                            {t(`assets.statuses.${asset.status?.toLowerCase() || 'unknown'}`, {
                              defaultValue: asset.status || t('assets.statuses.unknown'),
                            })}
                          </td>
                          <td>
                            <button
                              type="button"
                              className="assets-table__delete"
                              onClick={() => handleDelete(asset.assetId)}
                            >
                              {t('common.delete')}
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="assets-pagination">
                  <p className="assets-pagination__meta">
                    {t('assets.pagination.showing', { start: pageStart, end: pageEnd, total: processedAssets.length })}
                  </p>
                  <div className="assets-pagination__controls">
                    <button
                      type="button"
                      className="assets-pagination__btn"
                      onClick={() => setCurrentPage((prev) => Math.max(1, prev - 1))}
                      disabled={currentPage === 1}
                    >
                      {t('assets.pagination.prev')}
                    </button>
                    <span className="assets-pagination__page">
                      {t('assets.pagination.page', { page: currentPage, total: totalPages })}
                    </span>
                    <button
                      type="button"
                      className="assets-pagination__btn"
                      onClick={() => setCurrentPage((prev) => Math.min(totalPages, prev + 1))}
                      disabled={currentPage === totalPages}
                    >
                      {t('assets.pagination.next')}
                    </button>
                  </div>
                </div>
              </>
            )}
          </>
        )}

        {showModal && (
          <div className="modal-overlay" onClick={() => setShowModal(false)}>
            <div className="modal" onClick={(e) => e.stopPropagation()}>
              <h2>{t('assets.addAsset')}</h2>
              <form onSubmit={handleSubmit}>
                <div className="modal__field">
                  <label>{t('assets.form.ticker')}</label>
                  <input
                    type="text"
                    value={form.ticker}
                    onChange={(e) => setForm({ ...form, ticker: e.target.value })}
                    required
                  />
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.name')}</label>
                  <input
                    type="text"
                    value={form.name}
                    onChange={(e) => setForm({ ...form, name: e.target.value })}
                    required
                  />
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.class')}</label>
                  <select
                    value={form.assetClass}
                    onChange={(e) => setForm({ ...form, assetClass: e.target.value })}
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
                    onChange={(e) => setForm({ ...form, country: e.target.value })}
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
                    onChange={(e) => setForm({ ...form, currency: e.target.value })}
                  >
                    <option value="BRL">BRL</option>
                    <option value="USD">USD</option>
                    <option value="CAD">CAD</option>
                  </select>
                </div>
                <div className="modal__actions">
                  <button type="button" className="modal__btn modal__btn--cancel" onClick={() => setShowModal(false)}>
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
