import { useTranslation } from 'react-i18next';
import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import { api, type Asset, type Portfolio, type Transaction } from '../services/api';
import { formatCurrency, formatDate, formatNumber } from '../utils/formatters';
import './TransactionsPage.scss';

type SortKey =
  | 'date'
  | 'ticker'
  | 'name'
  | 'type'
  | 'quantity'
  | 'price'
  | 'amount'
  | 'currency'
  | 'status';
type SortDirection = 'asc' | 'desc';

interface TransactionRow extends Transaction {
  ticker: string;
  name: string;
}

const PAGE_SIZE_OPTIONS = [5, 10, 25, 50];

const TransactionsPage = () => {
  const { t, i18n } = useTranslation();
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>('');
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [assetsById, setAssetsById] = useState<Record<string, Asset>>({});
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [typeFilter, setTypeFilter] = useState('all');
  const [statusFilter, setStatusFilter] = useState('all');
  const [itemsPerPage, setItemsPerPage] = useState(10);
  const [currentPage, setCurrentPage] = useState(1);
  const [sortKey, setSortKey] = useState<SortKey>('date');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

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

    Promise.all([api.getTransactions(selectedPortfolio), api.getAssets(selectedPortfolio)])
      .then(([txItems, assetItems]) => {
        setTransactions(txItems);
        setAssetsById(
          assetItems.reduce<Record<string, Asset>>((acc, asset) => {
            acc[asset.assetId] = asset;
            return acc;
          }, {})
        );
      })
      .catch(() => {
        setTransactions([]);
        setAssetsById({});
      })
      .finally(() => setLoading(false));
  }, [selectedPortfolio]);

  useEffect(() => {
    setCurrentPage(1);
  }, [selectedPortfolio, searchTerm, typeFilter, statusFilter, itemsPerPage]);

  const rows = useMemo<TransactionRow[]>(() => {
    return transactions.map((tx) => {
      const asset = assetsById[tx.assetId];
      return {
        ...tx,
        ticker: asset?.ticker || tx.assetId,
        name: asset?.name || '-',
      };
    });
  }, [assetsById, transactions]);

  const typeOptions = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.type?.toLowerCase()).filter(Boolean))).sort();
  }, [rows]);

  const statusOptions = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.status?.toLowerCase() || 'unknown'))).sort();
  }, [rows]);

  const processedRows = useMemo(() => {
    const normalizedSearch = searchTerm.trim().toLowerCase();
    const filtered = rows.filter((row) => {
      const normalizedType = row.type?.toLowerCase() || '';
      const normalizedStatus = row.status?.toLowerCase() || 'unknown';
      const matchesType = typeFilter === 'all' || normalizedType === typeFilter;
      const matchesStatus = statusFilter === 'all' || normalizedStatus === statusFilter;
      if (!matchesType || !matchesStatus) return false;
      if (!normalizedSearch) return true;

      const searchable = [
        row.date,
        row.ticker,
        row.name,
        row.type,
        row.quantity,
        row.price,
        row.amount,
        row.currency,
        row.status,
        row.assetId,
      ]
        .join(' ')
        .toLowerCase();

      return searchable.includes(normalizedSearch);
    });

    return filtered.sort((a, b) => {
      let result = 0;

      // Preserve reliable chronological sorting when the active column is date.
      if (sortKey === 'date') {
        result = new Date(a.date || a.createdAt).getTime() - new Date(b.date || b.createdAt).getTime();
      } else if (sortKey === 'quantity' || sortKey === 'price' || sortKey === 'amount') {
        result = Number(a[sortKey] || 0) - Number(b[sortKey] || 0);
      } else if (sortKey === 'ticker' || sortKey === 'name') {
        const left = sortKey === 'ticker' ? a.ticker : a.name;
        const right = sortKey === 'ticker' ? b.ticker : b.name;
        result = left.localeCompare(right, undefined, {
          numeric: true,
          sensitivity: 'base',
        });
      } else {
        const left = String(a[sortKey] || '');
        const right = String(b[sortKey] || '');
        result = left.localeCompare(right, undefined, {
          numeric: true,
          sensitivity: 'base',
        });
      }

      return sortDirection === 'asc' ? result : -result;
    });
  }, [rows, searchTerm, statusFilter, typeFilter, sortDirection, sortKey]);

  const totalPages = Math.max(1, Math.ceil(processedRows.length / itemsPerPage));

  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages);
  }, [currentPage, totalPages]);

  const paginatedRows = useMemo(() => {
    const start = (currentPage - 1) * itemsPerPage;
    return processedRows.slice(start, start + itemsPerPage);
  }, [processedRows, currentPage, itemsPerPage]);

  const pageStart = processedRows.length === 0 ? 0 : (currentPage - 1) * itemsPerPage + 1;
  const pageEnd = Math.min(currentPage * itemsPerPage, processedRows.length);
  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const handleSort = (field: SortKey) => {
    setCurrentPage(1);
    if (sortKey === field) {
      setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(field);
    setSortDirection(field === 'date' ? 'desc' : 'asc');
  };

  const getSortIndicator = (field: SortKey) => {
    if (sortKey !== field) return '<>';
    return sortDirection === 'asc' ? '^' : 'v';
  };

  return (
    <Layout>
      <div className="transactions-page">
        <div className="transactions-page__header">
          <h1 className="transactions-page__title">{t('transactions.title')}</h1>
          <div className="transactions-page__controls">
            {portfolios.length > 0 && (
              <select
                className="transactions-page__select"
                value={selectedPortfolio}
                onChange={(e) => setSelectedPortfolio(e.target.value)}
              >
                {portfolios.map((portfolio) => (
                  <option key={portfolio.portfolioId} value={portfolio.portfolioId}>
                    {portfolio.name}
                  </option>
                ))}
              </select>
            )}
          </div>
        </div>

        {loading && <p className="transactions-page__loading">{t('common.loading')}</p>}

        {!loading && transactions.length === 0 && (
          <div className="transactions-page__empty">
            <p>{t('transactions.empty')}</p>
          </div>
        )}

        {!loading && transactions.length > 0 && (
          <>
            <div className="transactions-page__table-controls">
              <div className="transactions-page__filter-group">
                <label htmlFor="transactions-search">{t('transactions.filters.search')}</label>
                <input
                  id="transactions-search"
                  type="search"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder={t('transactions.filters.searchPlaceholder')}
                />
              </div>
              <div className="transactions-page__filter-group">
                <label htmlFor="transactions-type-filter">{t('transactions.filters.type.label')}</label>
                <select
                  id="transactions-type-filter"
                  value={typeFilter}
                  onChange={(e) => setTypeFilter(e.target.value)}
                >
                  <option value="all">{t('transactions.filters.type.all')}</option>
                  {typeOptions.map((type) => (
                    <option key={type} value={type}>
                      {t(`transactions.types.${type}`, { defaultValue: type })}
                    </option>
                  ))}
                </select>
              </div>
              <div className="transactions-page__filter-group">
                <label htmlFor="transactions-status-filter">{t('transactions.filters.status.label')}</label>
                <select
                  id="transactions-status-filter"
                  value={statusFilter}
                  onChange={(e) => setStatusFilter(e.target.value)}
                >
                  <option value="all">{t('transactions.filters.status.all')}</option>
                  {statusOptions.map((status) => (
                    <option key={status} value={status}>
                      {t(`transactions.statuses.${status}`, { defaultValue: status })}
                    </option>
                  ))}
                </select>
              </div>
              <div className="transactions-page__filter-group">
                <label htmlFor="transactions-page-size">{t('transactions.pagination.itemsPerPage')}</label>
                <select
                  id="transactions-page-size"
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

            {processedRows.length === 0 ? (
              <div className="transactions-page__empty">
                <p>{t('transactions.emptyFiltered')}</p>
              </div>
            ) : (
              <>
                <div className="transactions-table-wrapper">
                  <table className="transactions-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('date')}>
                            {t('transactions.date')}
                            <span>{getSortIndicator('date')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('ticker')}>
                            {t('transactions.ticker')}
                            <span>{getSortIndicator('ticker')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('name')}>
                            {t('transactions.name')}
                            <span>{getSortIndicator('name')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('type')}>
                            {t('transactions.type')}
                            <span>{getSortIndicator('type')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('quantity')}>
                            {t('transactions.quantity')}
                            <span>{getSortIndicator('quantity')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('price')}>
                            {t('transactions.price')}
                            <span>{getSortIndicator('price')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('amount')}>
                            {t('transactions.amount')}
                            <span>{getSortIndicator('amount')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('currency')}>
                            {t('transactions.currency')}
                            <span>{getSortIndicator('currency')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="transactions-table__sort-btn" onClick={() => handleSort('status')}>
                            {t('transactions.status')}
                            <span>{getSortIndicator('status')}</span>
                          </button>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {paginatedRows.map((row) => {
                        const normalizedStatus = row.status?.toLowerCase() || 'unknown';
                        return (
                          <tr key={row.transId}>
                            <td>{formatDate(row.date, numberLocale)}</td>
                            <td className="transactions-table__ticker">{row.ticker}</td>
                            <td>{row.name}</td>
                            <td>
                              <span className={`badge badge--${row.type}`}>
                                {t(`transactions.types.${row.type.toLowerCase()}`, { defaultValue: row.type })}
                              </span>
                            </td>
                            <td>{formatNumber(Math.trunc(Number(row.quantity || 0)), 0)}</td>
                            <td>{formatCurrency(Number(row.price || 0), row.currency || 'BRL', numberLocale)}</td>
                            <td>{formatCurrency(Number(row.amount || 0), row.currency || 'BRL', numberLocale)}</td>
                            <td>{row.currency}</td>
                            <td>
                              <span className={`badge badge--status-${normalizedStatus}`}>
                                {t(`transactions.statuses.${normalizedStatus}`, { defaultValue: row.status })}
                              </span>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                <div className="transactions-pagination">
                  <p className="transactions-pagination__meta">
                    {t('transactions.pagination.showing', {
                      start: pageStart,
                      end: pageEnd,
                      total: processedRows.length,
                    })}
                  </p>
                  <div className="transactions-pagination__controls">
                    <button
                      type="button"
                      className="transactions-pagination__btn"
                      onClick={() => setCurrentPage((prev) => Math.max(1, prev - 1))}
                      disabled={currentPage === 1}
                    >
                      {t('transactions.pagination.prev')}
                    </button>
                    <span className="transactions-pagination__page">
                      {t('transactions.pagination.page', { page: currentPage, total: totalPages })}
                    </span>
                    <button
                      type="button"
                      className="transactions-pagination__btn"
                      onClick={() => setCurrentPage((prev) => Math.min(totalPages, prev + 1))}
                      disabled={currentPage === totalPages}
                    >
                      {t('transactions.pagination.next')}
                    </button>
                  </div>
                </div>
              </>
            )}
          </>
        )}
      </div>
    </Layout>
  );
};

export default TransactionsPage;
