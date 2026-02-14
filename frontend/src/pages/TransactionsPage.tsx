import { useTranslation } from 'react-i18next';
import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import { api, type Asset, type Portfolio, type Transaction } from '../services/api';
import { formatCurrency, formatDate, formatNumber } from '../utils/formatters';
import './TransactionsPage.scss';

interface TransactionRow extends Transaction {
  ticker: string;
  name: string;
}

const PAGE_SIZE_OPTIONS = [5, 10, 25, 50];
const BASE_DETAIL_KEYS = new Set([
  'transId',
  'portfolioId',
  'assetId',
  'ticker',
  'name',
  'type',
  'status',
  'date',
  'quantity',
  'price',
  'amount',
  'currency',
  'sourceDocId',
  'createdAt',
]);

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
  const [selectedTransaction, setSelectedTransaction] = useState<TransactionRow | null>(null);

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
      .then(([transactionItems, assetItems]) => {
        setTransactions(transactionItems);
        setAssetsById(
          assetItems.reduce<Record<string, Asset>>((accumulator, asset) => {
            accumulator[asset.assetId] = asset;
            return accumulator;
          }, {})
        );
      })
      .catch(() => {
        setTransactions([]);
        setAssetsById({});
      })
      .finally(() => setLoading(false));
  }, [selectedPortfolio]);

  const rows = useMemo<TransactionRow[]>(() => {
    return transactions.map((transaction) => {
      const asset = assetsById[transaction.assetId];
      return {
        ...transaction,
        ticker: asset?.ticker || transaction.assetId,
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

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const columns: DataTableColumn<TransactionRow>[] = [
    {
      key: 'date',
      label: t('transactions.date'),
      sortable: true,
      sortValue: (row) => new Date(row.date || row.createdAt),
      initialDirection: 'desc',
      render: (row) => formatDate(row.date, numberLocale),
    },
    {
      key: 'ticker',
      label: t('transactions.ticker'),
      sortable: true,
      sortValue: (row) => row.ticker,
      cellClassName: 'transactions-page__cell--ticker',
      render: (row) => row.ticker,
    },
    {
      key: 'name',
      label: t('transactions.name'),
      sortable: true,
      sortValue: (row) => row.name,
      render: (row) => row.name,
    },
    {
      key: 'type',
      label: t('transactions.type'),
      sortable: true,
      sortValue: (row) => row.type,
      render: (row) => {
        const normalizedType = row.type?.toLowerCase() || 'unknown';
        return (
          <span className={`badge badge--${normalizedType}`}>
            {t(`transactions.types.${normalizedType}`, { defaultValue: row.type })}
          </span>
        );
      },
    },
    {
      key: 'quantity',
      label: t('transactions.quantity'),
      sortable: true,
      sortValue: (row) => Number(row.quantity || 0),
      render: (row) => formatNumber(Math.trunc(Number(row.quantity || 0)), 0),
    },
    {
      key: 'price',
      label: t('transactions.price'),
      sortable: true,
      sortValue: (row) => Number(row.price || 0),
      render: (row) => formatCurrency(Number(row.price || 0), row.currency || 'BRL', numberLocale),
    },
    {
      key: 'amount',
      label: t('transactions.amount'),
      sortable: true,
      sortValue: (row) => Number(row.amount || 0),
      render: (row) => formatCurrency(Number(row.amount || 0), row.currency || 'BRL', numberLocale),
    },
    {
      key: 'currency',
      label: t('transactions.currency'),
      sortable: true,
      sortValue: (row) => row.currency,
      render: (row) => row.currency,
    },
    {
      key: 'status',
      label: t('transactions.status'),
      sortable: true,
      sortValue: (row) => row.status,
      render: (row) => {
        const normalizedStatus = row.status?.toLowerCase() || 'unknown';
        return (
          <span className={`badge badge--status-${normalizedStatus}`}>
            {t(`transactions.statuses.${normalizedStatus}`, { defaultValue: row.status })}
          </span>
        );
      },
    },
  ];

  const filters: DataTableFilter<TransactionRow>[] = [
    {
      key: 'type',
      label: t('transactions.filters.type.label'),
      value: typeFilter,
      options: [
        { value: 'all', label: t('transactions.filters.type.all') },
        ...typeOptions.map((type) => ({
          value: type,
          label: t(`transactions.types.${type}`, { defaultValue: type }),
        })),
      ],
      onChange: setTypeFilter,
      matches: (row, filterValue) => filterValue === 'all' || (row.type?.toLowerCase() || '') === filterValue,
    },
    {
      key: 'status',
      label: t('transactions.filters.status.label'),
      value: statusFilter,
      options: [
        { value: 'all', label: t('transactions.filters.status.all') },
        ...statusOptions.map((status) => ({
          value: status,
          label: t(`transactions.statuses.${status}`, { defaultValue: status }),
        })),
      ],
      onChange: setStatusFilter,
      matches: (row, filterValue) => filterValue === 'all' || (row.status?.toLowerCase() || 'unknown') === filterValue,
    },
  ];

  const formatDetailValue = (value: unknown) => {
    if (value === undefined || value === null || value === '') return t('transactions.modal.noValue');
    return String(value);
  };

  const extraDetailEntries = useMemo(() => {
    if (!selectedTransaction) return [];
    return Object.entries(selectedTransaction).filter(([key]) => !BASE_DETAIL_KEYS.has(key));
  }, [selectedTransaction]);

  useEffect(() => {
    if (!selectedTransaction) return undefined;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSelectedTransaction(null);
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedTransaction]);

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
                onChange={(event) => setSelectedPortfolio(event.target.value)}
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
          <DataTable
            rows={rows}
            rowKey={(row) => row.transId}
            columns={columns}
            searchLabel={t('transactions.filters.search')}
            searchPlaceholder={t('transactions.filters.searchPlaceholder')}
            searchTerm={searchTerm}
            onSearchTermChange={setSearchTerm}
            matchesSearch={(row, normalizedSearch) =>
              [
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
                .toLowerCase()
                .includes(normalizedSearch)
            }
            filters={filters}
            itemsPerPage={itemsPerPage}
            onItemsPerPageChange={setItemsPerPage}
            pageSizeOptions={PAGE_SIZE_OPTIONS}
            emptyLabel={t('transactions.emptyFiltered')}
            labels={{
              itemsPerPage: t('transactions.pagination.itemsPerPage'),
              prev: t('transactions.pagination.prev'),
              next: t('transactions.pagination.next'),
              page: (page, total) => t('transactions.pagination.page', { page, total }),
              showing: (start, end, total) => t('transactions.pagination.showing', { start, end, total }),
            }}
            defaultSort={{ key: 'date', direction: 'desc' }}
            onRowClick={setSelectedTransaction}
            rowAriaLabel={(row) => t('transactions.modal.openDetails', { id: row.transId })}
          />
        )}

        {selectedTransaction && (
          <div className="transactions-modal-overlay" onClick={() => setSelectedTransaction(null)}>
            <div
              className="transactions-modal"
              onClick={(event) => event.stopPropagation()}
              role="dialog"
              aria-modal="true"
              aria-labelledby="transaction-modal-title"
            >
              <div className="transactions-modal__header">
                <div>
                  <h2 id="transaction-modal-title">
                    {selectedTransaction.type?.toLowerCase() === 'transfer'
                      ? t('transactions.modal.transferTitle')
                      : t('transactions.modal.transactionTitle')}
                  </h2>
                  <p>{t('transactions.modal.subtitle')}</p>
                </div>
                <button
                  type="button"
                  className="transactions-modal__close"
                  onClick={() => setSelectedTransaction(null)}
                >
                  {t('transactions.modal.close')}
                </button>
              </div>

              <div className="transactions-modal__grid">
                <section className="transactions-modal__section">
                  <h3>{t('transactions.modal.sections.overview')}</h3>
                  <dl>
                    <div>
                      <dt>{t('transactions.modal.fields.transId')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.transId)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.portfolioId')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.portfolioId)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.assetId')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.assetId)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.ticker')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.ticker)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.name')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.name)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.type')}</dt>
                      <dd>
                        {t(`transactions.types.${selectedTransaction.type?.toLowerCase() || 'unknown'}`, {
                          defaultValue: selectedTransaction.type,
                        })}
                      </dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.status')}</dt>
                      <dd>
                        {t(`transactions.statuses.${selectedTransaction.status?.toLowerCase() || 'unknown'}`, {
                          defaultValue: selectedTransaction.status,
                        })}
                      </dd>
                    </div>
                  </dl>
                </section>

                <section className="transactions-modal__section">
                  <h3>{t('transactions.modal.sections.financial')}</h3>
                  <dl>
                    <div>
                      <dt>{t('transactions.modal.fields.date')}</dt>
                      <dd>
                        {`${formatDate(selectedTransaction.date, numberLocale)} (${formatDetailValue(selectedTransaction.date)})`}
                      </dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.quantity')}</dt>
                      <dd>{formatNumber(Math.trunc(Number(selectedTransaction.quantity || 0)), 0)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.price')}</dt>
                      <dd>
                        {formatCurrency(
                          Number(selectedTransaction.price || 0),
                          selectedTransaction.currency || 'BRL',
                          numberLocale
                        )}
                      </dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.amount')}</dt>
                      <dd>
                        {formatCurrency(
                          Number(selectedTransaction.amount || 0),
                          selectedTransaction.currency || 'BRL',
                          numberLocale
                        )}
                      </dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.currency')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.currency)}</dd>
                    </div>
                  </dl>
                </section>

                <section className="transactions-modal__section transactions-modal__section--full">
                  <h3>{t('transactions.modal.sections.metadata')}</h3>
                  <dl>
                    <div>
                      <dt>{t('transactions.modal.fields.sourceDocId')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.sourceDocId)}</dd>
                    </div>
                    <div>
                      <dt>{t('transactions.modal.fields.createdAt')}</dt>
                      <dd>{formatDetailValue(selectedTransaction.createdAt)}</dd>
                    </div>
                    {extraDetailEntries.map(([key, value]) => (
                      <div key={key}>
                        <dt>{key}</dt>
                        <dd>{formatDetailValue(value)}</dd>
                      </div>
                    ))}
                  </dl>
                </section>

                <section className="transactions-modal__section transactions-modal__section--full">
                  <h3>{t('transactions.modal.sections.raw')}</h3>
                  <pre>{JSON.stringify(selectedTransaction, null, 2)}</pre>
                </section>
              </div>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
};

export default TransactionsPage;
