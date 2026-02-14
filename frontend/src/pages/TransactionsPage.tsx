import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
import { api, type Asset, type Portfolio, type Transaction } from '../services/api';
import { formatCurrency, formatDate, formatNumber } from '../utils/formatters';
import './TransactionsPage.scss';

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
      cellClassName: 'transactions-page__cell--name',
      render: (row) => (
        <span className="transactions-page__name-ellipsis" title={row.name}>
          {row.name}
        </span>
      ),
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

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('transactions.modal.noValue');
    return String(value);
  }, [t]);

  const transactionDetailsSections = useMemo<RecordDetailsSection[]>(() => {
    if (!selectedTransaction) return [];
    return [
      {
        key: 'overview',
        title: t('transactions.modal.sections.overview'),
        fields: [
          {
            key: 'transId',
            label: t('transactions.modal.fields.transId'),
            value: formatDetailValue(selectedTransaction.transId),
          },
          {
            key: 'portfolioId',
            label: t('transactions.modal.fields.portfolioId'),
            value: formatDetailValue(selectedTransaction.portfolioId),
          },
          {
            key: 'assetId',
            label: t('transactions.modal.fields.assetId'),
            value: formatDetailValue(selectedTransaction.assetId),
          },
          { key: 'ticker', label: t('transactions.modal.fields.ticker'), value: formatDetailValue(selectedTransaction.ticker) },
          { key: 'name', label: t('transactions.modal.fields.name'), value: formatDetailValue(selectedTransaction.name) },
          {
            key: 'type',
            label: t('transactions.modal.fields.type'),
            value: t(`transactions.types.${selectedTransaction.type?.toLowerCase() || 'unknown'}`, {
              defaultValue: selectedTransaction.type,
            }),
          },
          {
            key: 'status',
            label: t('transactions.modal.fields.status'),
            value: t(`transactions.statuses.${selectedTransaction.status?.toLowerCase() || 'unknown'}`, {
              defaultValue: selectedTransaction.status,
            }),
          },
          {
            key: 'sourceDocId',
            label: t('transactions.modal.fields.sourceDocId'),
            value: formatDetailValue(selectedTransaction.sourceDocId),
          },
          {
            key: 'createdAt',
            label: t('transactions.modal.fields.createdAt'),
            value: formatDetailValue(selectedTransaction.createdAt),
          },
        ],
      },
      {
        key: 'financial',
        title: t('transactions.modal.sections.financial'),
        fields: [
          {
            key: 'date',
            label: t('transactions.modal.fields.date'),
            value: `${formatDate(selectedTransaction.date, numberLocale)} (${formatDetailValue(selectedTransaction.date)})`,
          },
          {
            key: 'quantity',
            label: t('transactions.modal.fields.quantity'),
            value: formatNumber(Math.trunc(Number(selectedTransaction.quantity || 0)), 0),
          },
          {
            key: 'price',
            label: t('transactions.modal.fields.price'),
            value: formatCurrency(
              Number(selectedTransaction.price || 0),
              selectedTransaction.currency || 'BRL',
              numberLocale
            ),
          },
          {
            key: 'amount',
            label: t('transactions.modal.fields.amount'),
            value: formatCurrency(
              Number(selectedTransaction.amount || 0),
              selectedTransaction.currency || 'BRL',
              numberLocale
            ),
          },
          {
            key: 'currency',
            label: t('transactions.modal.fields.currency'),
            value: formatDetailValue(selectedTransaction.currency),
          },
        ],
      },
    ];
  }, [formatDetailValue, numberLocale, selectedTransaction, t]);

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

        <RecordDetailsModal
          open={Boolean(selectedTransaction)}
          title={
            selectedTransaction?.type?.toLowerCase() === 'transfer'
              ? t('transactions.modal.transferTitle')
              : t('transactions.modal.transactionTitle')
          }
          subtitle={t('transactions.modal.subtitle')}
          closeLabel={t('transactions.modal.close')}
          sections={transactionDetailsSections}
          rawTitle={t('transactions.modal.sections.raw')}
          rawData={selectedTransaction}
          onClose={() => setSelectedTransaction(null)}
        />
      </div>
    </Layout>
  );
};

export default TransactionsPage;
