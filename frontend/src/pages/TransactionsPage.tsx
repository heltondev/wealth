import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn, type DataTableFilter } from '../components/DataTable';
import RecordDetailsModal, { type RecordDetailsSection } from '../components/RecordDetailsModal';
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
import { formatCurrency, formatDate } from '../utils/formatters';
import './TransactionsPage.scss';

interface TransactionRow extends Transaction {
  ticker: string;
  name: string;
  assetClass: Asset['assetClass'] | 'unknown';
}

const DECIMAL_PRECISION = 2;
const DEFAULT_ITEMS_PER_PAGE = 10;

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

const toPageSizeOptions = (options: { value: string }[]): number[] => {
  const values = new Set<number>();

  for (const option of options) {
    const numeric = Number(option.value);
    if (!Number.isFinite(numeric) || numeric <= 0) continue;
    values.add(Math.round(numeric));
  }

  return Array.from(values).sort((left, right) => left - right);
};

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
  const [itemsPerPage, setItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);
  const [selectedTransaction, setSelectedTransaction] = useState<TransactionRow | null>(null);
  const [dropdownConfig, setDropdownConfig] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );

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
        assetClass: asset?.assetClass || 'unknown',
      };
    });
  }, [assetsById, transactions]);

  const configuredTypeOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'transactions.filters.type');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'transactions.filters.type');
  }, [dropdownConfig]);

  const configuredStatusOptions = useMemo(() => {
    const options = getDropdownOptions(dropdownConfig, 'transactions.filters.status');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'transactions.filters.status');
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

  const typeOptions = useMemo(() => {
    const labels = new Map(configuredTypeOptions.map((option) => [option.value, option.label]));
    const orderedValues = configuredTypeOptions.map((option) => option.value);

    for (const type of rows.map((row) => row.type?.toLowerCase()).filter(Boolean) as string[]) {
      if (labels.has(type)) continue;
      labels.set(type, type);
      orderedValues.push(type);
    }

    return orderedValues.map((value) => ({
      value,
      label: labels.get(value) || value,
    }));
  }, [configuredTypeOptions, rows]);

  const statusOptions = useMemo(() => {
    const labels = new Map(configuredStatusOptions.map((option) => [option.value, option.label]));
    const orderedValues = configuredStatusOptions.map((option) => option.value);

    for (const status of rows.map((row) => row.status?.toLowerCase() || 'unknown')) {
      if (labels.has(status)) continue;
      labels.set(status, status);
      orderedValues.push(status);
    }

    return orderedValues.map((value) => ({
      value,
      label: labels.get(value) || value,
    }));
  }, [configuredStatusOptions, rows]);

  useEffect(() => {
    if (!typeOptions.some((option) => option.value === typeFilter)) {
      setTypeFilter(typeOptions[0]?.value || 'all');
    }
  }, [typeFilter, typeOptions]);

  useEffect(() => {
    if (!statusOptions.some((option) => option.value === statusFilter)) {
      setStatusFilter(statusOptions[0]?.value || 'all');
    }
  }, [statusFilter, statusOptions]);

  useEffect(() => {
    if (pageSizeOptions.includes(itemsPerPage)) return;
    setItemsPerPage(pageSizeOptions[0] || DEFAULT_ITEMS_PER_PAGE);
  }, [itemsPerPage, pageSizeOptions]);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const formatTransactionQuantity = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('transactions.modal.noValue');
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return String(value);

    const hasFraction = Math.abs(numeric % 1) > Number.EPSILON;
    return numeric.toLocaleString(numberLocale, {
      minimumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
      maximumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
    });
  }, [numberLocale, t]);

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
      render: (row) => formatTransactionQuantity(row.quantity),
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
      options: typeOptions.map((option) => ({
        value: option.value,
        label: option.value === 'all'
          ? t('transactions.filters.type.all')
          : t(`transactions.types.${option.value}`, { defaultValue: option.label }),
      })),
      onChange: setTypeFilter,
      matches: (row, filterValue) => filterValue === 'all' || (row.type?.toLowerCase() || '') === filterValue,
    },
    {
      key: 'status',
      label: t('transactions.filters.status.label'),
      value: statusFilter,
      options: statusOptions.map((option) => ({
        value: option.value,
        label: option.value === 'all'
          ? t('transactions.filters.status.all')
          : t(`transactions.statuses.${option.value}`, { defaultValue: option.label }),
      })),
      onChange: setStatusFilter,
      matches: (row, filterValue) => filterValue === 'all' || (row.status?.toLowerCase() || 'unknown') === filterValue,
    },
  ];

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('transactions.modal.noValue');
    return String(value);
  }, [t]);

  const formatTransactionSource = useCallback((transaction: TransactionRow) => {
    const labels = new Set<string>();
    const sourceDocLabel = summarizeSourceValue(transaction.sourceDocId);
    const institutionLabel = summarizeSourceValue(transaction.institution);

    if (sourceDocLabel) labels.add(sourceDocLabel);
    if (institutionLabel) labels.add(institutionLabel);

    if (labels.size > 0) return Array.from(labels).join(', ');
    return t('transactions.modal.noValue');
  }, [t]);

  const transactionDetailsSections = useMemo<RecordDetailsSection[]>(() => {
    if (!selectedTransaction) return [];
    return [
      {
        key: 'overview',
        title: t('transactions.modal.sections.overview'),
        fields: [
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
            key: 'source',
            label: t('transactions.modal.fields.source'),
            value: formatTransactionSource(selectedTransaction),
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
            value: formatTransactionQuantity(selectedTransaction.quantity),
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
  }, [formatDetailValue, formatTransactionQuantity, formatTransactionSource, numberLocale, selectedTransaction, t]);

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
            matchesSearch={(row, normalizedSearch) => {
              const bondHints = row.assetClass === 'bond'
                ? 'bond renda fixa tesouro direto cdb cri'
                : '';

              return [
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
                row.assetClass,
                bondHints,
              ]
                .join(' ')
                .toLowerCase()
                .includes(normalizedSearch);
            }}
            filters={filters}
            itemsPerPage={itemsPerPage}
            onItemsPerPageChange={setItemsPerPage}
            pageSizeOptions={pageSizeOptions}
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
