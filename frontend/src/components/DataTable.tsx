import { useEffect, useId, useMemo, useState } from 'react';
import './DataTable.scss';

export type TableSortDirection = 'asc' | 'desc';

type SortableValue = string | number | Date | null | undefined;

export interface DataTableOption {
  value: string;
  label: string;
}

export interface DataTableColumn<Row> {
  key: string;
  label: string;
  sortable?: boolean;
  sortValue?: (row: Row) => SortableValue;
  initialDirection?: TableSortDirection;
  render: (row: Row) => React.ReactNode;
  headerClassName?: string;
  cellClassName?: string;
}

export interface DataTableFilter<Row> {
  key: string;
  label: string;
  value: string;
  options: DataTableOption[];
  onChange: (value: string) => void;
  matches: (row: Row, filterValue: string) => boolean;
}

export interface DataTableLabels {
  itemsPerPage: string;
  prev: string;
  next: string;
  page: (page: number, total: number) => string;
  showing: (start: number, end: number, total: number) => string;
}

interface DataTableProps<Row> {
  rows: Row[];
  rowKey: (row: Row) => string;
  columns: DataTableColumn<Row>[];
  searchLabel: string;
  searchPlaceholder: string;
  searchTerm: string;
  onSearchTermChange: (value: string) => void;
  matchesSearch: (row: Row, normalizedSearchTerm: string) => boolean;
  filters?: DataTableFilter<Row>[];
  itemsPerPage: number;
  onItemsPerPageChange: (value: number) => void;
  pageSizeOptions: number[];
  emptyLabel: string;
  labels: DataTableLabels;
  defaultSort?: {
    key: string;
    direction: TableSortDirection;
  };
  onRowClick?: (row: Row) => void;
  rowAriaLabel?: (row: Row) => string;
  rowClassName?: (row: Row) => string;
}

function normalizeSortValue(value: SortableValue): string | number {
  if (value instanceof Date) return value.getTime();
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (value === null || value === undefined) return '';
  return String(value).toLowerCase();
}

function compareSortValues(left: SortableValue, right: SortableValue): number {
  const normalizedLeft = normalizeSortValue(left);
  const normalizedRight = normalizeSortValue(right);

  if (typeof normalizedLeft === 'number' && typeof normalizedRight === 'number') {
    return normalizedLeft - normalizedRight;
  }

  return String(normalizedLeft).localeCompare(String(normalizedRight), undefined, {
    numeric: true,
    sensitivity: 'base',
  });
}

function DataTable<Row>({
  rows,
  rowKey,
  columns,
  searchLabel,
  searchPlaceholder,
  searchTerm,
  onSearchTermChange,
  matchesSearch,
  filters = [],
  itemsPerPage,
  onItemsPerPageChange,
  pageSizeOptions,
  emptyLabel,
  labels,
  defaultSort,
  onRowClick,
  rowAriaLabel,
  rowClassName,
}: DataTableProps<Row>) {
  const searchId = useId();
  const defaultSortKey = defaultSort?.key || columns.find((column) => column.sortable)?.key || columns[0]?.key;
  const defaultSortDirection = defaultSort?.direction || 'asc';
  const [sortKey, setSortKey] = useState(defaultSortKey);
  const [sortDirection, setSortDirection] = useState<TableSortDirection>(defaultSortDirection);
  const [currentPage, setCurrentPage] = useState(1);

  const filterStateKey = filters.map((filter) => `${filter.key}:${filter.value}`).join('|');

  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, filterStateKey, itemsPerPage, rows.length]);

  useEffect(() => {
    if (!sortKey && defaultSortKey) {
      setSortKey(defaultSortKey);
      setSortDirection(defaultSortDirection);
    }
  }, [defaultSortDirection, defaultSortKey, sortKey]);

  const processedRows = useMemo(() => {
    const normalizedSearch = searchTerm.trim().toLowerCase();
    const filteredRows = rows.filter((row) => {
      const matchesFilters = filters.every((filter) => filter.matches(row, filter.value));
      if (!matchesFilters) return false;
      if (!normalizedSearch) return true;
      return matchesSearch(row, normalizedSearch);
    });

    if (!sortKey) return filteredRows;
    const activeColumn = columns.find((column) => column.key === sortKey);
    const sortAccessor = activeColumn?.sortValue || ((row: Row) => (row as Record<string, SortableValue>)[sortKey]);

    return [...filteredRows].sort((left, right) => {
      const result = compareSortValues(sortAccessor(left), sortAccessor(right));
      return sortDirection === 'asc' ? result : -result;
    });
  }, [columns, filters, matchesSearch, rows, searchTerm, sortDirection, sortKey]);

  const totalPages = Math.max(1, Math.ceil(processedRows.length / itemsPerPage));

  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages);
  }, [currentPage, totalPages]);

  const paginatedRows = useMemo(() => {
    const start = (currentPage - 1) * itemsPerPage;
    return processedRows.slice(start, start + itemsPerPage);
  }, [currentPage, itemsPerPage, processedRows]);

  const pageStart = processedRows.length === 0 ? 0 : (currentPage - 1) * itemsPerPage + 1;
  const pageEnd = Math.min(currentPage * itemsPerPage, processedRows.length);

  const handleSort = (column: DataTableColumn<Row>) => {
    if (!column.sortable) return;
    if (sortKey === column.key) {
      setSortDirection((previous) => (previous === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(column.key);
    setSortDirection(column.initialDirection || 'asc');
    setCurrentPage(1);
  };

  const getSortIndicator = (columnKey: string) => {
    if (sortKey !== columnKey) return '<>';
    return sortDirection === 'asc' ? '^' : 'v';
  };

  return (
    <div className="data-table">
      <div className="data-table__controls">
        <div className="data-table__filter-group">
          <label htmlFor={searchId}>{searchLabel}</label>
          <input
            id={searchId}
            type="search"
            value={searchTerm}
            onChange={(event) => onSearchTermChange(event.target.value)}
            placeholder={searchPlaceholder}
          />
        </div>

        {filters.map((filter) => (
          <div key={filter.key} className="data-table__filter-group">
            <label htmlFor={`${searchId}-${filter.key}`}>{filter.label}</label>
            <select
              id={`${searchId}-${filter.key}`}
              value={filter.value}
              onChange={(event) => filter.onChange(event.target.value)}
            >
              {filter.options.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        ))}

        <div className="data-table__filter-group">
          <label htmlFor={`${searchId}-page-size`}>{labels.itemsPerPage}</label>
          <select
            id={`${searchId}-page-size`}
            value={itemsPerPage}
            onChange={(event) => onItemsPerPageChange(Number(event.target.value))}
          >
            {pageSizeOptions.map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </select>
        </div>
      </div>

      {processedRows.length === 0 ? (
        <div className="data-table__empty">
          <p>{emptyLabel}</p>
        </div>
      ) : (
        <>
          <div className="data-table__wrapper">
            <table className="data-table__table">
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th key={column.key} className={column.headerClassName}>
                      {column.sortable ? (
                        <button
                          type="button"
                          className="data-table__sort-btn"
                          onClick={() => handleSort(column)}
                        >
                          {column.label}
                          <span>{getSortIndicator(column.key)}</span>
                        </button>
                      ) : (
                        column.label
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {paginatedRows.map((row) => {
                  const isClickable = Boolean(onRowClick);
                  const additionalClassName = rowClassName ? rowClassName(row) : '';
                  const className = [
                    'data-table__row',
                    isClickable ? 'data-table__row--clickable' : '',
                    additionalClassName,
                  ]
                    .filter(Boolean)
                    .join(' ');

                  return (
                    <tr
                      key={rowKey(row)}
                      className={className}
                      onClick={isClickable ? () => onRowClick?.(row) : undefined}
                      onKeyDown={
                        isClickable
                          ? (event) => {
                              if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault();
                                onRowClick?.(row);
                              }
                            }
                          : undefined
                      }
                      role={isClickable ? 'button' : undefined}
                      tabIndex={isClickable ? 0 : undefined}
                      aria-label={isClickable && rowAriaLabel ? rowAriaLabel(row) : undefined}
                    >
                      {columns.map((column) => (
                        <td key={column.key} className={column.cellClassName}>
                          {column.render(row)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div className="data-table__pagination">
            <p className="data-table__meta">{labels.showing(pageStart, pageEnd, processedRows.length)}</p>
            <div className="data-table__pagination-controls">
              <button
                type="button"
                className="data-table__pagination-btn"
                onClick={() => setCurrentPage((previous) => Math.max(1, previous - 1))}
                disabled={currentPage === 1}
              >
                {labels.prev}
              </button>
              <span className="data-table__page-label">{labels.page(currentPage, totalPages)}</span>
              <button
                type="button"
                className="data-table__pagination-btn"
                onClick={() => setCurrentPage((previous) => Math.min(totalPages, previous + 1))}
                disabled={currentPage === totalPages}
              >
                {labels.next}
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default DataTable;
