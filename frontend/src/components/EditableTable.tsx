import './EditableTable.scss';

export interface EditableTableColumn<Row> {
  key: string;
  label: string;
  render: (row: Row, rowIndex: number) => React.ReactNode;
  headerClassName?: string;
  cellClassName?: string;
}

interface EditableTableProps<Row> {
  rows: Row[];
  rowKey: (row: Row, rowIndex: number) => string;
  columns: EditableTableColumn<Row>[];
  emptyLabel?: string;
  className?: string;
}

function EditableTable<Row>({
  rows,
  rowKey,
  columns,
  emptyLabel,
  className = '',
}: EditableTableProps<Row>) {
  const classes = ['editable-table', className].filter(Boolean).join(' ');

  if (rows.length === 0) {
    return (
      <div className={classes}>
        <div className="editable-table__empty">
          <p>{emptyLabel || 'No rows'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className={classes}>
      <div className="editable-table__wrapper">
        <table className="editable-table__table">
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key} className={column.headerClassName}>
                  {column.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={rowKey(row, rowIndex)}>
                {columns.map((column) => (
                  <td key={column.key} className={column.cellClassName}>
                    {column.render(row, rowIndex)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default EditableTable;
