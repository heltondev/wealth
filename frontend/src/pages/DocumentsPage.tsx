import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type ImportB3Response,
  type ParserDescriptor,
  type ReportContentResponse,
  type ReportRecord,
} from '../services/api';
import './DocumentsPage.scss';

type ReportType = 'portfolio' | 'tax' | 'dividends' | 'performance' | 'transactions';
type PeriodPreset = 'current' | '1M' | '3M' | '6M' | '1A' | '2A' | '5A' | 'MAX';

const REPORT_TYPES: ReportType[] = ['portfolio', 'tax', 'dividends', 'performance', 'transactions'];
const PERIOD_PRESETS: PeriodPreset[] = ['current', '1M', '3M', '6M', '1A', '2A', '5A', 'MAX'];

const toUpperText = (value: unknown): string => String(value || '').trim().toUpperCase();

const toIsoDate = (value: unknown): string => {
  const parsed = new Date(String(value || ''));
  return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString();
};

const toSafeText = (value: unknown): string => String(value || '').trim();

const decodeBase64ToBlob = (base64: string, contentType: string): Blob => {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return new Blob([bytes], { type: contentType || 'application/pdf' });
};

const normalizeReportType = (value: unknown): ReportType | 'other' => {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'portfolio') return 'portfolio';
  if (normalized === 'tax') return 'tax';
  if (normalized === 'dividends') return 'dividends';
  if (normalized === 'performance') return 'performance';
  if (['transactions', 'movement', 'movements', 'statement'].includes(normalized)) return 'transactions';
  return 'other';
};

const DocumentsPage = () => {
  const { t, i18n } = useTranslation();
  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    assets,
    transactions,
    refreshMetrics,
    refreshPortfolioData,
  } = usePortfolioData();

  const [selectedReportType, setSelectedReportType] = useState<ReportType>('portfolio');
  const [selectedPeriodPreset, setSelectedPeriodPreset] = useState<PeriodPreset>('current');
  const [reportTypeFilter, setReportTypeFilter] = useState<'all' | ReportType>('all');
  const [reportsLoading, setReportsLoading] = useState(false);
  const [reportsError, setReportsError] = useState<string | null>(null);
  const [reports, setReports] = useState<ReportRecord[]>([]);
  const [generating, setGenerating] = useState(false);
  const [generateMessage, setGenerateMessage] = useState<string | null>(null);
  const [activeContentReportId, setActiveContentReportId] = useState<string | null>(null);
  const [deletingReportId, setDeletingReportId] = useState<string | null>(null);
  const [deleteModalReport, setDeleteModalReport] = useState<ReportRecord | null>(null);
  const [previewState, setPreviewState] = useState<{ reportId: string; title: string; url: string } | null>(null);
  const [availableParsers, setAvailableParsers] = useState<ParserDescriptor[]>([]);
  const [selectedImportParser, setSelectedImportParser] = useState('auto');
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importing, setImporting] = useState(false);
  const [importSummary, setImportSummary] = useState<ImportB3Response | null>(null);
  const [importMessage, setImportMessage] = useState<string | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  useEffect(() => () => {
    if (previewState?.url) URL.revokeObjectURL(previewState.url);
  }, [previewState]);

  const portfolioOptions = useMemo(() => (
    portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name }))
  ), [portfolios]);

  const reportTypeOptions = useMemo(() => (
    REPORT_TYPES.map((type) => ({
      value: type,
      label: t(`documents.type.${type}`),
    }))
  ), [t]);

  const reportTypeFilterOptions = useMemo(() => ([
    { value: 'all', label: t('documents.type.all') },
    ...reportTypeOptions,
  ]), [reportTypeOptions, t]);

  const periodPresetOptions = useMemo(() => (
    PERIOD_PRESETS.map((period) => ({
      value: period,
      label: t(`documents.period.${period}`),
    }))
  ), [t]);

  const parserOptions = useMemo(() => ([
    { value: 'auto', label: t('documents.import.autoDetect') },
    ...availableParsers.map((parser) => ({
      value: parser.id,
      label: t(`documents.import.parsers.${parser.id}`, {
        defaultValue: `${parser.id} (${String(parser.provider || '').toUpperCase()})`,
      }),
    })),
  ]), [availableParsers, t]);

  const resolveStorageLabel = useCallback((report: ReportRecord): string => {
    const normalized = String(report.storage?.type || '').toLowerCase();
    if (normalized === 'local') return t('documents.storageType.local');
    if (normalized === 's3') return t('documents.storageType.s3');
    return t('documents.storageType.unknown');
  }, [t]);

  const resolvePeriodPayload = useCallback((): string | undefined => {
    if (selectedPeriodPreset === 'current') {
      if (selectedReportType === 'tax') return String(new Date().getUTCFullYear());
      return undefined;
    }
    if (selectedReportType === 'tax') {
      const nowYear = String(new Date().getUTCFullYear());
      if (selectedPeriodPreset === '1A') return nowYear;
      return nowYear;
    }
    return selectedPeriodPreset;
  }, [selectedPeriodPreset, selectedReportType]);

  const loadReports = useCallback(async () => {
    setReportsLoading(true);
    setReportsError(null);
    try {
      const payload = await api.listReports();
      setReports(Array.isArray(payload) ? payload : []);
    } catch (reason) {
      setReports([]);
      setReportsError(reason instanceof Error ? reason.message : t('documents.loadError'));
    } finally {
      setReportsLoading(false);
    }
  }, [t]);

  const loadParsers = useCallback(async () => {
    try {
      const payload = await api.listParsers();
      setAvailableParsers(Array.isArray(payload) ? payload : []);
    } catch {
      setAvailableParsers([]);
    }
  }, []);

  useEffect(() => {
    void loadReports();
  }, [loadReports]);

  useEffect(() => {
    void loadParsers();
  }, [loadParsers]);

  const sortedReports = useMemo(() => (
    [...reports].sort((left, right) => {
      const leftDate = toIsoDate(left.createdAt || left.updatedAt || left.fetched_at);
      const rightDate = toIsoDate(right.createdAt || right.updatedAt || right.fetched_at);
      return rightDate.localeCompare(leftDate);
    })
  ), [reports]);

  const filteredReports = useMemo(() => (
    sortedReports.filter((entry) => {
      if (reportTypeFilter === 'all') return true;
      return normalizeReportType(entry.reportType) === reportTypeFilter;
    })
  ), [reportTypeFilter, sortedReports]);

  const openReportContent = useCallback(async (report: ReportRecord, mode: 'preview' | 'download') => {
    if (!report.reportId) return;
    setActiveContentReportId(report.reportId);
    try {
      const content: ReportContentResponse = await api.getReportContent(report.reportId);
      const blob = decodeBase64ToBlob(content.dataBase64, content.contentType);
      const objectUrl = URL.createObjectURL(blob);

      if (mode === 'preview') {
        setPreviewState((previous) => {
          if (previous?.url) URL.revokeObjectURL(previous.url);
          return { reportId: report.reportId, title: content.filename || report.reportId, url: objectUrl };
        });
      } else {
        const anchor = document.createElement('a');
        anchor.href = objectUrl;
        anchor.download = content.filename || `${report.reportId}.pdf`;
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        setTimeout(() => URL.revokeObjectURL(objectUrl), 1500);
      }
    } catch (reason) {
      setGenerateMessage(reason instanceof Error ? reason.message : t('documents.downloadError'));
    } finally {
      setActiveContentReportId(null);
    }
  }, [t]);

  const closePreview = useCallback(() => {
    setPreviewState((previous) => {
      if (previous?.url) URL.revokeObjectURL(previous.url);
      return null;
    });
  }, []);

  const openDeleteModal = useCallback((report: ReportRecord) => {
    if (!report.reportId) return;
    setDeleteModalReport(report);
  }, []);

  const closeDeleteModal = useCallback(() => {
    setDeleteModalReport(null);
  }, []);

  const deleteReport = useCallback(async () => {
    if (!deleteModalReport?.reportId) return;
    setDeletingReportId(deleteModalReport.reportId);
    setGenerateMessage(null);
    try {
      await api.deleteReport(deleteModalReport.reportId);
      if (previewState?.reportId === deleteModalReport.reportId) {
        closePreview();
      }
      closeDeleteModal();
      await loadReports();
      setGenerateMessage(t('documents.deletedOk'));
    } catch (reason) {
      setGenerateMessage(reason instanceof Error ? reason.message : t('documents.deleteError'));
    } finally {
      setDeletingReportId(null);
    }
  }, [closeDeleteModal, closePreview, deleteModalReport?.reportId, loadReports, previewState?.reportId, t]);

  const generateReport = useCallback(async () => {
    if (!selectedPortfolio) return;
    setGenerating(true);
    setGenerateMessage(null);
    try {
      const period = resolvePeriodPayload();
      const created = await api.generateReport(
        selectedReportType,
        period,
        selectedPortfolio,
        i18n.language || 'pt-BR'
      );
      await loadReports();
      setGenerateMessage(t('documents.generatedOk'));
      if (created?.reportId) {
        await openReportContent(created, 'preview');
      }
    } catch (reason) {
      setGenerateMessage(reason instanceof Error ? reason.message : t('documents.generateError'));
    } finally {
      setGenerating(false);
    }
  }, [
    loadReports,
    openReportContent,
    resolvePeriodPayload,
    selectedPortfolio,
    selectedReportType,
    i18n.language,
    t,
  ]);

  const importB3File = useCallback(async () => {
    if (!selectedPortfolio) {
      setImportMessage(t('documents.import.errors.selectPortfolio'));
      return;
    }
    if (!importFile) {
      setImportMessage(t('documents.import.errors.selectFile'));
      return;
    }

    setImporting(true);
    setImportMessage(null);
    setImportSummary(null);
    try {
      const payload = await api.importB3File(
        selectedPortfolio,
        importFile,
        selectedImportParser === 'auto' ? undefined : selectedImportParser
      );
      setImportSummary(payload);
      await refreshPortfolioData();
      refreshMetrics();
      setImportMessage(t('documents.import.success'));
    } catch (reason) {
      setImportMessage(reason instanceof Error ? reason.message : t('documents.import.error'));
    } finally {
      setImporting(false);
    }
  }, [
    importFile,
    refreshMetrics,
    refreshPortfolioData,
    selectedImportParser,
    selectedPortfolio,
    t,
  ]);

  const warningLabel = useCallback((warning: string) => (
    t(`documents.import.warnings.${warning}`, { defaultValue: warning })
  ), [t]);

  const reasonLabel = useCallback((reason?: string) => (
    reason
      ? t(`documents.import.reasons.${reason}`, { defaultValue: reason })
      : t('assets.modal.noValue')
  ), [t]);

  const formatDateCell = useCallback((value?: string | null) => {
    if (!value) return t('assets.modal.noValue');
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleDateString(numberLocale);
  }, [numberLocale, t]);

  const exportTransactionsCsv = useCallback(() => {
    if (!selectedPortfolio || transactions.length === 0) {
      setGenerateMessage(t('documents.movementCsvError'));
      return;
    }

    const assetById = new Map(assets.map((asset) => [asset.assetId, asset]));
    const headers = [
      'date',
      'type',
      'ticker',
      'asset_name',
      'quantity',
      'price',
      'amount',
      'currency',
      'status',
      'source',
    ];

    const escapeCsv = (value: unknown) => {
      const text = String(value ?? '');
      if (!/[",\n]/.test(text)) return text;
      return `"${text.replace(/"/g, '""')}"`;
    };

    const rows = [...transactions]
      .sort((left, right) => String(right.date || '').localeCompare(String(left.date || '')))
      .map((tx) => {
        const asset = assetById.get(tx.assetId);
        return [
          tx.date || tx.createdAt || '',
          tx.type || '',
          toUpperText(asset?.ticker || ''),
          toSafeText(asset?.name || ''),
          tx.quantity ?? '',
          tx.price ?? '',
          tx.amount ?? '',
          tx.currency || asset?.currency || 'BRL',
          tx.status || '',
          tx.sourceDocId || tx.institution || '',
        ].map(escapeCsv).join(',');
      });

    const csv = `${headers.join(',')}\n${rows.join('\n')}`;
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const dateTag = new Date().toISOString().slice(0, 10);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `transactions-${selectedPortfolio}-${dateTag}.csv`;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }, [assets, selectedPortfolio, t, transactions]);

  return (
    <Layout>
      <div className="documents-page">
        <div className="documents-page__header">
          <div>
            <h1 className="documents-page__title">{t('documents.title')}</h1>
            <p className="documents-page__subtitle">{t('documents.subtitle')}</p>
          </div>
        </div>

        <section className="documents-card">
          <header className="documents-card__header">
            <h2>{t('documents.generateTitle')}</h2>
          </header>
          <div className="documents-page__controls">
            {portfolioOptions.length > 0 ? (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('documents.selectPortfolio')}
                className="documents-page__dropdown documents-page__dropdown--portfolio"
                size="sm"
              />
            ) : null}
            <SharedDropdown
              value={selectedReportType}
              options={reportTypeOptions}
              onChange={(value) => setSelectedReportType(value as ReportType)}
              ariaLabel={t('documents.selectType')}
              className="documents-page__dropdown"
              size="sm"
            />
            <SharedDropdown
              value={selectedPeriodPreset}
              options={periodPresetOptions}
              onChange={(value) => setSelectedPeriodPreset(value as PeriodPreset)}
              ariaLabel={t('documents.selectPeriod')}
              className="documents-page__dropdown"
              size="sm"
            />
            <button
              type="button"
              className="documents-page__action"
              onClick={generateReport}
              disabled={!selectedPortfolio || generating}
            >
              {generating ? t('documents.generating') : t('documents.generate')}
            </button>
            <button
              type="button"
              className="documents-page__action documents-page__action--ghost"
              onClick={() => void loadReports()}
              disabled={reportsLoading}
            >
              {t('documents.refresh')}
            </button>
          </div>

          <div className="documents-page__movement">
            <div className="documents-page__movement-copy">
              <h3>{t('documents.movementTitle')}</h3>
              <p>{t('documents.movementHint')}</p>
            </div>
            <button
              type="button"
              className="documents-page__action"
              onClick={exportTransactionsCsv}
              disabled={!selectedPortfolio || transactions.length === 0}
            >
              {t('documents.exportCsv')}
            </button>
          </div>

          {generateMessage ? (
            <p className="documents-page__message">{generateMessage}</p>
          ) : null}
        </section>

        <section className="documents-card">
          <header className="documents-card__header">
            <h2>{t('documents.import.title')}</h2>
          </header>
          <p className="documents-page__import-hint">{t('documents.import.hint')}</p>

          <div className="documents-page__controls">
            {portfolioOptions.length > 0 ? (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('documents.selectPortfolio')}
                className="documents-page__dropdown documents-page__dropdown--portfolio"
                size="sm"
              />
            ) : null}
            <SharedDropdown
              value={selectedImportParser}
              options={parserOptions}
              onChange={setSelectedImportParser}
              ariaLabel={t('documents.import.selectParser')}
              className="documents-page__dropdown"
              size="sm"
            />
            <label className="documents-page__file-picker">
              <input
                type="file"
                accept=".xlsx,.xlsm,.xls"
                className="documents-page__file-input"
                onChange={(event) => {
                  const file = event.target.files && event.target.files[0] ? event.target.files[0] : null;
                  setImportFile(file);
                }}
              />
              <span>{importFile?.name || t('documents.import.pickFile')}</span>
            </label>
            <button
              type="button"
              className="documents-page__action"
              onClick={() => void importB3File()}
              disabled={!selectedPortfolio || !importFile || importing}
            >
              {importing ? t('documents.import.importing') : t('documents.import.action')}
            </button>
          </div>

          {importMessage ? (
            <p className="documents-page__message">{importMessage}</p>
          ) : null}

          {importSummary ? (
            <div className="documents-page__import-summary">
              <div className="documents-page__import-summary-head">
                <h3>{t('documents.import.summary.title')}</h3>
                <span>{importSummary.sourceFile}</span>
              </div>
              <div className="documents-page__import-grid">
                <div className="documents-page__import-item">
                  <span>{t('documents.import.summary.parser')}</span>
                  <strong>{importSummary.parser}</strong>
                </div>
                <div className="documents-page__import-item">
                  <span>{t('documents.import.summary.detection')}</span>
                  <strong>{importSummary.detectionMode}</strong>
                </div>
                <div className="documents-page__import-item">
                  <span>{t('documents.import.summary.assets')}</span>
                  <strong>
                    {`+${importSummary.stats.assets.created} / ~${importSummary.stats.assets.updated || 0}`}
                  </strong>
                </div>
                <div className="documents-page__import-item">
                  <span>{t('documents.import.summary.transactions')}</span>
                  <strong>
                    {`+${importSummary.stats.transactions.created} / -${importSummary.stats.transactions.skipped}`}
                  </strong>
                </div>
                <div className="documents-page__import-item">
                  <span>{t('documents.import.summary.aliases')}</span>
                  <strong>
                    {`+${importSummary.stats.aliases.created} / -${importSummary.stats.aliases.skipped}`}
                  </strong>
                </div>
                <div className="documents-page__import-item">
                  <span>{t('documents.import.summary.filtered')}</span>
                  <strong>{importSummary.stats.transactions.filtered || 0}</strong>
                </div>
              </div>
              {Array.isArray(importSummary.warnings) && importSummary.warnings.length > 0 ? (
                <ul className="documents-page__import-warnings">
                  {importSummary.warnings.map((warning) => (
                    <li key={warning}>{warningLabel(warning)}</li>
                  ))}
                </ul>
              ) : null}

              <div className="documents-page__import-report">
                <details className="documents-page__import-report-section" open>
                  <summary>
                    {t('documents.import.report.assetsCreated', { count: importSummary.report.assets.created.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.name')}</th>
                          <th>{t('documents.import.report.table.class')}</th>
                          <th>{t('documents.import.report.table.quantity')}</th>
                          <th>{t('documents.import.report.table.status')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.assets.created.length === 0 ? (
                          <tr>
                            <td colSpan={6}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.assets.created.map((entry, index) => (
                          <tr key={`${entry.assetId || entry.ticker || 'asset-created'}-${index}`}>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.name || t('assets.modal.noValue')}</td>
                            <td>{entry.assetClass || t('assets.modal.noValue')}</td>
                            <td>{entry.quantity}</td>
                            <td>{entry.status || t('assets.modal.noValue')}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section">
                  <summary>
                    {t('documents.import.report.assetsUpdated', { count: importSummary.report.assets.updated.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.name')}</th>
                          <th>{t('documents.import.report.table.class')}</th>
                          <th>{t('documents.import.report.table.quantity')}</th>
                          <th>{t('documents.import.report.table.status')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.assets.updated.length === 0 ? (
                          <tr>
                            <td colSpan={6}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.assets.updated.map((entry, index) => (
                          <tr key={`${entry.assetId || entry.ticker || 'asset-updated'}-${index}`}>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.name || t('assets.modal.noValue')}</td>
                            <td>{entry.assetClass || t('assets.modal.noValue')}</td>
                            <td>{entry.quantity}</td>
                            <td>{entry.status || t('assets.modal.noValue')}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section">
                  <summary>
                    {t('documents.import.report.assetsSkipped', { count: importSummary.report.assets.skipped.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.name')}</th>
                          <th>{t('documents.import.report.table.class')}</th>
                          <th>{t('documents.import.report.table.quantity')}</th>
                          <th>{t('documents.import.report.table.status')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.assets.skipped.length === 0 ? (
                          <tr>
                            <td colSpan={6}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.assets.skipped.map((entry, index) => (
                          <tr key={`${entry.assetId || entry.ticker || 'asset-skipped'}-${index}`}>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.name || t('assets.modal.noValue')}</td>
                            <td>{entry.assetClass || t('assets.modal.noValue')}</td>
                            <td>{entry.quantity}</td>
                            <td>{entry.status || t('assets.modal.noValue')}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section" open>
                  <summary>
                    {t('documents.import.report.transactionsCreated', { count: importSummary.report.transactions.created.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.type')}</th>
                          <th>{t('documents.import.report.table.date')}</th>
                          <th>{t('documents.import.report.table.quantity')}</th>
                          <th>{t('documents.import.report.table.amount')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.transactions.created.length === 0 ? (
                          <tr>
                            <td colSpan={6}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.transactions.created.map((entry, index) => (
                          <tr key={`${entry.transId || entry.dedupKey || 'trans-created'}-${index}`}>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.type || t('assets.modal.noValue')}</td>
                            <td>{formatDateCell(entry.date)}</td>
                            <td>{entry.quantity}</td>
                            <td>{`${entry.currency || 'BRL'} ${entry.amount}`}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section">
                  <summary>
                    {t('documents.import.report.transactionsSkipped', { count: importSummary.report.transactions.skipped.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.type')}</th>
                          <th>{t('documents.import.report.table.date')}</th>
                          <th>{t('documents.import.report.table.quantity')}</th>
                          <th>{t('documents.import.report.table.amount')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.transactions.skipped.length === 0 ? (
                          <tr>
                            <td colSpan={6}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.transactions.skipped.map((entry, index) => (
                          <tr key={`${entry.dedupKey || `${entry.ticker}-${entry.date}-${index}`}`}>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.type || t('assets.modal.noValue')}</td>
                            <td>{formatDateCell(entry.date)}</td>
                            <td>{entry.quantity}</td>
                            <td>{`${entry.currency || 'BRL'} ${entry.amount}`}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section">
                  <summary>
                    {t('documents.import.report.transactionsFiltered', { count: importSummary.report.transactions.filtered.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.type')}</th>
                          <th>{t('documents.import.report.table.date')}</th>
                          <th>{t('documents.import.report.table.quantity')}</th>
                          <th>{t('documents.import.report.table.amount')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.transactions.filtered.length === 0 ? (
                          <tr>
                            <td colSpan={6}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.transactions.filtered.map((entry, index) => (
                          <tr key={`${entry.dedupKey || `${entry.ticker}-${entry.date}-${index}`}`}>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.type || t('assets.modal.noValue')}</td>
                            <td>{formatDateCell(entry.date)}</td>
                            <td>{entry.quantity}</td>
                            <td>{`${entry.currency || 'BRL'} ${entry.amount}`}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section" open>
                  <summary>
                    {t('documents.import.report.aliasesCreated', { count: importSummary.report.aliases.created.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.alias')}</th>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.source')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.aliases.created.length === 0 ? (
                          <tr>
                            <td colSpan={4}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.aliases.created.map((entry, index) => (
                          <tr key={`${entry.normalizedName || 'alias-created'}-${entry.ticker || index}`}>
                            <td>{entry.normalizedName || t('assets.modal.noValue')}</td>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.source || t('assets.modal.noValue')}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>

                <details className="documents-page__import-report-section">
                  <summary>
                    {t('documents.import.report.aliasesSkipped', { count: importSummary.report.aliases.skipped.length })}
                  </summary>
                  <div className="documents-page__table-wrap">
                    <table className="documents-page__table">
                      <thead>
                        <tr>
                          <th>{t('documents.import.report.table.alias')}</th>
                          <th>{t('documents.import.report.table.ticker')}</th>
                          <th>{t('documents.import.report.table.source')}</th>
                          <th>{t('documents.import.report.table.reason')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importSummary.report.aliases.skipped.length === 0 ? (
                          <tr>
                            <td colSpan={4}>{t('documents.import.report.empty')}</td>
                          </tr>
                        ) : importSummary.report.aliases.skipped.map((entry, index) => (
                          <tr key={`${entry.normalizedName || 'alias-skipped'}-${entry.ticker || index}`}>
                            <td>{entry.normalizedName || t('assets.modal.noValue')}</td>
                            <td>{entry.ticker || t('assets.modal.noValue')}</td>
                            <td>{entry.source || t('assets.modal.noValue')}</td>
                            <td>{reasonLabel(entry.reason)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              </div>
            </div>
          ) : null}
        </section>

        <section className="documents-card">
          <header className="documents-card__header">
            <h2>{t('documents.listTitle')}</h2>
            <SharedDropdown
              value={reportTypeFilter}
              options={reportTypeFilterOptions}
              onChange={(value) => setReportTypeFilter(value as 'all' | ReportType)}
              ariaLabel={t('documents.selectType')}
              className="documents-page__dropdown"
              size="sm"
            />
          </header>

          {reportsLoading ? (
            <div className="documents-page__state">{t('common.loading')}</div>
          ) : null}

          {!reportsLoading && reportsError ? (
            <div className="documents-page__state documents-page__state--error">
              <p>{t('documents.loadError')}</p>
              <code>{reportsError}</code>
            </div>
          ) : null}

          {!reportsLoading && !reportsError && filteredReports.length === 0 ? (
            <div className="documents-page__state">{t('documents.noReports')}</div>
          ) : null}

          {!reportsLoading && !reportsError && filteredReports.length > 0 ? (
            <div className="documents-page__table-wrap">
              <table className="documents-page__table">
                <thead>
                  <tr>
                    <th>{t('documents.table.createdAt')}</th>
                    <th>{t('documents.table.type')}</th>
                    <th>{t('documents.table.period')}</th>
                    <th>{t('documents.table.storage')}</th>
                    <th>{t('documents.table.actions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredReports.map((report) => {
                    const createdAt = report.createdAt || report.updatedAt || report.fetched_at;
                    const createdText = createdAt
                      ? new Date(createdAt).toLocaleString(numberLocale, {
                        day: '2-digit',
                        month: '2-digit',
                        year: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit',
                      })
                      : t('assets.modal.noValue');
                    const normalizedType = normalizeReportType(report.reportType);
                    const typeLabel = normalizedType === 'other'
                      ? toSafeText(report.reportType || '').toUpperCase()
                      : t(`documents.type.${normalizedType}`);
                    return (
                      <tr key={report.reportId}>
                        <td>{createdText}</td>
                        <td>{typeLabel}</td>
                        <td>{report.period || t('documents.period.current')}</td>
                        <td>{resolveStorageLabel(report)}</td>
                        <td>
                          <div className="documents-page__row-actions">
                            <button
                              type="button"
                              className="documents-page__row-btn"
                              onClick={() => void openReportContent(report, 'preview')}
                              disabled={activeContentReportId === report.reportId || deletingReportId === report.reportId}
                            >
                              {activeContentReportId === report.reportId
                                ? t('documents.loadingContent')
                                : t('documents.actions.preview')}
                            </button>
                            <button
                              type="button"
                              className="documents-page__row-btn"
                              onClick={() => void openReportContent(report, 'download')}
                              disabled={activeContentReportId === report.reportId || deletingReportId === report.reportId}
                            >
                              {t('documents.actions.download')}
                            </button>
                            <button
                              type="button"
                              className="documents-page__row-btn documents-page__row-btn--danger"
                              onClick={() => openDeleteModal(report)}
                              disabled={activeContentReportId === report.reportId || deletingReportId === report.reportId}
                            >
                              {deletingReportId === report.reportId
                                ? t('documents.actions.deleting')
                                : t('documents.actions.delete')}
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </section>

        {previewState ? (
          <div className="documents-preview" role="dialog" aria-modal="true" aria-label={t('documents.preview.title')}>
            <div className="documents-preview__backdrop" onClick={closePreview} />
            <div className="documents-preview__panel">
              <header className="documents-preview__header">
                <h3>{previewState.title}</h3>
                <button type="button" className="documents-preview__close" onClick={closePreview}>
                  {t('documents.preview.close')}
                </button>
              </header>
              <iframe title={previewState.title} src={previewState.url} className="documents-preview__frame" />
            </div>
          </div>
        ) : null}

        {deleteModalReport ? (
          <div className="documents-confirm" role="dialog" aria-modal="true" aria-label={t('documents.deleteModal.title')}>
            <div className="documents-confirm__backdrop" onClick={closeDeleteModal} />
            <div className="documents-confirm__panel">
              <h3>{t('documents.deleteModal.title')}</h3>
              <p>{t('documents.deleteModal.body', { reportId: deleteModalReport.reportId })}</p>
              <div className="documents-confirm__actions">
                <button
                  type="button"
                  className="documents-confirm__btn documents-confirm__btn--cancel"
                  onClick={closeDeleteModal}
                  disabled={deletingReportId === deleteModalReport.reportId}
                >
                  {t('common.cancel')}
                </button>
                <button
                  type="button"
                  className="documents-confirm__btn documents-confirm__btn--delete"
                  onClick={() => void deleteReport()}
                  disabled={deletingReportId === deleteModalReport.reportId}
                >
                  {deletingReportId === deleteModalReport.reportId
                    ? t('documents.actions.deleting')
                    : t('documents.actions.delete')}
                </button>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </Layout>
  );
};

export default DocumentsPage;
