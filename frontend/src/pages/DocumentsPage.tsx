import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type ReportContentResponse,
  type ReportRecord,
} from '../services/api';
import './DocumentsPage.scss';

type ReportType = 'portfolio' | 'tax' | 'dividends' | 'performance' | 'transactions';
type PeriodPreset = 'current' | '1M' | '3M' | '6M' | '1A' | '2A' | '5A' | 'MAX';
type CombineMode = 'preview' | 'download';

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
  } = usePortfolioData();

  const [selectedReportType, setSelectedReportType] = useState<ReportType>('portfolio');
  const [selectedPeriodPreset, setSelectedPeriodPreset] = useState<PeriodPreset>('current');
  const [reportTypeFilter, setReportTypeFilter] = useState<'all' | ReportType>('all');
  const [reportsLoading, setReportsLoading] = useState(false);
  const [reportsError, setReportsError] = useState<string | null>(null);
  const [reports, setReports] = useState<ReportRecord[]>([]);
  const [selectedReportIds, setSelectedReportIds] = useState<string[]>([]);
  const [generating, setGenerating] = useState(false);
  const [generateMessage, setGenerateMessage] = useState<string | null>(null);
  const [activeContentReportId, setActiveContentReportId] = useState<string | null>(null);
  const [deletingReportId, setDeletingReportId] = useState<string | null>(null);
  const [deleteModalReport, setDeleteModalReport] = useState<ReportRecord | null>(null);
  const [bulkDeleteModalOpen, setBulkDeleteModalOpen] = useState(false);
  const [bulkDeleteRunning, setBulkDeleteRunning] = useState(false);
  const [combineMode, setCombineMode] = useState<CombineMode | null>(null);
  const [previewState, setPreviewState] = useState<{ reportId: string; title: string; url: string } | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const isBulkBusy = bulkDeleteRunning || combineMode !== null;

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

  useEffect(() => {
    void loadReports();
  }, [loadReports]);

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

  useEffect(() => {
    const available = new Set(reports.map((item) => String(item.reportId || '').trim()).filter(Boolean));
    setSelectedReportIds((previous) => previous.filter((reportId) => available.has(reportId)));
  }, [reports]);

  const selectedReportIdSet = useMemo(() => new Set(selectedReportIds), [selectedReportIds]);
  const selectedCount = selectedReportIds.length;
  const selectedFilteredCount = useMemo(
    () => filteredReports.filter((report) => selectedReportIdSet.has(report.reportId)).length,
    [filteredReports, selectedReportIdSet]
  );
  const allFilteredSelected = filteredReports.length > 0 && selectedFilteredCount === filteredReports.length;

  const toggleSelectReport = useCallback((reportId: string, selected: boolean) => {
    if (!reportId) return;
    setSelectedReportIds((previous) => {
      const currentSet = new Set(previous);
      if (selected) currentSet.add(reportId);
      else currentSet.delete(reportId);
      return [...currentSet];
    });
  }, []);

  const toggleSelectAllFiltered = useCallback((selected: boolean) => {
    if (selected) {
      setSelectedReportIds((previous) => {
        const currentSet = new Set(previous);
        for (const report of filteredReports) {
          if (report.reportId) currentSet.add(report.reportId);
        }
        return [...currentSet];
      });
      return;
    }
    setSelectedReportIds((previous) => {
      const toRemove = new Set(filteredReports.map((report) => report.reportId));
      return previous.filter((reportId) => !toRemove.has(reportId));
    });
  }, [filteredReports]);

  const clearSelection = useCallback(() => {
    setSelectedReportIds([]);
  }, []);

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

  const combineSelectedReports = useCallback(async (mode: CombineMode) => {
    if (selectedReportIds.length < 2) {
      setGenerateMessage(t('documents.bulk.combineMin'));
      return;
    }
    setCombineMode(mode);
    setGenerateMessage(null);
    try {
      const content = await api.combineReports(selectedReportIds, i18n.language || 'pt-BR');
      const blob = decodeBase64ToBlob(content.dataBase64, content.contentType);
      const objectUrl = URL.createObjectURL(blob);
      if (mode === 'preview') {
        setPreviewState((previous) => {
          if (previous?.url) URL.revokeObjectURL(previous.url);
          return {
            reportId: content.reportId || 'combined',
            title: content.filename || 'combined-reports.pdf',
            url: objectUrl,
          };
        });
      } else {
        const anchor = document.createElement('a');
        anchor.href = objectUrl;
        anchor.download = content.filename || 'combined-reports.pdf';
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        setTimeout(() => URL.revokeObjectURL(objectUrl), 1500);
      }
      setGenerateMessage(t('documents.bulk.combineOk', { count: selectedReportIds.length }));
    } catch (reason) {
      setGenerateMessage(reason instanceof Error ? reason.message : t('documents.bulk.combineError'));
    } finally {
      setCombineMode(null);
    }
  }, [i18n.language, selectedReportIds, t]);

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
      setSelectedReportIds((previous) => previous.filter((reportId) => reportId !== deleteModalReport.reportId));
      closeDeleteModal();
      await loadReports();
      setGenerateMessage(t('documents.deletedOk'));
    } catch (reason) {
      setGenerateMessage(reason instanceof Error ? reason.message : t('documents.deleteError'));
    } finally {
      setDeletingReportId(null);
    }
  }, [closeDeleteModal, closePreview, deleteModalReport?.reportId, loadReports, previewState?.reportId, t]);

  const openBulkDeleteModal = useCallback(() => {
    if (selectedReportIds.length === 0) return;
    setBulkDeleteModalOpen(true);
  }, [selectedReportIds.length]);

  const closeBulkDeleteModal = useCallback(() => {
    setBulkDeleteModalOpen(false);
  }, []);

  const deleteSelectedReports = useCallback(async () => {
    if (selectedReportIds.length === 0) return;
    setBulkDeleteRunning(true);
    setGenerateMessage(null);
    try {
      const deleteTargets = [...selectedReportIds];
      const results = await Promise.allSettled(deleteTargets.map((reportId) => api.deleteReport(reportId)));
      const deletedIds: string[] = [];
      let failures = 0;
      results.forEach((result, index) => {
        if (result.status === 'fulfilled') {
          deletedIds.push(deleteTargets[index]);
        } else {
          failures += 1;
        }
      });

      if (previewState?.reportId && deletedIds.includes(previewState.reportId)) {
        closePreview();
      }
      setSelectedReportIds((previous) => previous.filter((reportId) => !deletedIds.includes(reportId)));
      closeBulkDeleteModal();
      await loadReports();

      if (failures === 0) {
        setGenerateMessage(t('documents.bulk.deleteOk', { count: deletedIds.length }));
      } else {
        setGenerateMessage(t('documents.bulk.deletePartial', { deleted: deletedIds.length, failed: failures }));
      }
    } catch (reason) {
      setGenerateMessage(reason instanceof Error ? reason.message : t('documents.bulk.deleteError'));
    } finally {
      setBulkDeleteRunning(false);
    }
  }, [closeBulkDeleteModal, closePreview, loadReports, previewState?.reportId, selectedReportIds, t]);

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
              disabled={!selectedPortfolio || generating || isBulkBusy}
            >
              {generating ? t('documents.generating') : t('documents.generate')}
            </button>
            <button
              type="button"
              className="documents-page__action documents-page__action--ghost"
              onClick={() => void loadReports()}
              disabled={reportsLoading || isBulkBusy}
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
              disabled={!selectedPortfolio || transactions.length === 0 || isBulkBusy}
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

          <div className="documents-page__bulk-actions">
            <span className="documents-page__bulk-summary">
              {t('documents.bulk.selectedCount', { count: selectedCount })}
            </span>
            <button
              type="button"
              className="documents-page__row-btn"
              onClick={clearSelection}
              disabled={selectedCount === 0 || isBulkBusy}
            >
              {t('documents.bulk.clear')}
            </button>
            <button
              type="button"
              className="documents-page__row-btn"
              onClick={() => void combineSelectedReports('preview')}
              disabled={selectedCount < 2 || isBulkBusy}
            >
              {combineMode === 'preview'
                ? t('documents.loadingContent')
                : t('documents.bulk.combinePreview')}
            </button>
            <button
              type="button"
              className="documents-page__row-btn"
              onClick={() => void combineSelectedReports('download')}
              disabled={selectedCount < 2 || isBulkBusy}
            >
              {combineMode === 'download'
                ? t('documents.loadingContent')
                : t('documents.bulk.combineDownload')}
            </button>
            <button
              type="button"
              className="documents-page__row-btn documents-page__row-btn--danger"
              onClick={openBulkDeleteModal}
              disabled={selectedCount === 0 || isBulkBusy}
            >
              {t('documents.bulk.deleteSelected')}
            </button>
          </div>

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
                    <th className="documents-page__checkbox-col">
                      <input
                        type="checkbox"
                        className="documents-page__checkbox"
                        aria-label={t('documents.bulk.selectAll')}
                        checked={allFilteredSelected}
                        disabled={isBulkBusy}
                        onChange={(event) => toggleSelectAllFiltered(event.target.checked)}
                      />
                    </th>
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
                    const isSelected = selectedReportIdSet.has(report.reportId);

                    return (
                      <tr key={report.reportId} className={isSelected ? 'documents-page__table-row--selected' : undefined}>
                        <td className="documents-page__checkbox-col">
                          <input
                            type="checkbox"
                            className="documents-page__checkbox"
                            aria-label={t('documents.bulk.selectOne')}
                            checked={isSelected}
                            onChange={(event) => toggleSelectReport(report.reportId, event.target.checked)}
                            disabled={isBulkBusy}
                          />
                        </td>
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
                              disabled={
                                activeContentReportId === report.reportId ||
                                deletingReportId === report.reportId ||
                                isBulkBusy
                              }
                            >
                              {activeContentReportId === report.reportId
                                ? t('documents.loadingContent')
                                : t('documents.actions.preview')}
                            </button>
                            <button
                              type="button"
                              className="documents-page__row-btn"
                              onClick={() => void openReportContent(report, 'download')}
                              disabled={
                                activeContentReportId === report.reportId ||
                                deletingReportId === report.reportId ||
                                isBulkBusy
                              }
                            >
                              {t('documents.actions.download')}
                            </button>
                            <button
                              type="button"
                              className="documents-page__row-btn documents-page__row-btn--danger"
                              onClick={() => openDeleteModal(report)}
                              disabled={
                                activeContentReportId === report.reportId ||
                                deletingReportId === report.reportId ||
                                isBulkBusy
                              }
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

        {bulkDeleteModalOpen ? (
          <div className="documents-confirm" role="dialog" aria-modal="true" aria-label={t('documents.bulk.deleteModal.title')}>
            <div className="documents-confirm__backdrop" onClick={closeBulkDeleteModal} />
            <div className="documents-confirm__panel">
              <h3>{t('documents.bulk.deleteModal.title')}</h3>
              <p>{t('documents.bulk.deleteModal.body', { count: selectedCount })}</p>
              <div className="documents-confirm__actions">
                <button
                  type="button"
                  className="documents-confirm__btn documents-confirm__btn--cancel"
                  onClick={closeBulkDeleteModal}
                  disabled={bulkDeleteRunning}
                >
                  {t('common.cancel')}
                </button>
                <button
                  type="button"
                  className="documents-confirm__btn documents-confirm__btn--delete"
                  onClick={() => void deleteSelectedReports()}
                  disabled={bulkDeleteRunning}
                >
                  {bulkDeleteRunning
                    ? t('documents.actions.deleting')
                    : t('documents.bulk.deleteSelected')}
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
