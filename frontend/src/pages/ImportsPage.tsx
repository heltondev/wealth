import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type ImportAliasReportEntry,
  type ImportAssetReportEntry,
  type ImportB3Response,
  type ImportTransactionReportEntry,
  type ParserDescriptor,
} from '../services/api';
import './DocumentsPage.scss';

type AssetSectionConfig = {
  key: 'created' | 'updated' | 'skipped';
  labelKey: string;
  defaultOpen?: boolean;
};

type TransactionSectionConfig = {
  key: 'created' | 'skipped' | 'filtered';
  labelKey: string;
  defaultOpen?: boolean;
};

type AliasSectionConfig = {
  key: 'created' | 'skipped';
  labelKey: string;
  defaultOpen?: boolean;
};

const ASSET_SECTIONS: AssetSectionConfig[] = [
  { key: 'created', labelKey: 'documents.import.report.assetsCreated', defaultOpen: true },
  { key: 'updated', labelKey: 'documents.import.report.assetsUpdated' },
  { key: 'skipped', labelKey: 'documents.import.report.assetsSkipped' },
];

const TRANSACTION_SECTIONS: TransactionSectionConfig[] = [
  { key: 'created', labelKey: 'documents.import.report.transactionsCreated', defaultOpen: true },
  { key: 'skipped', labelKey: 'documents.import.report.transactionsSkipped' },
  { key: 'filtered', labelKey: 'documents.import.report.transactionsFiltered' },
];

const ALIAS_SECTIONS: AliasSectionConfig[] = [
  { key: 'created', labelKey: 'documents.import.report.aliasesCreated', defaultOpen: true },
  { key: 'skipped', labelKey: 'documents.import.report.aliasesSkipped' },
];

const FALLBACK_PARSERS: ParserDescriptor[] = [
  { id: 'robinhood-activity', provider: 'robinhood' },
  { id: 'computershare-espp', provider: 'computershare' },
  { id: 'computershare-holdings-pdf', provider: 'computershare' },
];

const ImportsPage = () => {
  const { t, i18n } = useTranslation();
  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    refreshMetrics,
    refreshPortfolioData,
  } = usePortfolioData();

  const [availableParsers, setAvailableParsers] = useState<ParserDescriptor[]>([]);
  const [selectedImportParser, setSelectedImportParser] = useState('auto');
  const [importFile, setImportFile] = useState<File | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [importing, setImporting] = useState(false);
  const [importPreview, setImportPreview] = useState<ImportB3Response | null>(null);
  const [importSummary, setImportSummary] = useState<ImportB3Response | null>(null);
  const [importMessage, setImportMessage] = useState<string | null>(null);
  const [confirmImportModalOpen, setConfirmImportModalOpen] = useState(false);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const portfolioOptions = useMemo(() => (
    portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name }))
  ), [portfolios]);

  const parserCatalog = useMemo(() => {
    const map = new Map<string, ParserDescriptor>();
    for (const parser of [...FALLBACK_PARSERS, ...availableParsers]) {
      if (!parser?.id) continue;
      map.set(parser.id, parser);
    }
    return Array.from(map.values());
  }, [availableParsers]);

  const parserOptions = useMemo(() => ([
    { value: 'auto', label: t('documents.import.autoDetect') },
    ...parserCatalog.map((parser) => ({
      value: parser.id,
      label: t(`documents.import.parsers.${parser.id}`, {
        defaultValue: `${parser.id} (${String(parser.provider || '').toUpperCase()})`,
      }),
    })),
  ]), [parserCatalog, t]);

  const loadParsers = useCallback(async () => {
    try {
      const payload = await api.listParsers();
      setAvailableParsers(Array.isArray(payload) ? payload : []);
    } catch {
      setAvailableParsers([]);
    }
  }, []);

  useEffect(() => {
    void loadParsers();
  }, [loadParsers]);

  const previewImport = useCallback(async () => {
    if (!selectedPortfolio) {
      setImportMessage(t('documents.import.errors.selectPortfolio'));
      return;
    }
    if (!importFile) {
      setImportMessage(t('documents.import.errors.selectFile'));
      return;
    }

    setPreviewing(true);
    setImportMessage(null);
    setImportSummary(null);
    try {
      const payload = await api.importB3File(
        selectedPortfolio,
        importFile,
        {
          parserId: selectedImportParser === 'auto' ? undefined : selectedImportParser,
          dryRun: true,
        }
      );
      setImportPreview(payload);
      setImportMessage(t('documents.import.previewReady'));
    } catch (reason) {
      setImportPreview(null);
      setImportMessage(reason instanceof Error ? reason.message : t('documents.import.previewError'));
    } finally {
      setPreviewing(false);
    }
  }, [
    importFile,
    selectedImportParser,
    selectedPortfolio,
    t,
  ]);

  const executeImport = useCallback(async () => {
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
        {
          parserId: selectedImportParser === 'auto' ? undefined : selectedImportParser,
          dryRun: false,
        }
      );
      setImportSummary(payload);
      setImportPreview(null);
      setConfirmImportModalOpen(false);
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

  const renderAssetSection = useCallback((config: AssetSectionConfig, rows: ImportAssetReportEntry[]) => (
    <details className="documents-page__import-report-section" open={config.defaultOpen}>
      <summary>{t(config.labelKey, { count: rows.length })}</summary>
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
            {rows.length === 0 ? (
              <tr>
                <td colSpan={6}>{t('documents.import.report.empty')}</td>
              </tr>
            ) : rows.map((entry, index) => (
              <tr key={`${entry.assetId || entry.ticker || config.key}-${index}`}>
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
  ), [reasonLabel, t]);

  const renderTransactionSection = useCallback((config: TransactionSectionConfig, rows: ImportTransactionReportEntry[]) => (
    <details className="documents-page__import-report-section" open={config.defaultOpen}>
      <summary>{t(config.labelKey, { count: rows.length })}</summary>
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
            {rows.length === 0 ? (
              <tr>
                <td colSpan={6}>{t('documents.import.report.empty')}</td>
              </tr>
            ) : rows.map((entry, index) => (
              <tr key={`${entry.transId || entry.dedupKey || `${config.key}-${index}`}`}>
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
  ), [formatDateCell, reasonLabel, t]);

  const renderAliasSection = useCallback((config: AliasSectionConfig, rows: ImportAliasReportEntry[]) => (
    <details className="documents-page__import-report-section" open={config.defaultOpen}>
      <summary>{t(config.labelKey, { count: rows.length })}</summary>
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
            {rows.length === 0 ? (
              <tr>
                <td colSpan={4}>{t('documents.import.report.empty')}</td>
              </tr>
            ) : rows.map((entry, index) => (
              <tr key={`${entry.normalizedName || config.key}-${entry.ticker || index}`}>
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
  ), [reasonLabel, t]);

  const renderImportResult = useCallback((payload: ImportB3Response, title: string, withProceedAction = false) => (
    <div className="documents-page__import-summary">
      <div className="documents-page__import-summary-head">
        <h3>{title}</h3>
        <span>{payload.sourceFile}</span>
      </div>
      <div className="documents-page__import-grid">
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.mode')}</span>
          <strong>{payload.dryRun ? t('documents.import.modes.preview') : t('documents.import.modes.imported')}</strong>
        </div>
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.parser')}</span>
          <strong>{payload.parser}</strong>
        </div>
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.detection')}</span>
          <strong>{payload.detectionMode}</strong>
        </div>
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.assets')}</span>
          <strong>{`+${payload.stats.assets.created} / ~${payload.stats.assets.updated || 0}`}</strong>
        </div>
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.transactions')}</span>
          <strong>{`+${payload.stats.transactions.created} / -${payload.stats.transactions.skipped}`}</strong>
        </div>
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.aliases')}</span>
          <strong>{`+${payload.stats.aliases.created} / -${payload.stats.aliases.skipped}`}</strong>
        </div>
        <div className="documents-page__import-item">
          <span>{t('documents.import.summary.filtered')}</span>
          <strong>{payload.stats.transactions.filtered || 0}</strong>
        </div>
      </div>

      {withProceedAction ? (
        <div className="documents-page__import-actions">
          <button
            type="button"
            className="documents-page__action"
            onClick={() => setConfirmImportModalOpen(true)}
            disabled={importing || previewing}
          >
            {t('documents.import.confirm.action')}
          </button>
        </div>
      ) : null}

      {Array.isArray(payload.warnings) && payload.warnings.length > 0 ? (
        <ul className="documents-page__import-warnings">
          {payload.warnings.map((warning) => (
            <li key={warning}>{warningLabel(warning)}</li>
          ))}
        </ul>
      ) : null}

      <div className="documents-page__import-report">
        {ASSET_SECTIONS.map((section) => (
          <div key={`asset-${section.key}`}>
            {renderAssetSection(section, payload.report.assets[section.key])}
          </div>
        ))}
        {TRANSACTION_SECTIONS.map((section) => (
          <div key={`transaction-${section.key}`}>
            {renderTransactionSection(section, payload.report.transactions[section.key])}
          </div>
        ))}
        {ALIAS_SECTIONS.map((section) => (
          <div key={`alias-${section.key}`}>
            {renderAliasSection(section, payload.report.aliases[section.key])}
          </div>
        ))}
      </div>
    </div>
  ), [
    importing,
    previewing,
    renderAliasSection,
    renderAssetSection,
    renderTransactionSection,
    t,
    warningLabel,
  ]);

  return (
    <Layout>
      <div className="documents-page">
        <div className="documents-page__header">
          <div>
            <h1 className="documents-page__title">{t('imports.title')}</h1>
            <p className="documents-page__subtitle">{t('imports.subtitle')}</p>
          </div>
        </div>

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
                accept=".xlsx,.xlsm,.xls,.csv,text/csv,.pdf,application/pdf"
                className="documents-page__file-input"
                onChange={(event) => {
                  const file = event.target.files && event.target.files[0] ? event.target.files[0] : null;
                  setImportFile(file);
                  setImportPreview(null);
                  setImportSummary(null);
                  setImportMessage(null);
                }}
              />
              <span>{importFile?.name || t('documents.import.pickFile')}</span>
            </label>
            <button
              type="button"
              className="documents-page__action"
              onClick={() => void previewImport()}
              disabled={!selectedPortfolio || !importFile || importing || previewing}
            >
              {previewing ? t('documents.import.previewing') : t('documents.import.previewAction')}
            </button>
          </div>

          {importMessage ? (
            <p className="documents-page__message">{importMessage}</p>
          ) : null}

          {importPreview ? renderImportResult(importPreview, t('documents.import.preview.title'), true) : null}
          {importSummary ? renderImportResult(importSummary, t('documents.import.summary.title')) : null}
        </section>

        {confirmImportModalOpen && importPreview ? (
          <div className="documents-confirm" role="dialog" aria-modal="true" aria-label={t('documents.import.confirm.title')}>
            <div
              className="documents-confirm__backdrop"
              onClick={() => {
                if (!importing) setConfirmImportModalOpen(false);
              }}
            />
            <div className="documents-confirm__panel">
              <h3>{t('documents.import.confirm.title')}</h3>
              <p>
                {t('documents.import.confirm.body', {
                  assets: importPreview.stats.assets.created + (importPreview.stats.assets.updated || 0),
                  transactions: importPreview.stats.transactions.created,
                  aliases: importPreview.stats.aliases.created,
                })}
              </p>
              <div className="documents-confirm__actions">
                <button
                  type="button"
                  className="documents-confirm__btn documents-confirm__btn--cancel"
                  onClick={() => setConfirmImportModalOpen(false)}
                  disabled={importing}
                >
                  {t('common.cancel')}
                </button>
                <button
                  type="button"
                  className="documents-confirm__btn documents-confirm__btn--delete"
                  onClick={() => void executeImport()}
                  disabled={importing}
                >
                  {importing ? t('documents.import.importing') : t('documents.import.confirm.action')}
                </button>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </Layout>
  );
};

export default ImportsPage;
