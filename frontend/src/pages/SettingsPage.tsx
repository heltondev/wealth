import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn } from '../components/DataTable';
import EditableTable, { type EditableTableColumn } from '../components/EditableTable';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type UserSettings,
  type Alias,
  type DropdownConfigMap,
  type DropdownOption,
  type ThesisAssetClass,
  type ThesisCountry,
  type ThesisRecord,
  type ThesisUpsertPayload,
} from '../services/api';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import { useToast } from '../context/ToastContext';
import './SettingsPage.scss';

type SettingsTab = 'profile' | 'aliases' | 'preferences' | 'dropdowns' | 'theses';
const DEFAULT_ITEMS_PER_PAGE = 10;

const THESIS_COUNTRIES: ThesisCountry[] = ['BR', 'US', 'CA'];
const THESIS_ASSET_CLASSES: ThesisAssetClass[] = [
  'FII',
  'TESOURO',
  'ETF',
  'STOCK',
  'REIT',
  'BOND',
  'CRYPTO',
  'CASH',
  'RSU',
];

type ThesisFormState = {
  country: ThesisCountry;
  assetClass: ThesisAssetClass;
  title: string;
  thesisText: string;
  targetAllocation: string;
  minAllocation: string;
  maxAllocation: string;
  triggers: string;
  actionPlan: string;
  riskNotes: string;
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

const toScopeKey = (country: string, assetClass: string): string =>
  `${String(country || '').trim().toUpperCase()}:${String(assetClass || '').trim().toUpperCase()}`;

const parseNumberInput = (value: string): number | null => {
  const normalized = String(value || '').trim().replace(',', '.');
  if (!normalized) return null;
  const numeric = Number(normalized);
  return Number.isFinite(numeric) ? numeric : null;
};

const toPercentInput = (value: number | null | undefined): string =>
  typeof value === 'number' && Number.isFinite(value) ? String(value) : '';

const createEmptyThesisForm = (): ThesisFormState => ({
  country: 'BR',
  assetClass: 'FII',
  title: '',
  thesisText: '',
  targetAllocation: '',
  minAllocation: '',
  maxAllocation: '',
  triggers: '',
  actionPlan: '',
  riskNotes: '',
});

const SettingsPage = () => {
  const { t, i18n } = useTranslation();
  const { showToast } = useToast();
  const { portfolios, selectedPortfolio, setSelectedPortfolio } = usePortfolioData();
  const [activeTab, setActiveTab] = useState<SettingsTab>('profile');
  const [profile, setProfile] = useState<UserSettings>({});
  const [aliases, setAliases] = useState<Alias[]>([]);
  const [theses, setTheses] = useState<ThesisRecord[]>([]);
  const [dropdowns, setDropdowns] = useState<DropdownConfigMap>(() =>
    normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG)
  );
  const [loading, setLoading] = useState(true);
  const [aliasForm, setAliasForm] = useState({
    normalizedName: '',
    ticker: '',
    source: getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'settings.aliases.source')[0]?.value || 'manual',
  });
  const [aliasSearchTerm, setAliasSearchTerm] = useState('');
  const [aliasItemsPerPage, setAliasItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);
  const [newDropdown, setNewDropdown] = useState({ key: '', label: '' });
  const [dropdownSearchTerm, setDropdownSearchTerm] = useState('');
  const [thesisItemsPerPage, setThesisItemsPerPage] = useState(DEFAULT_ITEMS_PER_PAGE);
  const [thesisSearchTerm, setThesisSearchTerm] = useState('');
  const [editingThesisScopeKey, setEditingThesisScopeKey] = useState<string | null>(null);
  const [thesisForm, setThesisForm] = useState<ThesisFormState>(createEmptyThesisForm);
  const [thesisHistory, setThesisHistory] = useState<ThesisRecord[]>([]);
  const [thesisHistoryScopeKey, setThesisHistoryScopeKey] = useState<string | null>(null);
  const [loadingThesisHistory, setLoadingThesisHistory] = useState(false);

  useEffect(() => {
    api.getDropdownSettings()
      .then((settings) => {
        setDropdowns(normalizeDropdownConfig(settings.dropdowns));
      })
      .catch(() => {
        setDropdowns(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      });
  }, []);

  const loadTheses = useCallback(async (portfolioId: string) => {
    if (!portfolioId) {
      setTheses([]);
      return;
    }
    const payload = await api.getTheses(portfolioId);
    const nextItems = Array.isArray(payload?.items) ? payload.items : [];
    setTheses(nextItems);
  }, []);

  useEffect(() => {
    if (activeTab === 'profile') {
      setLoading(true);
      api.getProfile()
        .then(setProfile)
        .catch(() => setProfile({}))
        .finally(() => setLoading(false));
      return;
    }

    if (activeTab === 'aliases') {
      setLoading(true);
      api.getAliases()
        .then(setAliases)
        .catch(() => setAliases([]))
        .finally(() => setLoading(false));
      return;
    }

    if (activeTab === 'theses') {
      setLoading(true);
      loadTheses(selectedPortfolio)
        .catch(() => setTheses([]))
        .finally(() => setLoading(false));
      return;
    }

    setLoading(false);
  }, [activeTab, loadTheses, selectedPortfolio]);

  const preferredCurrencyOptions = useMemo(() => {
    const options = getDropdownOptions(dropdowns, 'settings.profile.preferredCurrency');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'settings.profile.preferredCurrency');
  }, [dropdowns]);

  const aliasSourceOptions = useMemo(() => {
    const options = getDropdownOptions(dropdowns, 'settings.aliases.source');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'settings.aliases.source');
  }, [dropdowns]);

  const languageOptions = useMemo(() => {
    const options = getDropdownOptions(dropdowns, 'settings.preferences.language');
    return options.length > 0
      ? options
      : getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'settings.preferences.language');
  }, [dropdowns]);

  const pageSizeOptions = useMemo(() => {
    const configuredOptions = toPageSizeOptions(
      getDropdownOptions(dropdowns, 'tables.pagination.itemsPerPage')
    );
    if (configuredOptions.length > 0) return configuredOptions;
    return toPageSizeOptions(
      getDropdownOptions(DEFAULT_DROPDOWN_CONFIG, 'tables.pagination.itemsPerPage')
    );
  }, [dropdowns]);

  useEffect(() => {
    if (preferredCurrencyOptions.length === 0) return;

    const selectedCurrency = profile.preferredCurrency || preferredCurrencyOptions[0].value;
    if (!preferredCurrencyOptions.some((option) => option.value === selectedCurrency)) {
      setProfile((previous) => ({
        ...previous,
        preferredCurrency: preferredCurrencyOptions[0].value,
      }));
    }
  }, [preferredCurrencyOptions, profile.preferredCurrency]);

  useEffect(() => {
    if (aliasSourceOptions.length === 0) return;
    if (!aliasSourceOptions.some((option) => option.value === aliasForm.source)) {
      setAliasForm((previous) => ({ ...previous, source: aliasSourceOptions[0].value }));
    }
  }, [aliasForm.source, aliasSourceOptions]);

  useEffect(() => {
    if (pageSizeOptions.includes(aliasItemsPerPage)) return;
    setAliasItemsPerPage(pageSizeOptions[0] || DEFAULT_ITEMS_PER_PAGE);
  }, [aliasItemsPerPage, pageSizeOptions]);

  useEffect(() => {
    if (pageSizeOptions.includes(thesisItemsPerPage)) return;
    setThesisItemsPerPage(pageSizeOptions[0] || DEFAULT_ITEMS_PER_PAGE);
  }, [pageSizeOptions, thesisItemsPerPage]);

  const handleSaveProfile = async () => {
    try {
      const updated = await api.updateProfile(profile);
      setProfile(updated);
      showToast('Profile saved', 'success');
    } catch {
      showToast('Failed to save profile', 'error');
    }
  };

  const handleAddAlias = async (event: React.FormEvent) => {
    event.preventDefault();
    try {
      const newAlias = await api.createAlias(aliasForm);
      setAliases((previous) => [...previous, newAlias]);
      setAliasForm((previous) => ({
        normalizedName: '',
        ticker: '',
        source: previous.source,
      }));
      showToast('Alias added', 'success');
    } catch {
      showToast('Failed to add alias', 'error');
    }
  };

  const handleResetThesisForm = useCallback(() => {
    setEditingThesisScopeKey(null);
    setThesisForm(createEmptyThesisForm());
  }, []);

  const handleEditThesis = useCallback((row: ThesisRecord) => {
    setEditingThesisScopeKey(row.scopeKey);
    setThesisForm({
      country: String(row.country || 'BR').toUpperCase() as ThesisCountry,
      assetClass: String(row.assetClass || 'FII').toUpperCase() as ThesisAssetClass,
      title: row.title || '',
      thesisText: row.thesisText || '',
      targetAllocation: toPercentInput(row.targetAllocation),
      minAllocation: toPercentInput(row.minAllocation),
      maxAllocation: toPercentInput(row.maxAllocation),
      triggers: row.triggers || '',
      actionPlan: row.actionPlan || '',
      riskNotes: row.riskNotes || '',
    });
  }, []);

  const handleSaveThesis = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!selectedPortfolio) {
      showToast(t('settings.theses.messages.selectPortfolio'), 'error');
      return;
    }

    const payload: ThesisUpsertPayload = {
      scopeKey: toScopeKey(thesisForm.country, thesisForm.assetClass),
      country: thesisForm.country,
      assetClass: thesisForm.assetClass,
      title: thesisForm.title.trim(),
      thesisText: thesisForm.thesisText.trim(),
      targetAllocation: parseNumberInput(thesisForm.targetAllocation),
      minAllocation: parseNumberInput(thesisForm.minAllocation),
      maxAllocation: parseNumberInput(thesisForm.maxAllocation),
      triggers: thesisForm.triggers.trim(),
      actionPlan: thesisForm.actionPlan.trim(),
      riskNotes: thesisForm.riskNotes.trim(),
    };

    try {
      await api.upsertThesis(selectedPortfolio, payload);
      await loadTheses(selectedPortfolio);
      setThesisHistory([]);
      setThesisHistoryScopeKey(null);
      handleResetThesisForm();
      showToast(
        editingThesisScopeKey
          ? t('settings.theses.messages.updated')
          : t('settings.theses.messages.created'),
        'success'
      );
    } catch (reason) {
      showToast(
        reason instanceof Error ? reason.message : t('settings.theses.messages.saveError'),
        'error'
      );
    }
  };

  const handleArchiveThesis = useCallback(async (scopeKey: string) => {
    if (!selectedPortfolio) return;
    try {
      await api.archiveThesis(selectedPortfolio, scopeKey);
      await loadTheses(selectedPortfolio);
      if (editingThesisScopeKey === scopeKey) {
        handleResetThesisForm();
      }
      if (thesisHistoryScopeKey === scopeKey) {
        setThesisHistory([]);
        setThesisHistoryScopeKey(null);
      }
      showToast(t('settings.theses.messages.archived'), 'success');
    } catch (reason) {
      showToast(
        reason instanceof Error ? reason.message : t('settings.theses.messages.archiveError'),
        'error'
      );
    }
  }, [
    editingThesisScopeKey,
    handleResetThesisForm,
    loadTheses,
    selectedPortfolio,
    showToast,
    t,
    thesisHistoryScopeKey,
  ]);

  const handleLoadThesisHistory = useCallback(async (scopeKey: string) => {
    if (!selectedPortfolio) return;
    setLoadingThesisHistory(true);
    try {
      const payload = await api.getThesis(selectedPortfolio, scopeKey);
      setThesisHistoryScopeKey(scopeKey);
      setThesisHistory(Array.isArray(payload.history) ? payload.history : []);
    } catch (reason) {
      setThesisHistoryScopeKey(scopeKey);
      setThesisHistory([]);
      showToast(
        reason instanceof Error ? reason.message : t('settings.theses.messages.historyError'),
        'error'
      );
    } finally {
      setLoadingThesisHistory(false);
    }
  }, [selectedPortfolio, showToast, t]);

  const handleLanguageChange = (language: string) => {
    i18n.changeLanguage(language);
    setProfile((previous) => ({ ...previous, locale: language }));
  };

  const handleSaveDropdowns = async () => {
    try {
      const updated = await api.updateDropdownSettings({ dropdowns });
      setDropdowns(normalizeDropdownConfig(updated.dropdowns));
      showToast('Dropdown options saved', 'success');
    } catch {
      showToast('Failed to save dropdown options', 'error');
    }
  };

  const handleAddDropdown = (event: React.FormEvent) => {
    event.preventDefault();
    const key = newDropdown.key.trim();
    if (!key) return;

    setDropdowns((previous) => {
      if (previous[key]) return previous;
      return {
        ...previous,
        [key]: {
          label: newDropdown.label.trim() || key,
          options: [],
        },
      };
    });

    setNewDropdown({ key: '', label: '' });
  };

  const handleRemoveDropdown = (key: string) => {
    setDropdowns((previous) => {
      const next = { ...previous };
      delete next[key];
      return next;
    });
  };

  const handleDropdownLabelChange = (key: string, label: string) => {
    setDropdowns((previous) => ({
      ...previous,
      [key]: {
        ...previous[key],
        label,
      },
    }));
  };

  const handleAddOption = (key: string) => {
    setDropdowns((previous) => ({
      ...previous,
      [key]: {
        ...previous[key],
        options: [...(previous[key]?.options || []), { value: '', label: '' }],
      },
    }));
  };

  const handleRemoveOption = (key: string, index: number) => {
    setDropdowns((previous) => ({
      ...previous,
      [key]: {
        ...previous[key],
        options: (previous[key]?.options || []).filter((_, optionIndex) => optionIndex !== index),
      },
    }));
  };

  const handleOptionChange = (
    key: string,
    index: number,
    field: 'value' | 'label',
    value: string
  ) => {
    setDropdowns((previous) => ({
      ...previous,
      [key]: {
        ...previous[key],
        options: (previous[key]?.options || []).map((option, optionIndex) =>
          optionIndex === index ? { ...option, [field]: value } : option
        ),
      },
    }));
  };

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const thesisCountryOptions = useMemo(
    () => THESIS_COUNTRIES.map((country) => ({
      value: country,
      label: t(`settings.theses.countries.${country}`, { defaultValue: country }),
    })),
    [t]
  );

  const thesisAssetClassOptions = useMemo(
    () => THESIS_ASSET_CLASSES.map((assetClass) => ({
      value: assetClass,
      label: t(`settings.theses.assetClasses.${assetClass}`, { defaultValue: assetClass }),
    })),
    [t]
  );

  const formatThesisDate = useCallback((value: string | null) => {
    if (!value) return '-';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleDateString(i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US');
  }, [i18n.language]);
  const thesisNumberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  const tabs = [
    { key: 'profile' as const, label: t('settings.profile') },
    { key: 'aliases' as const, label: t('settings.aliases') },
    { key: 'preferences' as const, label: t('settings.preferences') },
    { key: 'dropdowns' as const, label: t('settings.dropdowns') },
    { key: 'theses' as const, label: t('settings.theses.title') },
  ];

  const dropdownEntries = useMemo(() =>
    Object.entries(dropdowns).sort(([left], [right]) => left.localeCompare(right)), [dropdowns]);
  const filteredDropdownEntries = useMemo(() => {
    const normalizedSearch = dropdownSearchTerm.trim().toLowerCase();
    if (!normalizedSearch) return dropdownEntries;

    return dropdownEntries.filter(([key, config]) => {
      const options = config.options || [];
      const searchableContent = [
        key,
        config.label,
        ...options.map((option) => option.value),
        ...options.map((option) => option.label),
      ]
        .join(' ')
        .toLowerCase();

      return searchableContent.includes(normalizedSearch);
    });
  }, [dropdownEntries, dropdownSearchTerm]);

  const activeLanguage = i18n.language?.startsWith('pt') ? 'pt' : 'en';
  const isSystemDropdown = (key: string) => Boolean(DEFAULT_DROPDOWN_CONFIG[key]);

  const resolveTranslatedLabel = useCallback((key: string): string | null => {
    const normalized = key.trim();
    if (!normalized) return null;

    const directKey = normalized;
    if (i18n.exists(directKey)) {
      const translated = t(directKey);
      if (typeof translated === 'string' && translated.trim()) {
        return translated;
      }
    }

    const labelKey = `${normalized}.label`;
    if (i18n.exists(labelKey)) {
      const translated = t(labelKey);
      if (typeof translated === 'string' && translated.trim()) {
        return translated;
      }
    }

    return null;
  }, [i18n, t]);

  const resolveDropdownLabel = useCallback((dropdownKey: string, configuredLabel?: string): string => {
    const normalizedLabel = String(configuredLabel || '').trim();
    const translatedConfiguredLabel = normalizedLabel
      ? resolveTranslatedLabel(normalizedLabel)
      : null;

    if (translatedConfiguredLabel) return translatedConfiguredLabel;

    const translatedFromKey = resolveTranslatedLabel(dropdownKey);
    if (translatedFromKey) return translatedFromKey;

    return normalizedLabel || dropdownKey;
  }, [resolveTranslatedLabel]);

  const aliasColumns = useMemo<DataTableColumn<Alias>[]>(() => ([
    {
      key: 'normalizedName',
      label: t('settings.aliasTable.name'),
      sortable: true,
      sortValue: (row) => row.normalizedName,
      render: (row) => row.normalizedName,
    },
    {
      key: 'ticker',
      label: t('settings.aliasTable.ticker'),
      sortable: true,
      sortValue: (row) => row.ticker,
      cellClassName: 'aliases-table__ticker',
      render: (row) => row.ticker,
    },
    {
      key: 'source',
      label: t('settings.aliasTable.source'),
      sortable: true,
      sortValue: (row) => row.source,
      render: (row) => row.source,
    },
  ]), [t]);

  const thesisColumns = useMemo<DataTableColumn<ThesisRecord>[]>(() => ([
    {
      key: 'scopeKey',
      label: t('settings.theses.table.scope'),
      sortable: true,
      sortValue: (row) => row.scopeKey,
      render: (row) => row.scopeKey,
    },
    {
      key: 'title',
      label: t('settings.theses.table.title'),
      sortable: true,
      sortValue: (row) => row.title,
      render: (row) => row.title || '-',
    },
    {
      key: 'targetAllocation',
      label: t('settings.theses.table.target'),
      sortable: true,
      sortValue: (row) => row.targetAllocation ?? -1,
      render: (row) =>
        row.targetAllocation === null
          ? '-'
          : `${row.targetAllocation.toLocaleString(thesisNumberLocale, { maximumFractionDigits: 4 })}%`,
    },
    {
      key: 'bounds',
      label: t('settings.theses.table.bounds'),
      sortable: false,
      render: (row) => {
        const minLabel = row.minAllocation === null
          ? '-'
          : `${row.minAllocation.toLocaleString(thesisNumberLocale, { maximumFractionDigits: 4 })}%`;
        const maxLabel = row.maxAllocation === null
          ? '-'
          : `${row.maxAllocation.toLocaleString(thesisNumberLocale, { maximumFractionDigits: 4 })}%`;
        return `${minLabel} - ${maxLabel}`;
      },
    },
    {
      key: 'version',
      label: t('settings.theses.table.version'),
      sortable: true,
      sortValue: (row) => row.version,
      render: (row) => `v${row.version}`,
    },
    {
      key: 'updatedAt',
      label: t('settings.theses.table.updatedAt'),
      sortable: true,
      sortValue: (row) => row.updatedAt || '',
      render: (row) => formatThesisDate(row.updatedAt),
    },
    {
      key: 'actions',
      label: t('settings.theses.table.actions'),
      sortable: false,
      render: (row) => (
        <div className="thesis-table__actions">
          <button type="button" onClick={() => handleEditThesis(row)}>
            {t('common.edit')}
          </button>
          <button type="button" onClick={() => void handleLoadThesisHistory(row.scopeKey)}>
            {t('settings.theses.showHistory')}
          </button>
          <button type="button" className="thesis-table__danger" onClick={() => void handleArchiveThesis(row.scopeKey)}>
            {t('common.delete')}
          </button>
        </div>
      ),
    },
  ]), [
    formatThesisDate,
    handleArchiveThesis,
    handleEditThesis,
    handleLoadThesisHistory,
    thesisNumberLocale,
    t,
  ]);

  const optionColumnsFor = (dropdownKey: string): EditableTableColumn<DropdownOption>[] => ([
    {
      key: 'value',
      label: t('settings.dropdownValue'),
      render: (option, rowIndex) => (
        <input
          type="text"
          value={option.value}
          onChange={(event) => handleOptionChange(dropdownKey, rowIndex, 'value', event.target.value)}
        />
      ),
    },
    {
      key: 'label',
      label: t('settings.dropdownOptionLabel'),
      render: (option, rowIndex) => (
        <input
          type="text"
          value={option.label}
          onChange={(event) => handleOptionChange(dropdownKey, rowIndex, 'label', event.target.value)}
        />
      ),
    },
    {
      key: 'actions',
      label: t('settings.dropdownActions'),
      render: (_, rowIndex) => (
        <button type="button" onClick={() => handleRemoveOption(dropdownKey, rowIndex)}>
          {t('common.delete')}
        </button>
      ),
    },
  ]);

  return (
    <Layout>
      <div className="settings-page">
        <h1 className="settings-page__title">{t('settings.title')}</h1>

        <div className="settings-page__tabs">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              className={`settings-page__tab ${activeTab === tab.key ? 'settings-page__tab--active' : ''}`}
              onClick={() => setActiveTab(tab.key)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {loading && <p className="settings-page__loading">{t('common.loading')}</p>}

        {!loading && activeTab === 'profile' && (
          <div className="settings-section">
            <div className="settings-section__field">
              <label>{t('settings.displayName')}</label>
              <input
                type="text"
                value={profile.displayName || ''}
                onChange={(event) => setProfile({ ...profile, displayName: event.target.value })}
              />
            </div>
            <div className="settings-section__field">
              <label>{t('settings.email')}</label>
              <input type="email" value={profile.email || ''} disabled />
            </div>
            <div className="settings-section__field">
              <label>{t('settings.preferredCurrency')}</label>
              <select
                value={profile.preferredCurrency || preferredCurrencyOptions[0]?.value || ''}
                onChange={(event) => setProfile({ ...profile, preferredCurrency: event.target.value })}
              >
                {preferredCurrencyOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
            <button className="settings-section__save" onClick={handleSaveProfile}>
              {t('settings.save')}
            </button>
          </div>
        )}

        {!loading && activeTab === 'aliases' && (
          <div className="settings-section">
            <form className="alias-form" onSubmit={handleAddAlias}>
              <input
                type="text"
                placeholder="Normalized name (e.g. petrobras pn)"
                value={aliasForm.normalizedName}
                onChange={(event) => setAliasForm({ ...aliasForm, normalizedName: event.target.value })}
                required
              />
              <input
                type="text"
                placeholder="Ticker (e.g. PETR4)"
                value={aliasForm.ticker}
                onChange={(event) => setAliasForm({ ...aliasForm, ticker: event.target.value })}
                required
              />
              <select
                value={aliasForm.source}
                onChange={(event) => setAliasForm({ ...aliasForm, source: event.target.value })}
              >
                {aliasSourceOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
              <button type="submit">{t('common.create')}</button>
            </form>

            <DataTable
              rows={aliases}
              rowKey={(alias) => `${alias.normalizedName}-${alias.ticker}-${alias.source}`}
              columns={aliasColumns}
              searchLabel={t('common.search')}
              searchPlaceholder={t('settings.aliasTable.searchPlaceholder')}
              searchTerm={aliasSearchTerm}
              onSearchTermChange={setAliasSearchTerm}
              matchesSearch={(alias, normalizedSearch) =>
                [alias.normalizedName, alias.ticker, alias.source]
                  .join(' ')
                  .toLowerCase()
                  .includes(normalizedSearch)
              }
              itemsPerPage={aliasItemsPerPage}
              onItemsPerPageChange={setAliasItemsPerPage}
              pageSizeOptions={pageSizeOptions}
              emptyLabel={t('settings.aliasTable.empty')}
              labels={{
                itemsPerPage: t('assets.pagination.itemsPerPage'),
                prev: t('assets.pagination.prev'),
                next: t('assets.pagination.next'),
                page: (page, total) => t('assets.pagination.page', { page, total }),
                showing: (start, end, total) => t('assets.pagination.showing', { start, end, total }),
              }}
              defaultSort={{ key: 'normalizedName', direction: 'asc' }}
            />
          </div>
        )}

        {!loading && activeTab === 'preferences' && (
          <div className="settings-section">
            <div className="settings-section__field">
              <label>{t('settings.language')}</label>
              <select
                value={activeLanguage}
                onChange={(event) => handleLanguageChange(event.target.value)}
              >
                {languageOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
          </div>
        )}

        {!loading && activeTab === 'theses' && (
          <div className="settings-section theses-config">
            <p className="theses-config__description">{t('settings.theses.description')}</p>

            {portfolioOptions.length > 0 ? (
              <div className="theses-config__portfolio">
                <label>{t('documents.selectPortfolio')}</label>
                <SharedDropdown
                  value={selectedPortfolio}
                  options={portfolioOptions}
                  onChange={setSelectedPortfolio}
                  ariaLabel={t('documents.selectPortfolio')}
                  className="theses-config__portfolio-dropdown"
                  size="sm"
                />
              </div>
            ) : (
              <p className="theses-config__empty">{t('settings.theses.emptyPortfolio')}</p>
            )}

            <form className="theses-form" onSubmit={handleSaveThesis}>
              <div className="theses-form__row">
                <div className="theses-form__field">
                  <label>{t('assets.form.country')}</label>
                  <select
                    value={thesisForm.country}
                    onChange={(event) =>
                      setThesisForm((previous) => ({
                        ...previous,
                        country: event.target.value as ThesisCountry,
                      }))
                    }
                  >
                    {thesisCountryOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="theses-form__field">
                  <label>{t('settings.theses.assetClass')}</label>
                  <select
                    value={thesisForm.assetClass}
                    onChange={(event) =>
                      setThesisForm((previous) => ({
                        ...previous,
                        assetClass: event.target.value as ThesisAssetClass,
                      }))
                    }
                  >
                    {thesisAssetClassOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="theses-form__field theses-form__field--scope">
                  <label>{t('settings.theses.scopeKey')}</label>
                  <div className="theses-form__scope">
                    {toScopeKey(thesisForm.country, thesisForm.assetClass)}
                  </div>
                </div>
              </div>

              <div className="theses-form__field">
                <label>{t('settings.theses.fields.title')}</label>
                <input
                  type="text"
                  value={thesisForm.title}
                  onChange={(event) =>
                    setThesisForm((previous) => ({ ...previous, title: event.target.value }))
                  }
                  required
                />
              </div>

              <div className="theses-form__field">
                <label>{t('settings.theses.fields.thesisText')}</label>
                <textarea
                  value={thesisForm.thesisText}
                  onChange={(event) =>
                    setThesisForm((previous) => ({ ...previous, thesisText: event.target.value }))
                  }
                  required
                />
              </div>

              <div className="theses-form__row">
                <div className="theses-form__field">
                  <label>{t('settings.theses.fields.targetAllocation')}</label>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    step="0.0001"
                    value={thesisForm.targetAllocation}
                    onChange={(event) =>
                      setThesisForm((previous) => ({ ...previous, targetAllocation: event.target.value }))
                    }
                  />
                </div>
                <div className="theses-form__field">
                  <label>{t('settings.theses.fields.minAllocation')}</label>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    step="0.0001"
                    value={thesisForm.minAllocation}
                    onChange={(event) =>
                      setThesisForm((previous) => ({ ...previous, minAllocation: event.target.value }))
                    }
                  />
                </div>
                <div className="theses-form__field">
                  <label>{t('settings.theses.fields.maxAllocation')}</label>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    step="0.0001"
                    value={thesisForm.maxAllocation}
                    onChange={(event) =>
                      setThesisForm((previous) => ({ ...previous, maxAllocation: event.target.value }))
                    }
                  />
                </div>
              </div>

              <div className="theses-form__field">
                <label>{t('settings.theses.fields.triggers')}</label>
                <textarea
                  value={thesisForm.triggers}
                  onChange={(event) =>
                    setThesisForm((previous) => ({ ...previous, triggers: event.target.value }))
                  }
                />
              </div>

              <div className="theses-form__field">
                <label>{t('settings.theses.fields.actionPlan')}</label>
                <textarea
                  value={thesisForm.actionPlan}
                  onChange={(event) =>
                    setThesisForm((previous) => ({ ...previous, actionPlan: event.target.value }))
                  }
                />
              </div>

              <div className="theses-form__field">
                <label>{t('settings.theses.fields.riskNotes')}</label>
                <textarea
                  value={thesisForm.riskNotes}
                  onChange={(event) =>
                    setThesisForm((previous) => ({ ...previous, riskNotes: event.target.value }))
                  }
                />
              </div>

              <div className="theses-form__actions">
                <button type="submit" className="theses-form__save">
                  {editingThesisScopeKey
                    ? t('settings.theses.actions.updateVersion')
                    : t('settings.theses.actions.create')}
                </button>
                {editingThesisScopeKey ? (
                  <button type="button" className="theses-form__cancel" onClick={handleResetThesisForm}>
                    {t('common.cancel')}
                  </button>
                ) : null}
              </div>
            </form>

            <DataTable
              rows={theses}
              rowKey={(row) => row.scopeKey}
              columns={thesisColumns}
              searchLabel={t('common.search')}
              searchPlaceholder={t('settings.theses.searchPlaceholder')}
              searchTerm={thesisSearchTerm}
              onSearchTermChange={setThesisSearchTerm}
              matchesSearch={(row, normalizedSearch) =>
                [
                  row.scopeKey,
                  row.title,
                  row.thesisText,
                  row.country,
                  row.assetClass,
                  row.triggers,
                  row.actionPlan,
                  row.riskNotes,
                ]
                  .join(' ')
                  .toLowerCase()
                  .includes(normalizedSearch)
              }
              itemsPerPage={thesisItemsPerPage}
              onItemsPerPageChange={setThesisItemsPerPage}
              pageSizeOptions={pageSizeOptions}
              emptyLabel={t('settings.theses.empty')}
              labels={{
                itemsPerPage: t('assets.pagination.itemsPerPage'),
                prev: t('assets.pagination.prev'),
                next: t('assets.pagination.next'),
                page: (page, total) => t('assets.pagination.page', { page, total }),
                showing: (start, end, total) => t('assets.pagination.showing', { start, end, total }),
              }}
              defaultSort={{ key: 'scopeKey', direction: 'asc' }}
            />

            {thesisHistoryScopeKey ? (
              <section className="thesis-history">
                <h3>
                  {t('settings.theses.historyTitle', { scopeKey: thesisHistoryScopeKey })}
                </h3>
                {loadingThesisHistory ? (
                  <p>{t('common.loading')}</p>
                ) : thesisHistory.length === 0 ? (
                  <p>{t('settings.theses.historyEmpty')}</p>
                ) : (
                  <ul>
                    {thesisHistory.map((entry) => (
                      <li key={`${entry.scopeKey}-${entry.version}`}>
                        <strong>{`v${entry.version}`}</strong>
                        <span>{` • ${entry.status}`}</span>
                        <span>{` • ${formatThesisDate(entry.updatedAt)}`}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </section>
            ) : null}
          </div>
        )}

        {!loading && activeTab === 'dropdowns' && (
          <div className="settings-section dropdowns-config">
            <p className="dropdowns-config__description">
              {t('settings.dropdownsDescription')}
            </p>

            <form className="dropdowns-config__new" onSubmit={handleAddDropdown}>
              <input
                type="text"
                placeholder={t('settings.dropdownKeyPlaceholder')}
                value={newDropdown.key}
                onChange={(event) => setNewDropdown((previous) => ({ ...previous, key: event.target.value }))}
              />
              <input
                type="text"
                placeholder={t('settings.dropdownLabelPlaceholder')}
                value={newDropdown.label}
                onChange={(event) => setNewDropdown((previous) => ({ ...previous, label: event.target.value }))}
              />
              <button type="submit" className="dropdowns-config__btn dropdowns-config__btn--primary">
                {t('settings.addDropdown')}
              </button>
            </form>

            <div className="dropdowns-config__search">
              <label htmlFor="dropdown-config-search">{t('common.search')}</label>
              <input
                id="dropdown-config-search"
                type="text"
                value={dropdownSearchTerm}
                onChange={(event) => setDropdownSearchTerm(event.target.value)}
                placeholder={t('settings.dropdownSearchPlaceholder')}
              />
            </div>

            {filteredDropdownEntries.length === 0 && (
              <p className="dropdowns-config__empty-search">{t('settings.dropdownSearchNoResults')}</p>
            )}

            {filteredDropdownEntries.map(([key, config]) => (
              <section key={key} className="dropdowns-config__group">
                <div className="dropdowns-config__group-header">
                  <div className="dropdowns-config__group-meta">
                    <code className="dropdowns-config__group-key">{key}</code>
                    <span className="dropdowns-config__group-count">{config.options.length}</span>
                    <span className="dropdowns-config__group-label">{resolveDropdownLabel(key, config.label)}</span>
                  </div>
                  {!isSystemDropdown(key) && (
                    <button
                      type="button"
                      className="dropdowns-config__btn dropdowns-config__btn--danger"
                      onClick={() => handleRemoveDropdown(key)}
                    >
                      {t('settings.removeDropdown')}
                    </button>
                  )}
                </div>

                <div className="dropdowns-config__group-editor">
                  <label htmlFor={`dropdown-label-${key}`}>{t('settings.dropdownLabelPlaceholder')}</label>
                  <input
                    id={`dropdown-label-${key}`}
                    type="text"
                    value={config.label}
                    onChange={(event) => handleDropdownLabelChange(key, event.target.value)}
                  />
                </div>

                <div className="dropdowns-config__table-shell">
                  <EditableTable
                    rows={config.options}
                    rowKey={(_, rowIndex) => `${key}-${rowIndex}`}
                    columns={optionColumnsFor(key)}
                    emptyLabel={t('settings.dropdownOptionsEmpty')}
                    className="dropdowns-config__table"
                  />
                </div>

                <div className="dropdowns-config__group-actions">
                  <button
                    type="button"
                    className="dropdowns-config__btn dropdowns-config__btn--secondary"
                    onClick={() => handleAddOption(key)}
                  >
                    {t('settings.addOption')}
                  </button>
                </div>
              </section>
            ))}

            <button className="settings-section__save" onClick={handleSaveDropdowns}>
              {t('settings.saveDropdowns')}
            </button>
          </div>
        )}
      </div>
    </Layout>
  );
};

export default SettingsPage;
