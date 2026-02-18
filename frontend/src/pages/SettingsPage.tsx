import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import DataTable, { type DataTableColumn } from '../components/DataTable';
import EditableTable, { type EditableTableColumn } from '../components/EditableTable';
import {
  api,
  type UserSettings,
  type Alias,
  type DropdownConfigMap,
  type DropdownOption,
} from '../services/api';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import { useToast } from '../context/ToastContext';
import './SettingsPage.scss';

type SettingsTab = 'profile' | 'aliases' | 'preferences' | 'dropdowns';
const DEFAULT_ITEMS_PER_PAGE = 10;

const toPageSizeOptions = (options: { value: string }[]): number[] => {
  const values = new Set<number>();

  for (const option of options) {
    const numeric = Number(option.value);
    if (!Number.isFinite(numeric) || numeric <= 0) continue;
    values.add(Math.round(numeric));
  }

  return Array.from(values).sort((left, right) => left - right);
};

const SettingsPage = () => {
  const { t, i18n } = useTranslation();
  const { showToast } = useToast();
  const [activeTab, setActiveTab] = useState<SettingsTab>('profile');
  const [profile, setProfile] = useState<UserSettings>({});
  const [aliases, setAliases] = useState<Alias[]>([]);
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

  useEffect(() => {
    api.getDropdownSettings()
      .then((settings) => {
        setDropdowns(normalizeDropdownConfig(settings.dropdowns));
      })
      .catch(() => {
        setDropdowns(normalizeDropdownConfig(DEFAULT_DROPDOWN_CONFIG));
      });
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

    setLoading(false);
  }, [activeTab]);

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

  const tabs = [
    { key: 'profile' as const, label: t('settings.profile') },
    { key: 'aliases' as const, label: t('settings.aliases') },
    { key: 'preferences' as const, label: t('settings.preferences') },
    { key: 'dropdowns' as const, label: t('settings.dropdowns') },
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
