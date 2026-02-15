import { useTranslation } from 'react-i18next';
import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/Layout';
import {
  api,
  type UserSettings,
  type Alias,
  type DropdownConfigMap,
} from '../services/api';
import {
  DEFAULT_DROPDOWN_CONFIG,
  getDropdownOptions,
  normalizeDropdownConfig,
} from '../config/dropdowns';
import { useToast } from '../context/ToastContext';
import './SettingsPage.scss';

type SettingsTab = 'profile' | 'aliases' | 'preferences' | 'dropdowns';

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
  const [newDropdown, setNewDropdown] = useState({ key: '', label: '' });

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

  const activeLanguage = i18n.language?.startsWith('pt') ? 'pt' : 'en';
  const isSystemDropdown = (key: string) => Boolean(DEFAULT_DROPDOWN_CONFIG[key]);

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

            {aliases.length > 0 && (
              <table className="aliases-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Ticker</th>
                    <th>Source</th>
                  </tr>
                </thead>
                <tbody>
                  {aliases.map((alias, index) => (
                    <tr key={index}>
                      <td>{alias.normalizedName}</td>
                      <td className="aliases-table__ticker">{alias.ticker}</td>
                      <td>{alias.source}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
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
              <button type="submit">{t('settings.addDropdown')}</button>
            </form>

            {dropdownEntries.map(([key, config]) => (
              <section key={key} className="dropdowns-config__group">
                <div className="dropdowns-config__group-header">
                  <input type="text" value={key} disabled />
                  <input
                    type="text"
                    value={config.label}
                    onChange={(event) => handleDropdownLabelChange(key, event.target.value)}
                  />
                  {!isSystemDropdown(key) && (
                    <button type="button" onClick={() => handleRemoveDropdown(key)}>
                      {t('settings.removeDropdown')}
                    </button>
                  )}
                </div>

                <table className="dropdowns-config__table">
                  <thead>
                    <tr>
                      <th>{t('settings.dropdownValue')}</th>
                      <th>{t('settings.dropdownOptionLabel')}</th>
                      <th>{t('settings.dropdownActions')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {config.options.map((option, index) => (
                      <tr key={`${key}-${index}`}>
                        <td>
                          <input
                            type="text"
                            value={option.value}
                            onChange={(event) => handleOptionChange(key, index, 'value', event.target.value)}
                          />
                        </td>
                        <td>
                          <input
                            type="text"
                            value={option.label}
                            onChange={(event) => handleOptionChange(key, index, 'label', event.target.value)}
                          />
                        </td>
                        <td>
                          <button type="button" onClick={() => handleRemoveOption(key, index)}>
                            {t('common.delete')}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                <button type="button" onClick={() => handleAddOption(key)}>
                  {t('settings.addOption')}
                </button>
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
