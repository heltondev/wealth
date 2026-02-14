import { useTranslation } from 'react-i18next';
import { useEffect, useState } from 'react';
import Layout from '../components/Layout';
import { api, type UserSettings, type Alias } from '../services/api';
import { useToast } from '../context/ToastContext';
import './SettingsPage.scss';

const SettingsPage = () => {
  const { t, i18n } = useTranslation();
  const { showToast } = useToast();
  const [activeTab, setActiveTab] = useState<'profile' | 'aliases' | 'preferences'>('profile');
  const [profile, setProfile] = useState<UserSettings>({});
  const [aliases, setAliases] = useState<Alias[]>([]);
  const [loading, setLoading] = useState(true);
  const [aliasForm, setAliasForm] = useState({ normalizedName: '', ticker: '', source: 'manual' });

  useEffect(() => {
    if (activeTab === 'profile') {
      setLoading(true);
      api.getProfile()
        .then(setProfile)
        .catch(() => setProfile({}))
        .finally(() => setLoading(false));
    } else if (activeTab === 'aliases') {
      setLoading(true);
      api.getAliases()
        .then(setAliases)
        .catch(() => setAliases([]))
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, [activeTab]);

  const handleSaveProfile = async () => {
    try {
      const updated = await api.updateProfile(profile);
      setProfile(updated);
      showToast('Profile saved', 'success');
    } catch {
      showToast('Failed to save profile', 'error');
    }
  };

  const handleAddAlias = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const newAlias = await api.createAlias(aliasForm);
      setAliases((prev) => [...prev, newAlias]);
      setAliasForm({ normalizedName: '', ticker: '', source: 'manual' });
      showToast('Alias added', 'success');
    } catch {
      showToast('Failed to add alias', 'error');
    }
  };

  const handleLanguageChange = (lang: string) => {
    i18n.changeLanguage(lang);
    setProfile((prev) => ({ ...prev, locale: lang }));
  };

  const tabs = [
    { key: 'profile' as const, label: t('settings.profile') },
    { key: 'aliases' as const, label: t('settings.aliases') },
    { key: 'preferences' as const, label: t('settings.preferences') },
  ];

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
                onChange={(e) => setProfile({ ...profile, displayName: e.target.value })}
              />
            </div>
            <div className="settings-section__field">
              <label>{t('settings.email')}</label>
              <input type="email" value={profile.email || ''} disabled />
            </div>
            <div className="settings-section__field">
              <label>{t('settings.preferredCurrency')}</label>
              <select
                value={profile.preferredCurrency || 'BRL'}
                onChange={(e) => setProfile({ ...profile, preferredCurrency: e.target.value })}
              >
                <option value="BRL">BRL</option>
                <option value="USD">USD</option>
                <option value="CAD">CAD</option>
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
                onChange={(e) => setAliasForm({ ...aliasForm, normalizedName: e.target.value })}
                required
              />
              <input
                type="text"
                placeholder="Ticker (e.g. PETR4)"
                value={aliasForm.ticker}
                onChange={(e) => setAliasForm({ ...aliasForm, ticker: e.target.value })}
                required
              />
              <select
                value={aliasForm.source}
                onChange={(e) => setAliasForm({ ...aliasForm, source: e.target.value })}
              >
                <option value="manual">Manual</option>
                <option value="b3">B3</option>
                <option value="itau">Itau</option>
                <option value="robinhood">Robinhood</option>
                <option value="equate">Equate</option>
                <option value="coinbase">Coinbase</option>
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
                  {aliases.map((alias, i) => (
                    <tr key={i}>
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
                value={i18n.language?.startsWith('pt') ? 'pt' : 'en'}
                onChange={(e) => handleLanguageChange(e.target.value)}
              >
                <option value="en">English</option>
                <option value="pt">Português</option>
              </select>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
};

export default SettingsPage;
