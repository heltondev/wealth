import { useTranslation } from 'react-i18next';
import { useEffect, useState } from 'react';
import Layout from '../components/Layout';
import { api, type Asset, type Portfolio } from '../services/api';
import { useToast } from '../context/ToastContext';
import './AssetsPage.scss';

const AssetsPage = () => {
  const { t } = useTranslation();
  const { showToast } = useToast();
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>('');
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState<{
    ticker: string;
    name: string;
    assetClass: string;
    country: string;
    currency: string;
  }>({
    ticker: '',
    name: '',
    assetClass: 'stock',
    country: 'BR',
    currency: 'BRL',
  });

  useEffect(() => {
    api.getPortfolios()
      .then((p) => {
        setPortfolios(p);
        if (p.length > 0) setSelectedPortfolio(p[0].portfolioId);
      })
      .catch(() => setPortfolios([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!selectedPortfolio) return;
    setLoading(true);
    api.getAssets(selectedPortfolio)
      .then(setAssets)
      .catch(() => setAssets([]))
      .finally(() => setLoading(false));
  }, [selectedPortfolio]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedPortfolio) return;

    try {
      const newAsset = await api.createAsset(selectedPortfolio, form);
      setAssets((prev) => [...prev, newAsset]);
      setShowModal(false);
      setForm({ ticker: '', name: '', assetClass: 'stock', country: 'BR', currency: 'BRL' });
      showToast('Asset added', 'success');
    } catch {
      showToast('Failed to add asset', 'error');
    }
  };

  const handleDelete = async (assetId: string) => {
    if (!selectedPortfolio) return;
    try {
      await api.deleteAsset(selectedPortfolio, assetId);
      setAssets((prev) => prev.filter((a) => a.assetId !== assetId));
      showToast('Asset deleted', 'success');
    } catch {
      showToast('Failed to delete asset', 'error');
    }
  };

  return (
    <Layout>
      <div className="assets-page">
        <div className="assets-page__header">
          <h1 className="assets-page__title">{t('assets.title')}</h1>
          <div className="assets-page__controls">
            {portfolios.length > 0 && (
              <select
                className="assets-page__select"
                value={selectedPortfolio}
                onChange={(e) => setSelectedPortfolio(e.target.value)}
              >
                {portfolios.map((p) => (
                  <option key={p.portfolioId} value={p.portfolioId}>
                    {p.name}
                  </option>
                ))}
              </select>
            )}
            <button className="assets-page__add-btn" onClick={() => setShowModal(true)}>
              {t('assets.addAsset')}
            </button>
          </div>
        </div>

        {loading && <p className="assets-page__loading">{t('common.loading')}</p>}

        {!loading && assets.length === 0 && (
          <div className="assets-page__empty">
            <p>{t('assets.empty')}</p>
          </div>
        )}

        {!loading && assets.length > 0 && (
          <div className="assets-table-wrapper">
            <table className="assets-table">
              <thead>
                <tr>
                  <th>{t('assets.ticker')}</th>
                  <th>{t('assets.name')}</th>
                  <th>{t('assets.class')}</th>
                  <th>{t('assets.country')}</th>
                  <th>{t('assets.currency')}</th>
                  <th>{t('assets.status')}</th>
                  <th>{t('assets.actions')}</th>
                </tr>
              </thead>
              <tbody>
                {assets.map((asset) => (
                  <tr key={asset.assetId}>
                    <td className="assets-table__ticker">{asset.ticker}</td>
                    <td>{asset.name}</td>
                    <td>
                      <span className={`badge badge--${asset.assetClass}`}>
                        {t(`assets.classes.${asset.assetClass}`)}
                      </span>
                    </td>
                    <td>{asset.country}</td>
                    <td>{asset.currency}</td>
                    <td>{asset.status}</td>
                    <td>
                      <button
                        className="assets-table__delete"
                        onClick={() => handleDelete(asset.assetId)}
                      >
                        {t('common.delete')}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {showModal && (
          <div className="modal-overlay" onClick={() => setShowModal(false)}>
            <div className="modal" onClick={(e) => e.stopPropagation()}>
              <h2>{t('assets.addAsset')}</h2>
              <form onSubmit={handleSubmit}>
                <div className="modal__field">
                  <label>{t('assets.form.ticker')}</label>
                  <input
                    type="text"
                    value={form.ticker}
                    onChange={(e) => setForm({ ...form, ticker: e.target.value })}
                    required
                  />
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.name')}</label>
                  <input
                    type="text"
                    value={form.name}
                    onChange={(e) => setForm({ ...form, name: e.target.value })}
                    required
                  />
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.class')}</label>
                  <select
                    value={form.assetClass}
                    onChange={(e) => setForm({ ...form, assetClass: e.target.value })}
                  >
                    <option value="stock">{t('assets.classes.stock')}</option>
                    <option value="fii">{t('assets.classes.fii')}</option>
                    <option value="bond">{t('assets.classes.bond')}</option>
                    <option value="crypto">{t('assets.classes.crypto')}</option>
                    <option value="rsu">{t('assets.classes.rsu')}</option>
                  </select>
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.country')}</label>
                  <select
                    value={form.country}
                    onChange={(e) => setForm({ ...form, country: e.target.value })}
                  >
                    <option value="BR">Brazil</option>
                    <option value="US">United States</option>
                    <option value="CA">Canada</option>
                  </select>
                </div>
                <div className="modal__field">
                  <label>{t('assets.form.currency')}</label>
                  <select
                    value={form.currency}
                    onChange={(e) => setForm({ ...form, currency: e.target.value })}
                  >
                    <option value="BRL">BRL</option>
                    <option value="USD">USD</option>
                    <option value="CAD">CAD</option>
                  </select>
                </div>
                <div className="modal__actions">
                  <button type="button" className="modal__btn modal__btn--cancel" onClick={() => setShowModal(false)}>
                    {t('assets.form.cancel')}
                  </button>
                  <button type="submit" className="modal__btn modal__btn--submit">
                    {t('assets.form.submit')}
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
};

export default AssetsPage;
