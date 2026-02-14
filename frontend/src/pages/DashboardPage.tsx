import { useTranslation } from 'react-i18next';
import { useEffect, useState } from 'react';
import Layout from '../components/Layout';
import { api, type Portfolio } from '../services/api';
import { formatCurrency } from '../utils/formatters';
import './DashboardPage.scss';

const DashboardPage = () => {
  const { t } = useTranslation();
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getPortfolios()
      .then(setPortfolios)
      .catch(() => setPortfolios([]))
      .finally(() => setLoading(false));
  }, []);

  return (
    <Layout>
      <div className="dashboard">
        <h1 className="dashboard__title">{t('dashboard.title')}</h1>

        {loading && <p className="dashboard__loading">{t('common.loading')}</p>}

        {!loading && portfolios.length === 0 && (
          <div className="dashboard__empty">
            <p>{t('dashboard.noData')}</p>
          </div>
        )}

        {!loading && portfolios.length > 0 && (
          <>
            <div className="dashboard__kpi-grid">
              <div className="kpi-card">
                <span className="kpi-card__label">{t('dashboard.totalValue')}</span>
                <span className="kpi-card__value">{formatCurrency(0)}</span>
              </div>
              <div className="kpi-card">
                <span className="kpi-card__label">{t('dashboard.totalGain')}</span>
                <span className="kpi-card__value">{formatCurrency(0)}</span>
              </div>
            </div>

            <div className="dashboard__chart-placeholder">
              <h3>{t('dashboard.allocation')}</h3>
              <p className="dashboard__chart-note">Chart will be rendered here with Recharts</p>
            </div>

            <div className="dashboard__portfolios">
              {portfolios.map((p) => (
                <div key={p.portfolioId} className="portfolio-card">
                  <h3 className="portfolio-card__name">{p.name}</h3>
                  <p className="portfolio-card__desc">{p.description}</p>
                  <span className="portfolio-card__currency">{p.baseCurrency}</span>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default DashboardPage;
