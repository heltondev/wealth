import { useNavigate } from 'react-router';
import { useAuth } from '../context/AuthContext';
import { useTranslation } from 'react-i18next';
import { useEffect } from 'react';
import { logger } from '../utils/logger';
import './LoginPage.scss';

const LoginPage = () => {
  const { login, isAuthenticated, isAuthConfigured, isLoading } = useAuth();
  const navigate = useNavigate();
  const { t } = useTranslation();

  useEffect(() => {
    if (isAuthenticated) {
      navigate('/dashboard', { replace: true });
    }
  }, [isAuthenticated, navigate]);

  const handleMockLogin = async () => {
    await login();
    navigate('/dashboard', { replace: true });
  };

  const handleGoogleLogin = async () => {
    try {
      await login();
    } catch (error) {
      logger.error('Error signing in with Google', error);
    }
  };

  return (
    <div className="login-page">
      <div className="login-bg" aria-hidden="true">
        <span className="login-bg__aurora login-bg__aurora--one" />
        <span className="login-bg__aurora login-bg__aurora--two" />
        <span className="login-bg__aurora login-bg__aurora--three" />
        <span className="login-bg__ring login-bg__ring--left" />
        <span className="login-bg__ring login-bg__ring--right" />
        <span className="login-bg__grid" />
        <span className="login-bg__noise" />
      </div>

      <div className="login-card">
        <div className="login-card__brand">
          <span className="login-card__brand-dot" />
          <span className="login-card__brand-text">{t('login.title')}</span>
        </div>

        <h1 className="login-card__title">{t('login.title')}</h1>
        <p className="login-card__subtitle">{t('login.subtitle')}</p>

        <div className="login-card__actions">
          {!isAuthConfigured ? (
            <button className="login-card__btn login-card__btn--primary" onClick={handleMockLogin}>
              {t('login.mockLogin')}
            </button>
          ) : null}
          <button
            className="login-card__btn login-card__btn--google"
            disabled={!isAuthConfigured || isLoading}
            onClick={handleGoogleLogin}
          >
            <img
              className="login-card__google-icon"
              src="https://www.gstatic.com/firebasejs/ui/2.0.0/images/auth/google.svg"
              alt=""
              aria-hidden="true"
            />
            <span>{t('login.googleLogin')}</span>
          </button>
        </div>
      </div>
    </div>
  );
};

export default LoginPage;
