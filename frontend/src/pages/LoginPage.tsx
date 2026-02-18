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
      <div className="login-card">
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
            {t('login.googleLogin')}
          </button>
        </div>
      </div>
    </div>
  );
};

export default LoginPage;
