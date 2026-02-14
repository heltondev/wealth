import { useNavigate } from 'react-router';
import { useAuth } from '../context/AuthContext';
import { useTranslation } from 'react-i18next';
import { useEffect } from 'react';
import './LoginPage.scss';

const LoginPage = () => {
  const { login, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const { t } = useTranslation();

  useEffect(() => {
    if (isAuthenticated) {
      navigate('/dashboard', { replace: true });
    }
  }, [isAuthenticated, navigate]);

  const handleMockLogin = async () => {
    await login();
    navigate('/dashboard');
  };

  return (
    <div className="login-page">
      <div className="login-card">
        <h1 className="login-card__title">{t('login.title')}</h1>
        <p className="login-card__subtitle">{t('login.subtitle')}</p>

        <div className="login-card__actions">
          <button className="login-card__btn login-card__btn--primary" onClick={handleMockLogin}>
            {t('login.mockLogin')}
          </button>
          <button className="login-card__btn login-card__btn--google" disabled>
            {t('login.googleLogin')}
          </button>
        </div>
      </div>
    </div>
  );
};

export default LoginPage;
