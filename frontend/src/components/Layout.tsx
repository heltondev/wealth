import { NavLink, useNavigate } from 'react-router';
import { useAuth } from '../context/AuthContext';
import { useTheme } from '../context/ThemeContext';
import { useTranslation } from 'react-i18next';
import './Layout.scss';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout = ({ children }: LayoutProps) => {
  const { user, logout } = useAuth();
  const { theme, toggleTheme } = useTheme();
  const { t } = useTranslation();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  const navItems = [
    { path: '/dashboard', label: t('nav.dashboard'), icon: '📊' },
    { path: '/assets', label: t('nav.assets'), icon: '💰' },
    { path: '/transactions', label: t('nav.transactions'), icon: '🧾' },
    { path: '/dividends', label: t('nav.dividends'), icon: '💵' },
    { path: '/rebalance', label: t('nav.rebalance'), icon: '⚖️' },
    { path: '/risk', label: t('nav.risk'), icon: '🛡️' },
    { path: '/benchmarks', label: t('nav.benchmarks'), icon: '📈' },
    { path: '/compare', label: t('nav.compare'), icon: '🆚' },
    { path: '/multi-currency', label: t('nav.multiCurrency'), icon: '💱' },
    { path: '/tax', label: t('nav.tax'), icon: '📑' },
    { path: '/reports', label: t('nav.documents'), icon: '📄' },
    { path: '/imports', label: t('nav.imports'), icon: '📥' },
    { path: '/settings', label: t('nav.settings'), icon: '⚙️' },
  ];

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="sidebar__header">
          <h1 className="sidebar__logo">WealthHub</h1>
        </div>

        <nav className="sidebar__nav">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) =>
                `sidebar__link ${isActive ? 'sidebar__link--active' : ''}`
              }
            >
              <span className="sidebar__icon">{item.icon}</span>
              <span className="sidebar__label">{item.label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="sidebar__footer">
          <button className="sidebar__theme-toggle" onClick={toggleTheme}>
            {theme === 'dark' ? '☀️' : '🌙'}
          </button>
          <div className="sidebar__user">
            <span className="sidebar__user-name">{user?.name}</span>
            <button className="sidebar__logout" onClick={handleLogout}>
              {t('nav.logout')}
            </button>
          </div>
        </div>
      </aside>

      <main className="layout__content">
        {children}
      </main>
    </div>
  );
};

export default Layout;
