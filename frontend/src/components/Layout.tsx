import { NavLink, useNavigate } from 'react-router';
import { useAuth } from '../context/AuthContext';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { useTheme } from '../context/ThemeContext';
import { useTranslation } from 'react-i18next';
import './Layout.scss';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout = ({ children }: LayoutProps) => {
  const { user, logout } = useAuth();
  const { eventNotices, eventNoticesLoading } = usePortfolioData();
  const { theme, toggleTheme } = useTheme();
  const { t } = useTranslation();
  const navigate = useNavigate();
  const todayCount = Number(eventNotices?.unread_today_count ?? eventNotices?.today_count ?? 0);
  const weekCount = Number(eventNotices?.unread_week_count ?? eventNotices?.week_count ?? 0);

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  const navItems: Array<{ path: string; label: string; icon: string; badgeCount?: number }> = [
    { path: '/dashboard', label: t('nav.dashboard'), icon: '📊', badgeCount: weekCount },
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
      <div className="layout-bg" aria-hidden="true">
        <span className="layout-bg__orb layout-bg__orb--one" />
        <span className="layout-bg__orb layout-bg__orb--two" />
        <span className="layout-bg__orb layout-bg__orb--three" />
        <span className="layout-bg__grid" />
        <span className="layout-bg__noise" />
      </div>

      <aside className="sidebar">
        <div className="sidebar__header">
          <h1 className="sidebar__logo">Invest</h1>
          <div className="sidebar__notice">
            <span className={`sidebar__notice-dot ${
              todayCount > 0
                ? 'sidebar__notice-dot--today'
                : weekCount > 0
                  ? 'sidebar__notice-dot--week'
                  : 'sidebar__notice-dot--clear'
            }`}
            />
            <span className="sidebar__notice-text">
              {eventNoticesLoading
                ? t('dashboard.eventsNotice.loading')
                : t('dashboard.eventsNotice.menuSummary', {
                  today: todayCount,
                  week: weekCount,
                })}
            </span>
          </div>
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
              {typeof item.badgeCount === 'number' && Number(item.badgeCount) > 0 ? (
                <span className="sidebar__badge">
                  {item.badgeCount}
                </span>
              ) : null}
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
        <div className="layout__content-shell">
          {children}
        </div>
      </main>
    </div>
  );
};

export default Layout;
