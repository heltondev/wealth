import { BrowserRouter, Routes, Route } from 'react-router';
import { Suspense, lazy } from 'react';
import LoginPage from './pages/LoginPage';
import ProtectedRoute from './components/ProtectedRoute';
import type { AppRole } from './utils/authz';

const DashboardPage = lazy(() => import('./pages/DashboardPage'));
const AssetsPage = lazy(() => import('./pages/AssetsPage'));
const AssetDetailsPage = lazy(() => import('./pages/AssetDetailsPage'));
const TransactionsPage = lazy(() => import('./pages/TransactionsPage'));
const DividendsPage = lazy(() => import('./pages/DividendsPage'));
const DocumentsPage = lazy(() => import('./pages/DocumentsPage'));
const SettingsPage = lazy(() => import('./pages/SettingsPage'));

const ALL_ROLES: AppRole[] = ['VIEWER', 'EDITOR', 'ADMIN'];
const ADMIN_ONLY: AppRole[] = ['ADMIN'];

function App() {
  const routeFallback = (
    <div style={{ padding: '24px', color: 'var(--text-secondary)' }}>
      Loading...
    </div>
  );

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route
          path="/dashboard"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <DashboardPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/assets"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <AssetsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/assets/:assetId"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <AssetDetailsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/transactions"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <TransactionsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/dividends"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <DividendsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/documents"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <DocumentsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/settings"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ADMIN_ONLY}>
                <SettingsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route path="*" element={<LoginPage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
