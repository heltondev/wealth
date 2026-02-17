import { BrowserRouter, Navigate, Route, Routes } from 'react-router';
import { Suspense, lazy } from 'react';
import LoginPage from './pages/LoginPage';
import ProtectedRoute from './components/ProtectedRoute';
import type { AppRole } from './utils/authz';

const DashboardPage = lazy(() => import('./pages/DashboardPage'));
const AssetsPage = lazy(() => import('./pages/AssetsPage'));
const AssetDetailsPage = lazy(() => import('./pages/AssetDetailsPage'));
const TransactionsPage = lazy(() => import('./pages/TransactionsPage'));
const DividendsPage = lazy(() => import('./pages/DividendsPage'));
const RebalancePage = lazy(() => import('./pages/RebalancePage'));
const RiskPage = lazy(() => import('./pages/RiskPage'));
const BenchmarksPage = lazy(() => import('./pages/BenchmarksPage'));
const CompareAssetsPage = lazy(() => import('./pages/CompareAssetsPage'));
const MultiCurrencyPage = lazy(() => import('./pages/MultiCurrencyPage'));
const TaxPage = lazy(() => import('./pages/TaxPage'));
const DocumentsPage = lazy(() => import('./pages/DocumentsPage'));
const ImportsPage = lazy(() => import('./pages/ImportsPage'));
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
          path="/rebalance"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <RebalancePage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/benchmarks"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <BenchmarksPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/compare"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <CompareAssetsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/multi-currency"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <MultiCurrencyPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/risk"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <RiskPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/tax"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <TaxPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/reports"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <DocumentsPage />
              </ProtectedRoute>
            </Suspense>
          }
        />
        <Route
          path="/documents"
          element={<Navigate to="/reports" replace />}
        />
        <Route
          path="/imports"
          element={
            <Suspense fallback={routeFallback}>
              <ProtectedRoute allowedRoles={ALL_ROLES}>
                <ImportsPage />
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
