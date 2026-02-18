import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './styles/main.scss';
import App from './App';
import './i18n';
import { Amplify } from 'aws-amplify';
import awsExports, { isAmplifyAuthConfigured } from './aws-exports';
import { ThemeProvider } from './context/ThemeContext';
import { ToastProvider } from './context/ToastContext';
import { AuthProvider } from './context/AuthContext';
import { PortfolioDataProvider } from './context/PortfolioDataContext';

if (isAmplifyAuthConfigured) {
  Amplify.configure(awsExports);
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ThemeProvider>
      <ToastProvider>
        <AuthProvider>
          <PortfolioDataProvider>
            <App />
          </PortfolioDataProvider>
        </AuthProvider>
      </ToastProvider>
    </ThemeProvider>
  </StrictMode>,
);
