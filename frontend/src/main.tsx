import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './styles/main.scss';
import App from './App';
import './i18n';
import { ThemeProvider } from './context/ThemeContext';
import { ToastProvider } from './context/ToastContext';
import { AuthProvider } from './context/AuthContext';
import { PortfolioDataProvider } from './context/PortfolioDataContext';

// Amplify config stub - will be configured when AWS is set up
// import { Amplify } from 'aws-amplify';
// Amplify.configure(awsExports);

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
