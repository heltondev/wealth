import type { ResourcesConfig } from 'aws-amplify';

const userPoolId = (import.meta.env.VITE_USER_POOL_ID || '').toString().trim();
const userPoolClientId = (import.meta.env.VITE_APP_CLIENT_ID || '').toString().trim();
const cognitoDomain = (import.meta.env.VITE_COGNITO_DOMAIN || '').toString().trim();
const browserOrigin = typeof window !== 'undefined' ? window.location.origin : '';
const defaultOrigin = browserOrigin || 'http://localhost:5173';

const resolveRedirectUrls = (rawValue: unknown, fallbackPath: string): string[] => {
  const explicit = String(rawValue || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

  if (explicit.length > 0) return explicit;
  return [`${defaultOrigin}${fallbackPath}`];
};

const redirectSignIn = resolveRedirectUrls(import.meta.env.VITE_REDIRECT_SIGN_IN, '/dashboard');
const redirectSignOut = resolveRedirectUrls(import.meta.env.VITE_REDIRECT_SIGN_OUT, '/login');

export const isAmplifyAuthConfigured = Boolean(
  userPoolId && userPoolClientId && cognitoDomain
);

const config: ResourcesConfig = isAmplifyAuthConfigured
  ? {
      Auth: {
        Cognito: {
          userPoolId,
          userPoolClientId,
          loginWith: {
            oauth: {
              domain: cognitoDomain,
              scopes: ['email', 'profile', 'openid'],
              redirectSignIn,
              redirectSignOut,
              responseType: 'code',
            },
          },
        },
      },
    }
  : {};

export default config;
