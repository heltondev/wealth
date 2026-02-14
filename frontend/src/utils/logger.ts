const isDev = import.meta.env.DEV;

export const logger = {
  info: (...args: unknown[]) => isDev && console.log('[WealthHub]', ...args),
  warn: (...args: unknown[]) => isDev && console.warn('[WealthHub]', ...args),
  error: (...args: unknown[]) => console.error('[WealthHub]', ...args),
};
