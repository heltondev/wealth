const isDev = import.meta.env.DEV;

export const logger = {
  info: (...args: unknown[]) => isDev && console.log('[Invest]', ...args),
  warn: (...args: unknown[]) => isDev && console.warn('[Invest]', ...args),
  error: (...args: unknown[]) => console.error('[Invest]', ...args),
};
