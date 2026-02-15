const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso, toNumberOrNull } = require('../../utils');
const { extractByRegex } = require('../extractors');
const { resolveYahooSymbol } = require('../../symbol-resolver');

const deepFindNumber = (value, keyHints) => {
	if (!value || typeof value !== 'object') return null;
	for (const [key, entryValue] of Object.entries(value)) {
		const normalizedKey = key.toLowerCase();
		if (keyHints.some((hint) => normalizedKey.includes(hint))) {
			const candidate =
				toNumberOrNull(entryValue?.raw) ??
				toNumberOrNull(entryValue?.fmt) ??
				toNumberOrNull(entryValue);
			if (candidate !== null) return candidate;
		}
	}
	for (const entryValue of Object.values(value)) {
		if (entryValue && typeof entryValue === 'object') {
			const found = deepFindNumber(entryValue, keyHints);
			if (found !== null) return found;
		}
	}
	return null;
};

class YahooFinanceScraper extends BaseScraper {
	constructor(options = {}) {
		super({
			...options,
			name: 'scrape_yahoo',
		});
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_YAHOO_SCRAPE_TIMEOUT_MS || 15000);
	}

	canHandle(asset) {
		const market = String(asset.market || '').toUpperCase();
		return ['BR', 'US', 'CA'].includes(market);
	}

	async scrape(asset) {
		const symbol = resolveYahooSymbol(asset.ticker, asset.market);
		if (!symbol) return null;

		const url = `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}`;
		const response = await fetchWithTimeout(url, {
			timeoutMs: this.timeoutMs,
			headers: { Accept: 'text/html,*/*' },
		});
		if (!response.ok) return null;
		const html = await response.text();

		const appMainRaw = extractByRegex(html, [
			/root\.App\.main\s*=\s*({[\s\S]*?});\n/s,
		]);
		if (!appMainRaw) return null;

		let appMain;
		try {
			appMain = JSON.parse(appMainRaw);
		} catch {
			return null;
		}

		const stores = appMain?.context?.dispatcher?.stores || {};
		const quoteStore = stores.QuoteSummaryStore || {};
		const streamStore = stores.StreamDataStore || {};

		const currentPrice =
			deepFindNumber(quoteStore, ['regularmarketprice', 'currentprice']) ??
			deepFindNumber(streamStore, ['regularmarketprice', 'lastprice']);
		if (!currentPrice) return null;

		return {
			data_source: 'scrape_yahoo',
			is_scraped: true,
			quote: {
				currentPrice,
				currency:
					quoteStore?.price?.currency ||
					quoteStore?.price?.financialCurrency ||
					null,
				change: deepFindNumber(quoteStore, ['regularmarketchange']),
				changePercent: deepFindNumber(quoteStore, ['regularmarketchangepercent']),
				previousClose: deepFindNumber(quoteStore, ['previousclose']),
				marketCap: deepFindNumber(quoteStore, ['marketcap']),
				volume: deepFindNumber(quoteStore, ['volume']),
			},
			fundamentals: {
				info: quoteStore,
			},
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				url,
				storeKeys: Object.keys(stores),
				quoteStore,
			},
		};
	}

	async healthCheck() {
		try {
			const response = await fetchWithTimeout(
				'https://finance.yahoo.com/quote/AAPL',
				{ timeoutMs: this.timeoutMs }
			);
			return {
				scraper: this.name,
				ok: response.ok,
				checked_at: nowIso(),
				details: response.ok ? 'reachable' : `http_${response.status}`,
			};
		} catch (error) {
			return {
				scraper: this.name,
				ok: false,
				checked_at: nowIso(),
				details: error.message,
			};
		}
	}
}

module.exports = {
	YahooFinanceScraper,
};
