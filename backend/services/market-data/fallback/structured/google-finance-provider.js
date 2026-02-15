const { fetchWithTimeout, withRetry, toNumberOrNull } = require('../../utils');

const buildGoogleSymbols = (ticker, market) => {
	const normalized = String(ticker || '').toUpperCase().replace(/\.SA$|\.TO$/g, '');
	if (!normalized) return [];

	if (market === 'BR') return [`${normalized}:BVMF`];
	if (market === 'CA') return [`${normalized}:TSE`, `${normalized}:CVE`];
	return [`${normalized}:NASDAQ`, `${normalized}:NYSE`];
};

const deepFindNumberByKeyHint = (value, keyHints) => {
	if (!value || typeof value !== 'object') return null;

	const entries = Object.entries(value);
	for (const [key, entryValue] of entries) {
		const normalizedKey = key.toLowerCase();
		if (keyHints.some((hint) => normalizedKey.includes(hint))) {
			const numeric = toNumberOrNull(entryValue);
			if (numeric !== null) return numeric;
		}
	}

	for (const [, entryValue] of entries) {
		if (entryValue && typeof entryValue === 'object') {
			const found = deepFindNumberByKeyHint(entryValue, keyHints);
			if (found !== null) return found;
		}
	}

	return null;
};

class GoogleFinanceStructuredProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_GOOGLE_TIMEOUT_MS || 15000);
	}

	async fetch(asset) {
		const symbols = buildGoogleSymbols(asset.ticker, asset.market);
		for (const symbol of symbols) {
			const url = `https://www.google.com/finance/quote/${encodeURIComponent(symbol)}?hl=en&gl=us&output=json`;

			try {
				const response = await withRetry(
					() =>
						fetchWithTimeout(url, {
							timeoutMs: this.timeoutMs,
							headers: {
								Accept: 'application/json,text/plain,*/*',
							},
						}),
					{
						retries: 1,
						baseDelayMs: 400,
						factor: 2,
					}
				);
				if (!response.ok) continue;

				const contentType = String(response.headers.get('content-type') || '').toLowerCase();
				if (!contentType.includes('json')) continue;

				const json = await response.json();
				const price =
					deepFindNumberByKeyHint(json, ['price', 'last', 'close']) ??
					toNumberOrNull(json?.price);
				if (!price) continue;

				return {
					data_source: 'google_finance_structured',
					is_scraped: false,
					quote: {
						currentPrice: price,
						currency: json?.currency || null,
						change: deepFindNumberByKeyHint(json, ['change']),
						changePercent: deepFindNumberByKeyHint(json, ['changepct', 'changepercent']),
						previousClose: deepFindNumberByKeyHint(json, ['previous', 'prevclose']),
						marketCap: deepFindNumberByKeyHint(json, ['marketcap']),
						volume: deepFindNumberByKeyHint(json, ['volume']),
					},
					fundamentals: {
						info: json,
					},
					historical: {
						history_30d: [],
						dividends: [],
					},
					raw: json,
				};
			} catch {
				// Try next candidate symbol.
			}
		}

		return null;
	}
}

module.exports = {
	GoogleFinanceStructuredProvider,
};
