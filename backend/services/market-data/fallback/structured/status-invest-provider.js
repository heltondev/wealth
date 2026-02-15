const { fetchWithTimeout, withRetry, toNumberOrNull } = require('../../utils');
const { extractJsonScriptContent } = require('../extractors');

const buildStatusInvestUrl = (asset) => {
	const ticker = String(asset.ticker || '').toLowerCase().replace(/\.sa$/i, '');
	const assetClass = String(asset.assetClass || '').toLowerCase();

	if (!ticker) return null;
	if (assetClass === 'fii') {
		return `https://statusinvest.com.br/fundos-imobiliarios/${ticker}`;
	}
	return `https://statusinvest.com.br/acoes/${ticker}`;
};

const deepFindByHints = (value, hints) => {
	if (!value || typeof value !== 'object') return null;
	for (const [key, entry] of Object.entries(value)) {
		const normalized = key.toLowerCase();
		if (hints.some((hint) => normalized.includes(hint))) {
			const numeric = toNumberOrNull(entry);
			if (numeric !== null) return numeric;
		}
	}
	for (const entry of Object.values(value)) {
		if (entry && typeof entry === 'object') {
			const found = deepFindByHints(entry, hints);
			if (found !== null) return found;
		}
	}
	return null;
};

class StatusInvestStructuredProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_STATUSINVEST_TIMEOUT_MS || 15000);
	}

	async fetch(asset) {
		if (String(asset.market || '').toUpperCase() !== 'BR') return null;

		const url = buildStatusInvestUrl(asset);
		if (!url) return null;

		try {
			const response = await withRetry(
				() =>
					fetchWithTimeout(url, {
						timeoutMs: this.timeoutMs,
						headers: { Accept: 'text/html,*/*' },
					}),
				{ retries: 1, baseDelayMs: 300, factor: 2 }
			);
			if (!response.ok) return null;

			const html = await response.text();
			const nextDataRaw = extractJsonScriptContent(html, '__NEXT_DATA__');
			if (!nextDataRaw) return null;

			const nextData = JSON.parse(nextDataRaw);
			const price = deepFindByHints(nextData, ['price', 'cotacao', 'last']);
			if (!price) return null;

			return {
				data_source: 'statusinvest_structured',
				is_scraped: false,
				quote: {
					currentPrice: price,
					currency: 'BRL',
					change: deepFindByHints(nextData, ['change']),
					changePercent: deepFindByHints(nextData, ['percent']),
					previousClose: null,
					marketCap: deepFindByHints(nextData, ['marketcap', 'valorMercado']),
					volume: deepFindByHints(nextData, ['volume']),
				},
				fundamentals: {
					status_invest: nextData,
				},
				historical: {
					history_30d: [],
					dividends: [],
				},
				raw: nextData,
			};
		} catch {
			return null;
		}
	}
}

module.exports = {
	StatusInvestStructuredProvider,
};
