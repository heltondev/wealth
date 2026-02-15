const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso } = require('../../utils');
const { extractByRegex, extractFirstNumber } = require('../extractors');

const buildGoogleSymbol = (asset) => {
	const ticker = String(asset.ticker || '').toUpperCase().replace(/\.SA$|\.TO$/g, '');
	const market = String(asset.market || '').toUpperCase();
	if (!ticker) return null;
	if (market === 'BR') return `${ticker}:BVMF`;
	if (market === 'CA') return `${ticker}:TSE`;
	return `${ticker}:NASDAQ`;
};

class GoogleFinanceScraper extends BaseScraper {
	constructor(options = {}) {
		super({
			...options,
			name: 'scrape_google',
		});
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_GOOGLE_TIMEOUT_MS || 15000);
	}

	canHandle(asset) {
		const market = String(asset.market || '').toUpperCase();
		return ['BR', 'US', 'CA'].includes(market);
	}

	async scrape(asset) {
		const symbol = buildGoogleSymbol(asset);
		if (!symbol) return null;

		const url = `https://www.google.com/finance/quote/${encodeURIComponent(symbol)}?hl=en&gl=us`;
		const response = await fetchWithTimeout(url, {
			timeoutMs: this.timeoutMs,
			headers: { Accept: 'text/html,*/*' },
		});
		if (!response.ok) return null;

		const html = await response.text();
		const priceText = extractByRegex(html, [
			/<div[^>]*class="[^"]*YMlKec[^"]*"[^>]*>([^<]+)<\/div>/i,
			/"price"\s*:\s*"([^"]+)"/i,
			/"lastPrice"\s*:\s*\{"raw"\s*:\s*([0-9.]+)/i,
		]);
		const price = extractFirstNumber(priceText);
		if (!price) return null;

		const changeBlock = extractByRegex(html, [
			/<div[^>]*class="[^"]*JwB6zf[^"]*"[^>]*>([^<]+)<\/div>/i,
		]);
		const change = extractFirstNumber(changeBlock);
		const changePercentMatch = String(changeBlock || '').match(/(-?\d+(?:[.,]\d+)?)%/);
		const changePercent = changePercentMatch
			? extractFirstNumber(changePercentMatch[1])
			: null;

		return {
			data_source: 'scrape_google',
			is_scraped: true,
			quote: {
				currentPrice: price,
				currency: null,
				change,
				changePercent,
				previousClose: null,
				marketCap: null,
				volume: null,
			},
			fundamentals: {
				google_finance: {
					url,
				},
			},
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				url,
				html_excerpt: html.slice(0, 2000),
			},
		};
	}

	async healthCheck() {
		try {
			const response = await fetchWithTimeout(
				'https://www.google.com/finance/quote/AAPL:NASDAQ?hl=en&gl=us',
				{ timeoutMs: this.timeoutMs }
			);
			const ok = response.ok;
			return {
				scraper: this.name,
				ok,
				checked_at: nowIso(),
				details: ok ? 'reachable' : `http_${response.status}`,
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
	GoogleFinanceScraper,
};
