const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso } = require('../../utils');
const { extractByRegex, extractFirstNumber } = require('../extractors');

class FundamentusScraper extends BaseScraper {
	constructor(options = {}) {
		super({
			...options,
			name: 'scrape_fundamentus',
		});
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_FUNDAMENTUS_TIMEOUT_MS || 15000);
	}

	canHandle(asset) {
		return String(asset.market || '').toUpperCase() === 'BR';
	}

	async scrape(asset) {
		const ticker = String(asset.ticker || '').toUpperCase().replace(/\.SA$/i, '');
		if (!ticker) return null;

		const url = `https://www.fundamentus.com.br/detalhes.php?papel=${encodeURIComponent(ticker)}`;
		const response = await fetchWithTimeout(url, {
			timeoutMs: this.timeoutMs,
			headers: { Accept: 'text/html,*/*' },
		});
		if (!response.ok) return null;

		const html = await response.text();
		const priceText = extractByRegex(html, [
			/<td[^>]*>\s*Cotação\s*<\/td>\s*<td[^>]*>\s*([^<]+)\s*<\/td>/i,
			/Cotação[^0-9-]*(-?\d+(?:[.,]\d+)?)/i,
		]);
		const price = extractFirstNumber(priceText);
		if (!price) return null;

		const peText = extractByRegex(html, [
			/<td[^>]*>\s*P\/L\s*<\/td>\s*<td[^>]*>\s*([^<]+)\s*<\/td>/i,
		]);
		const dyText = extractByRegex(html, [
			/<td[^>]*>\s*Div\. Yield\s*<\/td>\s*<td[^>]*>\s*([^<]+)\s*<\/td>/i,
		]);

		return {
			data_source: 'scrape_fundamentus',
			is_scraped: true,
			quote: {
				currentPrice: price,
				currency: 'BRL',
				change: null,
				changePercent: null,
				previousClose: null,
				marketCap: null,
				volume: null,
			},
			fundamentals: {
				fundamentus: {
					pe: extractFirstNumber(peText),
					dividendYield: extractFirstNumber(dyText),
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
			const response = await fetchWithTimeout('https://www.fundamentus.com.br', {
				timeoutMs: this.timeoutMs,
			});
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
	FundamentusScraper,
};
