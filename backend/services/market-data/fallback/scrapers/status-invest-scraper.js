const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso } = require('../../utils');
const { extractByRegex, extractFirstNumber } = require('../extractors');

const buildStatusInvestUrl = (asset) => {
	const ticker = String(asset.ticker || '').toLowerCase().replace(/\.sa$/i, '');
	const assetClass = String(asset.assetClass || '').toLowerCase();
	if (!ticker) return null;

	if (assetClass === 'fii') {
		return `https://statusinvest.com.br/fundos-imobiliarios/${ticker}`;
	}
	return `https://statusinvest.com.br/acoes/${ticker}`;
};

class StatusInvestScraper extends BaseScraper {
	constructor(options = {}) {
		super({
			...options,
			name: 'scrape_statusinvest',
		});
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_STATUSINVEST_TIMEOUT_MS || 15000);
	}

	canHandle(asset) {
		return String(asset.market || '').toUpperCase() === 'BR';
	}

	async scrape(asset) {
		const url = buildStatusInvestUrl(asset);
		if (!url) return null;

		const response = await fetchWithTimeout(url, {
			timeoutMs: this.timeoutMs,
			headers: { Accept: 'text/html,*/*' },
		});
		if (!response.ok) return null;
		const html = await response.text();

		const priceText = extractByRegex(html, [
			/<strong[^>]*class="[^"]*value[^"]*"[^>]*>([^<]+)<\/strong>/i,
			/"price"\s*:\s*"([^"]+)"/i,
			/"price"\s*:\s*([0-9.,]+)/i,
		]);
		const price = extractFirstNumber(priceText);
		if (!price) return null;

		const dyText = extractByRegex(html, [
			/Dividend Yield[^0-9-]*(-?\d+(?:[.,]\d+)?)/i,
			/"dy"\s*:\s*([0-9.,]+)/i,
		]);
		const peText = extractByRegex(html, [
			/P\/L[^0-9-]*(-?\d+(?:[.,]\d+)?)/i,
			/"p_l"\s*:\s*([0-9.,]+)/i,
		]);

		return {
			data_source: 'scrape_statusinvest',
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
				status_invest: {
					dividendYield: extractFirstNumber(dyText),
					pe: extractFirstNumber(peText),
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
			const response = await fetchWithTimeout('https://statusinvest.com.br', {
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
	StatusInvestScraper,
};
