const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso } = require('../../utils');
const { extractByRegex, extractFirstNumber } = require('../extractors');

class TesouroDiretoScraper extends BaseScraper {
	constructor(options = {}) {
		super({
			...options,
			name: 'scrape_tesouro_direto',
		});
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_TESOURO_TIMEOUT_MS || 15000);
	}

	canHandle(asset) {
		return String(asset.market || '').toUpperCase() === 'TESOURO';
	}

	async scrape(asset) {
		const url = 'https://www.tesourodireto.com.br/titulos/precos-e-taxas.htm';
		const response = await fetchWithTimeout(url, {
			timeoutMs: this.timeoutMs,
			headers: { Accept: 'text/html,*/*' },
		});
		if (!response.ok) return null;
		const html = await response.text();

		const ticker = String(asset.ticker || '').toUpperCase();
		const yearMatch = ticker.match(/20\d{2}/);
		const titleHint = ticker
			.replace(/-/g, ' ')
			.replace(/\s+/g, ' ')
			.trim();
		const escapedTitleHint = titleHint.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

		// This parser is intentionally defensive: when page structure changes, it will
		// return null so the caller can mark source unavailable instead of crashing.
		const rowSnippet = extractByRegex(html, [
			new RegExp(`(${escapedTitleHint}[\\s\\S]{0,700})`, 'i'),
			yearMatch ? new RegExp(`(${yearMatch[0]}[\\s\\S]{0,700})`, 'i') : null,
		].filter(Boolean));
		if (!rowSnippet) return null;

		const sellPrice = extractFirstNumber(
			extractByRegex(rowSnippet, [/(?:PU Venda|Preço Venda|Venda)[^0-9-]*(-?\d+(?:[.,]\d+)?)/i])
		);
		const buyPrice = extractFirstNumber(
			extractByRegex(rowSnippet, [/(?:PU Compra|Preço Compra|Compra)[^0-9-]*(-?\d+(?:[.,]\d+)?)/i])
		);
		const currentPrice = sellPrice ?? buyPrice;
		if (!currentPrice) return null;

		return {
			data_source: 'scrape_tesouro_direto',
			is_scraped: true,
			quote: {
				currentPrice,
				currency: 'BRL',
				change: null,
				changePercent: null,
				previousClose: null,
				marketCap: null,
				volume: null,
			},
			fundamentals: {
				tesouro: {
					sellPrice,
					buyPrice,
					url,
				},
			},
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				url,
				rowSnippet,
			},
		};
	}

	async healthCheck() {
		try {
			const response = await fetchWithTimeout(
				'https://www.tesourodireto.com.br',
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
	TesouroDiretoScraper,
};
