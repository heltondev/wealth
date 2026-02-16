const { fetchWithTimeout, withRetry } = require('../../utils');

const DEFAULT_TIMEOUT_MS = 15000;
const BASE_URL = 'https://www.fundsexplorer.com.br/funds/';

/**
 * Parse a Brazilian-formatted number string (e.g. "132.352,72") into a float.
 */
const parseBrNumber = (str) => {
	if (!str) return 0;
	const cleaned = String(str).replace(/\./g, '').replace(',', '.').replace(/[^\d.]/g, '');
	const n = parseFloat(cleaned);
	return Number.isFinite(n) ? n : 0;
};

class FundsExplorerProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(
			options.timeoutMs || process.env.MARKET_DATA_FUNDSEXPLORER_TIMEOUT_MS || DEFAULT_TIMEOUT_MS
		);
	}

	canHandle(asset) {
		const market = String(asset?.market || '').toUpperCase();
		const assetClass = String(asset?.assetClass || '').toLowerCase();
		const ticker = String(asset?.ticker || '').toUpperCase().trim();
		if (market !== 'BR') return false;
		if (assetClass && assetClass !== 'fii') return false;
		return assetClass === 'fii' || ticker.endsWith('11');
	}

	async fetch(asset) {
		if (!this.canHandle(asset)) return null;

		const ticker = String(asset.ticker || '').toLowerCase().trim();
		if (!ticker) return null;

		try {
			const html = await withRetry(
				async () => {
					const url = `${BASE_URL}${ticker}`;
					const res = await fetchWithTimeout(url, {
						timeoutMs: this.timeoutMs,
						headers: {
							'User-Agent':
								'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
							Accept: 'text/html,application/xhtml+xml',
						},
					});
					if (!res.ok) throw new Error(`HTTP ${res.status}`);
					return res.text();
				},
				{ retries: 1, baseDelayMs: 500 }
			);

			const rows = this.#parseProperties(html);
			return {
				data_source: 'fundsexplorer',
				fund_portfolio: rows,
				portfolio_composition: rows,
			};
		} catch {
			return null;
		}
	}

	#parseProperties(html) {
		if (!html) return [];

		const containerMatch = html.match(
			/data-element=["']properties-swiper-container["'][^>]*>([\s\S]*?)(?:<div[^>]*class=["'][^"']*swiper-button|<\/section|$)/
		);
		if (!containerMatch) return [];

		const container = containerMatch[1];
		const slidePattern = /<div[^>]*class=["'][^"']*swiper-slide["'][^>]*>([\s\S]*?)(?=<div[^>]*class=["'][^"']*swiper-slide["']|$)/g;
		const slides = [];
		let match;
		while ((match = slidePattern.exec(container)) !== null) {
			slides.push(match[1]);
		}

		if (slides.length === 0) return [];

		const properties = [];
		for (const slide of slides) {
			const titleMatch = slide.match(
				/<div[^>]*class=["'][^"']*locationGrid__title["'][^>]*>\s*([\s\S]*?)\s*<\/div/
			);
			const name = titleMatch ? titleMatch[1].replace(/<[^>]*>/g, '').trim() : null;
			if (!name) continue;

			let city = '';
			let area = 0;

			const liPattern = /<li[^>]*>([\s\S]*?)<\/li/g;
			let liMatch;
			while ((liMatch = liPattern.exec(slide)) !== null) {
				const liContent = liMatch[1];
				const text = liContent.replace(/<[^>]*>/g, '').trim();

				if (/Cidade:\s*/i.test(text)) {
					city = text.replace(/.*Cidade:\s*/i, '').trim();
				} else if (/Área Bruta Locável:\s*/i.test(text)) {
					const areaStr = text.replace(/.*Área Bruta Locável:\s*/i, '').trim();
					area = parseBrNumber(areaStr);
				}
			}

			properties.push({ name, city, area });
		}

		const totalArea = properties.reduce((sum, p) => sum + p.area, 0);

		return properties.map((p) => ({
			label: p.name,
			allocation_pct: totalArea > 0 ? (p.area / totalArea) * 100 : 0,
			category: p.city || null,
			source: 'fundsexplorer',
		}));
	}
}

module.exports = { FundsExplorerProvider };
