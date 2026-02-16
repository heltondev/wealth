const { fetchWithTimeout, withRetry } = require('../../utils');

const DEFAULT_TIMEOUT_MS = 15000;
const BASE_URL = 'https://www.fundsexplorer.com.br/funds/';
const EMISSIONS_URL = 'https://www.fundsexplorer.com.br/emissoes-ipos';

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

	async fetchEmissions(asset) {
		const assetClass = String(asset?.assetClass || '').toLowerCase();
		const ticker = String(asset?.ticker || '').toUpperCase().trim();
		if (!ticker) return null;
		if (assetClass && assetClass !== 'fii' && !ticker.endsWith('11')) return null;

		try {
			const fetchUrl = (url) =>
				withRetry(
					async () => {
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

			const tickerLower = ticker.toLowerCase();

			// The emissions listing page ignores the ticker query param and always
			// returns the same global list, but it pins any active emission for the
			// requested ticker at the top of every page.  The fund detail page also
			// embeds the active emission card with the same HTML structure.  We
			// fetch both sources in parallel and merge/deduplicate by emission number.
			const [emissionsHtml, fundHtml] = await Promise.all([
				fetchUrl(`${EMISSIONS_URL}?filter=todos`),
				fetchUrl(`${BASE_URL}${tickerLower}`).catch(() => ''),
			]);

			const seen = new Set();
			const emissions = [];

			for (const html of [emissionsHtml, fundHtml]) {
				for (const card of this.#parseEmissions(html)) {
					if (card.ticker.toLowerCase() !== tickerLower) continue;
					const key = `${card.ticker}-${card.emissionNumber}`;
					if (seen.has(key)) continue;
					seen.add(key);
					emissions.push(card);
				}
			}

			return {
				ticker,
				emissions,
				data_source: 'fundsexplorer',
				fetched_at: new Date().toISOString(),
			};
		} catch {
			return null;
		}
	}

	#parseEmissions(html) {
		if (!html) return [];

		const cardPattern = /<div\s+class="emissaoCard"[^>]*>([\s\S]*?)(?=<div\s+class="emissaoCard"|<div\s+class="emissoesSearch__|$)/g;
		const emissions = [];
		let cardMatch;

		while ((cardMatch = cardPattern.exec(html)) !== null) {
			const card = cardMatch[0];

			const tickerAttr = card.match(/data-ticker="([^"]*)"/);
			const stageAttr = card.match(/data-stage="([^"]*)"/);
			const offerAttr = card.match(/data-offer="([^"]*)"/);

			const emissionText = card.match(/<span\s+class="rowPill">([^<]*)<\/span>/);
			const emissionNumber = emissionText
				? parseInt(emissionText[1].replace(/[^\d]/g, ''), 10) || 0
				: 0;

			const priceRow = card.match(/aria-label="Preço R\$ Desconto"[\s\S]*?<p>([^<]*)<\/p>/);
			const price = priceRow ? parseBrNumber(priceRow[1]) : null;

			const discountMatch = card.match(/class="rowPill--(baixa|alta)">([^<]*)<\/span>/);
			let discount = null;
			if (discountMatch) {
				discount = parseBrNumber(discountMatch[2]);
				if (discountMatch[1] === 'baixa') discount = -Math.abs(discount);
			}

			const baseDateRow = card.match(/aria-label="Data-base"[\s\S]*?<p>([^<]*)<\/p>/);
			const baseDate = baseDateRow ? baseDateRow[1].trim() : '';

			const factorRow = card.match(/aria-label="Fator de proporcao"[\s\S]*?<p>([^<]*)<\/p>/);
			const proportionFactor = factorRow ? factorRow[1].trim() : '';

			const parsePeriod = (label) => {
				const periodPattern = new RegExp(
					`aria-label="${label}"[\\s\\S]*?<div class="emissoesRange">[\\s\\S]*?data-status="([^"]*)"[\\s\\S]*?<div class="emissoesRange__date">[\\s\\S]*?<div>([^<]*)</div>[\\s\\S]*?<div>([^<]*)</div>`
				);
				const m = card.match(periodPattern);
				return m
					? { start: m[2].trim(), end: m[3].trim(), status: m[1].trim() }
					: { start: '', end: '', status: '' };
			};

			const preference = parsePeriod('Período de preferência');
			const sobras = parsePeriod('Período de sobras');
			const publicPeriod = parsePeriod('Período público');

			emissions.push({
				ticker: tickerAttr ? tickerAttr[1].toUpperCase() : '',
				emissionNumber,
				stage: stageAttr ? stageAttr[1] : '',
				offerType: offerAttr ? offerAttr[1] : '',
				price,
				discount,
				baseDate,
				proportionFactor,
				preferenceStart: preference.start,
				preferenceEnd: preference.end,
				preferenceStatus: preference.status,
				sobrasStart: sobras.start,
				sobrasEnd: sobras.end,
				sobrasStatus: sobras.status,
				publicStart: publicPeriod.start,
				publicEnd: publicPeriod.end,
				publicStatus: publicPeriod.status,
			});
		}

		return emissions;
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
			name: p.name,
			allocation_pct: totalArea > 0 ? (p.area / totalArea) * 100 : 0,
			category: p.city || null,
			source: 'fundsexplorer',
		}));
	}
}

module.exports = { FundsExplorerProvider };
