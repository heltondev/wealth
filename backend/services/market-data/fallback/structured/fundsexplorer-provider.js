const { fetchWithTimeout, withRetry } = require('../../utils');
const { execFile } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');

const DEFAULT_TIMEOUT_MS = 15000;
const BASE_URL = 'https://www.fundsexplorer.com.br/funds/';
const EMISSIONS_URL = 'https://www.fundsexplorer.com.br/emissoes-ipos';
const execFileAsync = promisify(execFile);
const LOCAL_CACHE_FILE = path.resolve(__dirname, '../../../../data/fundsexplorer-descriptions.json');
const HTML_ENTITY_MAP = {
	amp: '&',
	lt: '<',
	gt: '>',
	quot: '"',
	apos: "'",
	nbsp: ' ',
};

/**
 * Parse a Brazilian-formatted number string (e.g. "132.352,72") into a float.
 */
const parseBrNumber = (str) => {
	if (!str) return 0;
	const cleaned = String(str).replace(/\./g, '').replace(',', '.').replace(/[^\d.]/g, '');
	const n = parseFloat(cleaned);
	return Number.isFinite(n) ? n : 0;
};

const decodeHtmlEntities = (value) => {
	if (!value) return '';
	return String(value)
		.replace(/&#(\d+);/g, (_, decimal) => {
			const codePoint = Number(decimal);
			return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : '';
		})
		.replace(/&#x([0-9a-f]+);/gi, (_, hexadecimal) => {
			const codePoint = Number.parseInt(hexadecimal, 16);
			return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : '';
		})
		.replace(/&([a-z]+);/gi, (_, name) => HTML_ENTITY_MAP[name.toLowerCase()] || `&${name};`);
};

class FundsExplorerProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(
			options.timeoutMs || process.env.MARKET_DATA_FUNDSEXPLORER_TIMEOUT_MS || DEFAULT_TIMEOUT_MS
		);
		this.localDescriptionCache = null;
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
				async () => this.#fetchPageHtml(`${BASE_URL}${ticker}`),
				{ retries: 1, baseDelayMs: 500 }
			);

			const rows = this.#parseProperties(html);
			const descriptionHtml = this.#parseDescriptionHtml(html);
			const description = this.#parseDescription(html, descriptionHtml);
			const dividendsResume = this.#parseDividendsResume(html);
			const fundInfo = {
				source: 'fundsexplorer',
			};
			if (description) {
				fundInfo.description = description;
				fundInfo.description_html = descriptionHtml;
			}
			if (dividendsResume) {
				fundInfo.dividends_resume = dividendsResume;
			}
			return {
				data_source: 'fundsexplorer',
				fund_info: Object.keys(fundInfo).length > 1 ? fundInfo : null,
				fund_portfolio: rows,
				portfolio_composition: rows,
			};
		} catch {
			const localFallback = this.#readLocalDescriptionFallback(ticker);
			if (!localFallback) return null;
			return {
				data_source: localFallback.source || 'fundsexplorer_local_cache',
				fund_info: {
					description: localFallback.description || null,
					description_html: localFallback.description_html || null,
					dividends_resume: localFallback.dividends_resume || null,
					source: localFallback.source || 'fundsexplorer_local_cache',
				},
				fund_portfolio: [],
				portfolio_composition: [],
			};
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
					async () => this.#fetchPageHtml(url),
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

	async #fetchPageHtml(url) {
		try {
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
		} catch (error) {
			if (!this.#shouldUseCurlFallback(error)) throw error;
			return this.#fetchPageHtmlWithCurl(url);
		}
	}

	#shouldUseCurlFallback(error) {
		const errorCode = String(error?.cause?.code || error?.code || '').toUpperCase();
		const message = String(error?.message || '').toUpperCase();
		return (
			errorCode === 'ENOTFOUND' ||
			errorCode === 'EAI_AGAIN' ||
			errorCode === 'ENETUNREACH' ||
			errorCode === 'EHOSTUNREACH' ||
			message.includes('ENOTFOUND') ||
			message.includes('EAI_AGAIN')
		);
	}

	async #fetchPageHtmlWithCurl(url) {
		const timeoutSeconds = Math.max(5, Math.ceil(this.timeoutMs / 1000));
		const { stdout } = await execFileAsync(
			'curl',
			[
				'-sL',
				'--max-time',
				String(timeoutSeconds),
				'--connect-timeout',
				'10',
				'-A',
				'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
				'-H',
				'Accept: text/html,application/xhtml+xml',
				url,
			],
			{
				maxBuffer: 8 * 1024 * 1024,
			}
		);
		if (!stdout || String(stdout).trim().length === 0) {
			throw new Error('curl returned empty response body');
		}
		return String(stdout);
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

	#parseDescription(html, descriptionHtml = null) {
		if (!html) return null;
		if (descriptionHtml) {
			const descriptionText = this.#normalizeDescriptionText(descriptionHtml);
			if (descriptionText) return descriptionText;
		}

		const dataLayerContent = this.#parseDataLayerContent(html);
		const dataLayerDescription = this.#normalizeDescriptionText(
			dataLayerContent?.pagePostTerms?.meta?.tudo_sobre ||
				dataLayerContent?.pagePostTerms?.meta?.descricao ||
				dataLayerContent?.pagePostTerms?.meta?.description ||
				null
		);
		if (dataLayerDescription) return dataLayerDescription;

		return this.#normalizeDescriptionText(
			this.#extractMetaContent(html, 'description') ||
				this.#extractMetaContent(html, 'og:description') ||
				this.#extractMetaContent(html, 'twitter:description') ||
				null
		);
	}

	#parseDescriptionHtml(html) {
		const articleHtml = this.#extractDescriptionArticleHtml(html);
		if (!articleHtml) return null;
		return this.#sanitizeDescriptionHtml(articleHtml);
	}

	#parseDividendsResume(html) {
		const container = this.#extractDivContainerByClass(html, 'dividends-resume');
		if (!container?.html) return null;

		const block = container.html;
		const title = this.#normalizeInlineText(
			(block.match(/<h2[^>]*>([\s\S]*?)<\/h2>/i) || [])[1] || ''
		);

		const txtContainer = this.#extractDivContainerByClass(block, 'txt');
		const paragraphs = [];
		if (txtContainer?.html) {
			const paragraphPattern = /<p[^>]*>([\s\S]*?)<\/p>/gi;
			let paragraphMatch;
			while ((paragraphMatch = paragraphPattern.exec(txtContainer.html)) !== null) {
				const paragraphText = this.#normalizeInlineText(paragraphMatch[1]);
				if (!paragraphText) continue;
				paragraphs.push(paragraphText);
			}
		}

		const tableContainer = this.#extractDivContainerByClass(block, 'yieldChart__table');
		const tableBlocks = this.#extractAllDivContainersByClass(
			tableContainer?.html || block,
			'yieldChart__table__bloco'
		);
		const parsedTableBlocks = tableBlocks
			.map((item) => this.#extractTableLines(item.html))
			.filter((lines) => Array.isArray(lines) && lines.length > 0);

		let table = null;
		if (parsedTableBlocks.length >= 3) {
			const periods = parsedTableBlocks[0].slice(1);
			const returnByUnitLabel = parsedTableBlocks[1][0] || null;
			const returnByUnit = parsedTableBlocks[1].slice(1);
			const relativeToQuoteLabel = parsedTableBlocks[2][0] || null;
			const relativeToQuote = parsedTableBlocks[2].slice(1);
			const columnsCount = Math.min(periods.length, returnByUnit.length, relativeToQuote.length);

			table = {
				periods: periods.slice(0, columnsCount),
				return_by_unit_label: returnByUnitLabel,
				return_by_unit: returnByUnit.slice(0, columnsCount),
				relative_to_quote_label: relativeToQuoteLabel,
				relative_to_quote: relativeToQuote.slice(0, columnsCount),
			};
		}

		if (!title && paragraphs.length === 0 && !table) return null;
		return {
			title,
			paragraphs,
			table,
			source: 'fundsexplorer',
		};
	}

	#extractTableLines(html) {
		const lines = [];
		const linePattern = /<div[^>]*class=["'][^"']*table__linha[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi;
		let match;
		while ((match = linePattern.exec(String(html || ''))) !== null) {
			const text = this.#normalizeInlineText(match[1]);
			if (!text) continue;
			lines.push(text);
		}
		return lines;
	}

	#normalizeInlineText(value) {
		const raw = String(value || '');
		if (!raw) return '';
		return decodeHtmlEntities(raw)
			.replace(/<[^>]+>/g, ' ')
			.replace(/\s+/g, ' ')
			.trim();
	}

	#extractDivContainerByClass(html, className, startFrom = 0) {
		if (!html || !className) return null;
		const escapedClassName = String(className).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
		const openTagPattern = new RegExp(
			`<div[^>]*class=["'][^"']*\\b${escapedClassName}\\b[^"']*["'][^>]*>`,
			'gi'
		);
		openTagPattern.lastIndex = Math.max(0, Number(startFrom) || 0);
		const match = openTagPattern.exec(String(html));
		if (!match) return null;
		const extracted = this.#extractBalancedDivFromIndex(String(html), match.index);
		return extracted;
	}

	#extractAllDivContainersByClass(html, className) {
		const blocks = [];
		let cursor = 0;
		while (cursor < String(html || '').length) {
			const found = this.#extractDivContainerByClass(html, className, cursor);
			if (!found) break;
			blocks.push(found);
			cursor = found.endIndex;
		}
		return blocks;
	}

	#extractBalancedDivFromIndex(html, startIndex) {
		const source = String(html || '');
		const start = Number(startIndex);
		if (!Number.isFinite(start) || start < 0 || start >= source.length) return null;

		const startTagMatch = source.slice(start).match(/^<div\b[^>]*>/i);
		if (!startTagMatch?.[0]) return null;

		let cursor = start + startTagMatch[0].length;
		let depth = 1;
		while (depth > 0) {
			const nextOpen = source.indexOf('<div', cursor);
			const nextClose = source.indexOf('</div>', cursor);
			if (nextClose === -1) return null;
			if (nextOpen !== -1 && nextOpen < nextClose) {
				depth += 1;
				cursor = nextOpen + 4;
				continue;
			}
			depth -= 1;
			cursor = nextClose + 6;
		}

		return {
			html: source.slice(start, cursor),
			startIndex: start,
			endIndex: cursor,
		};
	}

	#extractDescriptionArticleHtml(html) {
		if (!html) return null;
		const htmlText = String(html);

		const extractArticleFromBlock = (blockHtml) => {
			if (!blockHtml) return null;
			const prioritized = blockHtml.match(
				/<article[^>]*class=["'][^"']*newsContent__article[^"']*["'][^>]*>[\s\S]*?<\/article>/i
			);
			if (prioritized?.[0]) return prioritized[0];

			const generic = blockHtml.match(/<article[^>]*>[\s\S]*?<\/article>/i);
			return generic?.[0] || null;
		};

		const sectionMatch = htmlText.match(
			/<section[^>]*id=["'][^"']*carbon_fields_fiis_description[^"']*["'][^>]*>([\s\S]*?)<\/section>/i
		);
		const sectionArticle = extractArticleFromBlock(sectionMatch?.[1] || '');
		if (sectionArticle) return sectionArticle;

		const headingScopedMatch = htmlText.match(
			/<h2[^>]*>\s*Descri(?:ç|c)[aã]o\s+do[\s\S]*?<\/h2>([\s\S]*?)(?:<\/section>|<section\b|$)/i
		);
		const headingScopedArticle = extractArticleFromBlock(headingScopedMatch?.[1] || '');
		if (headingScopedArticle) return headingScopedArticle;

		return null;
	}

	#sanitizeDescriptionHtml(value) {
		let html = String(value || '').trim();
		if (!html) return null;

		// Remove dangerous blocks and inline event/script vectors before allowing rich text tags.
		html = html
			.replace(/<!--[\s\S]*?-->/g, '')
			.replace(/<\s*(script|style|iframe|object|embed|form|svg|math|template)[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi, '')
			.replace(/\s(on[a-z]+)\s*=\s*(['"]).*?\2/gi, '')
			.replace(/\sstyle\s*=\s*(['"]).*?\1/gi, '')
			.replace(/\s(srcdoc)\s*=\s*(['"]).*?\2/gi, '')
			.replace(/\s(href|src)\s*=\s*(['"])\s*javascript:[\s\S]*?\2/gi, '');

		const allowedTags = new Set([
			'article',
			'h3',
			'b',
			'strong',
			'em',
			'i',
			'p',
			'br',
			'ul',
			'ol',
			'li',
			'a',
		]);

		html = html.replace(/<\/?([a-zA-Z0-9]+)(\s[^>]*)?>/g, (full, tagName) => {
			const normalizedTag = String(tagName || '').toLowerCase();
			if (!allowedTags.has(normalizedTag)) return '';
			return full.trim().startsWith('</') ? `</${normalizedTag}>` : `<${normalizedTag}>`;
		});

		const plainText = this.#normalizeDescriptionText(html);
		if (!plainText) return null;
		return html
			.replace(/\r/g, '\n')
			.replace(/\n{3,}/g, '\n\n')
			.trim();
	}

	#extractMetaContent(html, metaName) {
		const target = String(metaName || '').toLowerCase();
		if (!target) return null;

		const tags = String(html).match(/<meta\b[^>]*>/gi) || [];
		for (const tag of tags) {
			const attrs = {};
			const attrPattern = /([a-zA-Z_:.-]+)\s*=\s*["']([^"']*)["']/g;
			let attrMatch;
			while ((attrMatch = attrPattern.exec(tag)) !== null) {
				attrs[attrMatch[1].toLowerCase()] = attrMatch[2];
			}
			const name = String(attrs.name || attrs.property || '').toLowerCase();
			if (name !== target) continue;
			return attrs.content || null;
		}
		return null;
	}

	#parseDataLayerContent(html) {
		const match = String(html).match(
			/var\s+dataLayer_content\s*=\s*(\{[\s\S]*?\})\s*;\s*dataLayer\.push/i
		);
		if (!match?.[1]) return null;
		try {
			return JSON.parse(match[1]);
		} catch {
			return null;
		}
	}

	#normalizeDescriptionText(value) {
		const raw = String(value || '').trim();
		if (!raw) return null;

		const withLineBreaks = raw
			.replace(/<\s*br\s*\/?>/gi, '\n')
			.replace(/<\/\s*(p|div|li|h1|h2|h3|h4|h5|h6|article)\s*>/gi, '\n')
			.replace(/<\s*li[^>]*>/gi, '• ');
		const withoutTags = withLineBreaks.replace(/<[^>]+>/g, ' ');
		const decoded = decodeHtmlEntities(withoutTags)
			.replace(/\r/g, '\n')
			.replace(/[ \t]+\n/g, '\n')
			.replace(/\n{3,}/g, '\n\n');

		const lines = decoded
			.split('\n')
			.map((line) => line.replace(/\s+/g, ' ').trim())
			.filter(Boolean);

		if (lines.length === 0) return null;
		return lines.join('\n');
	}

	#readLocalDescriptionFallback(ticker) {
		const normalizedTicker = String(ticker || '').toUpperCase().trim();
		if (!normalizedTicker) return null;

		if (this.localDescriptionCache === null) {
			try {
				const content = fs.readFileSync(LOCAL_CACHE_FILE, 'utf8');
				this.localDescriptionCache = JSON.parse(content);
			} catch {
				this.localDescriptionCache = {};
			}
		}

		const items = this.localDescriptionCache?.items || this.localDescriptionCache || {};
		const entry = items?.[normalizedTicker] || null;
		if (!entry) return null;

		const descriptionHtml = this.#sanitizeDescriptionHtml(
			entry.description_html || entry.descriptionHtml || null
		);
		const description =
			this.#normalizeDescriptionText(entry.description || null) ||
			this.#normalizeDescriptionText(descriptionHtml || null);
		const dividendsResume = entry.dividends_resume || entry.dividendsResume || null;
		if (!description && !descriptionHtml && !dividendsResume) return null;

		return {
			description,
			description_html: descriptionHtml,
			dividends_resume: dividendsResume,
			source: 'fundsexplorer_local_cache',
		};
	}
}

module.exports = { FundsExplorerProvider };
