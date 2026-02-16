const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso, toNumberOrNull } = require('../../utils');
const { extractByRegex, extractFirstNumber } = require('../extractors');

const buildGoogleSymbol = (asset) => {
	const ticker = String(asset.ticker || '').toUpperCase().replace(/\.SA$|\.TO$/g, '');
	const market = String(asset.market || '').toUpperCase();
	if (!ticker) return null;
	if (market === 'BR') return `${ticker}:BVMF`;
	if (market === 'CA') return `${ticker}:TSE`;
	return `${ticker}:NASDAQ`;
};

const buildGoogleQuoteUrls = (symbol) => {
	const encoded = encodeURIComponent(symbol);
	return [
		`https://www.google.com/finance/beta/quote/${encoded}?hl=en&gl=us`,
		`https://www.google.com/finance/quote/${encoded}?hl=en&gl=us`,
	];
};

const toCurrencyFromSymbol = (symbol) => {
	const suffix = String(symbol || '').split(':')[1] || '';
	if (suffix === 'BVMF') return 'BRL';
	if (suffix === 'TSE' || suffix === 'CVE') return 'CAD';
	return 'USD';
};

const escapeRegExp = (value) =>
	String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const extractScriptClassContent = (html, className) => {
	const source = String(html || '');
	if (!source || !className) return null;
	const classPattern = escapeRegExp(className);
	const regex = new RegExp(
		`<script[^>]*class=["']${classPattern}["'][^>]*>([\\s\\S]*?)<\\/script>`,
		'i'
	);
	const match = source.match(regex);
	return match?.[1] || null;
};

const extractEntitySectionHtml = (html, symbol) => {
	const source = String(html || '');
	if (!source) return source;
	const labelPattern = new RegExp(
		`data-entity-label=["']${escapeRegExp(symbol)}["']`,
		'i'
	);
	const match = labelPattern.exec(source);
	if (!match) return source;
	const start = Math.max(0, match.index);
	const end = Math.min(source.length, match.index + 6400);
	return source.slice(start, end);
};

const parseEmbeddedQuoteForSymbol = (html, symbol) => {
	const [ticker, exchange] = String(symbol || '').toUpperCase().split(':');
	if (!ticker || !exchange) return null;

	const token = `["${ticker}","${exchange}"]`;
	const tokenPattern = escapeRegExp(token);
	const quoteRegex = new RegExp(
		`${tokenPattern}\\s*,\\s*"([^"]+)"\\s*,\\s*\\d+\\s*,\\s*"([A-Z]{3})"\\s*,\\s*\\[([^\\]]+)\\]\\s*,\\s*null\\s*,\\s*([-0-9.Ee+]+)`,
		'i'
	);

	const source = String(html || '');
	let searchIndex = 0;
	const windows = [];
	while (searchIndex < source.length) {
		const index = source.indexOf(token, searchIndex);
		if (index === -1) break;
		const start = Math.max(0, index - 300);
		const end = Math.min(source.length, index + 2600);
		windows.push(source.slice(start, end));
		searchIndex = index + token.length;
		if (windows.length >= 12) break;
	}

	for (const window of windows) {
		const match = window.match(quoteRegex);
		if (!match) continue;

		const quoteVector = String(match[3] || '')
			.split(',')
			.map((value) => toNumberOrNull(String(value).trim()));

		const currentPrice = quoteVector[0];
		if (!Number.isFinite(currentPrice) || currentPrice <= 0) continue;

		const change = quoteVector[1];
		const changePercent = quoteVector[2];
		const previousClose = toNumberOrNull(match[4]);

		return {
			name: String(match[1] || '').trim() || null,
			currency: String(match[2] || '').trim() || null,
			currentPrice,
			change: Number.isFinite(change) ? change : null,
			changePercent: Number.isFinite(changePercent) ? changePercent : null,
			previousClose: Number.isFinite(previousClose) ? previousClose : null,
		};
	}

	return null;
};

const parseLatestDailyBar = (html) => {
	const scriptContent = extractScriptClassContent(html, 'ds:11');
	if (!scriptContent) return null;

	const barRegex =
		/\[\[(\d{4}),(\d+),(\d+),(\d+)(?:,(\d+)|,null)[\s\S]{0,120}?\],\[\s*([-0-9.Ee+]+)\s*,\s*([-0-9.Ee+]+)\s*,\s*([-0-9.Ee+]+)\s*,[^\]]+\],\s*(\d+)\s*\]/g;

	let latestBar = null;
	let match = null;
	while ((match = barRegex.exec(scriptContent)) !== null) {
		const year = Number(match[1]);
		const month = Number(match[2]);
		const day = Number(match[3]);
		const hour = Number(match[4]);
		const minute = match[5] ? Number(match[5]) : 0;
		const close = toNumberOrNull(match[6]);
		const change = toNumberOrNull(match[7]);
		const changePercent = toNumberOrNull(match[8]);
		const volume = toNumberOrNull(match[9]);

		if (!Number.isFinite(close)) continue;
		if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) continue;
		if (!Number.isFinite(hour) || !Number.isFinite(minute)) continue;
		if (!Number.isFinite(volume)) continue;

		const timestamp = Date.UTC(year, month - 1, day, hour, minute, 0);
		if (Number.isNaN(timestamp)) continue;
		if (!latestBar || timestamp > latestBar.timestamp) {
			latestBar = {
				timestamp,
				close,
				change: Number.isFinite(change) ? change : null,
				changePercent: Number.isFinite(changePercent) ? changePercent : null,
				volume,
			};
		}
	}

	return latestBar;
};

const parseMetricFromHtmlByLabel = (html, labelPatterns = []) => {
	const source = String(html || '');
	if (!source) return null;

	for (const labelPattern of labelPatterns) {
		const regex = new RegExp(
			`${labelPattern}[\\s\\S]{0,220}?<div[^>]*class="[^"]*"[^>]*>\\s*([^<]+?)\\s*<\\/div>`,
			'i'
		);
		const match = source.match(regex);
		if (!match) continue;
		const numeric = extractFirstNumber(match[1]);
		if (numeric !== null && numeric !== undefined) return numeric;
	}

	return null;
};

const parseTextFromHtmlByLabel = (html, labelPatterns = []) => {
	const source = String(html || '');
	if (!source) return null;

	for (const labelPattern of labelPatterns) {
		const regex = new RegExp(
			`${labelPattern}[\\s\\S]{0,220}?<div[^>]*class="[^"]*"[^>]*>\\s*([^<]+?)\\s*<\\/div>`,
			'i'
		);
		const match = source.match(regex);
		if (!match) continue;
		const text = String(match[1] || '').replace(/\s+/g, ' ').trim();
		if (text) return text;
	}

	return null;
};

const normalizePercentToRatio = (value) => {
	if (!Number.isFinite(Number(value))) return null;
	const numeric = Number(value);
	return Math.abs(numeric) > 1 ? numeric / 100 : numeric;
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

		const urls = buildGoogleQuoteUrls(symbol);
		for (const url of urls) {
			try {
				const response = await fetchWithTimeout(url, {
					timeoutMs: this.timeoutMs,
					headers: {
						Accept: 'text/html,*/*',
						'Accept-Language': 'en-US,en;q=0.9',
						'User-Agent': 'Mozilla/5.0 (compatible; WealthBot/1.0)',
					},
				});
				if (!response.ok) continue;

				const html = await response.text();
				const entitySectionHtml = extractEntitySectionHtml(html, symbol);
				const embeddedQuote = parseEmbeddedQuoteForSymbol(html, symbol);
				const latestDailyBar = parseLatestDailyBar(html);

				const priceText = extractByRegex(entitySectionHtml, [
					/<div[^>]*class="[^"]*YMlKec[^"]*"[^>]*>([^<]+)<\/div>/i,
					/<div[^>]*class="[^"]*P6K39c[^"]*"[^>]*>([^<]+)<\/div>/i,
					/"lastPrice"\s*:\s*\{"raw"\s*:\s*([0-9.+-]+)/i,
					/"price"\s*:\s*\{"raw"\s*:\s*([0-9.+-]+)/i,
					/"price"\s*:\s*"([^"]+)"/i,
					/<meta[^>]+itemprop="price"[^>]+content="([^"]+)"/i,
				]) || extractByRegex(html, [
					/"lastPrice"\s*:\s*\{"raw"\s*:\s*([0-9.+-]+)/i,
					/"price"\s*:\s*\{"raw"\s*:\s*([0-9.+-]+)/i,
					/"price"\s*:\s*"([^"]+)"/i,
					/<meta[^>]+itemprop="price"[^>]+content="([^"]+)"/i,
				]);
				const price = embeddedQuote?.currentPrice ?? extractFirstNumber(priceText);
				if (!price || price <= 0) continue;

				const changeBlock = extractByRegex(entitySectionHtml, [
					/<div[^>]*class="[^"]*JwB6zf[^"]*"[^>]*>([^<]+)<\/div>/i,
					/"priceChange"\s*:\s*\{"raw"\s*:\s*([0-9.+-]+)/i,
				]);
				const change = embeddedQuote?.change ?? latestDailyBar?.change ?? extractFirstNumber(changeBlock);
				const changePercentText = extractByRegex(entitySectionHtml, [
					/<div[^>]*class="[^"]*JwB6zf[^"]*"[^>]*>[\s\S]*?(-?\d+(?:[.,]\d+)?)\s*%/i,
					/"priceChangePercent"\s*:\s*\{"raw"\s*:\s*([0-9.+-]+)/i,
				]);
				const changePercent =
					embeddedQuote?.changePercent ??
					latestDailyBar?.changePercent ??
					extractFirstNumber(changePercentText);
				const previousClose = embeddedQuote?.previousClose ?? null;

				const marketCap = parseMetricFromHtmlByLabel(entitySectionHtml, [
					'Market\\s*cap',
					'Mkt\\s*cap',
					'Valor\\s*de\\s*mercado',
				]) ?? parseMetricFromHtmlByLabel(html, [
					'Market\\s*cap',
					'Mkt\\s*cap',
					'Valor\\s*de\\s*mercado',
				]);
				const volume = latestDailyBar?.volume ?? parseMetricFromHtmlByLabel(entitySectionHtml, [
					'Avg\\.?\\s*volume',
					'Average\\s*volume',
					'Volume',
				]) ?? parseMetricFromHtmlByLabel(html, [
					'Avg\\.?\\s*volume',
					'Average\\s*volume',
					'Volume',
				]);
				const currency = embeddedQuote?.currency || extractByRegex(entitySectionHtml, [
					/"currency"\s*:\s*"([A-Z]{3})"/i,
					/<meta[^>]+itemprop="priceCurrency"[^>]+content="([A-Z]{3})"/i,
				]) || extractByRegex(html, [
					/"currency"\s*:\s*"([A-Z]{3})"/i,
					/<meta[^>]+itemprop="priceCurrency"[^>]+content="([A-Z]{3})"/i,
				]) || toCurrencyFromSymbol(symbol);
				const pe = parseMetricFromHtmlByLabel(entitySectionHtml, [
					'P\\s*\\/\\s*E',
					'P\\s*\\/\\s*L',
				]) ?? parseMetricFromHtmlByLabel(html, [
					'P\\s*\\/\\s*E',
					'P\\s*\\/\\s*L',
				]);
				const pb = parseMetricFromHtmlByLabel(entitySectionHtml, [
					'P\\s*\\/\\s*B',
					'P\\s*\\/\\s*VP',
					'Price\\s*to\\s*book',
				]) ?? parseMetricFromHtmlByLabel(html, [
					'P\\s*\\/\\s*B',
					'P\\s*\\/\\s*VP',
					'Price\\s*to\\s*book',
				]);
				const roe = parseMetricFromHtmlByLabel(entitySectionHtml, ['ROE']) ??
					parseMetricFromHtmlByLabel(html, ['ROE']);
				const payout = parseMetricFromHtmlByLabel(entitySectionHtml, ['Payout']) ??
					parseMetricFromHtmlByLabel(html, ['Payout']);
				const evEbitda = parseMetricFromHtmlByLabel(entitySectionHtml, ['EV\\s*\\/\\s*EBITDA']) ??
					parseMetricFromHtmlByLabel(html, ['EV\\s*\\/\\s*EBITDA']);
				const netMargin = parseMetricFromHtmlByLabel(entitySectionHtml, [
					'Net\\s*margin',
					'Profit\\s*margin',
					'Margem\\s*liquida',
				]) ?? parseMetricFromHtmlByLabel(html, [
					'Net\\s*margin',
					'Profit\\s*margin',
					'Margem\\s*liquida',
				]);
				const dividendYield = parseMetricFromHtmlByLabel(entitySectionHtml, [
					'Dividend\\s*yield',
					'DY',
				]) ?? parseMetricFromHtmlByLabel(html, [
					'Dividend\\s*yield',
					'DY',
				]);
				const sector = parseTextFromHtmlByLabel(entitySectionHtml, [
					'Sector',
					'Setor',
				]) ?? parseTextFromHtmlByLabel(html, [
					'Sector',
					'Setor',
				]);
				const industry = parseTextFromHtmlByLabel(entitySectionHtml, [
					'Industry',
					'Segment',
					'Segmento',
				]) ?? parseTextFromHtmlByLabel(html, [
					'Industry',
					'Segment',
					'Segmento',
				]);

				return {
					data_source: 'scrape_google',
					is_scraped: true,
					quote: {
						currentPrice: price,
						currency,
						change,
						changePercent,
						previousClose,
						marketCap,
						volume,
					},
					fundamentals: {
						pe,
						pb,
						roe: normalizePercentToRatio(roe),
						payout: normalizePercentToRatio(payout),
						evEbitda,
						netMargin: normalizePercentToRatio(netMargin),
						sector,
						industry,
						dividendYield: normalizePercentToRatio(dividendYield),
						google_finance: {
							url,
							symbol,
							marketCap,
							averageVolume: volume,
							pe,
							pb,
							roe,
							payout,
							evEbitda,
							netMargin,
							sector,
							industry,
							dividendYield,
						},
					},
					historical: {
						history_30d: [],
						dividends: [],
					},
						raw: {
							url,
							html_excerpt: entitySectionHtml.slice(0, 2000),
							parsed_from_embedded_data: Boolean(embeddedQuote),
							parsed_from_daily_bar: Boolean(latestDailyBar),
						},
					};
				} catch {
					// Try the next URL format.
				}
		}

		return null;
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
