const { BaseScraper } = require('./base-scraper');
const { fetchWithTimeout, nowIso, toNumberOrNull } = require('../../utils');
const { extractByRegex } = require('../extractors');
const { resolveYahooSymbol } = require('../../symbol-resolver');

const deepFindNumber = (value, keyHints) => {
	if (!value || typeof value !== 'object') return null;
	for (const [key, entryValue] of Object.entries(value)) {
		const normalizedKey = key.toLowerCase();
		if (keyHints.some((hint) => normalizedKey.includes(hint))) {
			const candidate =
				toNumberOrNull(entryValue?.raw) ??
				toNumberOrNull(entryValue?.fmt) ??
				toNumberOrNull(entryValue);
			if (candidate !== null) return candidate;
		}
	}
	for (const entryValue of Object.values(value)) {
		if (entryValue && typeof entryValue === 'object') {
			const found = deepFindNumber(entryValue, keyHints);
			if (found !== null) return found;
		}
	}
	return null;
};

const normalizeStatementDate = (value) => {
	if (value === null || value === undefined || value === '') return null;
	if (typeof value === 'object') {
		const candidate =
			value.raw ??
			value.fmt ??
			value.value ??
			value.date ??
			null;
		if (candidate !== null) return normalizeStatementDate(candidate);
	}

	const numeric = toNumberOrNull(value);
	if (numeric !== null) {
		if (numeric > 1e12) return new Date(numeric).toISOString().slice(0, 10);
		if (numeric > 1e9) return new Date(numeric * 1000).toISOString().slice(0, 10);
	}

	const text = String(value || '').trim();
	if (!text) return null;
	if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
	const parsed = new Date(text);
	if (!Number.isFinite(parsed.getTime())) return null;
	return parsed.toISOString().slice(0, 10);
};

const toStatementNumber = (value) => {
	if (value === null || value === undefined || value === '') return null;
	if (typeof value === 'object') {
		return (
			toNumberOrNull(value.raw) ??
			toNumberOrNull(value.value) ??
			toNumberOrNull(value.fmt) ??
			null
		);
	}
	return toNumberOrNull(value);
};

const normalizeStatementRows = (rows) => {
	if (!Array.isArray(rows)) return null;
	const normalized = rows
		.map((row) => {
			if (!row || typeof row !== 'object') return null;
			const period =
				normalizeStatementDate(row.endDate) ||
				normalizeStatementDate(row.asOfDate) ||
				normalizeStatementDate(row.date) ||
				normalizeStatementDate(row.period) ||
				null;
			if (!period) return null;

			const normalizedRow = { period };
			for (const [key, value] of Object.entries(row)) {
				if (key === 'maxAge' || key === 'endDate' || key === 'asOfDate' || key === 'date' || key === 'period') {
					continue;
				}
				const numeric = toStatementNumber(value);
				if (numeric === null) continue;
				normalizedRow[key] = numeric;
			}
			return Object.keys(normalizedRow).length > 1 ? normalizedRow : null;
		})
		.filter(Boolean);
	if (!normalized.length) return null;
	normalized.sort((left, right) => String(left.period).localeCompare(String(right.period)));
	return normalized;
};

const extractQuoteStoreFinancialStatements = (quoteStore) => ({
	financials: normalizeStatementRows(
		quoteStore?.incomeStatementHistory?.incomeStatementHistory || []
	),
	quarterly_financials: normalizeStatementRows(
		quoteStore?.incomeStatementHistoryQuarterly?.incomeStatementHistory || []
	),
	balance_sheet: normalizeStatementRows(
		quoteStore?.balanceSheetHistory?.balanceSheetStatements || []
	),
	quarterly_balance_sheet: normalizeStatementRows(
		quoteStore?.balanceSheetHistoryQuarterly?.balanceSheetStatements || []
	),
	cashflow: normalizeStatementRows(
		quoteStore?.cashflowStatementHistory?.cashflowStatements || []
	),
	quarterly_cashflow: normalizeStatementRows(
		quoteStore?.cashflowStatementHistoryQuarterly?.cashflowStatements || []
	),
});

class YahooFinanceScraper extends BaseScraper {
	constructor(options = {}) {
		super({
			...options,
			name: 'scrape_yahoo',
		});
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_YAHOO_SCRAPE_TIMEOUT_MS || 15000);
	}

	canHandle(asset) {
		const market = String(asset.market || '').toUpperCase();
		return ['BR', 'US', 'CA'].includes(market);
	}

	async scrape(asset) {
		const symbol = resolveYahooSymbol(asset.ticker, asset.market);
		if (!symbol) return null;

		const url = `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}`;
		const response = await fetchWithTimeout(url, {
			timeoutMs: this.timeoutMs,
			headers: { Accept: 'text/html,*/*' },
		});
		if (!response.ok) return null;
		const html = await response.text();

		const appMainRaw = extractByRegex(html, [
			/root\.App\.main\s*=\s*({[\s\S]*?});\n/s,
		]);
		if (!appMainRaw) return null;

		let appMain;
		try {
			appMain = JSON.parse(appMainRaw);
		} catch {
			return null;
		}

		const stores = appMain?.context?.dispatcher?.stores || {};
		const quoteStore = stores.QuoteSummaryStore || {};
		const streamStore = stores.StreamDataStore || {};
		const statementPayload = extractQuoteStoreFinancialStatements(quoteStore);

		const currentPrice =
			deepFindNumber(quoteStore, ['regularmarketprice', 'currentprice']) ??
			deepFindNumber(streamStore, ['regularmarketprice', 'lastprice']);
		if (!currentPrice) return null;

		return {
			data_source: 'scrape_yahoo',
			is_scraped: true,
			quote: {
				currentPrice,
				currency:
					quoteStore?.price?.currency ||
					quoteStore?.price?.financialCurrency ||
					null,
				change: deepFindNumber(quoteStore, ['regularmarketchange']),
				changePercent: deepFindNumber(quoteStore, ['regularmarketchangepercent']),
				previousClose: deepFindNumber(quoteStore, ['previousclose']),
				marketCap: deepFindNumber(quoteStore, ['marketcap']),
				volume: deepFindNumber(quoteStore, ['volume']),
			},
			fundamentals: {
				info: quoteStore,
				financials: statementPayload.financials,
				quarterly_financials: statementPayload.quarterly_financials,
				balance_sheet: statementPayload.balance_sheet,
				quarterly_balance_sheet: statementPayload.quarterly_balance_sheet,
				cashflow: statementPayload.cashflow,
				quarterly_cashflow: statementPayload.quarterly_cashflow,
			},
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				url,
				storeKeys: Object.keys(stores),
				quoteStore,
				financials: statementPayload.financials,
				quarterly_financials: statementPayload.quarterly_financials,
				balance_sheet: statementPayload.balance_sheet,
				quarterly_balance_sheet: statementPayload.quarterly_balance_sheet,
				cashflow: statementPayload.cashflow,
				quarterly_cashflow: statementPayload.quarterly_cashflow,
			},
		};
	}

	async healthCheck() {
		try {
			const response = await fetchWithTimeout(
				'https://finance.yahoo.com/quote/AAPL',
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
	YahooFinanceScraper,
};
