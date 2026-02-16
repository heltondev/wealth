const {
	DataIncompleteError,
	ProviderUnavailableError,
} = require('../errors');
const {
	fetchWithTimeout,
	toNumberOrNull,
	withRetry,
} = require('../utils');

const DEFAULT_TIMEOUT_MS = 30000;
const QUOTE_SUMMARY_MODULES = [
	'incomeStatementHistory',
	'incomeStatementHistoryQuarterly',
	'balanceSheetHistory',
	'balanceSheetHistoryQuarterly',
	'cashflowStatementHistory',
	'cashflowStatementHistoryQuarterly',
];

const toIsoDate = (epochSeconds) => {
	const parsed = Number(epochSeconds);
	if (!Number.isFinite(parsed)) return null;
	return new Date(parsed * 1000).toISOString().slice(0, 10);
};

const toIsoDateFromUnknown = (value) => {
	if (value === null || value === undefined || value === '') return null;
	if (typeof value === 'object') {
		const rawValue =
			value.raw ??
			value.fmt ??
			value.value ??
			value.date ??
			null;
		if (rawValue !== null) {
			return toIsoDateFromUnknown(rawValue);
		}
	}

	const numericValue = toNumberOrNull(value);
	if (numericValue !== null) {
		if (numericValue > 1e12) {
			return new Date(numericValue).toISOString().slice(0, 10);
		}
		if (numericValue > 1e9) {
			return toIsoDate(numericValue);
		}
	}

	const text = String(value).trim();
	if (!text) return null;
	if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
	const parsed = new Date(text);
	if (!Number.isFinite(parsed.getTime())) return null;
	return parsed.toISOString().slice(0, 10);
};

const toFinancialNumber = (value) => {
	if (value === null || value === undefined || value === '') return null;
	if (typeof value === 'object') {
		return (
			toNumberOrNull(value.raw) ??
			toNumberOrNull(value.value) ??
			toNumberOrNull(value.fmt) ??
			toNumberOrNull(value.longFmt) ??
			null
		);
	}
	return toNumberOrNull(value);
};

const normalizeFinancialStatementRows = (rows) => {
	if (!Array.isArray(rows)) return null;

	const normalizedRows = rows
		.map((row) => {
			if (!row || typeof row !== 'object') return null;
			const period =
				toIsoDateFromUnknown(row.endDate) ||
				toIsoDateFromUnknown(row.asOfDate) ||
				toIsoDateFromUnknown(row.date) ||
				toIsoDateFromUnknown(row.period) ||
				null;
			if (!period) return null;

			const normalized = { period };
			for (const [key, entryValue] of Object.entries(row)) {
				if (key === 'maxAge' || key === 'endDate' || key === 'asOfDate' || key === 'date' || key === 'period') {
					continue;
				}
				const numericValue = toFinancialNumber(entryValue);
				if (numericValue === null) continue;
				normalized[key] = numericValue;
			}

			return Object.keys(normalized).length > 1 ? normalized : null;
		})
		.filter(Boolean);

	if (normalizedRows.length === 0) return null;
	normalizedRows.sort((left, right) => String(left.period).localeCompare(String(right.period)));
	return normalizedRows;
};

const parseQuoteSummaryFinancialStatements = (result) => {
	const safeResult = result && typeof result === 'object' ? result : {};
	const annualIncome = normalizeFinancialStatementRows(
		safeResult?.incomeStatementHistory?.incomeStatementHistory || []
	);
	const quarterlyIncome = normalizeFinancialStatementRows(
		safeResult?.incomeStatementHistoryQuarterly?.incomeStatementHistory || []
	);
	const annualBalance = normalizeFinancialStatementRows(
		safeResult?.balanceSheetHistory?.balanceSheetStatements || []
	);
	const quarterlyBalance = normalizeFinancialStatementRows(
		safeResult?.balanceSheetHistoryQuarterly?.balanceSheetStatements || []
	);
	const annualCashflow = normalizeFinancialStatementRows(
		safeResult?.cashflowStatementHistory?.cashflowStatements || []
	);
	const quarterlyCashflow = normalizeFinancialStatementRows(
		safeResult?.cashflowStatementHistoryQuarterly?.cashflowStatements || []
	);

	return {
		financials: annualIncome,
		quarterly_financials: quarterlyIncome,
		balance_sheet: annualBalance,
		quarterly_balance_sheet: quarterlyBalance,
		cashflow: annualCashflow,
		quarterly_cashflow: quarterlyCashflow,
	};
};

const toYahooHistoryRow = (timestamp, index, quote, adjustedCloseSeries, dividendsByTimestamp, splitsByTimestamp) => {
	const date = toIsoDate(timestamp);
	if (!date) return null;

	const splitEvent = splitsByTimestamp[String(timestamp)] || null;
	const splitRatio =
		splitEvent?.numerator && splitEvent?.denominator
			? toNumberOrNull(splitEvent.numerator / splitEvent.denominator)
			: null;
	const closeValue = toNumberOrNull(quote.close?.[index]);
	if (closeValue === null) return null;

	return {
		date,
		open: toNumberOrNull(quote.open?.[index]),
		high: toNumberOrNull(quote.high?.[index]),
		low: toNumberOrNull(quote.low?.[index]),
		close: closeValue,
		adjusted_close: toNumberOrNull(adjustedCloseSeries?.[index]) ?? closeValue,
		volume: toNumberOrNull(quote.volume?.[index]),
		dividends: toNumberOrNull(dividendsByTimestamp[String(timestamp)]?.amount) || 0,
		stock_splits: toNumberOrNull(splitRatio) || 0,
	};
};

class YahooApiProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(
			options.yahooTimeoutMs ||
			options.timeoutMs ||
			process.env.MARKET_DATA_YAHOO_TIMEOUT_MS ||
			process.env.MARKET_DATA_YFINANCE_TIMEOUT_MS ||
			DEFAULT_TIMEOUT_MS
		);
	}

	async fetch(symbol, options = {}) {
		const normalizedSymbol = String(symbol || '').trim().toUpperCase();
		if (!normalizedSymbol) {
			throw new ProviderUnavailableError('symbol is required', {
				provider: 'yahoo_api',
			});
		}

		const historyDays = Math.max(1, Number(options.historyDays || 30));
		let quote = {};
		let quoteError = null;
		try {
			quote = await this.#fetchQuote(normalizedSymbol);
		} catch (error) {
			quoteError = {
				name: error.name,
				message: error.message,
			};
		}

		let chartPayload = {
			rows: [],
			dividends: [],
			raw: null,
			error: null,
		};
		try {
			chartPayload = await this.#fetchHistory(normalizedSymbol, historyDays);
		} catch (error) {
			chartPayload.error = {
				name: error.name,
				message: error.message,
			};
		}

		let financialStatements = {
			financials: null,
			quarterly_financials: null,
			balance_sheet: null,
			quarterly_balance_sheet: null,
			cashflow: null,
			quarterly_cashflow: null,
			raw: null,
			error: null,
		};
		try {
			financialStatements = await this.#fetchQuoteSummaryFinancials(normalizedSymbol);
		} catch (error) {
			financialStatements.error = {
				name: error.name,
				message: error.message,
			};
		}

		const latestHistoryClose =
			chartPayload.rows.length > 0
				? toNumberOrNull(chartPayload.rows[chartPayload.rows.length - 1]?.close)
				: null;

		const currentPrice =
			toNumberOrNull(quote.regularMarketPrice) ??
			toNumberOrNull(quote.postMarketPrice) ??
			toNumberOrNull(quote.preMarketPrice) ??
			latestHistoryClose;

		if (currentPrice === null) {
			throw new DataIncompleteError('yahoo quote did not return a current price', {
				symbol: normalizedSymbol,
				quote,
			});
		}

		return {
			data_source: 'yahoo_quote_api',
			is_scraped: false,
			quote: {
				currentPrice,
				currency: quote.currency || quote.financialCurrency || null,
				previousClose:
					toNumberOrNull(quote.regularMarketPreviousClose) ??
					toNumberOrNull(quote.previousClose),
				change:
					toNumberOrNull(quote.regularMarketChange) ??
					toNumberOrNull(quote.postMarketChange),
				changePercent:
					toNumberOrNull(quote.regularMarketChangePercent) ??
					toNumberOrNull(quote.postMarketChangePercent),
				volume: toNumberOrNull(quote.regularMarketVolume),
				marketCap: toNumberOrNull(quote.marketCap),
				regularMarketTime: quote.regularMarketTime || null,
			},
			fundamentals: {
				info: quote,
				financials: financialStatements.financials,
				quarterly_financials: financialStatements.quarterly_financials,
				balance_sheet: financialStatements.balance_sheet,
				quarterly_balance_sheet: financialStatements.quarterly_balance_sheet,
				cashflow: financialStatements.cashflow,
				quarterly_cashflow: financialStatements.quarterly_cashflow,
				recommendations: null,
				institutional_holders: null,
				major_holders: null,
				calendar: null,
			},
			historical: {
				history_30d: chartPayload.rows,
				dividends: chartPayload.dividends,
			},
			raw: {
				quote,
				quote_error: quoteError,
				chart: chartPayload.raw,
				chart_error: chartPayload.error,
				financials: financialStatements.financials,
				quarterly_financials: financialStatements.quarterly_financials,
				balance_sheet: financialStatements.balance_sheet,
				quarterly_balance_sheet: financialStatements.quarterly_balance_sheet,
				cashflow: financialStatements.cashflow,
				quarterly_cashflow: financialStatements.quarterly_cashflow,
				quote_summary: financialStatements.raw,
				quote_summary_error: financialStatements.error,
			},
		};
	}

	async #fetchQuote(symbol) {
		const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbol)}`;

		const response = await withRetry(
			() => fetchWithTimeout(url, { timeoutMs: this.timeoutMs }),
			{
				retries: 2,
				baseDelayMs: 500,
				factor: 2,
			}
		);

		if (!response.ok) {
			throw new ProviderUnavailableError(
				`Yahoo quote endpoint responded with ${response.status}`,
				{
					symbol,
					status: response.status,
				}
			);
		}

		const payload = await response.json();
		const quote = payload?.quoteResponse?.result?.[0];
		if (!quote) {
			throw new DataIncompleteError('Yahoo quote payload missing result', {
				symbol,
				payload,
			});
		}

		return quote;
	}

	async #fetchHistory(symbol, historyDays) {
		const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${historyDays}d&interval=1d&events=div,split`;

		const response = await withRetry(
			() => fetchWithTimeout(url, { timeoutMs: this.timeoutMs }),
			{
				retries: 2,
				baseDelayMs: 500,
				factor: 2,
			}
		);

		if (!response.ok) {
			throw new ProviderUnavailableError(
				`Yahoo chart endpoint responded with ${response.status}`,
				{
					symbol,
					status: response.status,
				}
			);
		}

		const payload = await response.json();
		const result = payload?.chart?.result?.[0];
		if (!result) {
			throw new DataIncompleteError('Yahoo chart payload missing result', {
				symbol,
				payload,
			});
		}

		const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
		const quote = result?.indicators?.quote?.[0] || {};
		const adjustedCloseSeries = result?.indicators?.adjclose?.[0]?.adjclose || [];
		const dividendsByTimestamp = result?.events?.dividends || {};
		const splitsByTimestamp = result?.events?.splits || {};

		const rows = timestamps
			.map((timestamp, index) =>
				toYahooHistoryRow(
					timestamp,
					index,
					quote,
					adjustedCloseSeries,
					dividendsByTimestamp,
					splitsByTimestamp
				)
			)
			.filter(Boolean);

		const dividends = Object.entries(dividendsByTimestamp)
			.map(([timestamp, item]) => ({
				date: toIsoDate(timestamp),
				value: toNumberOrNull(item?.amount),
			}))
			.filter((item) => item.date && item.value !== null);

		return {
			rows,
			dividends,
			raw: result,
			error: null,
		};
	}

	async #fetchQuoteSummaryFinancials(symbol) {
		const modules = QUOTE_SUMMARY_MODULES.join(',');
		const url = `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(symbol)}?modules=${encodeURIComponent(modules)}`;

		const response = await withRetry(
			() => fetchWithTimeout(url, { timeoutMs: this.timeoutMs }),
			{
				retries: 2,
				baseDelayMs: 500,
				factor: 2,
			}
		);

		if (!response.ok) {
			throw new ProviderUnavailableError(
				`Yahoo quoteSummary endpoint responded with ${response.status}`,
				{
					symbol,
					status: response.status,
				}
			);
		}

		const payload = await response.json();
		const result = payload?.quoteSummary?.result?.[0] || null;
		if (!result) {
			return {
				financials: null,
				quarterly_financials: null,
				balance_sheet: null,
				quarterly_balance_sheet: null,
				cashflow: null,
				quarterly_cashflow: null,
				raw: payload?.quoteSummary || null,
				error: null,
			};
		}

		return {
			...parseQuoteSummaryFinancialStatements(result),
			raw: result,
			error: null,
		};
	}
}

module.exports = {
	YahooApiProvider,
};
