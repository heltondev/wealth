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

const toIsoDate = (epochSeconds) => {
	const parsed = Number(epochSeconds);
	if (!Number.isFinite(parsed)) return null;
	return new Date(parsed * 1000).toISOString().slice(0, 10);
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
		const quote = await this.#fetchQuote(normalizedSymbol);

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

		const currentPrice =
			toNumberOrNull(quote.regularMarketPrice) ??
			toNumberOrNull(quote.postMarketPrice) ??
			toNumberOrNull(quote.preMarketPrice);

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
				financials: null,
				quarterly_financials: null,
				balance_sheet: null,
				quarterly_balance_sheet: null,
				cashflow: null,
				quarterly_cashflow: null,
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
				chart: chartPayload.raw,
				chart_error: chartPayload.error,
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
}

module.exports = {
	YahooApiProvider,
};
