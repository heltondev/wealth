const {
	ProviderUnavailableError,
	DataIncompleteError,
} = require('../../market-data/errors');
const {
	fetchWithTimeout,
	toNumberOrNull,
	withRetry,
} = require('../../market-data/utils');

const DEFAULT_TIMEOUT_MS = 30000;

const toDate = (epochSeconds) => {
	const parsed = Number(epochSeconds);
	if (!Number.isFinite(parsed)) return null;
	return new Date(parsed * 1000).toISOString().slice(0, 10);
};

const toHistoryRow = (
	timestamp,
	index,
	quote,
	adjCloseArray,
	dividendsByTimestamp,
	splitsByTimestamp
) => {
	const date = toDate(timestamp);
	if (!date) return null;

	const closeValue = toNumberOrNull(quote.close?.[index]);
	if (closeValue === null) return null;

	const splitEvent = splitsByTimestamp[String(timestamp)] || null;
	const splitFactor =
		splitEvent?.numerator && splitEvent?.denominator
			? toNumberOrNull(splitEvent.numerator / splitEvent.denominator)
			: null;

	return {
		date,
		open: toNumberOrNull(quote.open?.[index]),
		high: toNumberOrNull(quote.high?.[index]),
		low: toNumberOrNull(quote.low?.[index]),
		close: closeValue,
		adjusted_close: toNumberOrNull(adjCloseArray?.[index]) ?? closeValue,
		volume: toNumberOrNull(quote.volume?.[index]),
		dividends: toNumberOrNull(dividendsByTimestamp[String(timestamp)]?.amount) || 0,
		stock_splits: toNumberOrNull(splitFactor) || 0,
	};
};

class YahooHistoryProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(
			options.yahooTimeoutMs ||
			options.timeoutMs ||
			process.env.MARKET_DATA_YAHOO_TIMEOUT_MS ||
			process.env.MARKET_DATA_YFINANCE_TIMEOUT_MS ||
			DEFAULT_TIMEOUT_MS
		);
	}

	async fetchHistory(symbol, options = {}) {
		const normalizedSymbol = String(symbol || '').trim().toUpperCase();
		if (!normalizedSymbol) {
			throw new ProviderUnavailableError('symbol is required', {
				provider: 'yahoo_chart_api',
			});
		}

		const url = this.#buildUrl(normalizedSymbol, options);
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
					symbol: normalizedSymbol,
					status: response.status,
				}
			);
		}

		const payload = await response.json();
		const result = payload?.chart?.result?.[0];
		if (!result) {
			throw new DataIncompleteError('Yahoo chart payload missing result', {
				symbol: normalizedSymbol,
				payload,
			});
		}

		const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
		const quote = result?.indicators?.quote?.[0] || {};
		const adjCloseArray = result?.indicators?.adjclose?.[0]?.adjclose || [];
		const dividendsByTimestamp = result?.events?.dividends || {};
		const splitsByTimestamp = result?.events?.splits || {};
		const rows = timestamps
			.map((timestamp, index) =>
				toHistoryRow(
					timestamp,
					index,
					quote,
					adjCloseArray,
					dividendsByTimestamp,
					splitsByTimestamp
				)
			)
			.filter(Boolean);

		if (!rows.length && !options.allowEmpty) {
			throw new DataIncompleteError('Yahoo chart returned empty rows', {
				symbol: normalizedSymbol,
				result,
			});
		}

		return {
			data_source: 'yahoo_chart_api',
			is_scraped: false,
			currency: result?.meta?.currency || null,
			rows,
			raw: result,
		};
	}

	#buildUrl(symbol, options = {}) {
		const startDate = options.startDate ? String(options.startDate).trim() : '';
		const period = options.period ? String(options.period).trim() : '';

		if (startDate) {
			const startEpochSeconds = Date.parse(`${startDate}T00:00:00Z`);
			if (!Number.isFinite(startEpochSeconds)) {
				throw new ProviderUnavailableError('Invalid startDate for yahoo history', {
					symbol,
					startDate,
				});
			}
			const period1 = Math.floor(startEpochSeconds / 1000);
			const period2 = Math.floor(Date.now() / 1000);
			return `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${period1}&period2=${period2}&interval=1d&events=div,split`;
		}

		const resolvedRange = period || 'max';
		return `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${encodeURIComponent(resolvedRange)}&interval=1d&events=div,split`;
	}
}

module.exports = {
	YahooHistoryProvider,
};
