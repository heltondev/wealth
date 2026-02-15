const {
	fetchWithTimeout,
	withRetry,
	toNumberOrNull,
} = require('../../market-data/utils');
const {
	ProviderUnavailableError,
	DataIncompleteError,
} = require('../../market-data/errors');

const toDate = (epochSeconds) => {
	if (!Number.isFinite(Number(epochSeconds))) return null;
	return new Date(Number(epochSeconds) * 1000).toISOString().slice(0, 10);
};

class YahooChartProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(
			options.yahooTimeoutMs ||
			options.timeoutMs ||
			process.env.MARKET_DATA_YAHOO_TIMEOUT_MS ||
			process.env.MARKET_DATA_YFINANCE_TIMEOUT_MS ||
			30000
		);
	}

	async fetchHistory(symbol, options = {}) {
		const startDate = options.startDate || null;
		const url = startDate
			? `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${Math.floor(
				Date.parse(`${startDate}T00:00:00Z`) / 1000
			)}&period2=${Math.floor(Date.now() / 1000)}&interval=1d&events=div,split`
			: `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=max&interval=1d&events=div,split`;

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

		const data = await response.json();
		const result = data?.chart?.result?.[0];
		if (!result) {
			throw new DataIncompleteError('Yahoo chart payload missing result', {
				symbol,
				data,
			});
		}

		const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
		const quote = result?.indicators?.quote?.[0] || {};
		const adjCloseArray = result?.indicators?.adjclose?.[0]?.adjclose || [];
		const dividendsByTimestamp = result?.events?.dividends || {};
		const splitsByTimestamp = result?.events?.splits || {};

		const rows = timestamps.map((timestamp, index) => {
			const date = toDate(timestamp);
			const dividends = dividendsByTimestamp[String(timestamp)]?.amount;
			const stockSplits = splitsByTimestamp[String(timestamp)]?.numerator &&
				splitsByTimestamp[String(timestamp)]?.denominator
				? toNumberOrNull(
					splitsByTimestamp[String(timestamp)].numerator /
					splitsByTimestamp[String(timestamp)].denominator
				)
				: null;

			return {
				date,
				open: toNumberOrNull(quote.open?.[index]),
				high: toNumberOrNull(quote.high?.[index]),
				low: toNumberOrNull(quote.low?.[index]),
				close: toNumberOrNull(quote.close?.[index]),
				adjusted_close:
					toNumberOrNull(adjCloseArray?.[index]) ??
					toNumberOrNull(quote.close?.[index]),
				volume: toNumberOrNull(quote.volume?.[index]),
				dividends: toNumberOrNull(dividends) || 0,
				stock_splits: toNumberOrNull(stockSplits) || 0,
			};
		}).filter((row) => row.date && row.close !== null);

		return {
			data_source: 'yahoo_chart_api',
			is_scraped: false,
			currency: result?.meta?.currency || null,
			rows,
			raw: result,
		};
	}
}

module.exports = {
	YahooChartProvider,
};
