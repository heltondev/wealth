const {
	QueryCommand,
	PutCommand,
	UpdateCommand,
} = require('@aws-sdk/lib-dynamodb');
const {
	toNumberOrNull,
	nowIso,
} = require('../market-data/utils');
const {
	resolveAssetMarket,
	resolveYahooSymbol,
	TESOURO_MARKET,
} = require('../market-data/symbol-resolver');
const { createThrottledScheduler } = require('../market-data/throttle');
const { FallbackManager } = require('../market-data/fallback/fallback-manager');
const { YahooHistoryProvider } = require('./providers/yahoo-history-provider');
const { YahooChartProvider } = require('./providers/yahoo-chart-provider');
const { TesouroHistoryProvider } = require('./providers/tesouro-history-provider');

const BENCHMARK_BY_MARKET = {
	BR: '^BVSP',
	US: '^GSPC',
	CA: '^GSPTSE',
	TESOURO: '^BVSP',
};

const COST_METHODS = {
	FIFO: 'fifo',
	WEIGHTED_AVERAGE: 'weighted_average',
};

const PERIOD_TO_DAYS = {
	'1M': 30,
	'3M': 90,
	'6M': 180,
	'1A': 365,
	'2A': 730,
	'5A': 1825,
	MAX: null,
};

const SPLIT_FACTOR_EPSILON = 1e-9;
const SPLIT_DEDUP_WINDOW_DAYS = Number(
	process.env.PRICE_HISTORY_SPLIT_DEDUP_WINDOW_DAYS || 14
);

// Cooldown for failed price history fetches (default: 6 hours).
const FETCH_COOLDOWN_MS = Number(
	process.env.PRICE_HISTORY_FETCH_COOLDOWN_MS || 6 * 60 * 60 * 1000
);
const fetchCooldownCache = new Map();

const normalizeDate = (value) => {
	if (!value) return null;
	const input = String(value).trim();
	const iso = input.match(/^(\d{4})-(\d{2})-(\d{2})$/);
	if (iso) return input;
	const br = input.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
	if (br) return `${br[3]}-${br[2]}-${br[1]}`;
	return null;
};

const dateToEpoch = (date) => Date.parse(`${date}T00:00:00Z`);

const addDays = (date, days) => {
	const epoch = dateToEpoch(date);
	if (!Number.isFinite(epoch)) return date;
	return new Date(epoch + days * 86400000).toISOString().slice(0, 10);
};

const periodStartDate = (period, latestDate) => {
	const normalizedPeriod = String(period || 'MAX').toUpperCase();
	const days = PERIOD_TO_DAYS[normalizedPeriod];
	if (days === undefined) return null;
	if (days === null) return null;
	return addDays(latestDate, -days);
};

const compareDate = (left, right) => left.localeCompare(right);

const normalizeTicker = (value) =>
	String(value || '')
		.toUpperCase()
		.trim()
		.replace(/\.SA$|\.TO$/g, '');

const resolveDisplayDate = (date, market) => {
	if (!date) return null;
	if (!['BR', TESOURO_MARKET].includes(String(market || '').toUpperCase())) {
		return date;
	}
	try {
		const formatter = new Intl.DateTimeFormat('sv-SE', {
			timeZone: 'America/Sao_Paulo',
		});
		return formatter.format(new Date(`${date}T12:00:00Z`));
	} catch {
		return date;
	}
};

const parseTransactionType = (value) => String(value || '').toLowerCase().trim();

const parseTransaction = (transaction) => {
	const quantity = Math.abs(toNumberOrNull(transaction.quantity) || 0);
	const unitPrice =
		toNumberOrNull(transaction.unit_price) ??
		toNumberOrNull(transaction.price) ??
		0;
	const fees = toNumberOrNull(transaction.fees) || 0;
	const amount =
		toNumberOrNull(transaction.amount) ?? quantity * unitPrice;

	return {
		...transaction,
		type: parseTransactionType(transaction.type),
		date: normalizeDate(transaction.date) || null,
		quantity,
		unitPrice,
		fees,
		amount,
	};
};

const getCloseForRow = (row) =>
	toNumberOrNull(row?.close) ??
	toNumberOrNull(row?.adjusted_close) ??
	toNumberOrNull(row?.pu_venda) ??
	toNumberOrNull(row?.pu_compra);

const findPriceAtOrBeforeDate = (rows, date) => {
	const targetDate = normalizeDate(date);
	if (!targetDate || !rows.length) return null;

	for (let index = rows.length - 1; index >= 0; index -= 1) {
		if (rows[index].date <= targetDate) {
			return rows[index];
		}
	}
	return null;
};

const aggregateDividends = (transactions) =>
	transactions
		.filter((tx) => ['dividend', 'jcp'].includes(tx.type))
		.reduce((accumulator, tx) => accumulator + (toNumberOrNull(tx.amount) || 0), 0);

const normalizeSplitEvents = (splitEvents = []) =>
	{
		const normalized = (splitEvents || [])
			.map((event) => {
				const date = normalizeDate(event?.date);
				const factor =
					toNumberOrNull(event?.factor) ??
					toNumberOrNull(event?.stock_splits) ??
					toNumberOrNull(event?.stockSplits);
				if (!date || !factor || factor <= 0) return null;
				// 1.0 means no split adjustment.
				if (Math.abs(factor - 1) <= Number.EPSILON) return null;
				return { date, factor };
			})
			.filter(Boolean)
			.sort((left, right) => compareDate(left.date, right.date));

		if (!Number.isFinite(SPLIT_DEDUP_WINDOW_DAYS) || SPLIT_DEDUP_WINDOW_DAYS <= 0) {
			return normalized;
		}

		const dedupeWindowMs = SPLIT_DEDUP_WINDOW_DAYS * 86400000;
		const deduped = [];
		for (const event of normalized) {
			const eventEpoch = dateToEpoch(event.date);
			const isDuplicate = deduped.some((existing) => {
				if (Math.abs(existing.factor - event.factor) > SPLIT_FACTOR_EPSILON) {
					return false;
				}
				const existingEpoch = dateToEpoch(existing.date);
				if (!Number.isFinite(existingEpoch) || !Number.isFinite(eventEpoch)) {
					return false;
				}
				return Math.abs(existingEpoch - eventEpoch) <= dedupeWindowMs;
			});
			if (!isDuplicate) deduped.push(event);
		}

		return deduped;
	};

const buildSplitEventsFromPriceRows = (rows = []) =>
	normalizeSplitEvents(
		rows
			.filter((row) => toNumberOrNull(row?.stock_splits))
			.map((row) => ({
				date: row.date,
				factor: row.stock_splits,
			}))
	);

const calculateHoldings = (transactions, method = COST_METHODS.FIFO, options = {}) => {
	const normalizedMethod = method === COST_METHODS.WEIGHTED_AVERAGE
		? COST_METHODS.WEIGHTED_AVERAGE
		: COST_METHODS.FIFO;
	const splitEvents = normalizeSplitEvents(options.splitEvents);
	const sorted = [...transactions]
		.map(parseTransaction)
		.filter((tx) => tx.date)
		.sort((left, right) =>
			left.date === right.date
				? String(left.createdAt || '').localeCompare(String(right.createdAt || ''))
				: compareDate(left.date, right.date)
		);

	let quantityCurrent = 0;
	let costCurrent = 0;
	let totalBuysCost = 0;
	const lots = [];
	let firstBuyDate = null;
	let splitIndex = 0;

	const applySplit = (factor) => {
		if (!Number.isFinite(factor) || factor <= 0) return;
		if (Math.abs(factor - 1) <= Number.EPSILON) return;
		if (quantityCurrent <= 0) return;

		// Total invested cost remains unchanged in split/grouping events.
		quantityCurrent *= factor;

		if (normalizedMethod === COST_METHODS.FIFO) {
			for (const lot of lots) {
				lot.quantity *= factor;
				lot.costPerUnit = factor !== 0 ? lot.costPerUnit / factor : lot.costPerUnit;
			}
		}
	};

	const applyPendingSplitsUpTo = (date) => {
		while (
			splitIndex < splitEvents.length &&
			splitEvents[splitIndex].date <= date
		) {
			applySplit(splitEvents[splitIndex].factor);
			splitIndex += 1;
		}
	};

	for (const tx of sorted) {
		applyPendingSplitsUpTo(tx.date);

		if (tx.type === 'buy' || tx.type === 'subscription') {
			const totalCost = tx.quantity * tx.unitPrice + tx.fees;
			totalBuysCost += totalCost;
			quantityCurrent += tx.quantity;
			costCurrent += totalCost;
			if (!firstBuyDate) firstBuyDate = tx.date;

			if (normalizedMethod === COST_METHODS.FIFO) {
				lots.push({
					quantity: tx.quantity,
					costPerUnit: tx.quantity ? totalCost / tx.quantity : 0,
				});
			}
			continue;
		}

		if (tx.type !== 'sell') continue;
		let remainingToSell = tx.quantity;
		if (remainingToSell <= 0 || quantityCurrent <= 0) continue;

		if (normalizedMethod === COST_METHODS.WEIGHTED_AVERAGE) {
			const soldQuantity = Math.min(remainingToSell, quantityCurrent);
			const avgCost = quantityCurrent > 0 ? costCurrent / quantityCurrent : 0;
			costCurrent = Math.max(0, costCurrent - soldQuantity * avgCost);
			quantityCurrent = Math.max(0, quantityCurrent - soldQuantity);
			continue;
		}

		while (remainingToSell > 0 && lots.length > 0) {
			const lot = lots[0];
			const consumed = Math.min(remainingToSell, lot.quantity);
			costCurrent = Math.max(0, costCurrent - consumed * lot.costPerUnit);
			quantityCurrent = Math.max(0, quantityCurrent - consumed);
			lot.quantity -= consumed;
			remainingToSell -= consumed;
			if (lot.quantity <= 0) lots.shift();
		}
	}

	// Apply split events after the latest transaction, if any.
	applyPendingSplitsUpTo('9999-12-31');

	const averageCost = quantityCurrent > 0 ? costCurrent / quantityCurrent : 0;
	return {
		method: normalizedMethod,
		quantityCurrent,
		costCurrent,
		averageCost,
		totalBuysCost,
		firstBuyDate,
	};
};

const resolveHoldingsWithSplitHeuristic = (
	transactions,
	method,
	splitEvents,
	expectedQuantity
) => {
	const holdingsWithoutSplits = calculateHoldings(transactions, method);
	const normalizedSplitEvents = normalizeSplitEvents(splitEvents);
	if (!normalizedSplitEvents.length) {
		return {
			holdings: holdingsWithoutSplits,
			split_adjustment: 'without_splits',
		};
	}

	const holdingsWithSplits = calculateHoldings(transactions, method, {
		splitEvents: normalizedSplitEvents,
	});

	const hasExpectedQuantity =
		expectedQuantity !== null &&
		expectedQuantity !== undefined &&
		String(expectedQuantity).trim() !== '';
	if (!hasExpectedQuantity) {
		return {
			holdings: holdingsWithSplits,
			split_adjustment: 'with_splits',
		};
	}
	const numericExpectedQuantity = Number(expectedQuantity);
	if (!Number.isFinite(numericExpectedQuantity) || numericExpectedQuantity < 0) {
		return {
			holdings: holdingsWithSplits,
			split_adjustment: 'with_splits',
		};
	}

	const withSplitsDiff = Math.abs(
		(holdingsWithSplits.quantityCurrent || 0) - numericExpectedQuantity
	);
	const withoutSplitsDiff = Math.abs(
		(holdingsWithoutSplits.quantityCurrent || 0) - numericExpectedQuantity
	);

	if (withSplitsDiff < withoutSplitsDiff) {
		return {
			holdings: holdingsWithSplits,
			split_adjustment: 'with_splits',
		};
	}

	return {
		holdings: holdingsWithoutSplits,
		split_adjustment: 'without_splits',
	};
};

const calculatePeriodReturn = (rows, periodDays, fallbackDate = null) => {
	if (!rows.length) return null;
	const latest = rows[rows.length - 1];
	const latestClose = getCloseForRow(latest);
	if (!latestClose) return null;

	let reference = null;
	if (periodDays === null && fallbackDate) {
		reference = findPriceAtOrBeforeDate(rows, fallbackDate);
	} else if (typeof periodDays === 'number') {
		const referenceDate = addDays(latest.date, -periodDays);
		reference = findPriceAtOrBeforeDate(rows, referenceDate);
	}
	if (!reference) return null;

	const referenceClose = getCloseForRow(reference);
	if (!referenceClose || referenceClose === 0) return null;
	return ((latestClose / referenceClose) - 1) * 100;
};

const enrichTransactionsWithPrices = (transactions, priceRows) =>
	transactions.map((raw) => {
		const tx = parseTransaction(raw);
		const priceRow = tx.date ? findPriceAtOrBeforeDate(priceRows, tx.date) : null;
		const closeAtDate = getCloseForRow(priceRow);
		const slippageAbs =
			closeAtDate !== null && closeAtDate !== undefined
				? tx.unitPrice - closeAtDate
				: null;
		const slippagePct =
			closeAtDate && closeAtDate !== 0
				? ((tx.unitPrice / closeAtDate) - 1) * 100
				: null;
		return {
			...raw,
			close_at_date: closeAtDate,
			slippage_abs: slippageAbs,
			slippage_pct: slippagePct,
			operation_total: tx.quantity * tx.unitPrice + tx.fees,
		};
	});

class PortfolioPriceHistoryService {
	constructor(options = {}) {
		this.dynamo = options.dynamo;
		this.tableName = options.tableName || process.env.TABLE_NAME || 'wealth-main';
		this.logger = options.logger || console;
		this.yahooHistoryProvider =
			options.yahooHistoryProvider || new YahooHistoryProvider(options);
		this.yahooChartProvider =
			options.yahooChartProvider || new YahooChartProvider(options);
		this.tesouroHistoryProvider =
			options.tesouroHistoryProvider || new TesouroHistoryProvider(options);
		this.fallbackManager =
			options.fallbackManager || new FallbackManager(options);
		this.schedule =
			options.scheduler ||
			createThrottledScheduler({
				minDelayMs: Number(
					options.minDelayMs || process.env.PRICE_HISTORY_MIN_DELAY_MS || 250
				),
				maxConcurrent: Number(
					options.maxConcurrent || process.env.PRICE_HISTORY_MAX_CONCURRENT || 2
				),
			});
	}

	/**
	 * Returns existing price rows for an asset, attempting a single fetch
	 * if none exist and the ticker is not in cooldown from a recent failed fetch.
	 */
	async #ensurePriceRows(portfolioId, asset) {
		let rows = await this.#listAssetPriceRows(portfolioId, asset.assetId);
		if (rows.length) return rows;

		const cooldownKey = `${portfolioId}:${asset.assetId}`;
		const lastAttempt = fetchCooldownCache.get(cooldownKey);
		if (lastAttempt && (Date.now() - lastAttempt) < FETCH_COOLDOWN_MS) {
			return rows;
		}

		fetchCooldownCache.set(cooldownKey, Date.now());
		try {
			await this.fetchPriceHistory(asset.ticker, resolveAssetMarket(asset), {
				portfolioId,
				assetId: asset.assetId,
				assetClass: asset.assetClass,
				country: asset.country,
				currency: asset.currency,
				persist: true,
				incremental: false,
			});
			rows = await this.#listAssetPriceRows(portfolioId, asset.assetId);
		} catch (err) {
			this.logger.error?.({
				event: 'ensure_price_rows_failed',
				ticker: asset.ticker,
				error: err?.message || String(err),
			}) || this.logger.log?.(`ensure_price_rows_failed: ${asset.ticker} ${err?.message}`);
		}
		return rows;
	}

	async fetchPriceHistory(ticker, market, context = {}) {
		const normalizedTicker = String(ticker || '').toUpperCase();
		const normalizedMarket = String(market || 'US').toUpperCase();
		const fetchedAt = nowIso();
		const shouldPersist = Boolean(context.persist && context.portfolioId && context.assetId);
		const latestStoredDate = shouldPersist && context.incremental !== false
			? await this.#getLatestStoredDate(context.portfolioId, context.assetId)
			: null;
		const incrementalStartDate = latestStoredDate ? addDays(latestStoredDate, 1) : null;

		let providerPayload;
		if (normalizedMarket === TESOURO_MARKET) {
			providerPayload = await this.tesouroHistoryProvider.fetchHistory(normalizedTicker, {
				startDate: incrementalStartDate,
				allowEmpty: true,
			});
		} else {
			const yahooSymbol = resolveYahooSymbol(normalizedTicker, normalizedMarket);
			try {
				providerPayload = await this.yahooHistoryProvider.fetchHistory(yahooSymbol, {
					startDate: incrementalStartDate,
					period: incrementalStartDate ? null : 'max',
					allowEmpty: true,
				});
			} catch {
				providerPayload = await this.yahooChartProvider.fetchHistory(yahooSymbol, {
					startDate: incrementalStartDate,
					allowEmpty: true,
				});
			}
		}

		let rows = Array.isArray(providerPayload.rows) ? providerPayload.rows : [];
		let dataSource = providerPayload.data_source;
		let isScraped = Boolean(providerPayload.is_scraped);
		let currency = providerPayload.currency || context.currency || null;
		let rawPayload = providerPayload.raw || null;

		if (!rows.length) {
			const fallback = await this.fallbackManager.fetch({
				ticker: normalizedTicker,
				market: normalizedMarket,
				assetClass: context.assetClass,
				country: context.country,
			});
			if (fallback?.quote?.currentPrice !== null && fallback?.quote?.currentPrice !== undefined) {
				const today = fetchedAt.slice(0, 10);
				rows = [{
					date: today,
					open: null,
					high: null,
					low: null,
					close: toNumberOrNull(fallback.quote.currentPrice),
					adjusted_close: toNumberOrNull(fallback.quote.currentPrice),
					volume: null,
					dividends: 0,
					stock_splits: 0,
					pu_compra: null,
					pu_venda: null,
					taxa_compra: null,
					taxa_venda: null,
				}];
				dataSource = fallback.data_source || 'unavailable';
				isScraped = Boolean(fallback.is_scraped);
				currency = fallback.quote.currency || currency;
				rawPayload = fallback.raw || rawPayload;
			}
		}

		let persisted = 0;
		if (shouldPersist && rows.length > 0) {
			persisted = await this.#persistHistoryRows(
				{
					portfolioId: context.portfolioId,
					assetId: context.assetId,
					ticker: normalizedTicker,
					market: normalizedMarket,
					currency: currency || null,
				},
				rows,
				{
					dataSource,
					isScraped,
					fetchedAt,
				}
			);
		}

		this.logger.log(
			JSON.stringify({
				event: 'price_history_fetch',
				ticker: normalizedTicker,
				market: normalizedMarket,
				start_date: incrementalStartDate,
				rows_fetched: rows.length,
				rows_persisted: persisted,
				data_source: dataSource,
				is_scraped: isScraped,
				fetched_at: fetchedAt,
			})
		);

		return {
			ticker: normalizedTicker,
			market: normalizedMarket,
			currency,
			data_source: dataSource,
			is_scraped: isScraped,
			fetched_at: fetchedAt,
			latest_stored_date_before_run: latestStoredDate,
			rows_fetched: rows.length,
			rows_persisted: persisted,
			rows,
			raw: rawPayload,
		};
	}

	async fetchPortfolioPriceHistory(portfolioId, options = {}) {
		const assets = await this.#listPortfolioAssets(portfolioId);
		const selected = options.assetId
			? assets.filter((asset) => asset.assetId === options.assetId)
			: assets;

		const tasks = selected.map((asset) =>
			this.schedule(async () => {
				try {
					const market = resolveAssetMarket(asset);
					const result = await this.fetchPriceHistory(asset.ticker, market, {
						portfolioId: asset.portfolioId,
						assetId: asset.assetId,
						assetClass: asset.assetClass,
						country: asset.country,
						currency: asset.currency,
						persist: true,
						incremental: options.incremental !== false,
					});
					return {
						assetId: asset.assetId,
						ticker: asset.ticker,
						status: 'updated',
						rows_persisted: result.rows_persisted,
						data_source: result.data_source,
					};
				} catch (error) {
					this.logger.error(
						JSON.stringify({
							event: 'price_history_asset_failed',
							assetId: asset.assetId,
							ticker: asset.ticker,
							error: error.message,
							fetched_at: nowIso(),
						})
					);
					return {
						assetId: asset.assetId,
						ticker: asset.ticker,
						status: 'failed',
						error: error.message,
					};
				}
			})
		);

		const results = await Promise.all(tasks);
		return {
			portfolioId,
			processed: selected.length,
			updated: results.filter((item) => item.status === 'updated').length,
			failed: results.filter((item) => item.status === 'failed').length,
			results,
		};
	}

	async getPriceAtDate(ticker, date, options = {}) {
		const targetDate = normalizeDate(date);
		if (!targetDate) throw new Error('date must be YYYY-MM-DD');

		const { portfolioId, asset } = await this.#resolveAssetContext(
			String(ticker || ''),
			options.userId,
			options.portfolioId
		);

		const rows = await this.#ensurePriceRows(portfolioId, asset);

		const priceRow = findPriceAtOrBeforeDate(rows, targetDate);
		return {
			ticker: asset.ticker,
			assetId: asset.assetId,
			requested_date: targetDate,
			price_date: priceRow?.date || null,
			close: getCloseForRow(priceRow),
			currency: asset.currency || null,
			data_source: priceRow?.data_source || null,
		};
	}

	async getAverageCost(ticker, userId, options = {}) {
		const { portfolioId, asset } = await this.#resolveAssetContext(
			String(ticker || ''),
			userId,
			options.portfolioId
		);
		const priceRows = await this.#ensurePriceRows(portfolioId, asset);

		const transactions = await this.#listAssetTransactions(
			portfolioId,
			asset.assetId
		);
		const method = options.method || COST_METHODS.FIFO;
		const splitEvents = buildSplitEventsFromPriceRows(priceRows);
		const holdingsResolution = resolveHoldingsWithSplitHeuristic(
			transactions,
			method,
			splitEvents,
			asset.quantity
		);
		const holdings = holdingsResolution.holdings;
		const latestPrice = priceRows.length ? priceRows[priceRows.length - 1] : null;
		const currentPrice = getCloseForRow(latestPrice);
		const marketValue =
			currentPrice !== null
				? holdings.quantityCurrent * currentPrice
				: null;

		return {
			portfolioId,
			assetId: asset.assetId,
			ticker: asset.ticker,
			method: holdings.method,
			quantity_current: holdings.quantityCurrent,
			average_cost: holdings.averageCost,
			cost_total: holdings.costCurrent,
			current_price: currentPrice,
			market_value: marketValue,
			currency: asset.currency || null,
			split_adjustment: holdingsResolution.split_adjustment,
		};
	}

	async getPortfolioMetrics(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const transactions = await this.#listPortfolioTransactions(portfolioId);
		const method = options.method || COST_METHODS.FIFO;

		const metrics = [];
		for (const asset of assets) {
			const assetTransactions = transactions.filter((tx) => {
				if (tx.assetId && tx.assetId === asset.assetId) return true;
				return normalizeTicker(tx.ticker) === normalizeTicker(asset.ticker);
			});

			const priceRows = await this.#ensurePriceRows(portfolioId, asset);
				const splitEvents = buildSplitEventsFromPriceRows(priceRows);
				const holdingsResolution = resolveHoldingsWithSplitHeuristic(
					assetTransactions,
					method,
					splitEvents,
					asset.quantity
				);
				const holdings = holdingsResolution.holdings;

			const latestPriceRow = priceRows.length ? priceRows[priceRows.length - 1] : null;
			const latestClose = getCloseForRow(latestPriceRow);
			const marketValue =
				latestClose !== null
					? holdings.quantityCurrent * latestClose
					: null;
			const absoluteReturn =
				marketValue !== null ? marketValue - holdings.costCurrent : null;
			const percentReturn =
				holdings.costCurrent > 0 && absoluteReturn !== null
					? (absoluteReturn / holdings.costCurrent) * 100
					: null;

			const returnsByPeriod = {
				daily: calculatePeriodReturn(priceRows, 1),
				weekly: calculatePeriodReturn(priceRows, 7),
				monthly: calculatePeriodReturn(priceRows, 30),
				yearly: calculatePeriodReturn(priceRows, 365),
				since_first_buy: calculatePeriodReturn(
					priceRows,
					null,
					holdings.firstBuyDate
				),
			};

			const totalDividends = aggregateDividends(
				assetTransactions.map(parseTransaction)
			);
			const dividendYieldRealized =
				holdings.totalBuysCost > 0
					? (totalDividends / holdings.totalBuysCost) * 100
					: null;

			const benchmarkComparison = await this.#buildBenchmarkComparison(
				resolveAssetMarket(asset),
				priceRows,
				holdings.firstBuyDate
			);

			metrics.push({
				assetId: asset.assetId,
				ticker: asset.ticker,
				name: asset.name || null,
				market: resolveAssetMarket(asset),
				currency: asset.currency || null,
				method: holdings.method,
				quantity_current: holdings.quantityCurrent,
				average_cost: holdings.averageCost,
				cost_total: holdings.costCurrent,
				current_price: latestClose,
				market_value: marketValue,
				absolute_return: absoluteReturn,
				percent_return: percentReturn,
					returns_by_period: returnsByPeriod,
					total_dividends: totalDividends,
					dividend_yield_realized: dividendYieldRealized,
					benchmark_comparison: benchmarkComparison,
					split_adjustment: holdingsResolution.split_adjustment,
				});
			}

		const consolidated = metrics.reduce(
			(accumulator, metric) => {
				accumulator.total_cost += metric.cost_total || 0;
				accumulator.total_market_value += metric.market_value || 0;
				accumulator.total_dividends += metric.total_dividends || 0;
				return accumulator;
			},
			{
				total_cost: 0,
				total_market_value: 0,
				total_dividends: 0,
			}
		);
		consolidated.absolute_return =
			consolidated.total_market_value - consolidated.total_cost;
		consolidated.percent_return =
			consolidated.total_cost > 0
				? (consolidated.absolute_return / consolidated.total_cost) * 100
				: null;

		return {
			userId,
			portfolioId,
			method,
			assets: metrics,
			consolidated,
		};
	}

	async getChartData(ticker, userId, chartType, period, options = {}) {
		const { portfolioId, asset } = await this.#resolveAssetContext(
			String(ticker || ''),
			userId,
			options.portfolioId
		);
		const normalizedChartType = String(chartType || 'price_history').toLowerCase();
		const normalizedPeriod = String(period || 'MAX').toUpperCase();
		const market = resolveAssetMarket(asset);

		const priceRows = await this.#ensurePriceRows(portfolioId, asset);
		const transactions = await this.#listAssetTransactions(portfolioId, asset.assetId);
		const filteredRows = this.#filterRowsByPeriod(priceRows, normalizedPeriod);
		const enrichedTransactions = enrichTransactionsWithPrices(
			transactions,
			priceRows
		);
		const filteredTransactions = this.#filterTransactionsByPeriod(
			enrichedTransactions,
			normalizedPeriod,
			priceRows
		);

		if (normalizedChartType === 'price_history') {
			return {
				chart_type: 'price_history',
				period: normalizedPeriod,
				ticker: asset.ticker,
				market,
				currency: asset.currency || null,
				series: filteredRows.map((row) => ({
					date: row.date,
					display_date: resolveDisplayDate(row.date, market),
					close: getCloseForRow(row),
				})),
				markers: filteredTransactions
					.filter((tx) => ['buy', 'sell'].includes(parseTransactionType(tx.type)))
					.map((tx) => ({
						date: tx.date,
						display_date: resolveDisplayDate(tx.date, market),
						type: parseTransactionType(tx.type),
						quantity: toNumberOrNull(tx.quantity) || 0,
						unit_price:
							toNumberOrNull(tx.unit_price) ??
							toNumberOrNull(tx.price) ??
							0,
						close_at_date: toNumberOrNull(tx.close_at_date),
					})),
			};
		}

		if (normalizedChartType === 'average_cost') {
			const timeline = this.#buildAverageCostTimeline(
				filteredRows,
				filteredTransactions,
				options.method || COST_METHODS.FIFO,
				asset.quantity
			);
			return {
				chart_type: 'average_cost',
				period: normalizedPeriod,
				ticker: asset.ticker,
				market,
				currency: asset.currency || null,
				series: timeline,
			};
		}

		if (normalizedChartType === 'cumulative_return') {
			const firstBuy = calculateHoldings(
				filteredTransactions,
				options.method || COST_METHODS.FIFO
			).firstBuyDate;
			const series = this.#buildCumulativeReturnSeries(
				filteredRows,
				firstBuy,
				market
			);
			return {
				chart_type: 'cumulative_return',
				period: normalizedPeriod,
				ticker: asset.ticker,
				market,
				currency: asset.currency || null,
				series: series.asset,
				benchmark: series.benchmark,
			};
		}

		if (normalizedChartType === 'dividends') {
			const dividends = this.#buildDividendSeries(
				filteredTransactions,
				options.method || COST_METHODS.FIFO
			);
			return {
				chart_type: 'dividends',
				period: normalizedPeriod,
				ticker: asset.ticker,
				market,
				currency: asset.currency || null,
				series: dividends.series,
				cumulative_yield_on_cost: dividends.cumulativeYieldOnCost,
			};
		}

		throw new Error(
			`Unsupported chartType '${chartType}'. Use price_history, average_cost, cumulative_return, or dividends.`
		);
	}

	async #buildBenchmarkComparison(market, assetPriceRows, firstBuyDate) {
		const benchmarkSymbol = BENCHMARK_BY_MARKET[market] || BENCHMARK_BY_MARKET.US;
		if (!firstBuyDate || !assetPriceRows.length) {
			return {
				benchmark_symbol: benchmarkSymbol,
				asset_return_since_first_buy: null,
				benchmark_return_since_first_buy: null,
				alpha: null,
			};
		}

		const assetReturnSinceFirstBuy = calculatePeriodReturn(
			assetPriceRows,
			null,
			firstBuyDate
		);
		const benchmarkRows = await this.#fetchBenchmarkRows(benchmarkSymbol, firstBuyDate);
		const benchmarkReturnSinceFirstBuy = calculatePeriodReturn(
			benchmarkRows,
			null,
			firstBuyDate
		);

		return {
			benchmark_symbol: benchmarkSymbol,
			asset_return_since_first_buy: assetReturnSinceFirstBuy,
			benchmark_return_since_first_buy: benchmarkReturnSinceFirstBuy,
			alpha:
				assetReturnSinceFirstBuy !== null &&
				benchmarkReturnSinceFirstBuy !== null
					? assetReturnSinceFirstBuy - benchmarkReturnSinceFirstBuy
					: null,
		};
	}

	async #fetchBenchmarkRows(symbol, startDate) {
		const payload = await this.yahooHistoryProvider.fetchHistory(symbol, {
			startDate,
			period: startDate ? null : 'max',
			allowEmpty: true,
		});
		return (payload.rows || []).sort((left, right) =>
			compareDate(left.date, right.date)
		);
	}

	#buildAverageCostTimeline(priceRows, transactions, method, expectedFinalQuantity = null) {
		const sortedTransactions = transactions
			.map(parseTransaction)
			.filter((tx) => tx.date && ['buy', 'sell', 'subscription'].includes(tx.type))
			.sort((left, right) =>
				left.date === right.date
					? String(left.createdAt || '').localeCompare(String(right.createdAt || ''))
					: compareDate(left.date, right.date)
			);
		const splitEvents = buildSplitEventsFromPriceRows(priceRows);
		const splitResolution = resolveHoldingsWithSplitHeuristic(
			sortedTransactions,
			method,
			splitEvents,
			expectedFinalQuantity
		);
		const effectiveSplitEvents =
			splitResolution.split_adjustment === 'with_splits'
				? splitEvents
				: [];

		const timeline = [];
		const partialTransactions = [];
		for (const transaction of sortedTransactions) {
			partialTransactions.push(transaction);
			const partialSplitEvents = effectiveSplitEvents.filter(
				(splitEvent) => splitEvent.date <= transaction.date
			);
			const holdings = calculateHoldings(partialTransactions, method, {
				splitEvents: partialSplitEvents,
			});
			const row = findPriceAtOrBeforeDate(priceRows, transaction.date);
			timeline.push({
				date: transaction.date,
				average_cost: holdings.averageCost,
				market_close: getCloseForRow(row),
				quantity: holdings.quantityCurrent,
			});
		}

		const lastRow = priceRows[priceRows.length - 1];
		if (lastRow) {
			const finalHoldings = calculateHoldings(sortedTransactions, method, {
				splitEvents: effectiveSplitEvents,
			});
			timeline.push({
				date: lastRow.date,
				average_cost: finalHoldings.averageCost,
				market_close: getCloseForRow(lastRow),
				quantity: finalHoldings.quantityCurrent,
			});
		}

		return timeline;
	}

	async #buildCumulativeReturnSeries(priceRows, firstBuyDate, market) {
		if (!priceRows.length || !firstBuyDate) {
			return { asset: [], benchmark: [] };
		}
		const firstAssetRow = findPriceAtOrBeforeDate(priceRows, firstBuyDate);
		const baseClose = getCloseForRow(firstAssetRow);
		if (!baseClose || baseClose === 0) {
			return { asset: [], benchmark: [] };
		}

		const assetSeries = priceRows
			.filter((row) => row.date >= firstBuyDate)
			.map((row) => ({
				date: row.date,
				display_date: resolveDisplayDate(row.date, market),
				return_pct: ((getCloseForRow(row) / baseClose) - 1) * 100,
			}));

		const benchmarkSymbol = BENCHMARK_BY_MARKET[market] || BENCHMARK_BY_MARKET.US;
		const benchmarkRows = await this.#fetchBenchmarkRows(benchmarkSymbol, firstBuyDate);
		const firstBenchmarkRow = findPriceAtOrBeforeDate(benchmarkRows, firstBuyDate);
		const benchmarkBase = getCloseForRow(firstBenchmarkRow);
		const benchmarkSeries = benchmarkBase
			? benchmarkRows
				.filter((row) => row.date >= firstBuyDate)
				.map((row) => ({
					date: row.date,
					return_pct: ((getCloseForRow(row) / benchmarkBase) - 1) * 100,
				}))
			: [];

		return {
			asset: assetSeries,
			benchmark: benchmarkSeries,
		};
	}

	#buildDividendSeries(transactions, method) {
		const normalized = transactions
			.map(parseTransaction)
			.filter((tx) => tx.date)
			.sort((left, right) => compareDate(left.date, right.date));
		const holdings = calculateHoldings(normalized, method);

		const grouped = new Map();
		let cumulativeDividends = 0;
		const dividendTransactions = normalized.filter((tx) =>
			['dividend', 'jcp'].includes(tx.type)
		);
		for (const tx of dividendTransactions) {
			const month = tx.date.slice(0, 7);
			const amount = toNumberOrNull(tx.amount) || 0;
			cumulativeDividends += amount;
			grouped.set(month, (grouped.get(month) || 0) + amount);
		}

		const series = Array.from(grouped.entries())
			.sort(([left], [right]) => compareDate(left, right))
			.map(([period, amount]) => ({
				period,
				amount,
			}));
		const cumulativeYieldOnCost =
			holdings.totalBuysCost > 0
				? (cumulativeDividends / holdings.totalBuysCost) * 100
				: null;

		return {
			series,
			cumulativeYieldOnCost,
		};
	}

	#filterRowsByPeriod(rows, period) {
		if (!rows.length) return [];
		const latestDate = rows[rows.length - 1].date;
		const startDate = periodStartDate(period, latestDate);
		if (!startDate) return rows;
		return rows.filter((row) => row.date >= startDate);
	}

	#filterTransactionsByPeriod(transactions, period, priceRows) {
		if (!transactions.length || !priceRows.length) return transactions;
		const latestDate = priceRows[priceRows.length - 1].date;
		const startDate = periodStartDate(period, latestDate);
		if (!startDate) return transactions;
		return transactions.filter((tx) => {
			const date = normalizeDate(tx.date);
			return date ? date >= startDate : false;
		});
	}

	async #resolveAssetContext(ticker, userId, portfolioId) {
		const resolvedPortfolioId = await this.#resolvePortfolioId(
			userId,
			portfolioId
		);
		const asset = await this.#getAssetByTicker(resolvedPortfolioId, ticker);
		if (!asset) {
			throw new Error(`Asset ticker '${ticker}' not found in portfolio`);
		}
		return {
			portfolioId: resolvedPortfolioId,
			asset,
		};
	}

	async #resolvePortfolioId(userId, explicitPortfolioId) {
		if (explicitPortfolioId) return explicitPortfolioId;
		if (!userId) {
			throw new Error('userId is required when portfolioId is not provided');
		}

		const result = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'PORTFOLIO#',
			},
		});

		if (!result.length) {
			throw new Error(`No portfolios found for user '${userId}'`);
		}

		result.sort((left, right) =>
			String(right.updatedAt || right.createdAt || '').localeCompare(
				String(left.updatedAt || left.createdAt || '')
			)
		);
		return result[0].portfolioId;
	}

	async #listPortfolioAssets(portfolioId) {
		return this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'ASSET#',
			},
		});
	}

	async #listPortfolioTransactions(portfolioId) {
		return this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TRANS#',
			},
		});
	}

	async #listAssetTransactions(portfolioId, assetId) {
		const all = await this.#listPortfolioTransactions(portfolioId);
		return all.filter((transaction) => transaction.assetId === assetId);
	}

	async #listAssetPriceRows(portfolioId, assetId) {
		const items = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': `ASSET_PRICE#${assetId}#`,
			},
		});

		return items
			.map((item) => ({
				date: item.date,
				open: toNumberOrNull(item.open),
				high: toNumberOrNull(item.high),
				low: toNumberOrNull(item.low),
				close: toNumberOrNull(item.close),
				adjusted_close: toNumberOrNull(item.adjustedClose),
				volume: toNumberOrNull(item.volume),
				dividends: toNumberOrNull(item.dividends) || 0,
				stock_splits: toNumberOrNull(item.stockSplits) || 0,
				pu_compra: toNumberOrNull(item.puCompra),
				pu_venda: toNumberOrNull(item.puVenda),
				taxa_compra: toNumberOrNull(item.taxaCompra),
				taxa_venda: toNumberOrNull(item.taxaVenda),
				data_source: item.data_source || null,
			}))
			.filter((row) => row.date)
			.sort((left, right) => compareDate(left.date, right.date));
	}

	async #getLatestPriceRow(portfolioId, assetId) {
		const rows = await this.#listAssetPriceRows(portfolioId, assetId);
		return rows.length ? rows[rows.length - 1] : null;
	}

	async #getLatestStoredDate(portfolioId, assetId) {
		const result = await this.dynamo.send(
			new QueryCommand({
				TableName: this.tableName,
				KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
				ExpressionAttributeValues: {
					':pk': `PORTFOLIO#${portfolioId}`,
					':sk': `ASSET_PRICE#${assetId}#`,
				},
				ScanIndexForward: false,
				Limit: 1,
			})
		);

		const latest = Array.isArray(result.Items) && result.Items.length
			? result.Items[0]
			: null;
		return latest?.date || null;
	}

	async #persistHistoryRows(asset, rows, metadata) {
		let persisted = 0;
		for (const row of rows) {
			if (!row?.date) continue;
			const item = {
				PK: `PORTFOLIO#${asset.portfolioId}`,
				SK: `ASSET_PRICE#${asset.assetId}#${row.date}`,
				entityType: 'ASSET_PRICE_HISTORY',
				portfolioId: asset.portfolioId,
				assetId: asset.assetId,
				ticker: asset.ticker,
				market: asset.market,
				currency: asset.currency,
				date: row.date,
				open: row.open,
				high: row.high,
				low: row.low,
				close: row.close,
				adjustedClose: row.adjusted_close,
				volume: row.volume,
				dividends: row.dividends || 0,
				stockSplits: row.stock_splits || 0,
				puCompra: row.pu_compra ?? null,
				puVenda: row.pu_venda ?? null,
				taxaCompra: row.taxa_compra ?? null,
				taxaVenda: row.taxa_venda ?? null,
				data_source: metadata.dataSource,
				is_scraped: Boolean(metadata.isScraped),
				fetched_at: metadata.fetchedAt,
				updatedAt: nowIso(),
			};

			await this.dynamo.send(
				new PutCommand({
					TableName: this.tableName,
					Item: item,
				})
			);

			// Secondary access pattern for ticker+date queries.
			await this.dynamo.send(
				new PutCommand({
					TableName: this.tableName,
					Item: {
						PK: `PRICE#${asset.ticker}`,
						SK: `${row.date}#PORTFOLIO#${asset.portfolioId}#ASSET#${asset.assetId}`,
						entityType: 'ASSET_PRICE_INDEX',
						ticker: asset.ticker,
						date: row.date,
						portfolioId: asset.portfolioId,
						assetId: asset.assetId,
						close: row.close,
						adjustedClose: row.adjusted_close,
						currency: asset.currency,
						data_source: metadata.dataSource,
						fetched_at: metadata.fetchedAt,
					},
				})
			);

			persisted += 1;
		}

		if (rows.length) {
			const latestDate = rows[rows.length - 1].date;
			await this.dynamo.send(
				new UpdateCommand({
					TableName: this.tableName,
					Key: {
						PK: `PORTFOLIO#${asset.portfolioId}`,
						SK: `ASSET#${asset.assetId}`,
					},
					UpdateExpression:
						'SET lastHistoryDate = :lastHistoryDate, lastHistorySource = :source, lastHistoryAt = :lastHistoryAt, updatedAt = :updatedAt',
					ExpressionAttributeValues: {
						':lastHistoryDate': latestDate,
						':source': metadata.dataSource,
						':lastHistoryAt': metadata.fetchedAt,
						':updatedAt': nowIso(),
					},
				})
			);
		}

		return persisted;
	}

	async #getAssetByTicker(portfolioId, ticker) {
		const normalized = normalizeTicker(ticker);
		const assets = await this.#listPortfolioAssets(portfolioId);
		return (
			assets.find((asset) => normalizeTicker(asset.ticker) === normalized) ||
			null
		);
	}

	async #queryAll(queryInput) {
		const items = [];
		let lastEvaluatedKey;
		do {
			const result = await this.dynamo.send(
				new QueryCommand({
					...queryInput,
					ExclusiveStartKey: lastEvaluatedKey,
				})
			);
			if (Array.isArray(result.Items) && result.Items.length > 0) {
				items.push(...result.Items);
			}
			lastEvaluatedKey = result.LastEvaluatedKey;
		} while (lastEvaluatedKey);
		return items;
	}
}

module.exports = {
	PortfolioPriceHistoryService,
	COST_METHODS,
	calculateHoldings,
	buildSplitEventsFromPriceRows,
	calculatePeriodReturn,
	enrichTransactionsWithPrices,
	findPriceAtOrBeforeDate,
	resolveDisplayDate,
};
