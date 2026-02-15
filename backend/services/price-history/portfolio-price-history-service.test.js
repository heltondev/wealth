const test = require('node:test');
const assert = require('node:assert/strict');
const { QueryCommand, PutCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const {
	PortfolioPriceHistoryService,
	COST_METHODS,
	calculateHoldings,
	enrichTransactionsWithPrices,
	findPriceAtOrBeforeDate,
} = require('./portfolio-price-history-service');

const makeSilentLogger = () => ({
	log: () => {},
	error: () => {},
});

test('calculateHoldings FIFO and weighted average produce expected quantity/cost', () => {
	const transactions = [
		{ type: 'buy', date: '2025-01-01', quantity: 10, price: 10, fees: 1 },
		{ type: 'buy', date: '2025-01-02', quantity: 10, price: 20, fees: 1 },
		{ type: 'sell', date: '2025-01-03', quantity: 5, price: 30, fees: 0 },
	];

	const fifo = calculateHoldings(transactions, COST_METHODS.FIFO);
	assert.equal(fifo.quantityCurrent, 15);
	assert.ok(Math.abs(fifo.costCurrent - 251.5) < 1e-9);
	assert.ok(Math.abs(fifo.averageCost - (251.5 / 15)) < 1e-9);

	const weighted = calculateHoldings(transactions, COST_METHODS.WEIGHTED_AVERAGE);
	assert.equal(weighted.quantityCurrent, 15);
	assert.ok(Math.abs(weighted.costCurrent - 226.5) < 1e-9);
	assert.ok(Math.abs(weighted.averageCost - 15.1) < 1e-9);
});

test('findPriceAtOrBeforeDate returns nearest previous row', () => {
	const rows = [
		{ date: '2025-01-01', close: 10 },
		{ date: '2025-01-03', close: 12 },
		{ date: '2025-01-05', close: 13 },
	];
	const found = findPriceAtOrBeforeDate(rows, '2025-01-04');
	assert.deepEqual(found, rows[1]);
});

test('enrichTransactionsWithPrices computes close_at_date, slippage and operation total', () => {
	const transactions = [
		{ type: 'buy', date: '2025-01-04', quantity: 2, price: 15, fees: 1.5 },
	];
	const prices = [
		{ date: '2025-01-03', close: 14 },
		{ date: '2025-01-04', close: 13.5 },
	];

	const [enriched] = enrichTransactionsWithPrices(transactions, prices);
	assert.equal(enriched.close_at_date, 13.5);
	assert.ok(Math.abs(enriched.slippage_abs - 1.5) < 1e-9);
	assert.ok(Math.abs(enriched.operation_total - 31.5) < 1e-9);
});

test('fetchPriceHistory runs incremental and persists rows idempotently', async () => {
	const sentCommands = [];
	let providerStartDate = null;

	const dynamo = {
		send: async (command) => {
			sentCommands.push(command);
			if (command instanceof QueryCommand) {
				return {
					Items: [{ date: '2025-01-02' }],
				};
			}
			return {};
		},
	};

	const service = new PortfolioPriceHistoryService({
		dynamo,
		logger: makeSilentLogger(),
		yahooHistoryProvider: {
			fetchHistory: async (_symbol, options) => {
				providerStartDate = options.startDate;
				return {
					data_source: 'yfinance',
					is_scraped: false,
					currency: 'USD',
					rows: [
						{
							date: '2025-01-03',
							open: 101,
							high: 104,
							low: 100,
							close: 103,
							adjusted_close: 102.5,
							volume: 1000,
							dividends: 0,
							stock_splits: 0,
						},
						{
							date: '2025-01-04',
							open: 103,
							high: 105,
							low: 102,
							close: 104,
							adjusted_close: 103.8,
							volume: 900,
							dividends: 0,
							stock_splits: 0,
						},
					],
					raw: {},
				};
			},
		},
		tesouroHistoryProvider: { fetchHistory: async () => ({ rows: [] }) },
		fallbackManager: { fetch: async () => ({ data_source: 'unavailable', quote: { currentPrice: null } }) },
		scheduler: (task) => task(),
	});

	const result = await service.fetchPriceHistory('AAPL', 'US', {
		portfolioId: 'portfolio-1',
		assetId: 'asset-1',
		persist: true,
		incremental: true,
	});

	assert.equal(providerStartDate, '2025-01-03');
	assert.equal(result.rows_fetched, 2);
	assert.equal(result.rows_persisted, 2);

	const putCount = sentCommands.filter((command) => command instanceof PutCommand).length;
	const updateCount = sentCommands.filter((command) => command instanceof UpdateCommand).length;
	assert.equal(putCount, 4); // 2 daily rows + 2 ticker index rows
	assert.equal(updateCount, 1); // asset latest history markers
});

test('getAverageCost resolves ticker within portfolio and returns current valuation', async () => {
	const dynamo = {
		send: async (command) => {
			if (!(command instanceof QueryCommand)) return {};
			const sk = command.input.ExpressionAttributeValues[':sk'];

			if (sk === 'ASSET#') {
				return {
					Items: [
						{
							assetId: 'asset-aapl',
							portfolioId: 'portfolio-1',
							ticker: 'AAPL',
							currency: 'USD',
						},
					],
				};
			}

			if (sk === 'TRANS#') {
				return {
					Items: [
						{
							assetId: 'asset-aapl',
							type: 'buy',
							date: '2025-01-01',
							quantity: 10,
							price: 10,
							fees: 1,
						},
						{
							assetId: 'asset-aapl',
							type: 'sell',
							date: '2025-01-05',
							quantity: 2,
							price: 11,
							fees: 0,
						},
					],
				};
			}

			if (String(sk).startsWith('ASSET_PRICE#asset-aapl#')) {
				return {
					Items: [
						{
							date: '2025-01-04',
							close: 10.5,
							adjustedClose: 10.5,
						},
						{
							date: '2025-01-05',
							close: 11,
							adjustedClose: 11,
						},
					],
				};
			}

			return { Items: [] };
		},
	};

	const service = new PortfolioPriceHistoryService({
		dynamo,
		logger: makeSilentLogger(),
		yahooHistoryProvider: { fetchHistory: async () => ({ rows: [] }) },
		tesouroHistoryProvider: { fetchHistory: async () => ({ rows: [] }) },
		fallbackManager: { fetch: async () => ({ data_source: 'unavailable', quote: { currentPrice: null } }) },
		scheduler: (task) => task(),
	});

	const result = await service.getAverageCost('AAPL', 'user-1', {
		portfolioId: 'portfolio-1',
		method: COST_METHODS.FIFO,
	});

	assert.equal(result.assetId, 'asset-aapl');
	assert.equal(result.quantity_current, 8);
	assert.ok(Math.abs(result.cost_total - 80.8) < 1e-9);
	assert.ok(Math.abs(result.average_cost - 10.1) < 1e-9);
	assert.equal(result.current_price, 11);
	assert.ok(Math.abs(result.market_value - 88) < 1e-9);
});
