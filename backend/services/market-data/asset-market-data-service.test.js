const test = require('node:test');
const assert = require('node:assert/strict');
const { QueryCommand } = require('@aws-sdk/lib-dynamodb');

const {
	AssetMarketDataService,
	hasValidCurrentPrice,
} = require('./asset-market-data-service');
const {
	resolveAssetMarket,
	resolveYahooSymbol,
	TESOURO_MARKET,
} = require('./symbol-resolver');

const makeLogger = () => ({
	log: () => {},
	error: () => {},
});

test('resolveYahooSymbol maps BR/CA suffixes and keeps US ticker', () => {
	assert.equal(resolveYahooSymbol('petr4', 'BR'), 'PETR4.SA');
	assert.equal(resolveYahooSymbol('shop', 'CA'), 'SHOP.TO');
	assert.equal(resolveYahooSymbol('AAPL', 'US'), 'AAPL');
});

test('resolveAssetMarket infers TESOURO from bond BR assets', () => {
	assert.equal(
		resolveAssetMarket({ ticker: 'TESOURO-IPCA-2029', assetClass: 'bond', country: 'BR' }),
		TESOURO_MARKET
	);
	assert.equal(
		resolveAssetMarket({ ticker: 'AAPL', assetClass: 'stock', country: 'US' }),
		'US'
	);
});

test('hasValidCurrentPrice validates quote current price', () => {
	assert.equal(hasValidCurrentPrice({ quote: { currentPrice: 10 } }), true);
	assert.equal(hasValidCurrentPrice({ quote: { currentPrice: null } }), false);
	assert.equal(hasValidCurrentPrice(null), false);
});

test('fetchAssetData uses primary provider when price is present', async () => {
	const service = new AssetMarketDataService({
		dynamo: { send: async () => ({}) },
		logger: makeLogger(),
		scheduler: (task) => task(),
		yahooProvider: {
			fetch: async () => ({
				data_source: 'yahoo_quote_api',
				is_scraped: false,
				quote: { currentPrice: 123.45 },
				fundamentals: { info: { marketCap: 10 } },
				historical: { history_30d: [], dividends: [] },
				raw: { ok: true },
			}),
		},
		tesouroProvider: { fetch: async () => null },
		fallbackManager: {
			fetch: async () => ({
				data_source: 'scrape_google',
				is_scraped: true,
				quote: { currentPrice: 1 },
			}),
		},
	});

	const payload = await service.fetchAssetData('AAPL', 'US');
	assert.equal(payload.data_source, 'yahoo_quote_api');
	assert.equal(payload.is_scraped, false);
	assert.equal(payload.quote.currentPrice, 123.45);
});

test('fetchAssetData falls back when primary provider fails', async () => {
	const service = new AssetMarketDataService({
		dynamo: { send: async () => ({}) },
		logger: makeLogger(),
		scheduler: (task) => task(),
		yahooProvider: {
			fetch: async () => {
				throw new Error('yahoo quote api down');
			},
		},
		tesouroProvider: { fetch: async () => null },
		fallbackManager: {
			fetch: async () => ({
				data_source: 'scrape_yahoo',
				is_scraped: true,
				quote: { currentPrice: 88.3, currency: 'USD' },
				fundamentals: {},
				historical: { history_30d: [], dividends: [] },
				raw: { fallback: true },
			}),
		},
	});

	const payload = await service.fetchAssetData('AAPL', 'US');
	assert.equal(payload.data_source, 'scrape_yahoo');
	assert.equal(payload.is_scraped, true);
	assert.equal(payload.quote.currentPrice, 88.3);
	assert.equal(payload.raw.primary_error.message, 'yahoo quote api down');
});

test('refreshPortfolioAssets keeps processing when one asset fails', async () => {
	const mockAssets = [
		{
			portfolioId: 'p1',
			assetId: 'asset-ok',
			ticker: 'AAPL',
			assetClass: 'stock',
			country: 'US',
			quantity: 2,
		},
		{
			portfolioId: 'p1',
			assetId: 'asset-fail',
			ticker: 'FAIL',
			assetClass: 'stock',
			country: 'US',
			quantity: 1,
		},
	];

	const dynamo = {
		send: async (command) => {
			if (command instanceof QueryCommand) {
				return { Items: mockAssets };
			}
			return {};
		},
	};

	const persisted = [];
	const service = new AssetMarketDataService({
		dynamo,
		logger: makeLogger(),
		scheduler: (task) => task(),
		yahooProvider: { fetch: async () => null },
		tesouroProvider: { fetch: async () => null },
		fallbackManager: { fetch: async () => ({ data_source: 'unavailable', quote: { currentPrice: null } }) },
	});

	service.fetchAssetData = async (ticker, market) => {
		if (ticker === 'FAIL') throw new Error('simulated failure');
		return {
			ticker,
			market,
			data_source: 'yahoo_quote_api',
			is_scraped: false,
			fetched_at: new Date().toISOString(),
			quote: { currentPrice: 100, currency: 'USD' },
			fundamentals: {},
			historical: { history_30d: [], dividends: [] },
			raw: {},
		};
	};
	service.persistAssetData = async (asset, payload) => {
		persisted.push({ assetId: asset.assetId, data_source: payload.data_source });
	};

	const result = await service.refreshPortfolioAssets('p1');
	assert.equal(result.processed, 2);
	assert.equal(result.updated, 1);
	assert.equal(result.failed, 1);
	assert.deepEqual(persisted, [
		{ assetId: 'asset-ok', data_source: 'yahoo_quote_api' },
		{ assetId: 'asset-fail', data_source: 'unavailable' },
	]);
});
