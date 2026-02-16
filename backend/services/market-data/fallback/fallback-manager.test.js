const test = require('node:test');
const assert = require('node:assert/strict');

const { FallbackManager } = require('./fallback-manager');

const emptyQuote = {
	currentPrice: null,
	currency: null,
	change: null,
	changePercent: null,
	previousClose: null,
	marketCap: null,
	volume: null,
};

const buildPayload = (overrides = {}) => ({
	data_source: 'unknown',
	is_scraped: true,
	quote: { ...emptyQuote },
	fundamentals: {},
	historical: {
		history_30d: [],
		dividends: [],
	},
	raw: {},
	...overrides,
});

test('fallback manager selects higher-trust source even when it appears later', async () => {
	const manager = new FallbackManager({
		structuredProviders: [
			{
				fetch: async () =>
					buildPayload({
						data_source: 'statusinvest_structured',
						is_scraped: false,
						quote: {
							...emptyQuote,
							currentPrice: 10.1,
							currency: 'BRL',
						},
						fundamentals: {
							status_invest: { pe: 9.1 },
						},
					}),
			},
		],
		scrapers: [
			{
				name: 'scrape_google',
				fetch: async () =>
					buildPayload({
						data_source: 'scrape_google',
						is_scraped: true,
						quote: {
							...emptyQuote,
							currentPrice: 10.2,
							currency: 'BRL',
							change: 0.1,
							changePercent: 0.01,
							volume: 150000,
						},
						fundamentals: {
							google_finance: { pe: 10.2 },
						},
					}),
				healthCheck: async () => ({ ok: true }),
			},
		],
	});

	const payload = await manager.fetch({ ticker: 'ALZR11', market: 'BR' });
	assert.equal(payload.data_source, 'scrape_google');
	assert.equal(payload.quote.currentPrice, 10.2);
	assert.equal(payload.raw.fallback_candidates[0].source, 'scrape_google');
});

test('fallback manager deduplicates history and dividends across sources', async () => {
	const manager = new FallbackManager({
		structuredProviders: [
			{
				fetch: async () =>
					buildPayload({
						data_source: 'scrape_statusinvest',
						quote: {
							...emptyQuote,
							currentPrice: 10.05,
							currency: 'BRL',
						},
						historical: {
							history_30d: [
								{ date: '2026-01-02', close: 999, volume: 999 },
								{ date: '2026-01-03', close: 12, volume: 120 },
							],
							dividends: [
								{ date: '2026-01-15', value: 0.5 },
								{ date: '2026-01-25', value: 0.7 },
							],
						},
					}),
			},
		],
		scrapers: [
			{
				name: 'scrape_google',
				fetch: async () =>
					buildPayload({
						data_source: 'scrape_google',
						quote: {
							...emptyQuote,
							currentPrice: 10.2,
							currency: 'BRL',
							volume: 200000,
						},
						historical: {
							history_30d: [
								{ date: '2026-01-01', close: 10, volume: 100 },
								{ date: '2026-01-02', close: 11, volume: 110 },
							],
							dividends: [
								{ date: '2026-01-15', value: 0.5 },
								{ date: '2026-01-20', value: 0.6 },
							],
						},
					}),
				healthCheck: async () => ({ ok: true }),
			},
		],
	});

	const payload = await manager.fetch({ ticker: 'ALZR11', market: 'BR' });
	assert.equal(payload.historical.history_30d.length, 3);
	assert.equal(
		payload.historical.history_30d.find((row) => row.date === '2026-01-02')?.close,
		11
	);
	assert.equal(payload.historical.dividends.length, 3);
});

test('fallback manager keeps enrichment when all sources are missing quote prices', async () => {
	const manager = new FallbackManager({
		structuredProviders: [
			{
				fetch: async () =>
					buildPayload({
						data_source: 'bcb_sgs',
						is_scraped: false,
						fundamentals: {
							bcb_rates: {
								selic: { date: '14/02/2026', value: 13.75 },
							},
						},
					}),
			},
		],
		scrapers: [
			{
				name: 'scrape_statusinvest',
				fetch: async () =>
					buildPayload({
						data_source: 'scrape_statusinvest',
						fundamentals: {
							status_invest: {
								dividendYield: 0.11,
							},
						},
					}),
				healthCheck: async () => ({ ok: true }),
			},
		],
	});

	const payload = await manager.fetch({ ticker: 'ALZR11', market: 'BR' });
	assert.equal(payload.data_source, 'unavailable');
	assert.equal(payload.quote.currentPrice, null);
	assert.equal(payload.fundamentals?.bcb_rates?.selic?.value, 13.75);
	assert.equal(payload.raw.fallback_candidates.length, 2);
});
