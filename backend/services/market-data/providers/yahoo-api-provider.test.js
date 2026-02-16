const test = require('node:test');
const assert = require('node:assert/strict');

const { YahooApiProvider } = require('./yahoo-api-provider');

const withMockedFetch = async (fetchImpl, task) => {
	const originalFetch = global.fetch;
	global.fetch = fetchImpl;
	try {
		return await task();
	} finally {
		global.fetch = originalFetch;
	}
};

test('YahooApiProvider normalizes financial statements from quoteSummary', async () => {
	const provider = new YahooApiProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('/v7/finance/quote')) {
			return new Response(JSON.stringify({
				quoteResponse: {
					result: [{
						regularMarketPrice: 101.25,
						currency: 'BRL',
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('/v8/finance/chart/')) {
			return new Response(JSON.stringify({
				chart: {
					result: [{
						timestamp: [1767139200],
						indicators: {
							quote: [{
								open: [100],
								high: [102],
								low: [99],
								close: [101.25],
								volume: [150000],
							}],
							adjclose: [{
								adjclose: [101.25],
							}],
						},
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('/v10/finance/quoteSummary/')) {
			return new Response(JSON.stringify({
				quoteSummary: {
					result: [{
						incomeStatementHistory: {
							incomeStatementHistory: [{
								endDate: { raw: 1767139200 },
								totalRevenue: { raw: 1000 },
								netIncome: { raw: 120 },
							}],
						},
						incomeStatementHistoryQuarterly: {
							incomeStatementHistory: [{
								endDate: { raw: 1764547200 },
								totalRevenue: { raw: 255 },
							}],
						},
						balanceSheetHistory: {
							balanceSheetStatements: [{
								endDate: { raw: 1767139200 },
								totalAssets: { raw: 5000 },
							}],
						},
						balanceSheetHistoryQuarterly: {
							balanceSheetStatements: [{
								endDate: { raw: 1764547200 },
								totalAssets: { raw: 5200 },
							}],
						},
						cashflowStatementHistory: {
							cashflowStatements: [{
								endDate: { raw: 1767139200 },
								totalCashFromOperatingActivities: { raw: 300 },
							}],
						},
						cashflowStatementHistoryQuarterly: {
							cashflowStatements: [{
								endDate: { raw: 1764547200 },
								totalCashFromOperatingActivities: { raw: 85 },
							}],
						},
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		throw new Error(`Unexpected URL in mock: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () => provider.fetch('ALZR11.SA'));
	assert.equal(payload.quote.currentPrice, 101.25);
	assert.equal(payload.fundamentals.financials.length, 1);
	assert.equal(payload.fundamentals.financials[0].period, '2025-12-31');
	assert.equal(payload.fundamentals.financials[0].totalRevenue, 1000);
	assert.equal(payload.fundamentals.quarterly_financials.length, 1);
	assert.equal(payload.fundamentals.balance_sheet[0].totalAssets, 5000);
	assert.equal(payload.fundamentals.cashflow[0].totalCashFromOperatingActivities, 300);
	assert.ok(payload.raw.quote_summary);
});

test('YahooApiProvider falls back to chart close when quote endpoint fails', async () => {
	const provider = new YahooApiProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('/v7/finance/quote')) {
			return new Response('unauthorized', { status: 401 });
		}

		if (url.includes('/v8/finance/chart/')) {
			return new Response(JSON.stringify({
				chart: {
					result: [{
						timestamp: [1767139200],
						indicators: {
							quote: [{
								open: [54],
								high: [56],
								low: [53],
								close: [55],
								volume: [1000],
							}],
						},
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('/v10/finance/quoteSummary/')) {
			return new Response(JSON.stringify({
				quoteSummary: { result: [] },
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		throw new Error(`Unexpected URL in mock: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () => provider.fetch('BTLG11.SA'));
	assert.equal(payload.quote.currentPrice, 55);
	assert.equal(payload.raw.quote_error.message, 'Yahoo quote endpoint responded with 401');
});
