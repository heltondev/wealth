const test = require('node:test');
const assert = require('node:assert/strict');

const { YahooApiProvider, parseCalendarFromQuoteSummary } = require('./yahoo-api-provider');

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

test('parseCalendarFromQuoteSummary parses full calendarEvents and summaryDetail', () => {
	const result = parseCalendarFromQuoteSummary({
		calendarEvents: {
			exDividendDate: { raw: 1700006400, fmt: '2023-11-15' },
			dividendDate: { raw: 1700524800, fmt: '2023-11-21' },
		},
		summaryDetail: {
			dividendRate: { raw: 0.96, fmt: '0.96' },
			dividendYield: { raw: 0.0052, fmt: '0.52%' },
			trailingAnnualDividendRate: { raw: 0.95, fmt: '0.95' },
			trailingAnnualDividendYield: { raw: 0.0051, fmt: '0.51%' },
		},
	});
	assert.equal(result.exDividendDate, '2023-11-15');
	assert.equal(result.dividendDate, '2023-11-21');
	assert.equal(result.dividendRate, 0.96);
	assert.equal(result.dividendYield, 0.0052);
	assert.equal(result.trailingAnnualDividendRate, 0.95);
	assert.equal(result.trailingAnnualDividendYield, 0.0051);
});

test('parseCalendarFromQuoteSummary parses summaryDetail only', () => {
	const result = parseCalendarFromQuoteSummary({
		summaryDetail: {
			exDividendDate: { raw: 1700006400, fmt: '2023-11-15' },
			dividendRate: { raw: 1.5, fmt: '1.50' },
			dividendYield: { raw: 0.025, fmt: '2.50%' },
		},
	});
	assert.equal(result.exDividendDate, '2023-11-15');
	assert.equal(result.dividendDate, null);
	assert.equal(result.dividendRate, 1.5);
	assert.equal(result.dividendYield, 0.025);
	assert.equal(result.trailingAnnualDividendRate, null);
});

test('parseCalendarFromQuoteSummary returns null for empty input', () => {
	assert.equal(parseCalendarFromQuoteSummary({}), null);
	assert.equal(parseCalendarFromQuoteSummary(null), null);
	assert.equal(parseCalendarFromQuoteSummary(undefined), null);
});

test('parseCalendarFromQuoteSummary handles plain epoch numbers', () => {
	const result = parseCalendarFromQuoteSummary({
		calendarEvents: {
			exDividendDate: 1700006400,
		},
		summaryDetail: {
			dividendRate: 2.0,
		},
	});
	assert.equal(result.exDividendDate, '2023-11-15');
	assert.equal(result.dividendDate, null);
	assert.equal(result.dividendRate, 2.0);
});

test('YahooApiProvider fetch returns calendar from calendarEvents module', async () => {
	const provider = new YahooApiProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('/v7/finance/quote')) {
			return new Response(JSON.stringify({
				quoteResponse: {
					result: [{
						regularMarketPrice: 185.5,
						currency: 'USD',
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('/v8/finance/chart/')) {
			return new Response(JSON.stringify({
				chart: {
					result: [{
						timestamp: [1700006400],
						indicators: {
							quote: [{
								open: [184], high: [186], low: [183],
								close: [185.5], volume: [50000],
							}],
						},
						events: {
							dividends: {
								'1700006400': { amount: 0.24, date: 1700006400 },
							},
						},
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('/v10/finance/quoteSummary/')) {
			return new Response(JSON.stringify({
				quoteSummary: {
					result: [{
						calendarEvents: {
							exDividendDate: { raw: 1700006400, fmt: '2023-11-15' },
							dividendDate: { raw: 1700524800, fmt: '2023-11-21' },
						},
						summaryDetail: {
							dividendRate: { raw: 0.96, fmt: '0.96' },
							dividendYield: { raw: 0.0052, fmt: '0.52%' },
						},
					}],
				},
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		throw new Error(`Unexpected URL in mock: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () => provider.fetch('AAPL'));
	assert.ok(payload.fundamentals.calendar, 'calendar should not be null');
	assert.equal(payload.fundamentals.calendar.exDividendDate, '2023-11-15');
	assert.equal(payload.fundamentals.calendar.dividendDate, '2023-11-21');
	assert.equal(payload.fundamentals.calendar.dividendRate, 0.96);
	assert.ok(payload.raw.calendar_events, 'raw calendar_events should be present');
	assert.ok(payload.raw.summary_detail, 'raw summary_detail should be present');
	assert.equal(payload.historical.dividends.length, 1);
	assert.equal(payload.historical.dividends[0].value, 0.24);
});
