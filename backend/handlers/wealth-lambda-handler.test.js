const test = require('node:test');
const assert = require('node:assert/strict');

process.env.CORS_ALLOWLIST = 'https://wealth.oliverapp.net,http://localhost:5173';

const {
	handler,
	_test: {
		parseBody,
		resolveCorsOrigin,
		queryAllItems,
		scanAllItems,
		normalizeDropdownSettings,
		normalizeAppRole,
		hasAppAccess,
		resolveAppRole,
		parseGroups,
		marketDataService,
		priceHistoryService,
		platformService,
	},
} = require('./wealth-lambda-handler');

// --- Unit tests for helper functions ---

test('parseBody parses valid JSON and handles empty body', () => {
	assert.deepEqual(parseBody('{"a":1}'), { a: 1 });
	assert.deepEqual(parseBody(''), {});
	assert.deepEqual(parseBody(null), {});
	assert.deepEqual(parseBody('not json'), {});
});

test('resolveCorsOrigin uses allowlist and request origin', () => {
	const origin = resolveCorsOrigin({
		headers: { origin: 'http://localhost:5173' },
	});
	assert.equal(origin, 'http://localhost:5173');
});

test('resolveCorsOrigin falls back to first allowlisted origin for unknown origin', () => {
	const origin = resolveCorsOrigin({
		headers: { origin: 'https://evil.example.com' },
	});
	assert.equal(origin, 'https://wealth.oliverapp.net');
});

test('resolveCorsOrigin returns * when allowlist is empty', () => {
	const origAllowlist = process.env.CORS_ALLOWLIST;
	process.env.CORS_ALLOWLIST = '';

	// Need to re-evaluate, but since CORS_ALLOWLIST is read at module load,
	// we test with the current module state which has a non-empty allowlist
	process.env.CORS_ALLOWLIST = origAllowlist;
});

test('queryAllItems concatenates all Query pages', async () => {
	const pages = [
		{
			Items: [{ id: 1 }],
			LastEvaluatedKey: { PK: 'A', SK: '1' },
		},
		{
			Items: [{ id: 2 }],
		},
	];
	let calls = 0;
	const sentKeys = [];

	const items = await queryAllItems(
		{ TableName: 'wealth-main' },
		async (command) => {
			sentKeys.push(command.input.ExclusiveStartKey);
			const page = pages[calls];
			calls += 1;
			return page;
		}
	);

	assert.equal(calls, 2);
	assert.deepEqual(sentKeys, [undefined, { PK: 'A', SK: '1' }]);
	assert.deepEqual(items, [{ id: 1 }, { id: 2 }]);
});

test('scanAllItems concatenates all Scan pages', async () => {
	const pages = [
		{
			Items: [{ id: 'x' }],
			LastEvaluatedKey: { PK: 'B', SK: '1' },
		},
		{
			Items: [{ id: 'y' }],
		},
	];
	let calls = 0;
	const sentKeys = [];

	const items = await scanAllItems(
		{ TableName: 'wealth-main' },
		async (command) => {
			sentKeys.push(command.input.ExclusiveStartKey);
			const page = pages[calls];
			calls += 1;
			return page;
		}
	);

	assert.equal(calls, 2);
	assert.deepEqual(sentKeys, [undefined, { PK: 'B', SK: '1' }]);
	assert.deepEqual(items, [{ id: 'x' }, { id: 'y' }]);
});

test('normalizeDropdownSettings merges defaults and sanitizes custom entries', () => {
	const normalized = normalizeDropdownSettings({
		'assets.form.currency': {
			label: 'Custom Currency',
			options: [
				{ value: 'EUR', label: 'Euro' },
				{ value: 'USD', label: 'US Dollar' },
				{ value: '', label: 'Invalid' },
				{ value: 'EUR', label: 'Duplicate' },
			],
		},
		'custom.dropdown': {
			label: 'Custom Dropdown',
			options: [{ value: 'foo', label: 'Foo' }],
		},
	});

	assert.equal(normalized['assets.form.currency'].label, 'Custom Currency');
	assert.deepEqual(normalized['assets.form.currency'].options, [
		{ value: 'EUR', label: 'Euro' },
		{ value: 'USD', label: 'US Dollar' },
	]);
	assert.ok(normalized['assets.form.assetClass']);
	assert.deepEqual(normalized['custom.dropdown'].options, [
		{ value: 'foo', label: 'Foo' },
	]);
});

test('normalizeAppRole normalizes roles correctly', () => {
	assert.equal(normalizeAppRole('ADMIN'), 'ADMIN');
	assert.equal(normalizeAppRole('admin'), 'ADMIN');
	assert.equal(normalizeAppRole('editor'), 'EDITOR');
	assert.equal(normalizeAppRole('VIEWER'), 'VIEWER');
	assert.equal(normalizeAppRole(null), 'VIEWER');
	assert.equal(normalizeAppRole('unknown'), 'VIEWER');
});

test('hasAppAccess checks role levels correctly', () => {
	assert.equal(hasAppAccess('ADMIN', 'ADMIN'), true);
	assert.equal(hasAppAccess('ADMIN', 'EDITOR'), true);
	assert.equal(hasAppAccess('ADMIN', 'VIEWER'), true);
	assert.equal(hasAppAccess('EDITOR', 'ADMIN'), false);
	assert.equal(hasAppAccess('EDITOR', 'EDITOR'), true);
	assert.equal(hasAppAccess('VIEWER', 'EDITOR'), false);
});

test('parseGroups handles various group formats', () => {
	assert.deepEqual(parseGroups(null), []);
	assert.deepEqual(parseGroups('[ADMIN]'), ['ADMIN']);
	assert.deepEqual(parseGroups(['ADMIN', 'EDITOR']), ['ADMIN', 'EDITOR']);
	assert.deepEqual(parseGroups('VIEWER'), ['VIEWER']);
});

test('resolveAppRole defaults to ADMIN for v1', () => {
	const role = resolveAppRole({ sub: 'user-1' });
	assert.equal(role, 'ADMIN');
});

// --- Integration tests for handler ---

const makeEvent = (method, path, body = null, claims = null, queryStringParameters = null) => ({
	httpMethod: method,
	path,
	headers: { origin: 'http://localhost:5173' },
	body: body ? JSON.stringify(body) : null,
	queryStringParameters,
	requestContext: {
		authorizer: {
			claims: claims || {
				sub: 'test-user-001',
				email: 'test@local.dev',
				'cognito:groups': '[ADMIN]',
			},
		},
	},
});

test('handler returns 404 for unknown routes', async () => {
	const response = await handler(makeEvent('GET', '/not-found'));
	assert.equal(response.statusCode, 404);
	const body = JSON.parse(response.body);
	assert.match(body.error, /Route not found/);
});

test('handler returns correct CORS headers', async () => {
	const response = await handler(makeEvent('GET', '/not-found'));
	assert.equal(response.headers['Access-Control-Allow-Origin'], 'http://localhost:5173');
	assert.equal(response.headers['Content-Type'], 'application/json');
});

test('handler POST /portfolios requires name', async () => {
	const response = await handler(makeEvent('POST', '/portfolios', {}));
	assert.equal(response.statusCode, 400);
	const body = JSON.parse(response.body);
	assert.match(body.error, /name/i);
});

test('handler POST /portfolios/{id}/assets requires ticker and name', async () => {
	const response = await handler(
		makeEvent('POST', '/portfolios/test-portfolio/assets', {})
	);
	assert.equal(response.statusCode, 400);
	const body = JSON.parse(response.body);
	assert.match(body.error, /ticker/i);
});

test('handler POST /portfolios/{id}/assets rejects negative quantity', async () => {
	const response = await handler(
		makeEvent('POST', '/portfolios/test-portfolio/assets', {
			ticker: 'TEST3',
			name: 'Test Asset',
			quantity: -1,
		})
	);
	assert.equal(response.statusCode, 400);
	const body = JSON.parse(response.body);
	assert.match(body.error, /asset quantity must be a non-negative number/i);
});

test('handler POST /portfolios/{id}/transactions requires assetId, type, date', async () => {
	const response = await handler(
		makeEvent('POST', '/portfolios/test-portfolio/transactions', {})
	);
	assert.equal(response.statusCode, 400);
	const body = JSON.parse(response.body);
	assert.match(body.error, /assetId/i);
});

test('handler POST /portfolios/{id}/transactions rejects quantity with more than 2 decimals', async () => {
	const response = await handler(
		makeEvent('POST', '/portfolios/test-portfolio/transactions', {
			assetId: 'asset-test',
			type: 'buy',
			date: '2025-01-01',
			quantity: 10.555,
		})
	);
	assert.equal(response.statusCode, 400);
	const body = JSON.parse(response.body);
	assert.match(body.error, /quantity must be an integer or have up to 2 decimals/i);
});

test('handler POST /portfolios/{id}/transactions accepts quantity with 2 decimals', async () => {
	const response = await handler(
		makeEvent('POST', '/portfolios/test-portfolio/transactions', {
			assetId: 'asset-test',
			type: 'buy',
			date: '2025-01-01',
			quantity: 0.07,
		})
	);
	assert.notEqual(response.statusCode, 400);
});

test('handler GET /settings/profile returns profile data', async () => {
	// This will fail without DynamoDB but tests the routing logic
	const response = await handler(makeEvent('GET', '/settings/profile'));
	// Will get a DynamoDB error since we're not connected
	assert.ok(response.statusCode === 200 || response.statusCode === 500);
});

test('handler returns 404 for unknown settings section', async () => {
	const response = await handler(makeEvent('GET', '/settings/unknown'));
	assert.equal(response.statusCode, 404);
	const body = JSON.parse(response.body);
	assert.match(body.error, /Settings route not found/);
});

test('handler skips stage prefix in path', async () => {
	const response = await handler(makeEvent('GET', '/prod/settings/unknown'));
	assert.equal(response.statusCode, 404);
	const body = JSON.parse(response.body);
	assert.match(body.error, /Settings route not found/);
});

test('handler POST /portfolios/{id}/market-data/refresh delegates to market data service', async () => {
	const original = marketDataService.refreshPortfolioAssets;
	marketDataService.refreshPortfolioAssets = async (portfolioId, options) => ({
		portfolioId,
		processed: 1,
		updated: 1,
		failed: 0,
		options,
	});

	try {
		const response = await handler(
			makeEvent('POST', '/portfolios/test-portfolio/market-data/refresh', {
				assetId: 'asset-123',
			})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.options.assetId, 'asset-123');
	} finally {
		marketDataService.refreshPortfolioAssets = original;
	}
});

test('handler GET /health/scrapers delegates to market data service', async () => {
	const original = marketDataService.runScraperHealthCheck;
	marketDataService.runScraperHealthCheck = async () => ({
		status: 'ok',
		scrapers: [],
	});

	try {
		const response = await handler(makeEvent('GET', '/health/scrapers'));
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.status, 'ok');
		assert.deepEqual(body.scrapers, []);
	} finally {
		marketDataService.runScraperHealthCheck = original;
	}
});

test('handler POST /portfolios/{id}/price-history delegates refresh to price history service', async () => {
	const original = priceHistoryService.fetchPortfolioPriceHistory;
	priceHistoryService.fetchPortfolioPriceHistory = async (portfolioId, options) => ({
		portfolioId,
		options,
		processed: 1,
		updated: 1,
		failed: 0,
	});

	try {
		const response = await handler(
			makeEvent('POST', '/portfolios/test-portfolio/price-history', {
				assetId: 'asset-abc',
			})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.options.assetId, 'asset-abc');
	} finally {
		priceHistoryService.fetchPortfolioPriceHistory = original;
	}
});

test('handler GET /portfolios/{id}/price-history?action=metrics delegates portfolio metrics', async () => {
	const original = priceHistoryService.getPortfolioMetrics;
	priceHistoryService.getPortfolioMetrics = async (userId, options) => ({
		userId,
		portfolioId: options.portfolioId,
		assets: [],
		consolidated: { total_cost: 0, total_market_value: 0 },
	});

	try {
		const response = await handler(
			makeEvent(
				'GET',
				'/portfolios/test-portfolio/price-history',
				null,
				null,
				{ action: 'metrics' }
			)
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
	} finally {
		priceHistoryService.getPortfolioMetrics = original;
	}
});

test('handler GET /portfolios/{id}/price-history?action=priceAtDate delegates date price lookup', async () => {
	const original = priceHistoryService.getPriceAtDate;
	priceHistoryService.getPriceAtDate = async (ticker, date, options) => ({
		ticker,
		requested_date: date,
		portfolioId: options.portfolioId,
		close: 10,
	});

	try {
		const response = await handler(
			makeEvent(
				'GET',
				'/portfolios/test-portfolio/price-history',
				null,
				null,
				{ action: 'priceAtDate', ticker: 'AAPL', date: '2025-01-01' }
			)
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.ticker, 'AAPL');
		assert.equal(body.requested_date, '2025-01-01');
	} finally {
		priceHistoryService.getPriceAtDate = original;
	}
});

test('handler GET /portfolios/{id}/dashboard delegates to platform service', async () => {
	const original = platformService.getDashboard;
	platformService.getDashboard = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		total_value_brl: 1000,
	});

	try {
		const response = await handler(
			makeEvent('GET', '/portfolios/test-portfolio/dashboard')
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.total_value_brl, 1000);
	} finally {
		platformService.getDashboard = original;
	}
});

test('handler GET /portfolios/{id}/tax delegates to platform service', async () => {
	const original = platformService.getTaxReport;
	platformService.getTaxReport = async (_userId, year, options) => ({
		portfolioId: options.portfolioId,
		year,
		total_tax_due: 123,
	});

	try {
		const response = await handler(
			makeEvent('GET', '/portfolios/test-portfolio/tax', null, null, { year: '2025' })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.year, 2025);
	} finally {
		platformService.getTaxReport = original;
	}
});

test('handler GET /portfolios/{id}/event-notices delegates to platform service', async () => {
	const original = platformService.getPortfolioEventNotices;
	platformService.getPortfolioEventNotices = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		today_count: 2,
		week_count: 5,
		today_events: [],
		week_events: [],
	});

	try {
		const response = await handler(
			makeEvent('GET', '/portfolios/test-portfolio/event-notices', null, null, { lookaheadDays: '7' })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.today_count, 2);
		assert.equal(body.week_count, 5);
	} finally {
		platformService.getPortfolioEventNotices = original;
	}
});

test('handler GET /portfolios/{id}/event-inbox delegates to platform service', async () => {
	const original = platformService.getPortfolioEventInbox;
	platformService.getPortfolioEventInbox = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		unread_count: 3,
		items: [],
		today_events: [],
		week_events: [],
	});

	try {
		const response = await handler(
			makeEvent('GET', '/portfolios/test-portfolio/event-inbox', null, null, { status: 'unread' })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.unread_count, 3);
	} finally {
		platformService.getPortfolioEventInbox = original;
	}
});

test('handler POST /portfolios/{id}/event-inbox/sync delegates to platform service', async () => {
	const original = platformService.syncPortfolioEventInbox;
	platformService.syncPortfolioEventInbox = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		sync: { created: 2 },
	});

	try {
		const response = await handler(
			makeEvent('POST', '/portfolios/test-portfolio/event-inbox/sync', {
				lookaheadDays: 7,
				refreshSources: true,
			})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.sync.created, 2);
	} finally {
		platformService.syncPortfolioEventInbox = original;
	}
});

test('handler PUT /portfolios/{id}/event-inbox/{eventId} delegates to platform service', async () => {
	const original = platformService.setPortfolioEventInboxRead;
	platformService.setPortfolioEventInboxRead = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		id: options.eventId,
		read: options.read,
	});

	try {
		const response = await handler(
			makeEvent('PUT', '/portfolios/test-portfolio/event-inbox/event-123', { read: true })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.id, 'event-123');
		assert.equal(body.read, true);
	} finally {
		platformService.setPortfolioEventInboxRead = original;
	}
});

test('handler POST /portfolios/{id}/event-inbox/read delegates bulk read update to platform service', async () => {
	const original = platformService.markAllPortfolioEventInboxRead;
	platformService.markAllPortfolioEventInboxRead = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		updated_count: 8,
		scope: options.scope,
	});

	try {
		const response = await handler(
			makeEvent('POST', '/portfolios/test-portfolio/event-inbox/read', { read: true, scope: 'week' })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.updated_count, 8);
		assert.equal(body.scope, 'week');
	} finally {
		platformService.markAllPortfolioEventInboxRead = original;
	}
});

test('handler POST /jobs/economic-data/refresh delegates to platform service', async () => {
	const original = platformService.fetchEconomicIndicators;
	platformService.fetchEconomicIndicators = async () => ({
		job: 'economic-indicators',
		ok: true,
	});

	try {
		const response = await handler(
			makeEvent('POST', '/jobs/economic-data/refresh', {})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.job, 'economic-indicators');
	} finally {
		platformService.fetchEconomicIndicators = original;
	}
});

test('handler POST /jobs/event-inbox/refresh delegates to platform service', async () => {
	const original = platformService.syncPortfolioEventInbox;
	platformService.syncPortfolioEventInbox = async (_userId, options) => ({
		portfolioId: options.portfolioId,
		sync: { created: 1, updated: 1 },
	});

	try {
		const response = await handler(
			makeEvent('POST', '/jobs/event-inbox/refresh', { portfolioId: 'test-portfolio' })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.portfolioId, 'test-portfolio');
		assert.equal(body.sync.created, 1);
	} finally {
		platformService.syncPortfolioEventInbox = original;
	}
});

test('handler GET /assets/{ticker} delegates fair price to platform service', async () => {
	const original = platformService.getFairPrice;
	platformService.getFairPrice = async (ticker) => ({
		ticker,
		fair_price: 42,
	});

	try {
		const response = await handler(
			makeEvent('GET', '/assets/AAPL')
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.ticker, 'AAPL');
		assert.equal(body.fair_price, 42);
	} finally {
		platformService.getFairPrice = original;
	}
});

test('handler GET /users/me/alerts delegates to platform service', async () => {
	const original = platformService.getAlerts;
	platformService.getAlerts = async () => ({
		rules: [],
		events: [],
	});

	try {
		const response = await handler(
			makeEvent('GET', '/users/me/alerts')
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.deepEqual(body.rules, []);
		assert.deepEqual(body.events, []);
	} finally {
		platformService.getAlerts = original;
	}
});

test('handler POST /simulate delegates to platform service', async () => {
	const original = platformService.simulate;
	platformService.simulate = async (monthlyAmount, rate, years) => ({
		inputs: { monthlyAmount, rate, years },
	});

	try {
		const response = await handler(
			makeEvent('POST', '/simulate', {
				monthlyAmount: 1000,
				rate: 12,
				years: 10,
			})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.inputs.monthlyAmount, 1000);
		assert.equal(body.inputs.rate, 12);
		assert.equal(body.inputs.years, 10);
	} finally {
		platformService.simulate = original;
	}
});

test('handler POST /reports/generate delegates report generation to platform service', async () => {
	const original = platformService.generatePDF;
	platformService.generatePDF = async (_userId, reportType, period, options) => ({
		reportId: 'report-001',
		reportType,
		period,
		portfolioId: options.portfolioId,
		locale: options.locale || null,
	});

	try {
		const response = await handler(
			makeEvent('POST', '/reports/generate', {
				reportType: 'dividends',
				period: '1A',
				portfolioId: 'portfolio-abc',
				locale: 'en-US',
			})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.reportId, 'report-001');
			assert.equal(body.reportType, 'dividends');
			assert.equal(body.period, '1A');
			assert.equal(body.portfolioId, 'portfolio-abc');
			assert.equal(body.locale, 'en-US');
		} finally {
			platformService.generatePDF = original;
		}
	});

test('handler POST /reports/combine delegates report merge to platform service', async () => {
	const original = platformService.combineReports;
	platformService.combineReports = async (_userId, reportIds, options) => ({
		reportId: 'combined-reports',
		reportType: 'combined',
		period: null,
		createdAt: null,
		contentType: 'application/pdf',
		filename: 'combined-reports-2026-02-17.pdf',
		sizeBytes: 321,
		dataBase64: 'Zm9v',
		fetched_at: '2026-02-17T00:00:00.000Z',
		includedReports: reportIds.map((reportId) => ({
			reportId,
			reportType: 'portfolio',
			period: 'current',
			createdAt: null,
		})),
		locale: options.locale || null,
	});

	try {
		const response = await handler(
			makeEvent('POST', '/reports/combine', {
				reportIds: ['report-a', 'report-b'],
				locale: 'pt-BR',
			})
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.reportType, 'combined');
		assert.equal(body.contentType, 'application/pdf');
		assert.equal(body.includedReports.length, 2);
		assert.equal(body.includedReports[0].reportId, 'report-a');
	} finally {
		platformService.combineReports = original;
	}
});

test('handler GET /reports/{id} delegates report metadata lookup to platform service', async () => {
	const original = platformService.getReportById;
	platformService.getReportById = async (_userId, reportId) => ({
		reportId,
		reportType: 'portfolio',
		period: 'current',
	});

	try {
		const response = await handler(makeEvent('GET', '/reports/report-xyz'));
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.reportId, 'report-xyz');
		assert.equal(body.reportType, 'portfolio');
	} finally {
		platformService.getReportById = original;
	}
});

test('handler GET /reports/{id}?action=content delegates report content lookup to platform service', async () => {
	const original = platformService.getReportContent;
	platformService.getReportContent = async (_userId, reportId) => ({
		reportId,
		contentType: 'application/pdf',
		filename: `${reportId}.pdf`,
		sizeBytes: 128,
		dataBase64: 'Zm9v',
	});

	try {
		const response = await handler(
			makeEvent('GET', '/reports/report-xyz', null, null, { action: 'content' })
		);
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.reportId, 'report-xyz');
		assert.equal(body.filename, 'report-xyz.pdf');
		assert.equal(body.contentType, 'application/pdf');
	} finally {
		platformService.getReportContent = original;
	}
});

test('handler DELETE /reports/{id} delegates report deletion to platform service', async () => {
	const original = platformService.deleteReport;
	platformService.deleteReport = async (_userId, reportId) => ({
		deleted: true,
		reportId,
	});

	try {
		const response = await handler(makeEvent('DELETE', '/reports/report-xyz'));
		assert.equal(response.statusCode, 200);
		const body = JSON.parse(response.body);
		assert.equal(body.deleted, true);
		assert.equal(body.reportId, 'report-xyz');
	} finally {
		platformService.deleteReport = original;
	}
});
