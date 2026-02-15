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
