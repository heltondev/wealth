const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
	DynamoDBDocumentClient,
	QueryCommand,
	PutCommand,
	GetCommand,
	UpdateCommand,
	DeleteCommand,
	ScanCommand,
} = require('@aws-sdk/lib-dynamodb');
const { AssetMarketDataService } = require('../services/market-data');
const { PortfolioPriceHistoryService } = require('../services/price-history');
const { PlatformService } = require('../services/platform');
const {
	buildAwsClientConfig,
	resolveTableName,
} = require('../config/aws');

const TABLE_NAME = resolveTableName();
const ddbClient = new DynamoDBClient(
	buildAwsClientConfig({ service: 'dynamodb' })
);
const dynamo = DynamoDBDocumentClient.from(ddbClient);
const marketDataService = new AssetMarketDataService({
	dynamo,
	tableName: TABLE_NAME,
	logger: console,
});
const priceHistoryService = new PortfolioPriceHistoryService({
	dynamo,
	tableName: TABLE_NAME,
	logger: console,
});
const platformService = new PlatformService({
	dynamo,
	tableName: TABLE_NAME,
	logger: console,
	marketDataService,
	priceHistoryService,
});

const CORS_ALLOWLIST = (process.env.CORS_ALLOWLIST || '')
	.split(',')
	.map((v) => v.trim())
	.filter(Boolean);
const DEFAULT_DROPDOWN_SETTINGS = {
	'assets.form.assetClass': {
		label: 'Assets / Asset Class',
		options: [
			{ value: 'stock', label: 'Stock' },
			{ value: 'fii', label: 'FII' },
			{ value: 'bond', label: 'Bond' },
			{ value: 'crypto', label: 'Crypto' },
			{ value: 'rsu', label: 'RSU' },
		],
	},
	'assets.form.country': {
		label: 'Assets / Country',
		options: [
			{ value: 'BR', label: 'Brazil' },
			{ value: 'US', label: 'United States' },
			{ value: 'CA', label: 'Canada' },
		],
	},
	'assets.form.currency': {
		label: 'Assets / Currency',
		options: [
			{ value: 'BRL', label: 'BRL' },
			{ value: 'USD', label: 'USD' },
			{ value: 'CAD', label: 'CAD' },
		],
	},
	'assets.filters.status': {
		label: 'Assets / Status Filter',
		options: [
			{ value: 'active', label: 'Active' },
			{ value: 'inactive', label: 'Inactive' },
			{ value: 'all', label: 'All' },
		],
	},
	'transactions.filters.type': {
		label: 'Transactions / Type Filter',
		options: [
			{ value: 'all', label: 'All' },
			{ value: 'buy', label: 'Buy' },
			{ value: 'sell', label: 'Sell' },
			{ value: 'dividend', label: 'Dividend' },
			{ value: 'jcp', label: 'JCP' },
			{ value: 'tax', label: 'Tax' },
			{ value: 'subscription', label: 'Subscription' },
			{ value: 'transfer', label: 'Transfer' },
		],
	},
	'transactions.filters.status': {
		label: 'Transactions / Status Filter',
		options: [
			{ value: 'all', label: 'All' },
			{ value: 'confirmed', label: 'Confirmed' },
			{ value: 'pending', label: 'Pending' },
			{ value: 'failed', label: 'Failed' },
			{ value: 'canceled', label: 'Canceled' },
			{ value: 'unknown', label: 'Unknown' },
		],
	},
	'settings.profile.preferredCurrency': {
		label: 'Settings / Preferred Currency',
		options: [
			{ value: 'BRL', label: 'BRL' },
			{ value: 'USD', label: 'USD' },
			{ value: 'CAD', label: 'CAD' },
		],
	},
	'settings.aliases.source': {
		label: 'Settings / Alias Source',
		options: [
			{ value: 'manual', label: 'Manual' },
			{ value: 'b3', label: 'B3' },
			{ value: 'itau', label: 'Itau' },
			{ value: 'robinhood', label: 'Robinhood' },
			{ value: 'equate', label: 'Equate' },
			{ value: 'coinbase', label: 'Coinbase' },
		],
	},
	'settings.preferences.language': {
		label: 'Settings / Language',
		options: [
			{ value: 'en', label: 'English' },
			{ value: 'pt', label: 'Portugues' },
		],
	},
	'tables.pagination.itemsPerPage': {
		label: 'Tables / Items Per Page',
		options: [
			{ value: '5', label: '5' },
			{ value: '10', label: '10' },
			{ value: '25', label: '25' },
			{ value: '50', label: '50' },
		],
	},
};

// --- Helpers ---

const generateId = () =>
	`${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

const parseBody = (body) => {
	if (!body) return {};
	try {
		return JSON.parse(body);
	} catch {
		return {};
	}
};

const parseTransactionQuantity = (value) => {
	if (value === undefined || value === null || value === '') return 0;
	const normalized = value.toString().trim().replace(',', '.');
	if (!/^-?\d+(\.\d{1,2})?$/.test(normalized)) {
		throw errorResponse(400, 'quantity must be an integer or have up to 2 decimals');
	}
	const numeric = Number(normalized);
	if (!Number.isFinite(numeric)) {
		throw errorResponse(400, 'quantity must be numeric');
	}
	return numeric;
};

const parseAssetQuantity = (value) => {
	if (value === undefined || value === null || value === '') return 0;
	const numeric = Number(value);
	if (!Number.isFinite(numeric) || numeric < 0) {
		throw errorResponse(400, 'asset quantity must be a non-negative number');
	}
	return numeric;
};

const resolveCorsOrigin = (event) => {
	const origin = event?.headers?.origin || event?.headers?.Origin || '';
	if (CORS_ALLOWLIST.length === 0) return '*';
	if (CORS_ALLOWLIST.includes(origin)) return origin;
	return CORS_ALLOWLIST[0] || '*';
};

const errorResponse = (statusCode, message) => ({
	statusCode,
	message,
});

const sanitizeDropdownOption = (option) => {
	if (!option || typeof option !== 'object') return null;
	const value = String(option.value ?? '').trim();
	if (!value) return null;
	const label = String(option.label ?? value).trim() || value;
	return { value, label };
};

const sanitizeDropdownConfig = (key, config, fallbackConfig) => {
	const label =
		String(config?.label ?? fallbackConfig?.label ?? key).trim() || key;
	const rawOptions = Array.isArray(config?.options)
		? config.options
		: fallbackConfig?.options || [];
	const seen = new Set();
	const options = [];

	for (const option of rawOptions) {
		const sanitized = sanitizeDropdownOption(option);
		if (!sanitized || seen.has(sanitized.value)) continue;
		seen.add(sanitized.value);
		options.push(sanitized);
	}

	return { label, options };
};

const normalizeDropdownSettings = (settings = {}) => {
	const normalized = {};
	const keys = new Set([
		...Object.keys(DEFAULT_DROPDOWN_SETTINGS),
		...Object.keys(settings || {}),
	]);

	for (const key of keys) {
		normalized[key] = sanitizeDropdownConfig(
			key,
			settings?.[key],
			DEFAULT_DROPDOWN_SETTINGS[key]
		);
	}

	return normalized;
};

const queryAllItems = async (
	queryInput,
	sendCommand = (command) => dynamo.send(command)
) => {
	const items = [];
	let lastEvaluatedKey;

	do {
		const result = await sendCommand(
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
};

const scanAllItems = async (
	scanInput,
	sendCommand = (command) => dynamo.send(command)
) => {
	const items = [];
	let lastEvaluatedKey;

	do {
		const result = await sendCommand(
			new ScanCommand({
				...scanInput,
				ExclusiveStartKey: lastEvaluatedKey,
			})
		);
		if (Array.isArray(result.Items) && result.Items.length > 0) {
			items.push(...result.Items);
		}
		lastEvaluatedKey = result.LastEvaluatedKey;
	} while (lastEvaluatedKey);

	return items;
};

// --- Authorization ---

const APP_ROLE_LEVELS = {
	VIEWER: 1,
	EDITOR: 2,
	ADMIN: 3,
};

const normalizeAppRole = (role) => {
	if (!role) return 'VIEWER';
	const upper = role.toString().toUpperCase();
	return APP_ROLE_LEVELS[upper] ? upper : 'VIEWER';
};

const hasAppAccess = (role, requiredRole) => {
	const current = APP_ROLE_LEVELS[normalizeAppRole(role)] || 0;
	const required =
		APP_ROLE_LEVELS[normalizeAppRole(requiredRole)] ||
		APP_ROLE_LEVELS.VIEWER;
	return current >= required;
};

const ensureAppAccess = (role, requiredRole) => {
	if (!hasAppAccess(role, requiredRole)) {
		throw errorResponse(403, 'Access denied');
	}
};

const resolveAppRole = (claims) => {
	const groups = parseGroups(claims?.['cognito:groups']);
	if (groups.includes('ADMIN')) return 'ADMIN';
	if (groups.includes('EDITOR')) return 'EDITOR';
	return 'ADMIN'; // Default to ADMIN for v1 (single user)
};

const parseGroups = (groupsStr) => {
	if (!groupsStr) return [];
	if (Array.isArray(groupsStr)) return groupsStr;
	const str = groupsStr.toString().trim();
	// Handle Cognito format: "[ADMIN]" or "[ADMIN, EDITOR]"
	if (str.startsWith('[') && str.endsWith(']')) {
		return str
			.slice(1, -1)
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean);
	}
	return [str];
};

// --- Route Handlers ---

async function handlePortfolios(method, userId, body) {
	if (method === 'GET') {
		return queryAllItems({
			TableName: TABLE_NAME,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'PORTFOLIO#',
			},
		});
	}

	if (method === 'POST') {
		const { name, description, baseCurrency } = parseBody(body);
		if (!name) throw errorResponse(400, 'Portfolio name is required');

		const portfolioId = generateId();
		const now = new Date().toISOString();
		const item = {
			PK: `USER#${userId}`,
			SK: `PORTFOLIO#${portfolioId}`,
			portfolioId,
			name,
			description: description || '',
			baseCurrency: baseCurrency || 'BRL',
			createdAt: now,
			updatedAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		return item;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handlePortfolioById(method, userId, portfolioId, body) {
	const key = { PK: `USER#${userId}`, SK: `PORTFOLIO#${portfolioId}` };

	if (method === 'GET') {
		const result = await dynamo.send(
			new GetCommand({ TableName: TABLE_NAME, Key: key })
		);
		if (!result.Item) throw errorResponse(404, 'Portfolio not found');
		return result.Item;
	}

	if (method === 'PUT') {
		const { name, description, baseCurrency } = parseBody(body);
		const now = new Date().toISOString();
		const updates = [];
		const names = {};
		const values = { ':now': now };

		if (name !== undefined) {
			updates.push('#n = :name');
			names['#n'] = 'name';
			values[':name'] = name;
		}
		if (description !== undefined) {
			updates.push('description = :desc');
			values[':desc'] = description;
		}
		if (baseCurrency !== undefined) {
			updates.push('baseCurrency = :cur');
			values[':cur'] = baseCurrency;
		}
		updates.push('updatedAt = :now');

		const result = await dynamo.send(
			new UpdateCommand({
				TableName: TABLE_NAME,
				Key: key,
				UpdateExpression: `SET ${updates.join(', ')}`,
				ExpressionAttributeNames:
					Object.keys(names).length > 0 ? names : undefined,
				ExpressionAttributeValues: values,
				ReturnValues: 'ALL_NEW',
			})
		);
		return result.Attributes;
	}

	if (method === 'DELETE') {
		await dynamo.send(
			new DeleteCommand({ TableName: TABLE_NAME, Key: key })
		);
		return { message: 'Portfolio deleted', id: portfolioId };
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleAssets(method, portfolioId, body) {
	if (method === 'GET') {
		return queryAllItems({
			TableName: TABLE_NAME,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'ASSET#',
			},
		});
	}

	if (method === 'POST') {
		const { ticker, name, assetClass, country, currency, status, quantity, source } =
			parseBody(body);
		if (!ticker || !name)
			throw errorResponse(400, 'Ticker and name are required');
		const normalizedAssetQuantity = parseAssetQuantity(quantity);

		const assetId = generateId();
		const now = new Date().toISOString();
		const item = {
			PK: `PORTFOLIO#${portfolioId}`,
			SK: `ASSET#${assetId}`,
			assetId,
			portfolioId,
			ticker: ticker.toUpperCase(),
			name,
			assetClass: assetClass || 'stock',
			country: country || 'BR',
			currency: currency || 'BRL',
			status: status || 'active',
			quantity: normalizedAssetQuantity,
			source: source || null,
			createdAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		return item;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleAssetById(method, portfolioId, assetId, body) {
	const key = {
		PK: `PORTFOLIO#${portfolioId}`,
		SK: `ASSET#${assetId}`,
	};

	if (method === 'GET') {
		const result = await dynamo.send(
			new GetCommand({ TableName: TABLE_NAME, Key: key })
		);
		if (!result.Item) throw errorResponse(404, 'Asset not found');
		return result.Item;
	}

	if (method === 'PUT') {
		const { name, assetClass, country, currency, status, quantity, source } = parseBody(body);
		const now = new Date().toISOString();
		const updates = ['updatedAt = :now'];
		const names = {};
		const values = { ':now': now };

		if (name !== undefined) {
			updates.push('#n = :name');
			names['#n'] = 'name';
			values[':name'] = name;
		}
		if (assetClass !== undefined) {
			updates.push('assetClass = :cls');
			values[':cls'] = assetClass;
		}
		if (country !== undefined) {
			updates.push('country = :cty');
			values[':cty'] = country;
		}
		if (currency !== undefined) {
			updates.push('currency = :cur');
			values[':cur'] = currency;
		}
		if (status !== undefined) {
			updates.push('#s = :status');
			names['#s'] = 'status';
			values[':status'] = status;
		}
		if (quantity !== undefined) {
			updates.push('quantity = :quantity');
			values[':quantity'] = parseAssetQuantity(quantity);
		}
		if (source !== undefined) {
			updates.push('source = :source');
			values[':source'] = source || null;
		}

		const result = await dynamo.send(
			new UpdateCommand({
				TableName: TABLE_NAME,
				Key: key,
				UpdateExpression: `SET ${updates.join(', ')}`,
				ExpressionAttributeNames:
					Object.keys(names).length > 0 ? names : undefined,
				ExpressionAttributeValues: values,
				ReturnValues: 'ALL_NEW',
			})
		);
		return result.Attributes;
	}

	if (method === 'DELETE') {
		await dynamo.send(
			new DeleteCommand({ TableName: TABLE_NAME, Key: key })
		);
		return { message: 'Asset deleted', id: assetId };
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleTransactions(method, portfolioId, body) {
	if (method === 'GET') {
		return queryAllItems({
			TableName: TABLE_NAME,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TRANS#',
			},
		});
	}

	if (method === 'POST') {
		const {
			assetId,
			type,
			date,
			quantity,
			price,
			currency,
			amount,
			status,
			sourceDocId,
		} = parseBody(body);
		if (!assetId || !type || !date)
			throw errorResponse(400, 'assetId, type, and date are required');
		const normalizedQuantity = parseTransactionQuantity(quantity);

		const transId = generateId();
		const now = new Date().toISOString();
		const item = {
			PK: `PORTFOLIO#${portfolioId}`,
			SK: `TRANS#${transId}`,
			transId,
			portfolioId,
			assetId,
			type,
			date,
			quantity: normalizedQuantity,
			price: price || 0,
			currency: currency || 'BRL',
			amount: amount || 0,
			status: status || 'confirmed',
			sourceDocId: sourceDocId || null,
			createdAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		return item;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleTransactionById(method, portfolioId, transId) {
	const key = {
		PK: `PORTFOLIO#${portfolioId}`,
		SK: `TRANS#${transId}`,
	};

	if (method === 'GET') {
		const result = await dynamo.send(
			new GetCommand({ TableName: TABLE_NAME, Key: key })
		);
		if (!result.Item) throw errorResponse(404, 'Transaction not found');
		return result.Item;
	}

	if (method === 'DELETE') {
		await dynamo.send(
			new DeleteCommand({ TableName: TABLE_NAME, Key: key })
		);
		return { message: 'Transaction deleted', id: transId };
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleSettingsProfile(method, userId, body) {
	const key = { PK: `USER#${userId}`, SK: 'SETTINGS#profile' };

	if (method === 'GET') {
		const result = await dynamo.send(
			new GetCommand({ TableName: TABLE_NAME, Key: key })
		);
		return result.Item || { PK: key.PK, SK: key.SK };
	}

	if (method === 'PUT') {
		const data = parseBody(body);
		const now = new Date().toISOString();
		const item = {
			...data,
			PK: key.PK,
			SK: key.SK,
			updatedAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		return item;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleSettingsDropdowns(method, userId, body) {
	const key = { PK: `USER#${userId}`, SK: 'SETTINGS#dropdowns' };

	if (method === 'GET') {
		const result = await dynamo.send(
			new GetCommand({ TableName: TABLE_NAME, Key: key })
		);
		return {
			PK: key.PK,
			SK: key.SK,
			dropdowns: normalizeDropdownSettings(result.Item?.dropdowns || {}),
			updatedAt: result.Item?.updatedAt || null,
		};
	}

	if (method === 'PUT') {
		const data = parseBody(body);
		if (!data.dropdowns || typeof data.dropdowns !== 'object') {
			throw errorResponse(400, 'dropdowns object is required');
		}

		const now = new Date().toISOString();
		const item = {
			PK: key.PK,
			SK: key.SK,
			dropdowns: normalizeDropdownSettings(data.dropdowns),
			updatedAt: now,
		};
		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		return item;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleAliases(method, body) {
	if (method === 'GET') {
		const result = await dynamo.send(
			new QueryCommand({
				TableName: TABLE_NAME,
				KeyConditionExpression:
					'begins_with(PK, :prefix)',
				ExpressionAttributeValues: {
					':prefix': 'ALIAS#',
				},
				// Note: This scan-like query won't work well on DynamoDB
				// For v1 local dev, we'll use a fixed PK pattern
			})
		);
		return result.Items || [];
	}

	if (method === 'POST' || method === 'PUT') {
		const { normalizedName, ticker, source } = parseBody(body);
		if (!normalizedName || !ticker)
			throw errorResponse(400, 'normalizedName and ticker are required');

		const now = new Date().toISOString();
		const item = {
			PK: `ALIAS#${normalizedName.toLowerCase()}`,
			SK: `TICKER#${ticker.toUpperCase()}`,
			normalizedName: normalizedName.toLowerCase(),
			ticker: ticker.toUpperCase(),
			source: source || 'manual',
			createdAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		return item;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleAliasesList(method) {
	if (method === 'GET') {
		// For aliases, we scan with a filter since PK varies.
		return scanAllItems({
			TableName: TABLE_NAME,
			FilterExpression: 'begins_with(PK, :prefix)',
			ExpressionAttributeValues: {
				':prefix': 'ALIAS#',
			},
		});
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleImport(portfolioId, body) {
	const { fileName, parserId } = parseBody(body);
	if (!fileName) throw errorResponse(400, 'fileName is required');

	const { detectProvider, getParser } = require('../parsers/index');
	const XLSX = require('xlsx');

	let parser, workbook;
	if (parserId) {
		parser = getParser(parserId);
		if (!parser) throw errorResponse(400, `Unknown parser: ${parserId}`);
		workbook = XLSX.readFile(fileName);
	} else {
		const detected = detectProvider(fileName);
		if (!detected) throw errorResponse(400, 'Could not detect file format');
		parser = detected.parser;
		workbook = detected.workbook;
	}

	const parsed = parser.parse(workbook, { sourceFile: fileName });
	const sourceFile = require('path').basename(fileName);
	const now = new Date().toISOString();
	const results = { assets: 0, transactions: 0, aliases: 0 };

	// Create assets
	for (const asset of parsed.assets) {
		const assetId = `asset-${asset.ticker.toLowerCase()}`;
		await dynamo.send(new PutCommand({
			TableName: TABLE_NAME,
			Item: {
				PK: `PORTFOLIO#${portfolioId}`,
				SK: `ASSET#${assetId}`,
				assetId,
				portfolioId,
				ticker: asset.ticker,
				name: asset.name,
				assetClass: asset.assetClass,
				country: asset.country || 'BR',
				currency: asset.currency || 'BRL',
				quantity: parseAssetQuantity(asset.quantity),
				source: sourceFile,
				status: 'active',
				createdAt: now,
			},
			ConditionExpression: 'attribute_not_exists(PK)',
		}).catch(() => {}));
		results.assets++;
	}

	// Create transactions
	for (const trans of parsed.transactions) {
		const assetId = `asset-${trans.ticker.toLowerCase()}`;
		const transId = generateId();
		await dynamo.send(new PutCommand({
			TableName: TABLE_NAME,
			Item: {
				PK: `PORTFOLIO#${portfolioId}`,
				SK: `TRANS#${transId}`,
				transId,
				portfolioId,
				assetId,
				ticker: trans.ticker,
				type: trans.type,
				date: trans.date,
				quantity: trans.quantity,
				price: trans.price,
				currency: trans.currency || 'BRL',
				amount: trans.amount,
				status: 'confirmed',
				institution: trans.institution || null,
				direction: trans.direction || null,
				market: trans.market || null,
				sourceDocId: trans.source || null,
				createdAt: now,
			},
		}));
		results.transactions++;
	}

	// Create aliases
	for (const alias of parsed.aliases) {
		await dynamo.send(new PutCommand({
			TableName: TABLE_NAME,
			Item: {
				PK: `ALIAS#${alias.normalizedName}`,
				SK: `TICKER#${alias.ticker}`,
				normalizedName: alias.normalizedName,
				ticker: alias.ticker,
				source: alias.source || 'b3',
				createdAt: now,
			},
		}).catch(() => {}));
		results.aliases++;
	}

	return {
		parser: parser.id,
		provider: parser.provider,
		...results,
	};
}

async function handleMarketDataRefresh(method, portfolioId, body) {
	if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
	const { assetId } = parseBody(body);
	return marketDataService.refreshPortfolioAssets(portfolioId, {
		assetId: assetId || null,
	});
}

async function handleScraperHealth(method) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return marketDataService.runScraperHealthCheck();
}

async function handlePriceHistory(method, portfolioId, userId, body, query = {}) {
	const chartType = query.chartType || query.chart_type || 'price_history';
	const period = query.period || 'MAX';
	const ticker = query.ticker;
	const date = query.date;
	const methodOption = query.method || 'fifo';

	if (method === 'POST') {
		const { assetId } = parseBody(body);
		return priceHistoryService.fetchPortfolioPriceHistory(portfolioId, {
			assetId: assetId || null,
			incremental: true,
		});
	}

	if (method !== 'GET') {
		throw errorResponse(405, 'Method not allowed');
	}

	if (query.action === 'priceAtDate') {
		if (!ticker || !date) {
			throw errorResponse(400, 'ticker and date query params are required');
		}
		return priceHistoryService.getPriceAtDate(ticker, date, {
			userId,
			portfolioId,
		});
	}

	if (query.action === 'averageCost') {
		if (!ticker) throw errorResponse(400, 'ticker query param is required');
		return priceHistoryService.getAverageCost(ticker, userId, {
			portfolioId,
			method: methodOption,
		});
	}

	if (query.action === 'chart') {
		if (!ticker) throw errorResponse(400, 'ticker query param is required');
		return priceHistoryService.getChartData(ticker, userId, chartType, period, {
			portfolioId,
			method: methodOption,
		});
	}

	// Default GET action: portfolio metrics.
	return priceHistoryService.getPortfolioMetrics(userId, {
		portfolioId,
		method: methodOption,
	});
}

async function handleDashboard(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return platformService.getDashboard(userId, {
		portfolioId,
		method: query.method || 'fifo',
		period: query.period || 'MAX',
	});
}

async function handleDividends(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return platformService.getDividendAnalytics(userId, {
		portfolioId,
		method: query.method || 'fifo',
		fromDate: query.fromDate || query.from_date || null,
		periodMonths: query.periodMonths || query.period_months || null,
	});
}

async function handleTax(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	const year = Number(query.year || new Date().getUTCFullYear());
	return platformService.getTaxReport(userId, year, { portfolioId });
}

async function handleRebalance(method, portfolioId, userId, body, query = {}, subId = null) {
	if (subId === 'suggestion') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		const amount = Number(query.amount || 0);
		const scope = String(query.scope || query.targetScope || 'assetClass');
		return platformService.getRebalancingSuggestion(userId, amount, { portfolioId, scope });
	}

	if (subId === 'targets') {
		if (method === 'POST') {
			return platformService.setRebalanceTargets(userId, parseBody(body), { portfolioId });
		}
		if (method === 'GET') {
			return platformService.getRebalanceTargets(userId, { portfolioId });
		}
		throw errorResponse(405, 'Method not allowed');
	}

	throw errorResponse(404, 'Rebalance route not found');
}

async function handleRisk(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return platformService.getPortfolioRisk(userId, {
		portfolioId,
		concentrationThreshold: query.concentrationThreshold || query.concentration_threshold,
	});
}

async function handleBenchmarks(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	const benchmark = query.benchmark || 'IBOV';
	const period = query.period || '1A';
	return platformService.getBenchmarkComparison(userId, benchmark, period, { portfolioId });
}

async function handleMultiCurrency(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	const period = query.period || '1Y';
	return platformService.getMultiCurrencyAnalytics(userId, period, {
		portfolioId,
		method: query.method || 'fifo',
	});
}

async function handleContributions(method, portfolioId, userId, body) {
	if (method === 'POST') {
		return platformService.recordContribution(userId, parseBody(body), { portfolioId });
	}
	if (method === 'GET') {
		return platformService.getContributionProgress(userId, { portfolioId });
	}
	throw errorResponse(405, 'Method not allowed');
}

async function handleAlerts(method, userId, body, id = null, query = {}) {
	if (method === 'GET') {
		return platformService.getAlerts(userId, { limit: query.limit });
	}
	if (method === 'POST') {
		if (query.action === 'evaluate') {
			const payload = parseBody(body);
			return platformService.evaluateAlerts(userId, payload.portfolioId || null, {});
		}
		return platformService.createAlertRule(userId, parseBody(body));
	}
	if (method === 'PUT') {
		if (!id) throw errorResponse(400, 'alert rule id is required');
		return platformService.updateAlertRule(userId, id, parseBody(body));
	}
	if (method === 'DELETE') {
		if (!id) throw errorResponse(400, 'alert rule id is required');
		return platformService.deleteAlertRule(userId, id);
	}
	throw errorResponse(405, 'Method not allowed');
}

async function handleGoals(method, userId, body, goalId = null) {
	if (method === 'GET') {
		if (!goalId) return platformService.listGoals(userId);
		return platformService.getGoalProgress(userId, goalId, {});
	}
	if (method === 'POST') {
		return platformService.createGoal(userId, parseBody(body));
	}
	if (method === 'PUT') {
		if (!goalId) throw errorResponse(400, 'goal id is required');
		return platformService.updateGoal(userId, goalId, parseBody(body));
	}
	if (method === 'DELETE') {
		if (!goalId) throw errorResponse(400, 'goal id is required');
		return platformService.deleteGoal(userId, goalId);
	}
	throw errorResponse(405, 'Method not allowed');
}

async function handleAssetTools(method, id, body, userId, query = {}) {
	if (id === 'screen') {
		if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
		const payload = parseBody(body);
		return platformService.screenAssets(payload, {
			userId,
			portfolioId: payload.portfolioId || query.portfolioId || null,
		});
	}
	if (id === 'compare') {
		if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
		const payload = parseBody(body);
		return platformService.compareAssets(payload.tickers || [], {
			userId,
			portfolioId: payload.portfolioId || query.portfolioId || null,
		});
	}
	if (!id) throw errorResponse(400, 'asset route id is required');
	if (query.action === 'details') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.getAssetDetails(id, {
			userId,
			portfolioId: query.portfolioId || null,
		});
	}
	if (query.action === 'events') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.fetchCorporateEvents(id, {
			portfolioId: query.portfolioId || null,
		});
	}
	if (query.action === 'news') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.fetchNews(id, {
			portfolioId: query.portfolioId || null,
		});
	}
	if (query.action === 'fii-updates') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.getFiiUpdates(id, {
			portfolioId: query.portfolioId || null,
		});
	}
	if (query.action === 'fii-emissions') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.getFiiEmissions(id, {
			portfolioId: query.portfolioId || null,
		});
	}
	if (query.action === 'financials') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.getAssetFinancialStatements(id, {
			userId,
			portfolioId: query.portfolioId || null,
		});
	}
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return platformService.getFairPrice(id, {
		userId,
		portfolioId: query.portfolioId || null,
	});
}

async function handleSimulate(method, body, userId) {
	if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
	const payload = parseBody(body);
	return platformService.simulate(
		Number(payload.monthlyAmount || payload.monthly_amount || 0),
		Number(payload.rate || 0),
		Number(payload.years || 0),
		{
			userId,
			ticker: payload.ticker || null,
			initialAmount: payload.initialAmount || payload.initial_amount || null,
			portfolioId: payload.portfolioId || null,
		}
	);
}

async function handleReports(method, id, userId, body, query = {}) {
	if (id === 'generate') {
		if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
		const payload = parseBody(body);
		return platformService.generatePDF(
			userId,
			payload.reportType || payload.report_type || 'portfolio',
			payload.period || null,
			{ portfolioId: payload.portfolioId || null }
		);
	}
	if (method === 'GET') {
		return platformService.listReports(userId);
	}
	throw errorResponse(405, 'Method not allowed');
}

async function handleJobs(method, id, subResource, userId, body, query = {}) {
	if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
	if (subResource !== 'refresh') throw errorResponse(404, 'Job route not found');

	if (id === 'economic-data') return platformService.fetchEconomicIndicators();
	if (id === 'corporate-events') {
		const payload = parseBody(body);
		return platformService.fetchCorporateEvents(payload.ticker || query.ticker || null, {
			portfolioId: payload.portfolioId || query.portfolioId || null,
		});
	}
	if (id === 'news') {
		const payload = parseBody(body);
		return platformService.fetchNews(payload.ticker || query.ticker || null, {
			portfolioId: payload.portfolioId || query.portfolioId || null,
		});
	}
	if (id === 'alerts') {
		const payload = parseBody(body);
		return platformService.evaluateAlerts(userId, payload.portfolioId || query.portfolioId || null, {});
	}

	throw errorResponse(404, 'Unknown job id');
}

async function handleFixedIncome(method, userId, body, query = {}) {
	if (method === 'POST') {
		return platformService.calculatePrivateFixedIncomePosition(parseBody(body));
	}
	if (method === 'GET') {
		return platformService.getFixedIncomeComparison(userId, {
			portfolioId: query.portfolioId || null,
			fromDate: query.fromDate || query.from_date || null,
			toDate: query.toDate || query.to_date || null,
		});
	}
	throw errorResponse(405, 'Method not allowed');
}

async function handleCosts(method, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return platformService.getCostAnalysis(userId, { portfolioId: query.portfolioId || null });
}

async function handleCommunity(method, id, userId, body, query = {}) {
	if (id === 'ideas') {
		if (method === 'GET') return platformService.listIdeas({ limit: query.limit });
		if (method === 'POST') return platformService.publishIdea(userId, parseBody(body));
		throw errorResponse(405, 'Method not allowed');
	}
	if (id === 'ranking') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		return platformService.getLeagueRanking({ userId });
	}
	throw errorResponse(404, 'Community route not found');
}

// --- Main Handler ---

exports.handler = async (event) => {
	let body;
	let statusCode = 200;
	const headers = {
		'Content-Type': 'application/json',
		'Access-Control-Allow-Origin': resolveCorsOrigin(event),
		'Access-Control-Allow-Credentials': 'false',
	};

	try {
		const {
			httpMethod,
			path,
			requestContext,
			body: requestBody,
			queryStringParameters,
		} = event;
		const claims = requestContext?.authorizer?.claims || {};
		const userId = claims?.sub || 'anonymous';
		const appRole = resolveAppRole(claims);

		const pathSegments = path.split('/').filter((s) => s);

		// Skip stage prefix if present
		let startIndex = 0;
		if (
			pathSegments[0] === 'prod' ||
			pathSegments[0] === 'dev' ||
			pathSegments[0] === 'staging'
		) {
			startIndex = 1;
		}

		const resourceBase = pathSegments[startIndex];
		const id = pathSegments[startIndex + 1];
		const subResource = pathSegments[startIndex + 2];
		const subId = pathSegments[startIndex + 3];
		const subSubResource = pathSegments[startIndex + 4];

		if (resourceBase === 'portfolios') {
			ensureAppAccess(appRole, 'EDITOR');

			if (!id) {
				body = await handlePortfolios(httpMethod, userId, requestBody);
			} else if (subResource === 'assets') {
				if (!subId) {
					body = await handleAssets(httpMethod, id, requestBody);
				} else {
					body = await handleAssetById(
						httpMethod,
						id,
						subId,
						requestBody
					);
				}
			} else if (subResource === 'import') {
				if (httpMethod === 'POST') {
					body = await handleImport(id, requestBody);
				} else {
					throw errorResponse(405, 'Method not allowed');
				}
			} else if (subResource === 'market-data') {
				if (subId === 'refresh') {
					body = await handleMarketDataRefresh(httpMethod, id, requestBody);
				} else {
					throw errorResponse(404, 'Market data route not found');
				}
			} else if (subResource === 'price-history') {
				body = await handlePriceHistory(
					httpMethod,
					id,
					userId,
					requestBody,
					queryStringParameters || {}
				);
			} else if (subResource === 'dashboard') {
				body = await handleDashboard(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'dividends') {
				body = await handleDividends(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'tax') {
				body = await handleTax(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'rebalance') {
				body = await handleRebalance(
					httpMethod,
					id,
					userId,
					requestBody,
					queryStringParameters || {},
					subId
				);
			} else if (subResource === 'risk') {
				body = await handleRisk(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'benchmarks') {
				body = await handleBenchmarks(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'multi-currency') {
				body = await handleMultiCurrency(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'contributions') {
				body = await handleContributions(httpMethod, id, userId, requestBody);
			} else if (subResource === 'transactions') {
				if (!subId) {
					body = await handleTransactions(
						httpMethod,
						id,
						requestBody
					);
				} else {
					body = await handleTransactionById(httpMethod, id, subId);
				}
			} else {
				body = await handlePortfolioById(
					httpMethod,
					userId,
					id,
					requestBody
				);
			}
		} else if (resourceBase === 'parsers') {
			if (httpMethod === 'GET') {
				const { listParsers } = require('../parsers/index');
				body = listParsers();
			} else {
				throw errorResponse(405, 'Method not allowed');
			}
		} else if (resourceBase === 'health') {
			if (id === 'scrapers') {
				body = await handleScraperHealth(httpMethod);
			} else {
				throw errorResponse(404, 'Health route not found');
			}
		} else if (resourceBase === 'assets') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleAssetTools(
				httpMethod,
				id,
				requestBody,
				userId,
				queryStringParameters || {}
			);
		} else if (resourceBase === 'users') {
			ensureAppAccess(appRole, 'EDITOR');
			if (id !== 'me') throw errorResponse(404, 'Users route not found');

			if (subResource === 'alerts') {
				body = await handleAlerts(
					httpMethod,
					userId,
					requestBody,
					subId || null,
					queryStringParameters || {}
				);
			} else if (subResource === 'goals') {
				const goalId = subSubResource === 'progress' ? subId : subId || null;
				body = await handleGoals(httpMethod, userId, requestBody, goalId);
			} else {
				throw errorResponse(404, 'Users route not found');
			}
		} else if (resourceBase === 'simulate') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleSimulate(httpMethod, requestBody, userId);
		} else if (resourceBase === 'reports') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleReports(
				httpMethod,
				id || null,
				userId,
				requestBody,
				queryStringParameters || {}
			);
		} else if (resourceBase === 'jobs') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleJobs(
				httpMethod,
				id || null,
				subResource || null,
				userId,
				requestBody,
				queryStringParameters || {}
			);
		} else if (resourceBase === 'fixed-income') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleFixedIncome(
				httpMethod,
				userId,
				requestBody,
				queryStringParameters || {}
			);
		} else if (resourceBase === 'costs') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleCosts(httpMethod, userId, queryStringParameters || {});
		} else if (resourceBase === 'community') {
			ensureAppAccess(appRole, 'EDITOR');
			body = await handleCommunity(
				httpMethod,
				id || null,
				userId,
				requestBody,
				queryStringParameters || {}
			);
		} else if (resourceBase === 'settings') {
			ensureAppAccess(appRole, 'EDITOR');
			const section = id;

			if (section === 'profile') {
				body = await handleSettingsProfile(
					httpMethod,
					userId,
					requestBody
				);
			} else if (section === 'dropdowns') {
				body = await handleSettingsDropdowns(
					httpMethod,
					userId,
					requestBody
				);
			} else if (section === 'aliases') {
				body = await handleAliasesList(httpMethod);
				if (httpMethod === 'POST' || httpMethod === 'PUT') {
					body = await handleAliases(httpMethod, requestBody);
				}
			} else {
				throw errorResponse(404, 'Settings route not found');
			}
		} else {
			throw errorResponse(404, 'Route not found');
		}
	} catch (err) {
		statusCode = err.statusCode || 500;
		body = {
			error: err.message || 'Internal server error',
		};
		const runtime = String(process.env.APP_ENV || '').toLowerCase();
		if (statusCode >= 500 && runtime === 'local') {
			body.details = {
				name: err?.name || 'Error',
				code: err?.code || null,
				message: err?.message || null,
			};
		}
	}

	return {
		statusCode,
		headers,
		body: JSON.stringify(body),
	};
};

// Export internals for testing
exports._test = {
	generateId,
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
};
