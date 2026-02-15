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

const ddbClient = new DynamoDBClient({
	region: process.env.AWS_REGION || 'us-east-1',
	...(process.env.DYNAMODB_ENDPOINT && {
		endpoint: process.env.DYNAMODB_ENDPOINT,
	}),
});
const dynamo = DynamoDBDocumentClient.from(ddbClient);

const TABLE_NAME = process.env.TABLE_NAME || 'wealth-main';
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
		const { httpMethod, path, requestContext, body: requestBody } = event;
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
				if (method === 'POST') {
					body = await handleImport(id, requestBody);
				} else {
					throw errorResponse(405, 'Method not allowed');
				}
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
			if (method === 'GET') {
				const { listParsers } = require('../parsers/index');
				body = listParsers();
			} else {
				throw errorResponse(405, 'Method not allowed');
			}
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
};
