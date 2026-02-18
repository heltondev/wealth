const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
	DynamoDBDocumentClient,
	QueryCommand,
	PutCommand,
	GetCommand,
	UpdateCommand,
	DeleteCommand,
	BatchWriteCommand,
	ScanCommand,
} = require('@aws-sdk/lib-dynamodb');
const { AssetMarketDataService } = require('../services/market-data');
const { MemoryCache } = require('../services/market-data/cache');
const { PortfolioPriceHistoryService } = require('../services/price-history');
const { PlatformService } = require('../services/platform');
const { ApiResponseCache } = require('../services/cache/api-response-cache');
const {
	buildAwsClientConfig,
	resolveTableName,
} = require('../config/aws');

const TABLE_NAME = resolveTableName();
const ddbClient = new DynamoDBClient(
	buildAwsClientConfig({ service: 'dynamodb' })
);
const dynamo = DynamoDBDocumentClient.from(ddbClient);
const scraperCache = new MemoryCache(
	Number(process.env.MARKET_DATA_SCRAPER_CACHE_TTL_MS || 15 * 60 * 1000)
);
const marketDataService = new AssetMarketDataService({
	dynamo,
	tableName: TABLE_NAME,
	logger: console,
	cache: scraperCache,
	cacheTtlMs: Number(process.env.MARKET_DATA_SCRAPER_CACHE_TTL_MS || 15 * 60 * 1000),
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
const BACKUP_SCHEMA_VERSION = 1;
const MUTATION_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);
const RESPONSE_CACHE_DEFAULT_TTL_MS = Number(
	process.env.API_RESPONSE_CACHE_TTL_MS || 30 * 1000
);
const RESPONSE_CACHE_MAX_ENTRIES = Number(
	process.env.API_RESPONSE_CACHE_MAX_ENTRIES || 800
);
const RESPONSE_CACHE_MAX_BODY_BYTES = Number(
	process.env.API_RESPONSE_CACHE_MAX_BODY_BYTES || 1024 * 1024
);
const responseCache = new ApiResponseCache({
	defaultTtlMs: RESPONSE_CACHE_DEFAULT_TTL_MS,
	maxEntries: RESPONSE_CACHE_MAX_ENTRIES,
	maxBodyBytes: RESPONSE_CACHE_MAX_BODY_BYTES,
});

const THESIS_SUPPORTED_COUNTRIES = ['BR', 'US', 'CA'];
const THESIS_SUPPORTED_ASSET_CLASSES = [
	'FII',
	'TESOURO',
	'ETF',
	'STOCK',
	'REIT',
	'BOND',
	'CRYPTO',
	'CASH',
	'RSU',
];

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

const normalizeThesisCountry = (value) => {
	const normalized = String(value || '').trim().toUpperCase();
	if (!THESIS_SUPPORTED_COUNTRIES.includes(normalized)) {
		throw errorResponse(
			400,
			`country must be one of: ${THESIS_SUPPORTED_COUNTRIES.join(', ')}`
		);
	}
	return normalized;
};

const normalizeThesisAssetClass = (value) => {
	const normalized = String(value || '').trim().toUpperCase();
	if (!THESIS_SUPPORTED_ASSET_CLASSES.includes(normalized)) {
		throw errorResponse(
			400,
			`assetClass must be one of: ${THESIS_SUPPORTED_ASSET_CLASSES.join(', ')}`
		);
	}
	return normalized;
};

const buildThesisScopeKey = (country, assetClass) =>
	`${normalizeThesisCountry(country)}:${normalizeThesisAssetClass(assetClass)}`;

const parseThesisScopeKey = (scopeKey) => {
	const normalized = String(scopeKey || '').trim().toUpperCase();
	if (!normalized.includes(':')) {
		throw errorResponse(400, 'scopeKey must follow COUNTRY:ASSETCLASS');
	}
	const [countryPart, assetClassPart, ...rest] = normalized.split(':');
	if (rest.length > 0 || !countryPart || !assetClassPart) {
		throw errorResponse(400, 'scopeKey must follow COUNTRY:ASSETCLASS');
	}
	const country = normalizeThesisCountry(countryPart);
	const assetClass = normalizeThesisAssetClass(assetClassPart);
	return {
		scopeKey: `${country}:${assetClass}`,
		country,
		assetClass,
	};
};

const parsePercentageValue = (value, fieldName) => {
	if (value === undefined || value === null || value === '') return null;
	const numeric = Number(value);
	if (!Number.isFinite(numeric) || numeric < 0 || numeric > 100) {
		throw errorResponse(400, `${fieldName} must be a number between 0 and 100`);
	}
	return Number(numeric.toFixed(4));
};

const toVersionToken = (version) => String(version).padStart(6, '0');

const thesisItemToResponse = (item) => ({
	thesisId: item?.thesisId || null,
	portfolioId: item?.portfolioId || null,
	scopeKey: item?.scopeKey || null,
	country: item?.country || null,
	assetClass: item?.assetClass || null,
	title: item?.title || '',
	thesisText: item?.thesisText || '',
	targetAllocation: item?.targetAllocation ?? null,
	minAllocation: item?.minAllocation ?? null,
	maxAllocation: item?.maxAllocation ?? null,
	triggers: item?.triggers || '',
	actionPlan: item?.actionPlan || '',
	riskNotes: item?.riskNotes || '',
	status: item?.status || 'active',
	version: Number(item?.version || 1),
	createdAt: item?.createdAt || null,
	updatedAt: item?.updatedAt || null,
	archivedAt: item?.archivedAt || null,
});

const extractLatestThesisPerScope = (items) => {
	const latestByScope = new Map();
	for (const item of items || []) {
		if (!item || String(item.entityType || '') !== 'thesis') continue;
		const scopeKey = String(item.scopeKey || '').trim().toUpperCase();
		if (!scopeKey) continue;
		const version = Number(item.version || 0);
		const existing = latestByScope.get(scopeKey);
		if (!existing || version > Number(existing.version || 0)) {
			latestByScope.set(scopeKey, item);
		}
	}
	return Array.from(latestByScope.values()).sort((left, right) =>
		String(left.scopeKey || '').localeCompare(String(right.scopeKey || ''))
	);
};

const validateThesisAllocations = ({ minAllocation, targetAllocation, maxAllocation }) => {
	if (minAllocation !== null && maxAllocation !== null && minAllocation > maxAllocation) {
		throw errorResponse(400, 'minAllocation must be less than or equal to maxAllocation');
	}
	if (
		targetAllocation !== null
		&& minAllocation !== null
		&& targetAllocation < minAllocation
	) {
		throw errorResponse(400, 'targetAllocation must be greater than or equal to minAllocation');
	}
	if (
		targetAllocation !== null
		&& maxAllocation !== null
		&& targetAllocation > maxAllocation
	) {
		throw errorResponse(400, 'targetAllocation must be less than or equal to maxAllocation');
	}
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

const normalizeQueryEntries = (query = {}) =>
	Object.entries(query || {})
		.filter(([rawKey, rawValue]) => {
			if (!rawKey) return false;
			return rawValue !== undefined && rawValue !== null && rawValue !== '';
		})
		.map(([rawKey, rawValue]) => [String(rawKey).trim(), String(rawValue).trim()])
		.filter(([key, value]) => Boolean(key) && value !== '')
		.sort((left, right) => left[0].localeCompare(right[0]));

const buildResponseCacheKey = ({ userId, appRole, method, path, query }) => {
	const normalizedQuery = normalizeQueryEntries(query)
		.map(([key, value]) =>
			`${encodeURIComponent(key)}=${encodeURIComponent(value)}`
		)
		.join('&');
	return [
		`u:${String(userId || 'anonymous')}`,
		`r:${String(appRole || 'VIEWER')}`,
		`m:${String(method || 'GET').toUpperCase()}`,
		`p:${String(path || '/')}`,
		`q:${normalizedQuery}`,
	].join('|');
};

const isTruthyValue = (value) => {
	const normalized = String(value || '').trim().toLowerCase();
	return ['1', 'true', 'yes', 'on'].includes(normalized);
};

const shouldBypassResponseCache = (query = {}) =>
	isTruthyValue(query.noCache)
	|| isTruthyValue(query.nocache)
	|| isTruthyValue(query.bypassCache)
	|| isTruthyValue(query.bypasscache);

const isCacheableGetRoute = (
	resourceBase,
	id,
	subResource,
	subId,
	query = {}
) => {
	if (resourceBase === 'health') return false;
	if (resourceBase === 'parsers') return false;
	if (resourceBase === 'jobs') return false;
	if (resourceBase === 'settings' && id === 'backup') return false;
	if (resourceBase === 'settings' && id === 'cache') return false;
	if (resourceBase === 'reports' && String(query.action || '').toLowerCase() === 'content') {
		return false;
	}
	if (resourceBase === 'portfolios' && subResource === 'event-inbox' && subId === 'sync') {
		return false;
	}
	return true;
};

const resolveRouteResponseCacheTtlMs = (
	resourceBase,
	id,
	subResource
) => {
	if (resourceBase === 'settings' && id === 'dropdowns') return 10 * 60 * 1000;
	if (resourceBase === 'settings' && id === 'profile') return 2 * 60 * 1000;
	if (resourceBase === 'portfolios' && subResource === 'event-inbox') return 10 * 1000;
	if (resourceBase === 'portfolios' && subResource === 'event-notices') return 15 * 1000;
	if (
		resourceBase === 'portfolios'
		&& ['dashboard', 'price-history', 'dividends', 'risk', 'benchmarks', 'multi-currency', 'rebalance'].includes(subResource)
	) {
		return 45 * 1000;
	}
	if (resourceBase === 'portfolios' && ['assets', 'transactions', 'tax', 'theses'].includes(subResource)) {
		return 30 * 1000;
	}
	if (resourceBase === 'portfolios' && !subResource) return 30 * 1000;
	return RESPONSE_CACHE_DEFAULT_TTL_MS;
};

const toCacheDiagnosticsResponse = () => {
	const responseStats = responseCache.stats();
	const scraperStats = typeof scraperCache.stats === 'function'
		? scraperCache.stats()
		: {
			entries: scraperCache.items?.size || 0,
			defaultTtlMs: scraperCache.defaultTtlMs || 0,
		};
	const responseRequests = responseStats.hitCount + responseStats.missCount;
	const responseHitRatePct = responseRequests > 0
		? Number(((responseStats.hitCount / responseRequests) * 100).toFixed(2))
		: 0;
	const scraperRequests = Number(scraperStats.hitCount || 0) + Number(scraperStats.missCount || 0);
	const scraperHitRatePct = scraperRequests > 0
		? Number(((Number(scraperStats.hitCount || 0) / scraperRequests) * 100).toFixed(2))
		: 0;

	return {
		responseCache: {
			...responseStats,
			requests: responseRequests,
			hitRatePct: responseHitRatePct,
		},
		scraperCache: {
			...scraperStats,
			requests: scraperRequests,
			hitRatePct: scraperHitRatePct,
		},
		fetchedAt: new Date().toISOString(),
	};
};

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

const chunkArray = (items = [], size = 25) => {
	const chunks = [];
	for (let index = 0; index < items.length; index += size) {
		chunks.push(items.slice(index, index + size));
	}
	return chunks;
};

const isPlainObject = (value) =>
	Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const dedupeItemsByPrimaryKey = (items = []) => {
	const byKey = new Map();
	for (const item of items) {
		if (!isPlainObject(item)) continue;
		const pk = String(item.PK || '').trim();
		const sk = String(item.SK || '').trim();
		if (!pk || !sk) continue;
		byKey.set(`${pk}||${sk}`, {
			...item,
			PK: pk,
			SK: sk,
		});
	}
	return Array.from(byKey.values());
};

const derivePortfolioId = (item) => {
	if (!isPlainObject(item)) return null;
	const direct = String(item.portfolioId || '').trim();
	if (direct) return direct;
	const pk = String(item.PK || '').trim();
	if (pk.startsWith('PORTFOLIO#')) {
		const fromPk = pk.slice('PORTFOLIO#'.length).trim();
		if (fromPk) return fromPk;
	}
	const sk = String(item.SK || '').trim();
	if (sk.startsWith('PORTFOLIO#')) {
		const fromSk = sk.slice('PORTFOLIO#'.length).trim();
		if (fromSk) return fromSk;
	}
	return null;
};

const normalizeAliasItem = (item, now) => {
	if (!isPlainObject(item)) return null;
	const fromPk = String(item.PK || '').startsWith('ALIAS#')
		? String(item.PK).slice('ALIAS#'.length)
		: '';
	const fromSk = String(item.SK || '').startsWith('TICKER#')
		? String(item.SK).slice('TICKER#'.length)
		: '';
	const normalizedName = String(item.normalizedName || fromPk || '')
		.trim()
		.toLowerCase();
	const ticker = String(item.ticker || fromSk || '')
		.trim()
		.toUpperCase();
	if (!normalizedName || !ticker) return null;
	return {
		...item,
		PK: `ALIAS#${normalizedName}`,
		SK: `TICKER#${ticker}`,
		normalizedName,
		ticker,
		source: String(item.source || 'manual'),
		createdAt: item.createdAt || now,
	};
};

const normalizeUserItem = (item, userId) => {
	if (!isPlainObject(item)) return null;
	const sk = String(item.SK || '').trim();
	if (!sk) return null;
	const normalized = {
		...item,
		PK: `USER#${userId}`,
		SK: sk,
	};
	const portfolioId = derivePortfolioId(item);
	if (sk.startsWith('PORTFOLIO#')) {
		const fromSk = sk.slice('PORTFOLIO#'.length).trim();
		normalized.portfolioId = fromSk || portfolioId;
	}
	if (Object.prototype.hasOwnProperty.call(normalized, 'userId')) {
		normalized.userId = userId;
	}
	return normalized;
};

const normalizePortfolioItem = (item, userId) => {
	if (!isPlainObject(item)) return null;
	const portfolioId = derivePortfolioId(item);
	const sk = String(item.SK || '').trim();
	if (!portfolioId || !sk) return null;
	const normalized = {
		...item,
		PK: `PORTFOLIO#${portfolioId}`,
		SK: sk,
		portfolioId,
	};
	if (Object.prototype.hasOwnProperty.call(normalized, 'userId')) {
		normalized.userId = userId;
	}
	return normalized;
};

const deleteItemsByPrimaryKey = async (items = []) => {
	const requests = [];
	for (const item of items) {
		const pk = String(item.PK || '').trim();
		const sk = String(item.SK || '').trim();
		if (!pk || !sk) continue;
		requests.push({
			DeleteRequest: {
				Key: { PK: pk, SK: sk },
			},
		});
	}
	if (requests.length === 0) return;

	for (const batch of chunkArray(requests, 25)) {
		let pending = batch;
		let attempts = 0;
		while (pending.length > 0) {
			attempts += 1;
			const response = await dynamo.send(
				new BatchWriteCommand({
					RequestItems: {
						[TABLE_NAME]: pending,
					},
				})
			);
			const unprocessed = response?.UnprocessedItems?.[TABLE_NAME] || [];
			if (!Array.isArray(unprocessed) || unprocessed.length === 0) break;
			if (attempts >= 8) {
				throw errorResponse(500, 'Failed to delete all backup records after retries');
			}
			pending = unprocessed;
			await new Promise((resolve) => setTimeout(resolve, attempts * 60));
		}
	}
};

const putItemsByPrimaryKey = async (items = []) => {
	const requests = [];
	for (const item of items) {
		const pk = String(item.PK || '').trim();
		const sk = String(item.SK || '').trim();
		if (!pk || !sk) continue;
		requests.push({
			PutRequest: {
				Item: {
					...item,
					PK: pk,
					SK: sk,
				},
			},
		});
	}
	if (requests.length === 0) return;

	for (const batch of chunkArray(requests, 25)) {
		let pending = batch;
		let attempts = 0;
		while (pending.length > 0) {
			attempts += 1;
			const response = await dynamo.send(
				new BatchWriteCommand({
					RequestItems: {
						[TABLE_NAME]: pending,
					},
				})
			);
			const unprocessed = response?.UnprocessedItems?.[TABLE_NAME] || [];
			if (!Array.isArray(unprocessed) || unprocessed.length === 0) break;
			if (attempts >= 8) {
				throw errorResponse(500, 'Failed to import all backup records after retries');
			}
			pending = unprocessed;
			await new Promise((resolve) => setTimeout(resolve, attempts * 60));
		}
	}
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

async function handleSettingsBackup(method, userId, body) {
	if (method === 'GET') {
		const userItems = await queryAllItems({
			TableName: TABLE_NAME,
			KeyConditionExpression: 'PK = :pk',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
			},
		});

		const portfolioIds = Array.from(
			new Set(
				(userItems || [])
					.map((item) => derivePortfolioId(item))
					.filter(Boolean)
			)
		);

		const portfolioGroups = await Promise.all(
			portfolioIds.map((portfolioId) =>
				queryAllItems({
					TableName: TABLE_NAME,
					KeyConditionExpression: 'PK = :pk',
					ExpressionAttributeValues: {
						':pk': `PORTFOLIO#${portfolioId}`,
					},
				})
			)
		);
		const portfolioItems = portfolioGroups.flat();

		const aliases = await scanAllItems({
			TableName: TABLE_NAME,
			FilterExpression: 'begins_with(PK, :prefix)',
			ExpressionAttributeValues: {
				':prefix': 'ALIAS#',
			},
		});

		const exportedAt = new Date().toISOString();
		return {
			schemaVersion: BACKUP_SCHEMA_VERSION,
			exportedAt,
			exportedBy: userId,
			data: {
				userItems,
				portfolioItems,
				aliases,
			},
			stats: {
				userItems: userItems.length,
				portfolios: portfolioIds.length,
				portfolioItems: portfolioItems.length,
				aliases: aliases.length,
				totalItems: userItems.length + portfolioItems.length + aliases.length,
			},
		};
	}

	if (method === 'POST') {
		const payload = parseBody(body);
		const mode = String(payload.mode || '').toLowerCase() === 'merge'
			? 'merge'
			: 'replace';
		const backupEnvelope = isPlainObject(payload.backup)
			? payload.backup
			: isPlainObject(payload.payload)
				? payload.payload
				: payload;
		const backupData = isPlainObject(backupEnvelope.data)
			? backupEnvelope.data
			: backupEnvelope;

		const rawUserItems = Array.isArray(backupData.userItems) ? backupData.userItems : [];
		const rawPortfolioItems = Array.isArray(backupData.portfolioItems)
			? backupData.portfolioItems
			: [];
		const rawAliases = Array.isArray(backupData.aliases) ? backupData.aliases : [];

		if (rawUserItems.length === 0 && rawPortfolioItems.length === 0 && rawAliases.length === 0) {
			throw errorResponse(
				400,
				'Backup payload is empty. Expected data.userItems, data.portfolioItems, or data.aliases.'
			);
		}

		const now = new Date().toISOString();
		const normalizedUserItems = dedupeItemsByPrimaryKey(
			rawUserItems
				.map((item) => normalizeUserItem(item, userId))
				.filter(Boolean)
		);
		const normalizedPortfolioItems = dedupeItemsByPrimaryKey(
			rawPortfolioItems
				.map((item) => normalizePortfolioItem(item, userId))
				.filter(Boolean)
		);
		const normalizedAliases = dedupeItemsByPrimaryKey(
			rawAliases
				.map((item) => normalizeAliasItem(item, now))
				.filter(Boolean)
		);

		const portfolioIdsInItems = new Set(
			normalizedPortfolioItems
				.map((item) => derivePortfolioId(item))
				.filter(Boolean)
		);
		const userPortfolioRecordIds = new Set(
			normalizedUserItems
				.filter((item) => String(item.SK || '').startsWith('PORTFOLIO#'))
				.map((item) => derivePortfolioId(item))
				.filter(Boolean)
		);

		for (const portfolioId of portfolioIdsInItems) {
			if (userPortfolioRecordIds.has(portfolioId)) continue;
			normalizedUserItems.push({
				PK: `USER#${userId}`,
				SK: `PORTFOLIO#${portfolioId}`,
				portfolioId,
				name: portfolioId,
				description: '',
				baseCurrency: 'BRL',
				createdAt: now,
				updatedAt: now,
			});
		}

		const dedupedUserItems = dedupeItemsByPrimaryKey(normalizedUserItems);
		const allItemsToWrite = [
			...dedupedUserItems,
			...normalizedPortfolioItems,
			...normalizedAliases,
		];

		if (mode === 'replace') {
			const existingUserItems = await queryAllItems({
				TableName: TABLE_NAME,
				KeyConditionExpression: 'PK = :pk',
				ExpressionAttributeValues: {
					':pk': `USER#${userId}`,
				},
			});
			const existingPortfolioIds = Array.from(
				new Set(
					(existingUserItems || [])
						.map((item) => derivePortfolioId(item))
						.filter(Boolean)
				)
			);
			const existingPortfolioGroups = await Promise.all(
				existingPortfolioIds.map((portfolioId) =>
					queryAllItems({
						TableName: TABLE_NAME,
						KeyConditionExpression: 'PK = :pk',
						ExpressionAttributeValues: {
							':pk': `PORTFOLIO#${portfolioId}`,
						},
					})
				)
			);
			const existingPortfolioItems = existingPortfolioGroups.flat();
			const existingAliases = await scanAllItems({
				TableName: TABLE_NAME,
				FilterExpression: 'begins_with(PK, :prefix)',
				ExpressionAttributeValues: {
					':prefix': 'ALIAS#',
				},
			});

			await deleteItemsByPrimaryKey([
				...existingPortfolioItems,
				...existingUserItems,
				...existingAliases,
			]);
		}

		await putItemsByPrimaryKey(allItemsToWrite);

		return {
			mode,
			importedAt: now,
			stats: {
				userItems: dedupedUserItems.length,
				portfolioItems: normalizedPortfolioItems.length,
				aliases: normalizedAliases.length,
				totalItems: allItemsToWrite.length,
			},
		};
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleSettingsCache(method, userId, body) {
	void userId;

	if (method === 'GET') {
		return toCacheDiagnosticsResponse();
	}

	if (method === 'POST' || method === 'DELETE') {
		const payload = parseBody(body);
		const action = String(payload.action || '').trim().toLowerCase();
		if (method === 'POST' && action && action !== 'clear') {
			throw errorResponse(400, 'Only action=clear is supported');
		}

		const scope = String(payload.scope || 'all').trim().toLowerCase();
		if (!['all', 'response', 'scraper'].includes(scope)) {
			throw errorResponse(400, 'scope must be one of: all, response, scraper');
		}

		const clearedResponseCache = scope === 'all' || scope === 'response';
		const clearedScraperCache = scope === 'all' || scope === 'scraper';

		if (clearedResponseCache) {
			responseCache.clear();
		}
		if (clearedScraperCache && typeof scraperCache.clear === 'function') {
			scraperCache.clear();
		}

		return {
			cleared: {
				responseCache: clearedResponseCache,
				scraperCache: clearedScraperCache,
			},
			...toCacheDiagnosticsResponse(),
		};
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
	const payload = parseBody(body);
	const { fileName, parserId, fileContentBase64 } = payload;
	const dryRun = Boolean(
		payload?.dryRun
		|| payload?.previewOnly
		|| String(payload?.mode || '').toLowerCase() === 'preview'
	);
	if (!fileName && !fileContentBase64) {
		throw errorResponse(400, 'fileName or fileContentBase64 is required');
	}

	const { detectProvider, detectProviderFromWorkbook, getParser } = require('../parsers/index');
	const { importParsedB3 } = require('../services/import/b3-import-service');
	const XLSX = require('xlsx');
	const path = require('path');

	const safeFileName = path.basename(fileName || 'b3-upload.xlsx');
	let parser;
	let workbook;
	let detectionMode = parserId ? 'manual' : 'auto';

	if (fileContentBase64) {
		let fileBuffer;
		try {
			fileBuffer = Buffer.from(fileContentBase64, 'base64');
		} catch {
			throw errorResponse(400, 'Invalid fileContentBase64');
		}
		if (!fileBuffer || fileBuffer.length === 0) {
			throw errorResponse(400, 'fileContentBase64 is empty');
		}

		try {
			workbook = XLSX.read(fileBuffer, { type: 'buffer' });
		} catch {
			throw errorResponse(400, 'Invalid XLSX payload');
		}

		if (parserId) {
			parser = getParser(parserId);
			if (!parser) throw errorResponse(400, `Unknown parser: ${parserId}`);
		} else {
			const detected = detectProviderFromWorkbook(safeFileName, workbook);
			if (!detected) {
				if (/\.csv$/i.test(safeFileName)) {
					parser = getParser('robinhood-activity');
					if (!parser) throw errorResponse(400, 'Could not detect file format');
					detectionMode = 'auto_csv_fallback';
				} else {
					throw errorResponse(400, 'Could not detect file format');
				}
			} else {
				parser = detected.parser;
				workbook = detected.workbook;
			}
		}
	} else {
		if (parserId) {
			parser = getParser(parserId);
			if (!parser) throw errorResponse(400, `Unknown parser: ${parserId}`);
			workbook = XLSX.readFile(fileName);
		} else {
			const detected = detectProvider(fileName);
			if (!detected) {
				if (/\.csv$/i.test(safeFileName)) {
					parser = getParser('robinhood-activity');
					if (!parser) throw errorResponse(400, 'Could not detect file format');
					workbook = XLSX.readFile(fileName);
					detectionMode = 'auto_csv_fallback';
				} else {
					throw errorResponse(400, 'Could not detect file format');
				}
			} else {
				parser = detected.parser;
				workbook = detected.workbook;
			}
		}
	}

	const parsed = parser.parse(workbook, { sourceFile: safeFileName });
	return importParsedB3({
		dynamo,
		tableName: TABLE_NAME,
		portfolioId,
		parser,
		parsed,
		sourceFile: safeFileName,
		detectionMode,
		dryRun,
	});
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

async function handleEventNotices(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	return platformService.getPortfolioEventNotices(userId, {
		portfolioId,
		lookaheadDays: query.lookaheadDays || query.lookahead_days || 7,
	});
}

async function handleEventInbox(method, portfolioId, userId, body, query = {}, subId = null) {
	if (method === 'GET') {
		return platformService.getPortfolioEventInbox(userId, {
			portfolioId,
			lookaheadDays: query.lookaheadDays || query.lookahead_days || 7,
			status: query.status || 'all',
			severity: query.severity || null,
			limit: query.limit || 200,
			sync: String(query.sync || '').toLowerCase() === 'true',
			refreshSources: String(query.refreshSources || query.refresh_sources || '').toLowerCase() === 'true',
		});
	}

	if (method === 'POST') {
		const payload = parseBody(body);
		if (subId === 'sync') {
			return platformService.syncPortfolioEventInbox(userId, {
				portfolioId,
				lookaheadDays:
					payload.lookaheadDays
					|| payload.lookahead_days
					|| query.lookaheadDays
					|| query.lookahead_days
					|| 7,
				includePastDays:
					payload.includePastDays
					|| payload.include_past_days
					|| query.includePastDays
					|| query.include_past_days
					|| null,
				refreshSources:
					payload.refreshSources === false || payload.refresh_sources === false
						? false
						: true,
				pruneDaysPast:
					payload.pruneDaysPast
					|| payload.prune_days_past
					|| query.pruneDaysPast
					|| query.prune_days_past
					|| null,
			});
		}

		if (subId === 'read') {
			return platformService.markAllPortfolioEventInboxRead(userId, {
				portfolioId,
				read: payload.read !== false,
				scope: payload.scope || query.scope || 'all',
				lookaheadDays: payload.lookaheadDays || query.lookaheadDays || 7,
			});
		}

		throw errorResponse(404, 'Event inbox route not found');
	}

	if (method === 'PUT') {
		if (!subId) throw errorResponse(400, 'event inbox id is required');
		const payload = parseBody(body);
		const result = await platformService.setPortfolioEventInboxRead(userId, {
			portfolioId,
			eventId: subId,
			read: payload.read !== false,
		});
		if (!result) throw errorResponse(404, 'Event inbox item not found');
		return result;
	}

	throw errorResponse(405, 'Method not allowed');
}

async function handleTax(method, portfolioId, userId, query = {}) {
	if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
	const year = Number(query.year || new Date().getUTCFullYear());
	return platformService.getTaxReport(userId, year, { portfolioId });
}

function resolveQueryValue(query = {}, keys = []) {
	if (!query || typeof query !== 'object') return null;
	for (const key of keys) {
		if (!Object.prototype.hasOwnProperty.call(query, key)) continue;
		const value = query[key];
		if (value === undefined || value === null) continue;
		return value;
	}
	for (const [rawKey, value] of Object.entries(query)) {
		const normalizedKey = String(rawKey || '').trim().toLowerCase();
		if (!normalizedKey) continue;
		if (!keys.some((key) => normalizedKey === String(key || '').trim().toLowerCase())) continue;
		if (value === undefined || value === null) continue;
		return value;
	}
	return null;
}

async function handleRebalance(method, portfolioId, userId, body, query = {}, subId = null) {
	if (subId === 'suggestion') {
		if (method !== 'GET') throw errorResponse(405, 'Method not allowed');
		const amount = Number(resolveQueryValue(query, ['amount']) || 0);
		const scope = String(resolveQueryValue(query, ['scope', 'targetScope', 'target_scope']) || 'assetClass');
		const amountBrl = resolveQueryValue(query, ['amountBrl', 'amount_brl', 'amountbrl']);
		const amountUsd = resolveQueryValue(query, ['amountUsd', 'amount_usd', 'amountusd']);
		return platformService.getRebalancingSuggestion(userId, amount, {
			portfolioId,
			scope,
			amountBrl,
			amountUsd,
		});
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

async function listPortfolioThesisItems(portfolioId) {
	return queryAllItems({
		TableName: TABLE_NAME,
		KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
		ExpressionAttributeValues: {
			':pk': `PORTFOLIO#${portfolioId}`,
			':sk': 'THESIS#',
		},
	});
}

async function listScopeThesisVersions(portfolioId, scopeKey) {
	return queryAllItems({
		TableName: TABLE_NAME,
		KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
		ExpressionAttributeValues: {
			':pk': `PORTFOLIO#${portfolioId}`,
			':sk': `THESIS#${scopeKey}#V#`,
		},
	});
}

async function archiveThesisItem(portfolioId, item, now) {
	if (!item) return null;
	const key = {
		PK: `PORTFOLIO#${portfolioId}`,
		SK: item.SK,
	};
	const result = await dynamo.send(
		new UpdateCommand({
			TableName: TABLE_NAME,
			Key: key,
			UpdateExpression: 'SET #status = :status, archivedAt = :archivedAt, updatedAt = :updatedAt',
			ExpressionAttributeNames: {
				'#status': 'status',
			},
			ExpressionAttributeValues: {
				':status': 'archived',
				':archivedAt': now,
				':updatedAt': now,
			},
			ReturnValues: 'ALL_NEW',
		})
	);
	return result.Attributes || null;
}

async function handleTheses(method, portfolioId, body, query = {}, subId = null) {
	if (method === 'GET') {
		if (!subId) {
			const includeHistory = String(query.includeHistory || '').toLowerCase() === 'true';
			const items = await listPortfolioThesisItems(portfolioId);
			const latestItems = extractLatestThesisPerScope(items);
			const payload = {
				portfolioId,
				taxonomy: {
					countries: THESIS_SUPPORTED_COUNTRIES,
					assetClasses: THESIS_SUPPORTED_ASSET_CLASSES,
				},
				items: latestItems.map(thesisItemToResponse),
			};
			if (includeHistory) {
				payload.history = (items || [])
					.filter((item) => String(item.entityType || '') === 'thesis')
					.sort((left, right) => {
						const scopeDiff = String(left.scopeKey || '').localeCompare(
							String(right.scopeKey || '')
						);
						if (scopeDiff !== 0) return scopeDiff;
						return Number(right.version || 0) - Number(left.version || 0);
					})
					.map(thesisItemToResponse);
			}
			return payload;
		}

		const parsedScope = parseThesisScopeKey(decodeURIComponent(subId));
		const versions = await listScopeThesisVersions(portfolioId, parsedScope.scopeKey);
		if (!versions || versions.length === 0) {
			throw errorResponse(404, 'Thesis scope not found');
		}
		const sorted = [...versions]
			.filter((item) => String(item.entityType || '') === 'thesis')
			.sort((left, right) => Number(right.version || 0) - Number(left.version || 0));
		return {
			portfolioId,
			scopeKey: parsedScope.scopeKey,
			current: thesisItemToResponse(sorted[0] || null),
			history: sorted.map(thesisItemToResponse),
		};
	}

	if (method === 'POST') {
		const payload = parseBody(body);
		const parsedScope = payload.scopeKey
			? parseThesisScopeKey(payload.scopeKey)
			: parseThesisScopeKey(
				buildThesisScopeKey(payload.country || '', payload.assetClass || '')
			);

		const title = String(payload.title || '').trim();
		const thesisText = String(payload.thesisText || '').trim();
		const triggers = String(payload.triggers || '').trim();
		const actionPlan = String(payload.actionPlan || '').trim();
		const riskNotes = String(payload.riskNotes || '').trim();
		const targetAllocation = parsePercentageValue(payload.targetAllocation, 'targetAllocation');
		const minAllocation = parsePercentageValue(payload.minAllocation, 'minAllocation');
		const maxAllocation = parsePercentageValue(payload.maxAllocation, 'maxAllocation');

		if (!title) throw errorResponse(400, 'title is required');
		if (!thesisText) throw errorResponse(400, 'thesisText is required');

		validateThesisAllocations({
			targetAllocation,
			minAllocation,
			maxAllocation,
		});

		const existingVersions = await listScopeThesisVersions(
			portfolioId,
			parsedScope.scopeKey
		);
		const sortedExisting = [...existingVersions]
			.filter((item) => String(item.entityType || '') === 'thesis')
			.sort((left, right) => Number(right.version || 0) - Number(left.version || 0));
		const currentActive = sortedExisting.find(
			(item) => String(item.status || '').toLowerCase() === 'active'
		);
		const latestVersion = sortedExisting.length > 0
			? Math.max(...sortedExisting.map((item) => Number(item.version || 0)))
			: 0;
		const nextVersion = latestVersion + 1;
		const now = new Date().toISOString();

		let archivedPrevious = null;
		if (currentActive) {
			archivedPrevious = await archiveThesisItem(portfolioId, currentActive, now);
		}

		const thesisItem = {
			PK: `PORTFOLIO#${portfolioId}`,
			SK: `THESIS#${parsedScope.scopeKey}#V#${toVersionToken(nextVersion)}`,
			entityType: 'thesis',
			thesisId: `thesis-${parsedScope.scopeKey}-${nextVersion}`,
			portfolioId,
			scopeKey: parsedScope.scopeKey,
			country: parsedScope.country,
			assetClass: parsedScope.assetClass,
			title,
			thesisText,
			targetAllocation,
			minAllocation,
			maxAllocation,
			triggers,
			actionPlan,
			riskNotes,
			status: 'active',
			version: nextVersion,
			createdAt: now,
			updatedAt: now,
			archivedAt: null,
		};

		await dynamo.send(
			new PutCommand({
				TableName: TABLE_NAME,
				Item: thesisItem,
			})
		);

		return {
			portfolioId,
			thesis: thesisItemToResponse(thesisItem),
			previous: thesisItemToResponse(archivedPrevious),
		};
	}

	if (method === 'DELETE') {
		if (!subId) throw errorResponse(400, 'scopeKey is required');
		const parsedScope = parseThesisScopeKey(decodeURIComponent(subId));
		const versions = await listScopeThesisVersions(portfolioId, parsedScope.scopeKey);
		const currentActive = [...versions]
			.filter((item) => String(item.entityType || '') === 'thesis')
			.filter((item) => String(item.status || '').toLowerCase() === 'active')
			.sort((left, right) => Number(right.version || 0) - Number(left.version || 0))[0];
		if (!currentActive) throw errorResponse(404, 'Active thesis not found for scope');

		const now = new Date().toISOString();
		const archived = await archiveThesisItem(portfolioId, currentActive, now);
		return {
			portfolioId,
			scopeKey: parsedScope.scopeKey,
			thesis: thesisItemToResponse(archived),
		};
	}

	throw errorResponse(405, 'Method not allowed');
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
			{
				portfolioId: payload.portfolioId || null,
				locale: payload.locale || payload.language || null,
			}
		);
	}
	if (id === 'combine') {
		if (method !== 'POST') throw errorResponse(405, 'Method not allowed');
		const payload = parseBody(body);
		return platformService.combineReports(
			userId,
			payload.reportIds || payload.report_ids || [],
			{
				locale: payload.locale || payload.language || null,
			}
		);
	}
	if (method === 'GET' && id && String(query.action || '').toLowerCase() === 'content') {
		return platformService.getReportContent(userId, id);
	}
	if (method === 'DELETE' && id) {
		return platformService.deleteReport(userId, id);
	}
	if (method === 'GET' && id) {
		return platformService.getReportById(userId, id);
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
	if (id === 'event-inbox') {
		const payload = parseBody(body);
		return platformService.syncPortfolioEventInbox(userId, {
			portfolioId: payload.portfolioId || query.portfolioId || null,
			lookaheadDays: payload.lookaheadDays || query.lookaheadDays || 7,
			includePastDays: payload.includePastDays || query.includePastDays || null,
			refreshSources:
				payload.refreshSources === false || payload.refresh_sources === false
					? false
					: true,
			pruneDaysPast: payload.pruneDaysPast || query.pruneDaysPast || null,
		});
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
			shockBps: query.shockBps || query.shock_bps || null,
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
	let requestMethod = '';
	let requestPath = '';
	let requestUserId = 'anonymous';
	let cacheConfig = null;
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
		requestMethod = String(httpMethod || '').toUpperCase();
		requestPath = String(path || '/');
		requestUserId = userId;
		const query = queryStringParameters || {};

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

		if (
			requestMethod === 'GET'
			&& !shouldBypassResponseCache(query)
			&& isCacheableGetRoute(resourceBase, id, subResource, subId, query)
		) {
			cacheConfig = {
				key: buildResponseCacheKey({
					userId,
					appRole,
					method: requestMethod,
					path: requestPath,
					query,
				}),
				ttlMs: resolveRouteResponseCacheTtlMs(resourceBase, id, subResource),
			};
			const cached = responseCache.get(cacheConfig.key);
			if (cached) {
				headers['X-Cache'] = 'HIT';
				return {
					statusCode: cached.statusCode,
					headers,
					body: cached.body,
				};
			}
		}

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
			} else if (subResource === 'event-notices') {
				body = await handleEventNotices(httpMethod, id, userId, queryStringParameters || {});
			} else if (subResource === 'event-inbox') {
				body = await handleEventInbox(
					httpMethod,
					id,
					userId,
					requestBody,
					queryStringParameters || {},
					subId || null
				);
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
			} else if (subResource === 'theses') {
				body = await handleTheses(
					httpMethod,
					id,
					requestBody,
					queryStringParameters || {},
					subId || null
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
			} else if (section === 'cache') {
				body = await handleSettingsCache(
					httpMethod,
					userId,
					requestBody
				);
			} else if (section === 'aliases') {
				body = await handleAliasesList(httpMethod);
				if (httpMethod === 'POST' || httpMethod === 'PUT') {
					body = await handleAliases(httpMethod, requestBody);
				}
			} else if (section === 'backup') {
				body = await handleSettingsBackup(httpMethod, userId, requestBody);
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

	const responseBody = JSON.stringify(body);

	if (cacheConfig) {
		if (statusCode >= 200 && statusCode < 400) {
			const stored = responseCache.set(
				cacheConfig.key,
				{ statusCode, body: responseBody },
				cacheConfig.ttlMs
			);
			headers['X-Cache'] = stored ? 'MISS-STORE' : 'MISS-SKIP';
		} else {
			headers['X-Cache'] = 'MISS-ERROR';
		}
	} else if (!headers['X-Cache']) {
		headers['X-Cache'] = 'BYPASS';
	}

	if (
		MUTATION_METHODS.has(requestMethod)
		&& statusCode >= 200
		&& statusCode < 400
	) {
		const invalidationPrefix = `u:${requestUserId}|`;
		const removed = responseCache.invalidateByPrefix(invalidationPrefix);
		headers['X-Cache-Invalidated'] = String(removed);
	}

	return {
		statusCode,
		headers,
		body: responseBody,
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
	normalizeThesisCountry,
	normalizeThesisAssetClass,
	buildThesisScopeKey,
	parseThesisScopeKey,
	parsePercentageValue,
	validateThesisAllocations,
	normalizeAppRole,
	hasAppAccess,
	resolveAppRole,
	parseGroups,
	marketDataService,
	priceHistoryService,
	platformService,
	responseCache,
	clearResponseCache: () => responseCache.clear(),
};
