const {
	QueryCommand,
	ScanCommand,
	PutCommand,
	UpdateCommand,
} = require('@aws-sdk/lib-dynamodb');
const BaseParser = require('../../parsers/base-parser');

const RELATORIO_TRADE_TYPES = new Set(['buy', 'sell', 'subscription']);
const RELATORIO_INCOME_TYPES = new Set(['dividend', 'jcp', 'reimbursement']);
const AUTHORITATIVE_SNAPSHOT_PARSERS = new Set(['b3-posicao', 'robinhood-activity', 'cold-wallet-crypto']);
const TRANSACTION_QUANTITY_PRECISION = 6;
const POSITION_QUANTITY_EPSILON = 1e-5;

const generateId = () =>
	`${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

const normalizeTicker = (value) => {
	const ticker = String(value || '').trim().toUpperCase();
	if (!ticker) return '';
	return BaseParser.normalizeTicker(ticker) || ticker;
};

const toFiniteNumber = (value, fallback = 0) => {
	const numeric = Number(value);
	if (!Number.isFinite(numeric)) return fallback;
	return numeric;
};

const normalizeQuantityForKey = (value) => {
	const numeric = Number(value);
	if (!Number.isFinite(numeric)) return '0';
	if (Number.isInteger(numeric)) return String(numeric);
	return numeric.toFixed(8).replace(/\.?0+$/, '');
};

const parseTransactionQuantity = (value) => {
	if (value === undefined || value === null || value === '') return 0;
	const numeric = Number(value);
	if (!Number.isFinite(numeric)) return 0;
	const factor = 10 ** TRANSACTION_QUANTITY_PRECISION;
	const scaled = Math.round(numeric * factor);
	return scaled / factor;
};

const resolveSnapshotCurrentValue = (asset) => {
	const numeric = Number(asset?.value);
	if (!Number.isFinite(numeric)) return null;
	if (numeric <= 0) return null;
	return numeric;
};

const resolveSnapshotCurrentPrice = (asset, quantity) => {
	const direct = Number(asset?.price);
	if (Number.isFinite(direct) && direct > 0) return direct;

	const value = resolveSnapshotCurrentValue(asset);
	if (value === null) return null;
	if (!Number.isFinite(quantity) || Math.abs(quantity) <= Number.EPSILON) return null;
	return value / quantity;
};

const buildTransactionDedupKey = (transaction) => {
	const ticker = normalizeTicker(transaction.ticker);
	const date = String(transaction.date || '').slice(0, 10);
	const type = String(transaction.type || '').trim().toLowerCase();
	const amount = toFiniteNumber(transaction.amount, 0);
	const quantity = parseTransactionQuantity(transaction.quantity);
	return `${ticker}|${date}|${type}|${amount}|${normalizeQuantityForKey(quantity)}`;
};

const queryAllItems = async (dynamo, queryInput) => {
	const items = [];
	let lastEvaluatedKey;

	do {
		const result = await dynamo.send(
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

const scanAllItems = async (dynamo, scanInput) => {
	const items = [];
	let lastEvaluatedKey;

	do {
		const result = await dynamo.send(
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

const mergeImportedAssetsByTicker = (assets) => {
	const merged = new Map();

	for (const rawAsset of assets || []) {
		const ticker = normalizeTicker(rawAsset?.ticker);
		if (!ticker) continue;

		const quantity = toFiniteNumber(rawAsset?.quantity, 0);
		const value = toFiniteNumber(rawAsset?.value, 0);
		const price = toFiniteNumber(rawAsset?.price, 0);

		if (!merged.has(ticker)) {
			merged.set(ticker, {
				ticker,
				name: String(rawAsset?.name || ticker).trim() || ticker,
				assetClass: String(rawAsset?.assetClass || '').trim().toLowerCase(),
				country: String(rawAsset?.country || 'BR').trim().toUpperCase() || 'BR',
				currency: String(rawAsset?.currency || 'BRL').trim().toUpperCase() || 'BRL',
				quantity,
				price,
				value,
			});
			continue;
		}

		const current = merged.get(ticker);
		current.quantity += quantity;
		current.value += value;
		if (price > 0) current.price = price;
		if (!current.name && rawAsset?.name) current.name = String(rawAsset.name).trim();
		if (!current.assetClass && rawAsset?.assetClass) {
			current.assetClass = String(rawAsset.assetClass).trim().toLowerCase();
		}
	}

	return merged;
};

const loadExistingAssets = async ({ dynamo, tableName, portfolioId }) => {
	const items = await queryAllItems(dynamo, {
		TableName: tableName,
		KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
		ExpressionAttributeValues: {
			':pk': `PORTFOLIO#${portfolioId}`,
			':sk': 'ASSET#',
		},
	});

	const byTicker = new Map();
	for (const item of items) {
		const ticker = normalizeTicker(item?.ticker);
		const assetId = String(item?.assetId || '').trim();
		if (!ticker || !assetId) continue;
		byTicker.set(ticker, { assetId, item });
	}

	return { byTicker, items };
};

const loadExistingTransactions = async ({ dynamo, tableName, portfolioId }) => {
	const items = await queryAllItems(dynamo, {
		TableName: tableName,
		KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
		ExpressionAttributeValues: {
			':pk': `PORTFOLIO#${portfolioId}`,
			':sk': 'TRANS#',
		},
	});

	const dedupeKeys = new Set();
	let hasDetailedNegotiationTrades = false;
	let hasDetailedIncomeEvents = false;

	for (const item of items) {
		dedupeKeys.add(buildTransactionDedupKey(item));

		const source = String(item?.sourceDocId || '').toLowerCase();
		const type = String(item?.type || '').toLowerCase();
		if (source.includes('b3-negociacao') && RELATORIO_TRADE_TYPES.has(type)) {
			hasDetailedNegotiationTrades = true;
		}
		if (source.includes('b3-movimentacao') && RELATORIO_INCOME_TYPES.has(type)) {
			hasDetailedIncomeEvents = true;
		}
	}

	return {
		dedupeKeys,
		hasDetailedNegotiationTrades,
		hasDetailedIncomeEvents,
	};
};

const loadExistingAliases = async ({ dynamo, tableName }) => {
	const items = await scanAllItems(dynamo, {
		TableName: tableName,
		FilterExpression: 'begins_with(PK, :prefix)',
		ExpressionAttributeValues: { ':prefix': 'ALIAS#' },
	});

	const keys = new Set();
	for (const item of items) {
		const normalizedName = String(item?.normalizedName || '').trim().toLowerCase();
		const ticker = normalizeTicker(item?.ticker);
		if (!normalizedName || !ticker) continue;
		keys.add(`${normalizedName}|${ticker}`);
	}

	return keys;
};

const createPlaceholderAsset = async ({
	dynamo,
	tableName,
	portfolioId,
	ticker,
	source,
	now,
	isActive,
	dryRun = false,
}) => {
	const assetId = `asset-${ticker.toLowerCase()}`;
	const item = {
		PK: `PORTFOLIO#${portfolioId}`,
		SK: `ASSET#${assetId}`,
		assetId,
		portfolioId,
		ticker,
		name: ticker,
		assetClass: BaseParser.inferAssetClass(ticker),
		country: 'BR',
		currency: 'BRL',
		quantity: 0,
		currentPrice: null,
		currentValue: null,
		source: source || 'b3-import',
		status: isActive ? 'active' : 'inactive',
		createdAt: now,
	};

	if (!dryRun) {
		await dynamo.send(
			new PutCommand({
				TableName: tableName,
				Item: item,
			})
		);
	}

	return { assetId, item };
};

const toAssetReportEntry = (asset, extra = {}) => ({
	assetId: asset?.assetId || null,
	ticker: normalizeTicker(asset?.ticker),
	name: String(asset?.name || '').trim() || null,
	assetClass: String(asset?.assetClass || '').trim().toLowerCase() || null,
	country: String(asset?.country || '').trim().toUpperCase() || null,
	currency: String(asset?.currency || '').trim().toUpperCase() || null,
	quantity: toFiniteNumber(asset?.quantity, 0),
	currentPrice: Number.isFinite(Number(asset?.currentPrice))
		? Number(asset.currentPrice)
		: null,
	currentValue: Number.isFinite(Number(asset?.currentValue))
		? Number(asset.currentValue)
		: null,
	status: String(asset?.status || '').trim().toLowerCase() || null,
	...extra,
});

const toTransactionReportEntry = (transaction, extra = {}) => ({
	transId: transaction?.transId || null,
	ticker: normalizeTicker(transaction?.ticker),
	type: String(transaction?.type || '').trim().toLowerCase() || null,
	date: String(transaction?.date || '').slice(0, 10) || null,
	quantity: parseTransactionQuantity(transaction?.quantity),
	price: toFiniteNumber(transaction?.price, 0),
	amount: toFiniteNumber(transaction?.amount, 0),
	currency: String(transaction?.currency || 'BRL').trim().toUpperCase() || 'BRL',
	source: String(transaction?.sourceDocId || transaction?.source || '').trim() || null,
	...extra,
});

const toAliasReportEntry = (alias, extra = {}) => ({
	normalizedName: String(alias?.normalizedName || '').trim().toLowerCase() || null,
	ticker: normalizeTicker(alias?.ticker),
	source: String(alias?.source || '').trim().toLowerCase() || null,
	...extra,
});

const upsertImportedAssets = async ({
	dynamo,
	tableName,
	portfolioId,
	parserId,
	sourceFile,
	now,
	dryRun = false,
	importedAssetsByTicker,
	existingAssetsByTicker,
	stats,
	report,
}) => {
	const authoritativeSnapshot = AUTHORITATIVE_SNAPSHOT_PARSERS.has(parserId);

	for (const [ticker, importedAsset] of importedAssetsByTicker) {
		const quantity = toFiniteNumber(importedAsset.quantity, 0);
		const currentValue = resolveSnapshotCurrentValue(importedAsset);
		const currentPrice = resolveSnapshotCurrentPrice(importedAsset, quantity);
		const existing = existingAssetsByTicker.get(ticker);
		const hasPosition = Number.isFinite(quantity) && Math.abs(quantity) > POSITION_QUANTITY_EPSILON;
		const resolvedStatus = hasPosition ? 'active' : 'inactive';
		const resolvedQuantity = hasPosition ? quantity : 0;

		if (existing) {
			if (!authoritativeSnapshot) {
				stats.assets.skipped += 1;
				report.assets.skipped.push(
					toAssetReportEntry(
						{
							assetId: existing.assetId,
							ticker,
							name: importedAsset.name || ticker,
							assetClass:
								importedAsset.assetClass
								|| BaseParser.inferAssetClass(ticker, importedAsset.name || ''),
							country: importedAsset.country || 'BR',
							currency: importedAsset.currency || 'BRL',
							quantity,
							currentPrice,
							currentValue,
							status: existing?.item?.status || 'active',
						},
						{ reason: 'existing_asset_not_snapshot_source' }
					)
				);
				continue;
			}

			if (!dryRun) {
				await dynamo.send(
					new UpdateCommand({
						TableName: tableName,
						Key: {
							PK: `PORTFOLIO#${portfolioId}`,
							SK: `ASSET#${existing.assetId}`,
						},
						UpdateExpression: 'SET #s = :status, #n = :name, assetClass = :assetClass, country = :country, currency = :currency, quantity = :quantity, currentPrice = :currentPrice, currentValue = :currentValue, #src = :source, updatedAt = :updatedAt',
						ExpressionAttributeNames: {
							'#s': 'status',
							'#n': 'name',
							'#src': 'source',
						},
						ExpressionAttributeValues: {
							':status': resolvedStatus,
							':name': importedAsset.name || ticker,
							':assetClass': importedAsset.assetClass || BaseParser.inferAssetClass(ticker, importedAsset.name || ''),
							':country': importedAsset.country || 'BR',
							':currency': importedAsset.currency || 'BRL',
							':quantity': resolvedQuantity,
							':currentPrice': currentPrice,
							':currentValue': currentValue,
							':source': sourceFile || null,
							':updatedAt': now,
						},
					})
				);
			}
			stats.assets.updated += 1;
			report.assets.updated.push(
				toAssetReportEntry(
					{
						assetId: existing.assetId,
						ticker,
						name: importedAsset.name || ticker,
						assetClass:
							importedAsset.assetClass
							|| BaseParser.inferAssetClass(ticker, importedAsset.name || ''),
							country: importedAsset.country || 'BR',
							currency: importedAsset.currency || 'BRL',
							quantity: resolvedQuantity,
							currentPrice,
							currentValue,
							status: resolvedStatus,
					},
					{ reason: 'updated_from_authoritative_snapshot' }
				)
			);
			continue;
		}

		const shouldBeActive = hasPosition;
		const assetId = `asset-${ticker.toLowerCase()}`;
		const item = {
			PK: `PORTFOLIO#${portfolioId}`,
			SK: `ASSET#${assetId}`,
			assetId,
			portfolioId,
			ticker,
			name: importedAsset.name || ticker,
				assetClass: importedAsset.assetClass || BaseParser.inferAssetClass(ticker, importedAsset.name || ''),
				country: importedAsset.country || 'BR',
				currency: importedAsset.currency || 'BRL',
				quantity: resolvedQuantity,
				currentPrice: currentPrice,
			currentValue: currentValue,
			source: sourceFile || null,
			status: shouldBeActive ? 'active' : 'inactive',
			createdAt: now,
		};

		if (!dryRun) {
			await dynamo.send(
				new PutCommand({
					TableName: tableName,
					Item: item,
				})
			);
		}

		existingAssetsByTicker.set(ticker, { assetId, item });
		stats.assets.created += 1;
		report.assets.created.push(
			toAssetReportEntry(item, {
				reason: authoritativeSnapshot
					? 'created_from_authoritative_snapshot'
					: 'created_from_non_snapshot_import',
			})
		);
	}
};

const importTransactions = async ({
	dynamo,
	tableName,
	portfolioId,
	parserId,
	sourceFile,
	now,
	dryRun = false,
	parsedTransactions,
	existingAssetsByTicker,
	existingTransactionKeys,
	hasDetailedNegotiationTrades,
	hasDetailedIncomeEvents,
	stats,
	report,
}) => {
	const intraImportKeys = new Set();
	const authoritativeSnapshot = AUTHORITATIVE_SNAPSHOT_PARSERS.has(parserId);

	for (const rawTransaction of parsedTransactions || []) {
		const ticker = normalizeTicker(rawTransaction?.ticker);
		const date = String(rawTransaction?.date || '').slice(0, 10);
		const type = String(rawTransaction?.type || '').trim().toLowerCase();
		if (!ticker || !date || !type) {
			stats.transactions.skipped += 1;
			report.transactions.skipped.push(
				toTransactionReportEntry(rawTransaction, {
					reason: 'invalid_transaction_record',
				})
			);
			continue;
		}

		if (
			parserId === 'b3-relatorio'
			&& hasDetailedNegotiationTrades
			&& RELATORIO_TRADE_TYPES.has(type)
		) {
			stats.transactions.filtered += 1;
			report.transactions.filtered.push(
				toTransactionReportEntry(rawTransaction, {
					reason: 'filtered_relatorio_duplicate_source',
				})
			);
			continue;
		}

		if (
			parserId === 'b3-relatorio'
			&& hasDetailedIncomeEvents
			&& RELATORIO_INCOME_TYPES.has(type)
		) {
			stats.transactions.filtered += 1;
			report.transactions.filtered.push(
				toTransactionReportEntry(rawTransaction, {
					reason: 'filtered_relatorio_duplicate_source',
				})
			);
			continue;
		}

		const quantity = parseTransactionQuantity(rawTransaction.quantity);
		const amount = toFiniteNumber(rawTransaction.amount, 0);
		const dedupKey = `${ticker}|${date}|${type}|${amount}|${normalizeQuantityForKey(quantity)}`;

		if (existingTransactionKeys.has(dedupKey) || intraImportKeys.has(dedupKey)) {
			stats.transactions.skipped += 1;
			report.transactions.skipped.push(
				toTransactionReportEntry(rawTransaction, {
					reason: 'duplicate_transaction',
					dedupKey,
				})
			);
			continue;
		}

		let assetRef = existingAssetsByTicker.get(ticker);
		if (!assetRef) {
			assetRef = await createPlaceholderAsset({
				dynamo,
				tableName,
				portfolioId,
				ticker,
				source: sourceFile,
				now,
				isActive: authoritativeSnapshot,
				dryRun,
			});
			existingAssetsByTicker.set(ticker, assetRef);
			stats.assets.created += 1;
			report.assets.created.push(
				toAssetReportEntry(assetRef.item, {
					reason: 'auto_created_from_transaction',
				})
			);
		}

		const transId = generateId();
		const item = {
			PK: `PORTFOLIO#${portfolioId}`,
			SK: `TRANS#${transId}`,
			transId,
			portfolioId,
			assetId: assetRef.assetId,
			ticker,
			type,
			date,
			quantity,
			price: toFiniteNumber(rawTransaction.price, 0),
			currency: String(rawTransaction.currency || 'BRL').trim().toUpperCase() || 'BRL',
			amount,
			status: 'confirmed',
			institution: rawTransaction.institution || null,
			direction: rawTransaction.direction || null,
			market: rawTransaction.market || null,
			sourceDocId: rawTransaction.source || parserId,
			createdAt: now,
		};

		if (!dryRun) {
			await dynamo.send(
				new PutCommand({
					TableName: tableName,
					Item: item,
				})
			);
		}

		existingTransactionKeys.add(dedupKey);
		intraImportKeys.add(dedupKey);
		stats.transactions.created += 1;
		report.transactions.created.push(
			toTransactionReportEntry(item, {
				reason: 'created',
				dedupKey,
			})
		);
	}
};

const importAliases = async ({
	dynamo,
	tableName,
	now,
	dryRun = false,
	parsedAliases,
	existingAliasKeys,
	stats,
	report,
}) => {
	for (const rawAlias of parsedAliases || []) {
		const normalizedName = String(rawAlias?.normalizedName || '').trim().toLowerCase();
		const ticker = normalizeTicker(rawAlias?.ticker);
		if (!normalizedName || !ticker) {
			stats.aliases.skipped += 1;
			report.aliases.skipped.push(
				toAliasReportEntry(rawAlias, {
					reason: 'invalid_alias_record',
				})
			);
			continue;
		}

		const aliasKey = `${normalizedName}|${ticker}`;
		if (existingAliasKeys.has(aliasKey)) {
			stats.aliases.skipped += 1;
			report.aliases.skipped.push(
				toAliasReportEntry(
					{ normalizedName, ticker, source: rawAlias?.source || 'b3' },
					{ reason: 'duplicate_alias' }
				)
			);
			continue;
		}

		if (!dryRun) {
			await dynamo.send(
				new PutCommand({
					TableName: tableName,
					Item: {
						PK: `ALIAS#${normalizedName}`,
						SK: `TICKER#${ticker}`,
						normalizedName,
						ticker,
						source: String(rawAlias?.source || 'b3').trim().toLowerCase() || 'b3',
						createdAt: now,
					},
				})
			);
		}

		existingAliasKeys.add(aliasKey);
		stats.aliases.created += 1;
		report.aliases.created.push(
			toAliasReportEntry(
				{ normalizedName, ticker, source: rawAlias?.source || 'b3' },
				{ reason: 'created' }
			)
		);
	}
};

async function importParsedB3({
	dynamo,
	tableName,
	portfolioId,
	parser,
	parsed,
	sourceFile,
	detectionMode = 'auto',
	dryRun = false,
	now = new Date().toISOString(),
}) {
	if (!dynamo) throw new Error('dynamo client is required');
	if (!tableName) throw new Error('tableName is required');
	if (!portfolioId) throw new Error('portfolioId is required');
	if (!parser?.id) throw new Error('parser is required');

	const importedAssetsByTicker = mergeImportedAssetsByTicker(parsed?.assets || []);
	const parsedTransactions = Array.isArray(parsed?.transactions) ? parsed.transactions : [];
	const parsedAliases = Array.isArray(parsed?.aliases) ? parsed.aliases : [];

	const [{ byTicker: existingAssetsByTicker }, existingTransactions, existingAliasKeys] = await Promise.all([
		loadExistingAssets({ dynamo, tableName, portfolioId }),
		loadExistingTransactions({ dynamo, tableName, portfolioId }),
		loadExistingAliases({ dynamo, tableName }),
	]);

	const stats = {
		assets: {
			created: 0,
			updated: 0,
			skipped: 0,
		},
		transactions: {
			created: 0,
			skipped: 0,
			filtered: 0,
		},
		aliases: {
			created: 0,
			skipped: 0,
		},
	};
	const report = {
		assets: {
			created: [],
			updated: [],
			skipped: [],
		},
		transactions: {
			created: [],
			skipped: [],
			filtered: [],
		},
		aliases: {
			created: [],
			skipped: [],
		},
	};

	await upsertImportedAssets({
		dynamo,
		tableName,
		portfolioId,
		parserId: parser.id,
		sourceFile,
		now,
		dryRun,
		importedAssetsByTicker,
		existingAssetsByTicker,
		stats,
		report,
	});

	await importTransactions({
		dynamo,
		tableName,
		portfolioId,
		parserId: parser.id,
		sourceFile,
		now,
		dryRun,
		parsedTransactions,
		existingAssetsByTicker,
		existingTransactionKeys: existingTransactions.dedupeKeys,
		hasDetailedNegotiationTrades: existingTransactions.hasDetailedNegotiationTrades,
		hasDetailedIncomeEvents: existingTransactions.hasDetailedIncomeEvents,
		stats,
		report,
	});

	await importAliases({
		dynamo,
		tableName,
		now,
		dryRun,
		parsedAliases,
		existingAliasKeys,
		stats,
		report,
	});

	const warnings = [];
	if (stats.transactions.filtered > 0 && parser.id === 'b3-relatorio') {
		warnings.push('relatorio_transactions_filtered_due_to_detailed_sources');
	}

	return {
		portfolioId,
		parser: parser.id,
		provider: parser.provider,
		detectionMode,
		dryRun: Boolean(dryRun),
		sourceFile,
		importedAt: now,
		stats,
		report,
		warnings,
	};
}

module.exports = {
	importParsedB3,
	_test: {
		normalizeTicker,
		normalizeQuantityForKey,
		parseTransactionQuantity,
		buildTransactionDedupKey,
		mergeImportedAssetsByTicker,
	},
};
