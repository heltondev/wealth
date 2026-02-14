/**
 * CLI script to import B3 data files into DynamoDB.
 *
 * Scans .data/B3/ recursively for .xlsx files, auto-detects the parser,
 * extracts assets/transactions/aliases, deduplicates, and writes to DynamoDB.
 *
 * Usage: node backend/scripts/import-b3.js [--dry-run]
 */
const fs = require('fs');
const path = require('path');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand, QueryCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { detectProvider } = require('../parsers/index');

const ENDPOINT = process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000';
const TABLE_NAME = process.env.TABLE_NAME || 'wealth-main';
const REGION = process.env.AWS_REGION || 'us-east-1';
const DATA_DIR = path.resolve(__dirname, '../../.data/B3');
const DRY_RUN = process.argv.includes('--dry-run');

const USER_ID = 'local-user-001';
const PORTFOLIO_ID = 'demo-portfolio-001';

const client = new DynamoDBClient({
	region: REGION,
	endpoint: ENDPOINT,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'local',
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'local',
	},
});
const dynamo = DynamoDBDocumentClient.from(client);

const generateId = () =>
	`${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

/**
 * Find all .xlsx files recursively in a directory.
 */
function findXlsxFiles(dir) {
	const results = [];
	if (!fs.existsSync(dir)) return results;
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			results.push(...findXlsxFiles(full));
		} else if (entry.name.endsWith('.xlsx') && !entry.name.startsWith('~')) {
			results.push(full);
		}
	}
	return results;
}

/**
 * Load existing assets from DynamoDB to avoid duplicates.
 * Returns a Map of ticker → assetId.
 */
async function loadExistingAssets() {
	const map = new Map();
	const result = await dynamo.send(new QueryCommand({
		TableName: TABLE_NAME,
		KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
		ExpressionAttributeValues: {
			':pk': `PORTFOLIO#${PORTFOLIO_ID}`,
			':sk': 'ASSET#',
		},
	}));
	for (const item of (result.Items || [])) {
		if (item.ticker) map.set(item.ticker, item.assetId);
	}
	return map;
}

/**
 * Load existing transaction dedup keys to avoid duplicates.
 * Returns a Set of "ticker|date|type|amount|quantity" strings.
 */
async function loadExistingTransactionKeys() {
	const keys = new Set();
	let lastKey = undefined;
	do {
		const result = await dynamo.send(new QueryCommand({
			TableName: TABLE_NAME,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${PORTFOLIO_ID}`,
				':sk': 'TRANS#',
			},
			ExclusiveStartKey: lastKey,
		}));
		for (const item of (result.Items || [])) {
			keys.add(`${item.ticker || ''}|${item.date}|${item.type}|${item.amount}|${item.quantity}`);
		}
		lastKey = result.LastEvaluatedKey;
	} while (lastKey);
	return keys;
}

/**
 * Load existing aliases to avoid duplicates.
 */
async function loadExistingAliases() {
	const keys = new Set();
	const result = await dynamo.send(new ScanCommand({
		TableName: TABLE_NAME,
		FilterExpression: 'begins_with(PK, :prefix)',
		ExpressionAttributeValues: { ':prefix': 'ALIAS#' },
	}));
	for (const item of (result.Items || [])) {
		keys.add(`${item.normalizedName}|${item.ticker}`);
	}
	return keys;
}

async function run() {
	console.log(`Scanning ${DATA_DIR} for .xlsx files...`);
	const files = findXlsxFiles(DATA_DIR);
	if (!files.length) {
		console.log('No .xlsx files found.');
		return;
	}
	console.log(`Found ${files.length} files.\n`);

	// Collect parsed data per file, tracking which position snapshot is the latest
	const allTransactions = [];
	const allAliases = new Map(); // normalizedName → alias data

	// Track position snapshots per parser type to pick only the most recent
	// Each entry: { file, assets: Map<ticker, asset> }
	const positionSnapshots = [];
	const posicaoSnapshots = [];

	const POSITION_PARSERS = new Set(['b3-posicao', 'b3-relatorio']);

	for (const filePath of files) {
		const relPath = path.relative(DATA_DIR, filePath);
		const result = detectProvider(filePath);

		if (!result) {
			console.log(`  SKIP  ${relPath} (no parser matched)`);
			continue;
		}

		const { parser, workbook } = result;
		console.log(`  PARSE ${relPath} → ${parser.id}`);

		const parsed = parser.parse(workbook, { sourceFile: path.basename(filePath) });

		// Track position snapshots separately — each file is a point-in-time snapshot
		if (parser.id === 'b3-posicao') {
			const snapshot = new Map();
			for (const asset of parsed.assets) snapshot.set(asset.ticker, asset);
			posicaoSnapshots.push({ file: filePath, assets: snapshot });
		} else if (parser.id === 'b3-relatorio') {
			const snapshot = new Map();
			for (const asset of parsed.assets) snapshot.set(asset.ticker, asset);
			positionSnapshots.push({ file: filePath, assets: snapshot });
		}

		// Collect transactions from all parsers
		allTransactions.push(...parsed.transactions);

		// Merge aliases from all parsers
		for (const alias of parsed.aliases) {
			allAliases.set(`${alias.normalizedName}|${alias.ticker}`, alias);
		}
	}

	// Use the LATEST position snapshot as the source of truth for active assets.
	// Priority: posicao-*.xlsx (most current) > last relatorio (most recent annual/monthly)
	// Files are sorted alphabetically which means chronologically for these naming patterns.
	let allAssets;
	if (posicaoSnapshots.length > 0) {
		const latest = posicaoSnapshots[posicaoSnapshots.length - 1];
		console.log(`\nUsing position snapshot: ${path.relative(DATA_DIR, latest.file)}`);
		allAssets = latest.assets;
	} else if (positionSnapshots.length > 0) {
		const latest = positionSnapshots[positionSnapshots.length - 1];
		console.log(`\nUsing relatorio snapshot: ${path.relative(DATA_DIR, latest.file)}`);
		allAssets = latest.assets;
	} else {
		allAssets = new Map();
	}

	console.log(`\nParsed totals: ${allAssets.size} active assets, ${allTransactions.length} transactions, ${allAliases.size} aliases`);

	if (DRY_RUN) {
		console.log('\n[DRY RUN] No data written to DynamoDB.');
		printSummary(allAssets, allTransactions, allAliases);
		return;
	}

	// Load existing data for deduplication
	console.log('\nLoading existing data for deduplication...');
	const existingAssets = await loadExistingAssets();
	const existingTransKeys = await loadExistingTransactionKeys();
	const existingAliasKeys = await loadExistingAliases();

	const now = new Date().toISOString();
	let stats = { assets: 0, transactions: 0, aliases: 0, skipped: 0, deactivated: 0 };

	// Deactivate existing assets not in the current position snapshot
	const { UpdateCommand } = require('@aws-sdk/lib-dynamodb');
	for (const [ticker, assetId] of existingAssets) {
		if (!allAssets.has(ticker)) {
			await dynamo.send(new UpdateCommand({
				TableName: TABLE_NAME,
				Key: { PK: `PORTFOLIO#${PORTFOLIO_ID}`, SK: `ASSET#${assetId}` },
				UpdateExpression: 'SET #s = :status',
				ExpressionAttributeNames: { '#s': 'status' },
				ExpressionAttributeValues: { ':status': 'inactive' },
			}));
			stats.deactivated++;
		}
	}

	// Write/update active assets
	for (const [ticker, asset] of allAssets) {
		if (existingAssets.has(ticker)) {
			// Update existing asset to ensure it's active with correct class
			const assetId = existingAssets.get(ticker);
			await dynamo.send(new UpdateCommand({
				TableName: TABLE_NAME,
				Key: { PK: `PORTFOLIO#${PORTFOLIO_ID}`, SK: `ASSET#${assetId}` },
				UpdateExpression: 'SET #s = :status, assetClass = :cls, #n = :name',
				ExpressionAttributeNames: { '#s': 'status', '#n': 'name' },
				ExpressionAttributeValues: { ':status': 'active', ':cls': asset.assetClass, ':name': asset.name },
			}));
			stats.skipped++;
			continue;
		}

		const assetId = `asset-${ticker.toLowerCase()}`;
		const item = {
			PK: `PORTFOLIO#${PORTFOLIO_ID}`,
			SK: `ASSET#${assetId}`,
			assetId,
			portfolioId: PORTFOLIO_ID,
			ticker,
			name: asset.name,
			assetClass: asset.assetClass,
			country: asset.country || 'BR',
			currency: asset.currency || 'BRL',
			status: 'active',
			createdAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		existingAssets.set(ticker, assetId);
		stats.assets++;
	}

	// Write transactions (with dedup)
	for (const trans of allTransactions) {
		const ticker = trans.ticker;
		const dedupKey = `${ticker}|${trans.date}|${trans.type}|${trans.amount}|${trans.quantity}`;

		if (existingTransKeys.has(dedupKey)) {
			stats.skipped++;
			continue;
		}

		// Resolve asset ID — auto-create as inactive if not in current positions
		let assetId = existingAssets.get(ticker);
		if (!assetId) {
			assetId = `asset-${ticker.toLowerCase()}`;
			const isActive = allAssets.has(ticker);
			const BaseParser = require('../parsers/base-parser');
			const assetItem = {
				PK: `PORTFOLIO#${PORTFOLIO_ID}`,
				SK: `ASSET#${assetId}`,
				assetId,
				portfolioId: PORTFOLIO_ID,
				ticker,
				name: ticker,
				assetClass: BaseParser.inferAssetClass(ticker),
				country: 'BR',
				currency: 'BRL',
				status: isActive ? 'active' : 'inactive',
				createdAt: now,
			};
			await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: assetItem }));
			existingAssets.set(ticker, assetId);
			stats.assets++;
		}

		const transId = generateId();
		const item = {
			PK: `PORTFOLIO#${PORTFOLIO_ID}`,
			SK: `TRANS#${transId}`,
			transId,
			portfolioId: PORTFOLIO_ID,
			assetId,
			ticker,
			type: trans.type,
			date: trans.date,
			quantity: trans.quantity,
			price: trans.price,
			currency: trans.currency || 'BRL',
			amount: trans.amount,
			status: 'confirmed',
			sourceDocId: trans.source || null,
			createdAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		existingTransKeys.add(dedupKey);
		stats.transactions++;
	}

	// Write aliases
	for (const [key, alias] of allAliases) {
		if (existingAliasKeys.has(key)) {
			stats.skipped++;
			continue;
		}

		const item = {
			PK: `ALIAS#${alias.normalizedName}`,
			SK: `TICKER#${alias.ticker}`,
			normalizedName: alias.normalizedName,
			ticker: alias.ticker,
			source: alias.source || 'b3',
			createdAt: now,
		};

		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		existingAliasKeys.add(key);
		stats.aliases++;
	}

	console.log(`\nImport complete:`);
	console.log(`  Assets created:       ${stats.assets}`);
	console.log(`  Assets deactivated:   ${stats.deactivated}`);
	console.log(`  Transactions created: ${stats.transactions}`);
	console.log(`  Aliases created:      ${stats.aliases}`);
	console.log(`  Duplicates skipped:   ${stats.skipped}`);
}

function printSummary(assets, transactions, aliases) {
	console.log('\n--- Active Assets (current holdings) ---');
	for (const [ticker, asset] of assets) {
		console.log(`  ${ticker} (${asset.assetClass}) - ${asset.name}`);
	}

	console.log('\n--- Transaction Types ---');
	const typeCounts = {};
	for (const t of transactions) {
		typeCounts[t.type] = (typeCounts[t.type] || 0) + 1;
	}
	for (const [type, count] of Object.entries(typeCounts)) {
		console.log(`  ${type}: ${count}`);
	}

	console.log('\n--- Aliases ---');
	for (const [, alias] of aliases) {
		console.log(`  ${alias.ticker} ← "${alias.normalizedName.substring(0, 60)}"`);
	}
}

run().catch((err) => {
	console.error('Import failed:', err);
	process.exit(1);
});
