/**
 * CLI script to import Robinhood CSV activity into DynamoDB.
 *
 * Usage:
 *   node backend/scripts/import-robinhood.js [--dry-run]
 *   node backend/scripts/import-robinhood.js --file .data/Robinhood/activity.csv
 */
try {
	const path = require('path');
	const dotenv = require('dotenv');
	const repoRoot = path.resolve(__dirname, '../..');
	dotenv.config({ path: path.join(repoRoot, '.env'), override: true });
	dotenv.config({ path: path.join(repoRoot, '.env.local'), override: true });
} catch {}

const fs = require('fs');
const path = require('path');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
	DynamoDBDocumentClient,
	GetCommand,
	PutCommand,
} = require('@aws-sdk/lib-dynamodb');
const {
	buildAwsClientConfig,
	resolveTableName,
	resolveAwsRegion,
	resolveRuntimeEnvironment,
} = require('../config/aws');
const { detectProvider } = require('../parsers/index');
const { importParsedB3 } = require('../services/import/b3-import-service');

const TABLE_NAME = resolveTableName();
const REGION = resolveAwsRegion();
const RUNTIME_ENV = resolveRuntimeEnvironment();
const DATA_DIR = path.resolve(__dirname, '../../.data/Robinhood');
const DRY_RUN = process.argv.includes('--dry-run');
const TARGET_PARSER_ID = 'robinhood-activity';

function parseArg(name) {
	const inline = process.argv.find((value) => value.startsWith(`--${name}=`));
	if (inline) return inline.slice(name.length + 3).trim();
	const index = process.argv.indexOf(`--${name}`);
	if (index >= 0 && process.argv[index + 1] && !process.argv[index + 1].startsWith('--')) {
		return process.argv[index + 1].trim();
	}
	return '';
}

const USER_ID = parseArg('user-id') || process.env.IMPORT_USER_ID || 'local-user-001';
const PORTFOLIO_ID = parseArg('portfolio-id') || process.env.IMPORT_PORTFOLIO_ID || 'main-portfolio';
const PORTFOLIO_NAME = parseArg('portfolio-name') || process.env.IMPORT_PORTFOLIO_NAME || 'Main Portfolio';
const BASE_CURRENCY = (parseArg('base-currency') || process.env.IMPORT_BASE_CURRENCY || 'USD').toUpperCase();
const INPUT_FILE = parseArg('file');

const client = new DynamoDBClient(buildAwsClientConfig({ service: 'dynamodb' }));
const dynamo = DynamoDBDocumentClient.from(client);

function findCsvFiles(dir) {
	const results = [];
	if (!fs.existsSync(dir)) return results;
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			results.push(...findCsvFiles(full));
		} else if (/\.csv$/i.test(entry.name)) {
			results.push(full);
		}
	}
	return results.sort();
}

async function ensurePortfolioExists() {
	const key = {
		PK: `USER#${USER_ID}`,
		SK: `PORTFOLIO#${PORTFOLIO_ID}`,
	};
	const existing = await dynamo.send(
		new GetCommand({
			TableName: TABLE_NAME,
			Key: key,
		})
	);
	if (existing.Item) return false;

	const now = new Date().toISOString();
	await dynamo.send(
		new PutCommand({
			TableName: TABLE_NAME,
			Item: {
				...key,
				portfolioId: PORTFOLIO_ID,
				name: PORTFOLIO_NAME,
				description: 'Imported from Robinhood activity CSV',
				baseCurrency: BASE_CURRENCY,
				createdAt: now,
				updatedAt: now,
			},
		})
	);
	return true;
}

async function run() {
	console.log(`Runtime: env=${RUNTIME_ENV}, region=${REGION}, table=${TABLE_NAME}`);
	console.log(`Target: user=${USER_ID}, portfolio=${PORTFOLIO_ID}, dryRun=${DRY_RUN}`);

	const files = INPUT_FILE
		? [path.resolve(INPUT_FILE)]
		: findCsvFiles(DATA_DIR);
	if (files.length === 0) {
		console.log(`No CSV files found in ${INPUT_FILE || DATA_DIR}.`);
		return;
	}

	console.log(`Found ${files.length} file(s).`);

	if (!DRY_RUN) {
		const created = await ensurePortfolioExists();
		if (created) {
			console.log(`Created portfolio ${PORTFOLIO_ID} (${PORTFOLIO_NAME}).`);
		}
	}

	const totals = {
		assetsCreated: 0,
		assetsUpdated: 0,
		assetsSkipped: 0,
		transactionsCreated: 0,
		transactionsSkipped: 0,
		transactionsFiltered: 0,
		aliasesCreated: 0,
		aliasesSkipped: 0,
	};

	for (const filePath of files) {
		const relPath = path.relative(process.cwd(), filePath);
		const detected = detectProvider(filePath);
		if (!detected || detected.parser?.id !== TARGET_PARSER_ID) {
			console.log(`SKIP ${relPath} (could not detect ${TARGET_PARSER_ID})`);
			continue;
		}

		const sourceFile = path.basename(filePath);
		const parsed = detected.parser.parse(detected.workbook, { sourceFile });
		const result = await importParsedB3({
			dynamo,
			tableName: TABLE_NAME,
			portfolioId: PORTFOLIO_ID,
			parser: detected.parser,
			parsed,
			sourceFile,
			detectionMode: 'auto',
			dryRun: DRY_RUN,
		});

		totals.assetsCreated += result.stats.assets.created;
		totals.assetsUpdated += result.stats.assets.updated;
		totals.assetsSkipped += result.stats.assets.skipped;
		totals.transactionsCreated += result.stats.transactions.created;
		totals.transactionsSkipped += result.stats.transactions.skipped;
		totals.transactionsFiltered += result.stats.transactions.filtered;
		totals.aliasesCreated += result.stats.aliases.created;
		totals.aliasesSkipped += result.stats.aliases.skipped;

		console.log(`IMPORTED ${relPath}`);
		console.log(`  assets: created=${result.stats.assets.created}, updated=${result.stats.assets.updated}, skipped=${result.stats.assets.skipped}`);
		console.log(`  transactions: created=${result.stats.transactions.created}, skipped=${result.stats.transactions.skipped}, filtered=${result.stats.transactions.filtered}`);
		console.log(`  aliases: created=${result.stats.aliases.created}, skipped=${result.stats.aliases.skipped}`);
	}

	console.log('\nTotals');
	console.log(`  assets: created=${totals.assetsCreated}, updated=${totals.assetsUpdated}, skipped=${totals.assetsSkipped}`);
	console.log(`  transactions: created=${totals.transactionsCreated}, skipped=${totals.transactionsSkipped}, filtered=${totals.transactionsFiltered}`);
	console.log(`  aliases: created=${totals.aliasesCreated}, skipped=${totals.aliasesSkipped}`);
}

run().catch((error) => {
	console.error(error);
	process.exitCode = 1;
});
