const { DynamoDBClient, CreateTableCommand, DescribeTableCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const {
	buildAwsClientConfig,
	resolveTableName,
	resolveAwsRegion,
	resolveRuntimeEnvironment,
} = require('../config/aws');

const TABLE_NAME = resolveTableName();
const REGION = resolveAwsRegion();
const RUNTIME_ENV = resolveRuntimeEnvironment();
const argv = new Set(process.argv.slice(2));
const SHOULD_SEED_DEMO = argv.has('--seed-demo');
const SHOULD_IMPORT_B3 = argv.has('--import-b3');
const TABLE_ONLY = argv.has('--table-only');

const client = new DynamoDBClient(buildAwsClientConfig({ service: 'dynamodb' }));
const dynamo = DynamoDBDocumentClient.from(client);

async function createTable() {
	try {
		await client.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
		console.log(`Table "${TABLE_NAME}" already exists.`);
		return;
	} catch (err) {
		if (err.name !== 'ResourceNotFoundException') throw err;
	}

	console.log(`Creating table "${TABLE_NAME}"...`);
	await client.send(
		new CreateTableCommand({
			TableName: TABLE_NAME,
			KeySchema: [
				{ AttributeName: 'PK', KeyType: 'HASH' },
				{ AttributeName: 'SK', KeyType: 'RANGE' },
			],
			AttributeDefinitions: [
				{ AttributeName: 'PK', AttributeType: 'S' },
				{ AttributeName: 'SK', AttributeType: 'S' },
				{ AttributeName: 'portfolioId', AttributeType: 'S' },
				{ AttributeName: 'ticker', AttributeType: 'S' },
				{ AttributeName: 'status', AttributeType: 'S' },
				{ AttributeName: 'createdAt', AttributeType: 'S' },
			],
			GlobalSecondaryIndexes: [
				{
					IndexName: 'portfolioId-index',
					KeySchema: [
						{ AttributeName: 'portfolioId', KeyType: 'HASH' },
						{ AttributeName: 'SK', KeyType: 'RANGE' },
					],
					Projection: { ProjectionType: 'ALL' },
				},
				{
					IndexName: 'ticker-index',
					KeySchema: [
						{ AttributeName: 'ticker', KeyType: 'HASH' },
						{ AttributeName: 'PK', KeyType: 'RANGE' },
					],
					Projection: { ProjectionType: 'ALL' },
				},
				{
					IndexName: 'status-index',
					KeySchema: [
						{ AttributeName: 'status', KeyType: 'HASH' },
						{ AttributeName: 'createdAt', KeyType: 'RANGE' },
					],
					Projection: { ProjectionType: 'ALL' },
				},
			],
			BillingMode: 'PAY_PER_REQUEST',
		})
	);
	console.log(`Table "${TABLE_NAME}" created.`);
}

async function seedData() {
	const now = new Date().toISOString();
	const userId = 'local-user-001';
	const portfolioId = 'demo-portfolio-001';

	const items = [
		// Portfolio
		{
			PK: `USER#${userId}`,
			SK: `PORTFOLIO#${portfolioId}`,
			portfolioId,
			name: 'Main Portfolio',
			description: 'Multi-currency investment portfolio',
			baseCurrency: 'BRL',
			createdAt: now,
			updatedAt: now,
		},
		// Assets
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'ASSET#asset-petr4',
			assetId: 'asset-petr4',
			portfolioId,
			ticker: 'PETR4',
			name: 'Petrobras PN',
			assetClass: 'stock',
			country: 'BR',
			currency: 'BRL',
			status: 'active',
			createdAt: now,
		},
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'ASSET#asset-aapl',
			assetId: 'asset-aapl',
			portfolioId,
			ticker: 'AAPL',
			name: 'Apple Inc.',
			assetClass: 'stock',
			country: 'US',
			currency: 'USD',
			status: 'active',
			createdAt: now,
		},
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'ASSET#asset-hglg11',
			assetId: 'asset-hglg11',
			portfolioId,
			ticker: 'HGLG11',
			name: 'CSHG Logistica FII',
			assetClass: 'fii',
			country: 'BR',
			currency: 'BRL',
			status: 'active',
			createdAt: now,
		},
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'ASSET#asset-btc',
			assetId: 'asset-btc',
			portfolioId,
			ticker: 'BTC',
			name: 'Bitcoin',
			assetClass: 'crypto',
			country: 'US',
			currency: 'USD',
			status: 'active',
			createdAt: now,
		},
		// Transactions
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'TRANS#trans-001',
			transId: 'trans-001',
			portfolioId,
			assetId: 'asset-petr4',
			type: 'buy',
			date: '2025-06-15',
			quantity: 100,
			price: 38.5,
			currency: 'BRL',
			amount: 3850.0,
			status: 'confirmed',
			createdAt: now,
		},
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'TRANS#trans-002',
			transId: 'trans-002',
			portfolioId,
			assetId: 'asset-aapl',
			type: 'buy',
			date: '2025-07-20',
			quantity: 10,
			price: 195.0,
			currency: 'USD',
			amount: 1950.0,
			status: 'confirmed',
			createdAt: now,
		},
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'TRANS#trans-003',
			transId: 'trans-003',
			portfolioId,
			assetId: 'asset-hglg11',
			type: 'buy',
			date: '2025-08-10',
			quantity: 50,
			price: 162.0,
			currency: 'BRL',
			amount: 8100.0,
			status: 'confirmed',
			createdAt: now,
		},
		{
			PK: `PORTFOLIO#${portfolioId}`,
			SK: 'TRANS#trans-004',
			transId: 'trans-004',
			portfolioId,
			assetId: 'asset-hglg11',
			type: 'dividend',
			date: '2025-09-05',
			quantity: 50,
			price: 0,
			currency: 'BRL',
			amount: 45.0,
			status: 'confirmed',
			createdAt: now,
		},
		// Settings
		{
			PK: `USER#${userId}`,
			SK: 'SETTINGS#profile',
			displayName: 'Oliver',
			email: 'oliver@local.dev',
			preferredCurrency: 'BRL',
			locale: 'en',
			updatedAt: now,
		},
		// Aliases
		{
			PK: 'ALIAS#petrobras pn',
			SK: 'TICKER#PETR4',
			normalizedName: 'petrobras pn',
			ticker: 'PETR4',
			source: 'b3',
			createdAt: now,
		},
		// FX Rate
		{
			PK: 'FX#USD#BRL',
			SK: `RATE#2025-09-01`,
			rate: 5.45,
			source: 'manual',
			fetchedAt: now,
		},
	];

	console.log(`Seeding ${items.length} items...`);
	for (const item of items) {
		await dynamo.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
		console.log(`  + ${item.PK} | ${item.SK}`);
	}
	console.log('Seeding complete.');
}

async function run() {
	await createTable();
	if (TABLE_ONLY || !SHOULD_SEED_DEMO) {
		console.log('Table setup complete. Demo seed skipped.');
	} else {
		await seedData();
	}

	// Optionally import B3 data.
	if (SHOULD_IMPORT_B3) {
		console.log('\nRunning B3 import...');
		const { execFileSync } = require('child_process');
		const passThroughArgs = process.argv.slice(2).filter((value) =>
			value.startsWith('--portfolio-') ||
			value.startsWith('--user-') ||
			value.startsWith('--base-currency')
		);
		execFileSync('node', ['backend/scripts/import-b3.js', ...passThroughArgs], {
			stdio: 'inherit',
			cwd: require('path').resolve(__dirname, '../..'),
			env: { ...process.env },
		});
	}
}

run().catch((err) => {
	console.error(`Seed failed (env=${RUNTIME_ENV}, region=${REGION}):`, err);
	process.exit(1);
});
