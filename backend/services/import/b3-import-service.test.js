const test = require('node:test');
const assert = require('node:assert/strict');

const { importParsedB3, _test } = require('./b3-import-service');

test('normalizeQuantityForKey removes trailing zeros and keeps integers stable', () => {
	assert.equal(_test.normalizeQuantityForKey(10), '10');
	assert.equal(_test.normalizeQuantityForKey(10.5), '10.5');
	assert.equal(_test.normalizeQuantityForKey(10.50000001), '10.50000001');
	assert.equal(_test.normalizeQuantityForKey(null), '0');
});

test('parseTransactionQuantity rounds to six decimal places', () => {
	assert.equal(_test.parseTransactionQuantity('3'), 3);
	assert.equal(_test.parseTransactionQuantity('3.456'), 3.456);
	assert.equal(_test.parseTransactionQuantity('3.4567899'), 3.45679);
	assert.equal(_test.parseTransactionQuantity(null), 0);
	assert.equal(_test.parseTransactionQuantity('invalid'), 0);
});

test('buildTransactionDedupKey normalizes ticker, type, amount and quantity', () => {
	const key = _test.buildTransactionDedupKey({
		ticker: 'alzr11l',
		date: '2026-02-10T00:00:00-03:00',
		type: 'BUY',
		amount: '100.00',
		quantity: '10.0',
	});
	assert.equal(key, 'ALZR11|2026-02-10|buy|100|10');
});

test('mergeImportedAssetsByTicker aggregates duplicated rows by ticker', () => {
	const merged = _test.mergeImportedAssetsByTicker([
		{
			ticker: 'ALZR11',
			name: 'ALIANZA',
			assetClass: 'fii',
			quantity: 100,
			value: 1000,
			price: 10,
		},
		{
			ticker: 'alzr11',
			quantity: 5,
			value: 55,
			price: 11,
		},
	]);

	const item = merged.get('ALZR11');
	assert.ok(item);
	assert.equal(item.quantity, 105);
	assert.equal(item.value, 1055);
	assert.equal(item.price, 11);
});

test('importParsedB3 dryRun builds preview without persisting writes', async () => {
	const commandNames = [];
	const dynamo = {
		send: async (command) => {
			const name = command?.constructor?.name || 'UnknownCommand';
			commandNames.push(name);
			if (name === 'QueryCommand') return { Items: [] };
			if (name === 'ScanCommand') return { Items: [] };
			return {};
		},
	};

	const result = await importParsedB3({
		dynamo,
		tableName: 'wealth-main',
		portfolioId: 'oliver-main',
		parser: { id: 'b3-negociacao', provider: 'b3' },
		parsed: {
			assets: [
				{
					ticker: 'ALZR11',
					name: 'ALIANZA TRUST',
					assetClass: 'fii',
					country: 'BR',
					currency: 'BRL',
					quantity: 10,
					price: 100,
					value: 1000,
				},
			],
			transactions: [
				{
					ticker: 'ALZR11',
					type: 'buy',
					date: '2026-02-17',
					quantity: 1,
					price: 100,
					amount: 100,
					currency: 'BRL',
					source: 'b3-negociacao',
				},
			],
			aliases: [
				{
					normalizedName: 'alianza trust',
					ticker: 'ALZR11',
					source: 'b3',
				},
			],
		},
		sourceFile: 'preview.xlsx',
		detectionMode: 'manual',
		dryRun: true,
		now: '2026-02-17T00:00:00.000Z',
	});

	assert.equal(result.dryRun, true);
	assert.equal(result.stats.assets.created, 1);
	assert.equal(result.stats.transactions.created, 1);
	assert.equal(result.stats.aliases.created, 1);

	const writeCommands = commandNames.filter((name) => name === 'PutCommand' || name === 'UpdateCommand');
	assert.equal(writeCommands.length, 0);
});

test('importParsedB3 authoritative parser marks existing asset inactive when quantity is zero', async () => {
	const updateInputs = [];
	const dynamo = {
		send: async (command) => {
			const name = command?.constructor?.name || 'UnknownCommand';
			if (name === 'QueryCommand') {
				const sk = command?.input?.ExpressionAttributeValues?.[':sk'];
				if (sk === 'ASSET#') {
					return {
						Items: [{
							PK: 'PORTFOLIO#oliver-main',
							SK: 'ASSET#asset-o',
							assetId: 'asset-o',
							ticker: 'O',
							status: 'active',
						}],
					};
				}
				return { Items: [] };
			}
			if (name === 'ScanCommand') return { Items: [] };
			if (name === 'UpdateCommand') {
				updateInputs.push(command.input);
				return {};
			}
			return {};
		},
	};

	const result = await importParsedB3({
		dynamo,
		tableName: 'wealth-main',
		portfolioId: 'oliver-main',
		parser: { id: 'robinhood-activity', provider: 'robinhood' },
		parsed: {
			assets: [
				{
					ticker: 'O',
					name: 'Realty Income',
					assetClass: 'stock',
					country: 'US',
					currency: 'USD',
					quantity: 0,
					price: 66.11,
					value: 0,
				},
			],
			transactions: [],
			aliases: [],
		},
		sourceFile: 'robinhood.csv',
		detectionMode: 'manual',
		dryRun: false,
		now: '2026-02-17T00:00:00.000Z',
	});

	assert.equal(result.stats.assets.updated, 1);
	assert.equal(updateInputs.length, 1);
	assert.equal(updateInputs[0].ExpressionAttributeValues[':status'], 'inactive');
	assert.equal(updateInputs[0].ExpressionAttributeValues[':quantity'], 0);
});
