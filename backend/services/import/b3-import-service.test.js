const test = require('node:test');
const assert = require('node:assert/strict');

const { _test } = require('./b3-import-service');

test('normalizeQuantityForKey removes trailing zeros and keeps integers stable', () => {
	assert.equal(_test.normalizeQuantityForKey(10), '10');
	assert.equal(_test.normalizeQuantityForKey(10.5), '10.5');
	assert.equal(_test.normalizeQuantityForKey(10.50000001), '10.50000001');
	assert.equal(_test.normalizeQuantityForKey(null), '0');
});

test('parseTransactionQuantity rounds to two decimal places', () => {
	assert.equal(_test.parseTransactionQuantity('3'), 3);
	assert.equal(_test.parseTransactionQuantity('3.456'), 3.46);
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
