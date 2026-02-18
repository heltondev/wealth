const test = require('node:test');
const assert = require('node:assert/strict');
const XLSX = require('xlsx');

const parser = require('./cold-wallet-crypto');

test('cold-wallet-crypto detects expected CSV headers', () => {
	const sampleRows = [[
		'date',
		'event_type',
		'ticker',
		'quantity',
		'value_usd',
		'from_address',
		'notes',
	]];

	assert.equal(
		parser.detect('cold-wallet.csv', ['Sheet1'], sampleRows),
		true
	);
	assert.equal(
		parser.detect('random.csv', ['Sheet1'], [['foo', 'bar']]),
		false
	);
});

test('cold-wallet-crypto parses receives and mint events as crypto assets/transactions', () => {
	const data = [
		['date', 'event_type', 'ticker', 'quantity', 'value_usd', 'from_address', 'notes'],
		['2024-12-17', 'receive', 'BTC', '0.0178', '1179.10', 'bc1qlq...4pna', ''],
		['2024-12-17', 'receive', 'BTC', '0.00275', '182.22', 'bc1qpr...vpxk', ''],
		['2025-03-30', 'receive', 'NFT', '1', '', '', 'Received NFT'],
		['2025-12-24', 'mint', 'NFT', '1', '', '', 'Minted NFT'],
	];

	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

	const result = parser.parse(wb);

	assert.equal(result.transactions.length, 4);
	assert.equal(result.transactions[0].ticker, 'BTC');
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].date, '2024-12-17');
	assert.equal(result.transactions[0].quantity, 0.0178);
	assert.equal(result.transactions[0].amount, 1179.1);
	assert.equal(result.transactions[0].currency, 'USD');

	const btcAsset = result.assets.find((item) => item.ticker === 'BTC');
	assert.ok(btcAsset);
	assert.equal(btcAsset.assetClass, 'crypto');
	assert.equal(btcAsset.country, 'GLOBAL');
	assert.equal(btcAsset.currency, 'USD');
	assert.equal(btcAsset.quantity, 0.02055);
	assert.ok(Number(btcAsset.value) > 0);

	const nftAsset = result.assets.find((item) => item.ticker === 'NFT');
	assert.ok(nftAsset);
	assert.equal(nftAsset.assetClass, 'crypto');
	assert.equal(nftAsset.quantity, 2);
});
