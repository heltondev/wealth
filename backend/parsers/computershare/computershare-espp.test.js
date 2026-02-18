const test = require('node:test');
const assert = require('node:assert/strict');
const XLSX = require('xlsx');

const parser = require('./computershare-espp');

test('computershare-espp detects expected CSV headers', () => {
	const sampleRows = [[
		'ticker',
		'event_type',
		'allocation_date',
		'contribution_date',
		'trade_date',
		'settlement_date',
		'source',
		'contribution_amount_cad',
		'fees_cad',
		'fair_market_value_cad',
		'purchase_price_cad',
		'purchased_shares',
		'residual_amount_cad',
		'currency',
		'custodian',
	]];

	assert.equal(
		parser.detect('computershare.csv', ['Sheet1'], sampleRows),
		true
	);
	assert.equal(
		parser.detect('robinhood.csv', ['Sheet1'], [['Activity Date', 'Instrument', 'Trans Code']]),
		false
	);
});

test('computershare-espp parses buys and aggregates position by ticker', () => {
	const data = [
		[
			'ticker',
			'event_type',
			'allocation_date',
			'contribution_date',
			'trade_date',
			'settlement_date',
			'source',
			'contribution_amount_cad',
			'fees_cad',
			'fair_market_value_cad',
			'purchase_price_cad',
			'purchased_shares',
			'residual_amount_cad',
			'currency',
			'custodian',
		],
		[
			'CSU.TO',
			'buy',
			'2026-02-06',
			'2026-01-31',
			'2026-01-23',
			'2026-02-09',
			'company',
			'76.00',
			'0.00',
			'0.00',
			'2360.9809',
			'0.03219',
			'0.00',
			'CAD',
			'Computershare',
		],
		[
			'CSU.TO',
			'buy',
			'2026-02-06',
			'2026-01-31',
			'2026-01-23',
			'2026-02-09',
			'participant',
			'382.00',
			'0.00',
			'0.00',
			'2360.9809',
			'0.161797',
			'0.00',
			'CAD',
			'Computershare',
		],
	];

	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

	const result = parser.parse(wb);

	assert.equal(result.transactions.length, 2);
	assert.equal(result.transactions[0].ticker, 'CSU.TO');
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].date, '2026-01-23');
	assert.equal(result.transactions[0].quantity, 0.03219);
	assert.equal(result.transactions[0].price, 2360.9809);
	assert.equal(result.transactions[0].amount, 76);
	assert.equal(result.transactions[0].currency, 'CAD');
	assert.equal(result.transactions[0].institution, 'Computershare');
	assert.equal(result.transactions[0].market, 'CA');

	assert.equal(result.assets.length, 1);
	assert.equal(result.assets[0].ticker, 'CSU.TO');
	assert.equal(result.assets[0].assetClass, 'stock');
	assert.equal(result.assets[0].country, 'CA');
	assert.equal(result.assets[0].currency, 'CAD');
	assert.equal(result.assets[0].quantity, 0.193987);
	assert.equal(result.assets[0].price, 2360.9809);
	assert.ok(result.assets[0].value > 0);
});

test('computershare-espp handles excel serial dates and sells', () => {
	const data = [
		[
			'ticker',
			'event_type',
			'trade_date',
			'purchase_price_cad',
			'purchased_shares',
			'contribution_amount_cad',
			'currency',
			'custodian',
		],
		['CSU.TO', 'buy', 46070, 1000, 1, 1000, 'CAD', 'Computershare'],
		['CSU.TO', 'sell', 46071, 1100, 0.4, 440, 'CAD', 'Computershare'],
	];

	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

	const result = parser.parse(wb);

	assert.equal(result.transactions.length, 2);
	assert.equal(result.transactions[0].date, '2026-02-17');
	assert.equal(result.transactions[1].type, 'sell');
	assert.equal(result.assets.length, 1);
	assert.equal(result.assets[0].quantity, 0.6);
	assert.equal(result.assets[0].price, 1100);
});
