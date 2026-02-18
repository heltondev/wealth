const test = require('node:test');
const assert = require('node:assert/strict');
const XLSX = require('xlsx');

const parser = require('./robinhood-activity');

test('robinhood-activity detects expected CSV headers', () => {
	const sampleRows = [[
		'Activity Date',
		'Process Date',
		'Settle Date',
		'Instrument',
		'Description',
		'Trans Code',
		'Quantity',
		'Price',
		'Amount',
	]];

	assert.equal(
		parser.detect('account-activity.csv', ['Sheet1'], sampleRows),
		true
	);
	assert.equal(
		parser.detect('negociacao.xlsx', ['Negociação'], [['Data do Negócio', 'Código de Negociação']]),
		false
	);
});

test('robinhood-activity parses transactions, aliases and asset quantities', () => {
	const data = [
		['Activity Date', 'Process Date', 'Settle Date', 'Instrument', 'Description', 'Trans Code', 'Quantity', 'Price', 'Amount'],
		['2/17/2026', '2/17/2026', '2/18/2026', 'O', 'Realty Income\nCUSIP: 756109104\nDividend Reinvestment', 'Buy', '0.140069', '$66.11', '($9.26)'],
		['2/13/2026', '2/13/2026', '2/13/2026', 'O', 'Cash Div: R/D 2026-01-30 P/D 2026-02-13', 'CDIV', '', '', '$9.26'],
		['2/11/2026', '2/11/2026', '2/11/2026', 'O', 'Foreign Tax Witholding at $0.01', 'DTAX', '', '', '($0.01)'],
		['2/10/2026', '2/10/2026', '2/10/2026', 'O', 'Realty Income', 'Sell', '0.040000', '$60.00', '$2.40'],
		['2/09/2026', '2/09/2026', '2/09/2026', 'O', 'Realty Income', 'REC', '0.500000', '', ''],
	];

	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

	const result = parser.parse(wb);

	assert.equal(result.transactions.length, 4);
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].ticker, 'O');
	assert.equal(result.transactions[0].date, '2026-02-17');
	assert.equal(result.transactions[0].quantity, 0.140069);
	assert.equal(result.transactions[0].amount, 9.26);
	assert.equal(result.transactions[0].currency, 'USD');

	assert.equal(result.transactions[1].type, 'dividend');
	assert.equal(result.transactions[1].amount, 9.26);
	assert.equal(result.transactions[2].type, 'tax');
	assert.equal(result.transactions[2].amount, -0.01);
	assert.equal(result.transactions[3].type, 'sell');

	assert.equal(result.assets.length, 1);
	assert.equal(result.assets[0].ticker, 'O');
	assert.equal(result.assets[0].country, 'US');
	assert.equal(result.assets[0].currency, 'USD');
	assert.equal(result.assets[0].quantity, 0.600069);

	const alias = result.aliases.find((entry) => entry.ticker === 'O');
	assert.ok(alias);
	assert.equal(alias.normalizedName, 'realty income');
});

test('robinhood-activity parses Excel serial dates from CSV-like sheets', () => {
	const data = [
		['Activity Date', 'Process Date', 'Settle Date', 'Instrument', 'Description', 'Trans Code', 'Quantity', 'Price', 'Amount'],
		[46070, 46070, 46071, 'SGOV', 'iShares 0-3 Month Treasury Bond\nCUSIP: 46436E718', 'Buy', 0.016527, 100.44, -1.66],
	];

	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

	const result = parser.parse(wb);
	assert.equal(result.transactions.length, 1);
	assert.equal(result.transactions[0].date, '2026-02-17');
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].amount, 1.66);
});
