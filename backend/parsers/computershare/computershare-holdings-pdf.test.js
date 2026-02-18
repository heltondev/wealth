const test = require('node:test');
const assert = require('node:assert/strict');

const parser = require('./computershare-holdings-pdf');

test('computershare-holdings-pdf detects statement markers', () => {
	const sampleRows = [
		['Plan Holdings Statement'],
		['Constellation Software Inc Computershare Trust Company of Canada'],
		['Purchases'],
	];

	assert.equal(
		parser.detect('plan-holdings.pdf', ['PDF_TEXT'], sampleRows),
		true
	);
	assert.equal(
		parser.detect('activity.csv', ['Sheet1'], [['ticker,event_type,trade_date']]),
		false
	);
});

test('computershare-holdings-pdf parses purchase rows into buy transactions', () => {
	const pdfText = `
Plan Holdings Statement
Constellation Software Inc. - CSU.TO: 4934.84 CAD as of 17 Feb 2025 (Toronto)
Purchases

10 Feb 2025   31 Jan 2025    6 Feb 2025       11 Feb 2025       77.00 CAD        0.00 CAD      0.00 CAD      0.00 CAD     4 938.00 CAD    0.015593       0.00 CAD
              Company

10 Feb 2025   31 Jan 2025    6 Feb 2025       11 Feb 2025      383.00 CAD        0.00 CAD      0.00 CAD      0.00 CAD     4 938.00 CAD    0.077562       0.00 CAD
              Participant
`;

	const workbook = {
		__pdfText: pdfText,
		__pdfLines: pdfText.split(/\r?\n/),
	};

	const result = parser.parse(workbook);

	assert.equal(result.transactions.length, 2);
	assert.equal(result.transactions[0].ticker, 'CSU.TO');
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].date, '2025-02-06');
	assert.equal(result.transactions[0].quantity, 0.015593);
	assert.equal(result.transactions[0].price, 4938);
	assert.equal(result.transactions[0].amount, 77);
	assert.equal(result.transactions[0].currency, 'CAD');
	assert.equal(result.transactions[0].direction, 'company');

	assert.equal(result.transactions[1].direction, 'participant');
	assert.equal(result.assets.length, 1);
	assert.equal(result.assets[0].ticker, 'CSU.TO');
	assert.equal(result.assets[0].country, 'CA');
	assert.equal(result.assets[0].assetClass, 'stock');
	assert.equal(result.assets[0].quantity, 0.093155);
	assert.equal(result.assets[0].price, 4938);
	assert.ok(result.assets[0].value > 0);

	assert.equal(result.aliases.length, 1);
	assert.equal(result.aliases[0].normalizedName, 'constellation software inc.');
});
