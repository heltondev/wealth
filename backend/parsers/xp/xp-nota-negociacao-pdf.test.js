const test = require('node:test');
const assert = require('node:assert/strict');

const parser = require('./xp-nota-negociacao-pdf');

test('xp-nota-negociacao-pdf detects xp nota markers', () => {
	const sampleRows = [
		['NOTA DE NEGOCIAÇÃO'],
		['XP INVESTIMENTOS CORRETORA DE CÂMBIO, TÍTULOS E VALORES MOBILIÁRIOS S.A.'],
		['Negócios realizados'],
	];

	assert.equal(
		parser.detect('NotaNegociacao-123.pdf', ['PDF_TEXT'], sampleRows),
		true
	);
	assert.equal(
		parser.detect('activity.csv', ['Sheet1'], [['ticker,event_type,trade_date']]),
		false
	);
});

test('xp-nota-negociacao-pdf parses negocios realizados rows', () => {
	const pdfText = `
NOTA DE NEGOCIAÇÃO
Nr. nota Folha Data pregão
130306155 1 18/02/2026
XP INVESTIMENTOS CORRETORA DE CÂMBIO, TÍTULOS E VALORES MOBILIÁRIOS S.A.
Negócios realizados
Q Negociação C/V Tipo mercado Prazo Especificação do título Obs. (*) Quantidade Preço / Ajuste Valor Operação / Ajuste D/C
1-BOVESPA C VISTA FII GGRCOVEP GGRC11 CI @ 6 10,06 60,36 D
1-BOVESPA C VISTA FII VINCI SC VISC11 CI @# 2 108,80 217,60 D
Resumo dos Negócios
`;

	const workbook = {
		__pdfText: pdfText,
		__pdfLines: pdfText.split(/\r?\n/),
	};

	const result = parser.parse(workbook);
	assert.equal(result.transactions.length, 2);

	assert.equal(result.transactions[0].ticker, 'GGRC11');
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].date, '2026-02-18');
	assert.equal(result.transactions[0].quantity, 6);
	assert.equal(result.transactions[0].price, 10.06);
	assert.equal(result.transactions[0].amount, 60.36);
	assert.equal(result.transactions[0].currency, 'BRL');
	assert.equal(result.transactions[0].institution, 'XP Investimentos');
	assert.equal(result.transactions[0].source, 'xp-nota-negociacao-pdf');

	assert.equal(result.assets.length, 2);
	assert.equal(result.assets[0].ticker, 'GGRC11');
	assert.equal(result.assets[0].assetClass, 'fii');
	assert.equal(result.assets[0].country, 'BR');
	assert.equal(result.assets[0].currency, 'BRL');

	assert.ok(result.aliases.some((entry) => entry.ticker === 'GGRC11'));
	assert.ok(result.aliases.some((entry) => entry.ticker === 'VISC11'));
});
