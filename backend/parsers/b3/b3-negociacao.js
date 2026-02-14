/**
 * B3 Negociação (Trades) parser.
 * Handles files like negociacao-*.xlsx with sheet "Negociação".
 *
 * Columns: Data do Negócio, Tipo de Movimentação, Mercado, Prazo/Vencimento,
 *          Instituição, Código de Negociação, Quantidade, Preço, Valor
 */
const BaseParser = require('../base-parser');

const TYPE_MAP = {
	'Compra': 'buy',
	'Venda': 'sell',
};

const parser = new BaseParser({ id: 'b3-negociacao', provider: 'b3' });

parser.detect = function (fileName, sheetNames) {
	if (/negociacao/i.test(fileName) && sheetNames.includes('Negociação')) {
		return true;
	}
	// Also match by sheet name alone
	return sheetNames.length === 1 && sheetNames[0] === 'Negociação';
};

parser.parse = function (workbook, options = {}) {
	const rows = BaseParser.sheetToRows(workbook, 'Negociação');
	const assets = new Map(); // ticker → asset info
	const transactions = [];
	const aliases = [];

	for (const row of rows) {
		const ticker = (row['Código de Negociação'] || '').toString().trim().toUpperCase();
		if (!ticker) continue;

		const date = BaseParser.parseDate(row['Data do Negócio']);
		if (!date) continue;

		const typeStr = (row['Tipo de Movimentação'] || '').toString().trim();
		const type = TYPE_MAP[typeStr];
		if (!type) continue;

		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const price = BaseParser.parseNumber(row['Preço']);
		const amount = BaseParser.parseNumber(row['Valor']);

		transactions.push({
			ticker,
			type,
			date,
			quantity,
			price,
			amount,
			currency: 'BRL',
			institution: (row['Instituição'] || '').toString().trim(),
			market: (row['Mercado'] || '').toString().trim(),
			source: 'b3-negociacao',
		});

		// Track unique tickers for asset creation
		if (!assets.has(ticker)) {
			assets.set(ticker, {
				ticker,
				name: ticker,
				assetClass: BaseParser.inferAssetClass(ticker),
				country: 'BR',
				currency: 'BRL',
			});
		}
	}

	return {
		assets: Array.from(assets.values()),
		transactions,
		aliases,
	};
};

module.exports = parser;
