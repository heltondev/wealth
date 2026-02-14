/**
 * B3 Movimentação (Movements) parser.
 * Handles files like movimentacao-*.xlsx with sheet "Movimentação".
 *
 * Columns: Entrada/Saída, Data, Movimentação, Produto, Instituição,
 *          Quantidade, Preço unitário, Valor da Operação
 */
const BaseParser = require('../base-parser');

const MOVEMENT_TYPE_MAP = {
	'Rendimento': 'dividend',
	'Dividendo': 'dividend',
	'Juros Sobre Capital Próprio': 'jcp',
	'Transferência - Liquidação': 'transfer',
	'Direito de Subscrição': 'subscription',
	'Cessão de Direitos': 'subscription',
	'Cessão de Direitos - Solicitada': 'subscription',
	'RESGATE': 'sell',
	'Resgate': 'sell',
	'Leilão de Fração': 'sell',
	'Bonificação em Ativos': 'dividend',
	'Atualização': 'update',
	'Desdobramento': 'split',
	'Grupamento': 'split',
	'Fração em Ativos': 'dividend',
	'Recibo de Subscrição': 'subscription',
};

const parser = new BaseParser({ id: 'b3-movimentacao', provider: 'b3' });

parser.detect = function (fileName, sheetNames) {
	if (/movimentacao/i.test(fileName) && sheetNames.includes('Movimentação')) {
		return true;
	}
	return sheetNames.length === 1 && sheetNames[0] === 'Movimentação';
};

parser.parse = function (workbook, options = {}) {
	const rows = BaseParser.sheetToRows(workbook, 'Movimentação');
	const assets = new Map();
	const transactions = [];
	const aliases = new Map(); // normalizedName → { ticker, productName }

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		const ticker = BaseParser.extractTicker(product);
		if (!ticker) continue;

		const date = BaseParser.parseDate(row['Data']);
		if (!date) continue;

		const movType = (row['Movimentação'] || '').toString().trim();
		const type = resolveType(movType, row);
		if (!type) continue;

		// Skip non-financial movements (updates, splits tracked differently)
		if (type === 'update' || type === 'split') continue;

		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const price = BaseParser.parseNumber(row['Preço unitário']);
		const amount = BaseParser.parseNumber(row['Valor da Operação']);
		const direction = (row['Entrada/Saída'] || '').toString().trim();

		transactions.push({
			ticker,
			type,
			date,
			quantity,
			price,
			amount,
			currency: 'BRL',
			direction,
			institution: (row['Instituição'] || '').toString().trim(),
			source: 'b3-movimentacao',
		});

		// Track unique tickers
		if (!assets.has(ticker)) {
			assets.set(ticker, {
				ticker,
				name: BaseParser.extractProductName(product) || ticker,
				assetClass: BaseParser.inferAssetClass(ticker),
				country: 'BR',
				currency: 'BRL',
			});
		}

		// Create alias from product string
		const productName = BaseParser.extractProductName(product);
		if (productName && !aliases.has(productName.toLowerCase())) {
			aliases.set(productName.toLowerCase(), {
				normalizedName: productName.toLowerCase(),
				ticker,
				source: 'b3',
			});
		}
	}

	return {
		assets: Array.from(assets.values()),
		transactions,
		aliases: Array.from(aliases.values()),
	};
};

function resolveType(movType, row) {
	// Direct match
	if (MOVEMENT_TYPE_MAP[movType]) return MOVEMENT_TYPE_MAP[movType];

	// Partial match for compound types
	for (const [key, value] of Object.entries(MOVEMENT_TYPE_MAP)) {
		if (movType.includes(key)) return value;
	}

	return null;
}

module.exports = parser;
