/**
 * B3 Movimentação (Movements) parser.
 * Handles files like movimentacao-*.xlsx with sheet "Movimentação".
 *
 * Columns: Entrada/Saída, Data, Movimentação, Produto, Instituição,
 *          Quantidade, Preço unitário, Valor da Operação
 */
const BaseParser = require('../base-parser');

const MOVEMENT_TYPE_MAP = {
	'rendimento': 'dividend',
	'dividendo': 'dividend',
	'juros sobre capital proprio': 'jcp',
	'transferencia - liquidacao': 'transfer',
	'direito de subscricao': 'subscription',
	'cessao de direitos': 'subscription',
	'cessao de direitos - solicitada': 'subscription',
	'resgate': 'sell',
	'leilao de fracao': 'sell',
	'bonificacao em ativos': 'dividend',
	'atualizacao': 'update',
	'desdobramento': 'split',
	'grupamento': 'split',
	'fracao em ativos': 'dividend',
	'recibo de subscricao': 'subscription',
	'compra': 'buy',
	'venda': 'sell',
	'transferencia': 'transfer',
	'vencimento': 'sell',
	'pagamento de juros': 'dividend',
	'juros': 'dividend',
	'cobranca de taxa semestral': 'tax',
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
			const productName = BaseParser.extractProductName(product);
			assets.set(ticker, {
				ticker,
				name: productName || ticker,
				assetClass: BaseParser.inferAssetClass(ticker, product),
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
	const normalizeText = (value) =>
		(value || '')
			.toString()
			.normalize('NFD')
			.replace(/[\u0300-\u036f]/g, '')
			.toLowerCase()
			.trim();

	const normalizedMovType = normalizeText(movType);
	const normalizedDirection = normalizeText(row['Entrada/Saída'] || row['Entrada/Saida'] || '');

	if (normalizedMovType === 'compra / venda') {
		if (normalizedDirection.includes('credito')) return 'buy';
		if (normalizedDirection.includes('debito')) return 'sell';
	}

	// Direct match
	if (MOVEMENT_TYPE_MAP[normalizedMovType]) return MOVEMENT_TYPE_MAP[normalizedMovType];

	// Partial match for compound types
	for (const [key, value] of Object.entries(MOVEMENT_TYPE_MAP)) {
		if (normalizedMovType.includes(key)) return value;
	}

	return null;
}

module.exports = parser;
