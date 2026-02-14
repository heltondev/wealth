/**
 * B3 Posição (Position/Holdings) parser.
 * Handles files like posicao-*.xlsx with sheets: COE, Fundo de Investimento,
 * Renda Fixa, Tesouro Direto.
 *
 * Extracts current holdings as assets with quantity and value snapshots.
 */
const BaseParser = require('../base-parser');

const POSITION_SHEETS = ['Fundo de Investimento', 'Renda Fixa', 'Tesouro Direto', 'COE'];

const parser = new BaseParser({ id: 'b3-posicao', provider: 'b3' });

parser.detect = function (fileName, sheetNames) {
	// Match posicao-*.xlsx (not relatorio-*)
	if (/posicao/i.test(fileName)) {
		return sheetNames.some(s => POSITION_SHEETS.includes(s));
	}
	return false;
};

parser.parse = function (workbook, options = {}) {
	const assets = new Map(); // ticker → aggregated asset
	const transactions = [];
	const aliases = new Map();

	// Parse Fundo de Investimento (FIIs)
	parseFundos(workbook, assets, aliases);

	// Parse Tesouro Direto (Treasury bonds)
	parseTesouro(workbook, assets);

	// Parse Renda Fixa (Fixed income - CDB, etc.)
	parseRendaFixa(workbook, assets);

	return {
		assets: Array.from(assets.values()),
		transactions,
		aliases: Array.from(aliases.values()),
	};
};

function parseFundos(workbook, assets, aliases) {
	const rows = BaseParser.sheetToRows(workbook, 'Fundo de Investimento');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		const ticker = (row['Código de Negociação'] || '').toString().trim().toUpperCase();
		if (!ticker) continue;

		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const price = BaseParser.parseNumber(row['Preço de Fechamento']);
		const value = BaseParser.parseNumber(row['Valor Atualizado']);

		// Aggregate across institutions (same ticker at NU + XP)
		if (assets.has(ticker)) {
			const existing = assets.get(ticker);
			existing.quantity += quantity;
			existing.value += value;
		} else {
			assets.set(ticker, {
				ticker,
				name: BaseParser.extractProductName(product) || ticker,
				assetClass: 'fii',
				country: 'BR',
				currency: 'BRL',
				quantity,
				price,
				value,
			});
		}

		// Create alias
		const productName = BaseParser.extractProductName(product);
		if (productName && !aliases.has(productName.toLowerCase())) {
			aliases.set(productName.toLowerCase(), {
				normalizedName: productName.toLowerCase(),
				ticker,
				source: 'b3',
			});
		}
	}
}

function parseTesouro(workbook, assets) {
	const rows = BaseParser.sheetToRows(workbook, 'Tesouro Direto');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		if (!product) continue;

		// Use product name as ticker for treasury bonds (e.g. "Tesouro IPCA+ 2029")
		const ticker = product.replace(/\s+/g, '-').toUpperCase();
		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const appliedValue = BaseParser.parseNumber(row['Valor Aplicado']);
		const currentValue = BaseParser.parseNumber(row['Valor Atualizado']);
		const indexer = (row['Indexador'] || '').toString().trim();
		const maturity = BaseParser.parseDate(row['Vencimento']);

		if (assets.has(ticker)) {
			const existing = assets.get(ticker);
			existing.quantity += quantity;
			existing.value += currentValue;
			existing.appliedValue = (existing.appliedValue || 0) + appliedValue;
		} else {
			assets.set(ticker, {
				ticker,
				name: product,
				assetClass: 'bond',
				country: 'BR',
				currency: 'BRL',
				quantity,
				value: currentValue,
				appliedValue,
				indexer,
				maturity,
			});
		}
	}
}

function parseRendaFixa(workbook, assets) {
	const rows = BaseParser.sheetToRows(workbook, 'Renda Fixa');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		if (!product) continue;

		const code = (row['Código'] || '').toString().trim();
		const ticker = code || product.replace(/\s+/g, '-').substring(0, 20).toUpperCase();
		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const indexer = (row['Indexador'] || '').toString().trim();
		const maturity = BaseParser.parseDate(row['Vencimento']);

		// Try different value columns (format varies across years)
		const value = BaseParser.parseNumber(row['Valor Atualizado CURVA'])
			|| BaseParser.parseNumber(row['Valor Atualizado MTM'])
			|| 0;
		const unitPrice = BaseParser.parseNumber(row['Preço Atualizado CURVA'])
			|| BaseParser.parseNumber(row['Preço Atualizado MTM'])
			|| 0;

		if (assets.has(ticker)) {
			const existing = assets.get(ticker);
			existing.quantity += quantity;
			existing.value += value;
		} else {
			assets.set(ticker, {
				ticker,
				name: product,
				assetClass: 'bond',
				country: 'BR',
				currency: 'BRL',
				quantity,
				price: unitPrice,
				value,
				indexer,
				maturity,
			});
		}
	}
}

module.exports = parser;
