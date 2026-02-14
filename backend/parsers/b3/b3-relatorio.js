/**
 * B3 Relatório Consolidado (Consolidated Report) parser.
 * Handles files like relatorio-consolidado-anual-*.xlsx and relatorio-consolidado-mensal-*.xlsx.
 *
 * Variable sheets across years:
 * - Proventos Recebidos (all years) - dividends/JCP received
 * - Posição - Fundos (most years) - FII holdings snapshot
 * - Posição - Ações (some years) - Stock holdings snapshot
 * - Posição - Tesouro Direto (some years)
 * - Posição - Renda Fixa (some years)
 * - Negociações (monthly reports) - trade summary
 * - Reembolsos de Empréstimo (some years) - lending reimbursements
 */
const BaseParser = require('../base-parser');

const RELATORIO_INDICATORS = [
	'Proventos Recebidos',
	'Posição - Fundos',
	'Posição - Ações',
	'Negociações',
	'Reembolsos de Empréstimo',
];

const parser = new BaseParser({ id: 'b3-relatorio', provider: 'b3' });

parser.detect = function (fileName, sheetNames) {
	if (/relatorio/i.test(fileName)) return true;
	// Match by having multiple position sheets + proventos
	const hasProventos = sheetNames.includes('Proventos Recebidos');
	const hasPosition = sheetNames.some(s => s.startsWith('Posição'));
	return hasProventos && hasPosition;
};

parser.parse = function (workbook, options = {}) {
	const assets = new Map();
	const transactions = [];
	const aliases = new Map();

	// Extract report period from filename if possible
	const sourceFile = options.sourceFile || '';
	const yearMatch = sourceFile.match(/(\d{4})/);
	const reportYear = yearMatch ? yearMatch[1] : null;

	// Parse Proventos Recebidos (dividends/JCP) - present in all reports
	parseProventos(workbook, transactions, reportYear);

	// Parse Posição - Fundos (FII holdings)
	parsePosicaoFundos(workbook, assets, aliases);

	// Parse Posição - Ações (Stock holdings)
	parsePosicaoAcoes(workbook, assets, aliases);

	// Parse Posição - Tesouro Direto
	parsePosicaoTesouro(workbook, assets);

	// Parse Posição - Renda Fixa
	parsePosicaoRendaFixa(workbook, assets);

	// Parse Negociações (monthly trade summary)
	parseNegociacoes(workbook, transactions);

	// Parse Reembolsos de Empréstimo
	parseReembolsos(workbook, transactions, reportYear);

	return {
		assets: Array.from(assets.values()),
		transactions,
		aliases: Array.from(aliases.values()),
	};
};

function parseProventos(workbook, transactions, reportYear) {
	const rows = BaseParser.sheetToRows(workbook, 'Proventos Recebidos');
	if (!rows.length) return;

	// Monthly reports have more columns (Pagamento, Instituição, Quantidade, Preço unitário)
	const isMonthly = rows[0] && 'Pagamento' in rows[0];

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		const ticker = BaseParser.extractTicker(product) || product.toUpperCase();
		if (!ticker) continue;

		const eventType = (row['Tipo de Evento'] || '').toString().trim();
		const type = eventType.toLowerCase().includes('juros') ? 'jcp'
			: eventType.toLowerCase().includes('reembolso') ? 'reimbursement'
			: 'dividend';
		const amount = BaseParser.parseNumber(row['Valor líquido']);

		let date, quantity, price;
		if (isMonthly) {
			date = BaseParser.parseDate(row['Pagamento']);
			quantity = BaseParser.parseNumber(row['Quantidade']);
			price = BaseParser.parseNumber(row['Preço unitário']);
		} else {
			// Annual reports don't have date per row - use Dec 31 of report year
			date = reportYear ? `${reportYear}-12-31` : null;
			quantity = 0;
			price = 0;
		}

		if (!date) continue;

		transactions.push({
			ticker,
			type,
			date,
			quantity,
			price,
			amount,
			currency: 'BRL',
			source: 'b3-relatorio',
		});
	}
}

function parsePosicaoFundos(workbook, assets, aliases) {
	const rows = BaseParser.sheetToRows(workbook, 'Posição - Fundos');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		const ticker = (row['Código de Negociação'] || '').toString().trim().toUpperCase();
		if (!ticker) continue;

		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const price = BaseParser.parseNumber(row['Preço de Fechamento']);
		const value = BaseParser.parseNumber(row['Valor Atualizado']);

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

function parsePosicaoAcoes(workbook, assets, aliases) {
	const rows = BaseParser.sheetToRows(workbook, 'Posição - Ações');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		const ticker = (row['Código de Negociação'] || '').toString().trim().toUpperCase();
		if (!ticker) continue;

		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const price = BaseParser.parseNumber(row['Preço de Fechamento']);
		const value = BaseParser.parseNumber(row['Valor Atualizado']);

		if (assets.has(ticker)) {
			const existing = assets.get(ticker);
			existing.quantity += quantity;
			existing.value += value;
		} else {
			assets.set(ticker, {
				ticker,
				name: BaseParser.extractProductName(product) || ticker,
				assetClass: 'stock',
				country: 'BR',
				currency: 'BRL',
				quantity,
				price,
				value,
			});
		}

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

function parsePosicaoTesouro(workbook, assets) {
	const rows = BaseParser.sheetToRows(workbook, 'Posição - Tesouro Direto');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		if (!product) continue;

		const ticker = product.replace(/\s+/g, '-').toUpperCase();
		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const appliedValue = BaseParser.parseNumber(row['Valor Aplicado']);
		const currentValue = BaseParser.parseNumber(row['Valor Atualizado']);

		if (assets.has(ticker)) {
			const existing = assets.get(ticker);
			existing.quantity += quantity;
			existing.value += currentValue;
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
				indexer: (row['Indexador'] || '').toString().trim(),
				maturity: BaseParser.parseDate(row['Vencimento']),
			});
		}
	}
}

function parsePosicaoRendaFixa(workbook, assets) {
	const rows = BaseParser.sheetToRows(workbook, 'Posição - Renda Fixa');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		if (!product) continue;

		const code = (row['Código'] || '').toString().trim();
		const ticker = code || product.replace(/\s+/g, '-').substring(0, 20).toUpperCase();
		const quantity = BaseParser.parseNumber(row['Quantidade']);
		const value = BaseParser.parseNumber(row['Valor Atualizado CURVA'])
			|| BaseParser.parseNumber(row['Valor Atualizado MTM'])
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
				value,
				indexer: (row['Indexador'] || '').toString().trim(),
				maturity: BaseParser.parseDate(row['Vencimento']),
			});
		}
	}
}

function parseNegociacoes(workbook, transactions) {
	const rows = BaseParser.sheetToRows(workbook, 'Negociações');

	for (const row of rows) {
		const ticker = (row['Código de Negociação'] || '').toString().trim().toUpperCase();
		if (!ticker) continue;

		const startDate = BaseParser.parseDate(row['Período (Inicial)']);
		const endDate = BaseParser.parseDate(row['Período (Final)']) || startDate;
		const buyQty = BaseParser.parseNumber(row['Quantidade (Compra)']);
		const sellQty = BaseParser.parseNumber(row['Quantidade (Venda)']);
		const avgBuyPrice = BaseParser.parseNumber(row['Preço Médio (Compra)']);
		const avgSellPrice = BaseParser.parseNumber(row['Preço Médio (Venda)']);

		if (buyQty > 0) {
			transactions.push({
				ticker,
				type: 'buy',
				date: endDate || startDate,
				quantity: buyQty,
				price: avgBuyPrice,
				amount: buyQty * avgBuyPrice,
				currency: 'BRL',
				source: 'b3-relatorio',
			});
		}

		if (sellQty > 0) {
			transactions.push({
				ticker,
				type: 'sell',
				date: endDate || startDate,
				quantity: sellQty,
				price: avgSellPrice,
				amount: sellQty * avgSellPrice,
				currency: 'BRL',
				source: 'b3-relatorio',
			});
		}
	}
}

function parseReembolsos(workbook, transactions, reportYear) {
	const rows = BaseParser.sheetToRows(workbook, 'Reembolsos de Empréstimo');

	for (const row of rows) {
		const product = (row['Produto'] || '').toString().trim();
		const ticker = BaseParser.extractTicker(product) || product.toUpperCase();
		if (!ticker) continue;

		const amount = BaseParser.parseNumber(row['Valor líquido']);
		const date = reportYear ? `${reportYear}-12-31` : null;
		if (!date) continue;

		transactions.push({
			ticker,
			type: 'reimbursement',
			date,
			quantity: 0,
			price: 0,
			amount,
			currency: 'BRL',
			source: 'b3-relatorio',
		});
	}
}

module.exports = parser;
