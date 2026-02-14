const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');
const BaseParser = require('../base-parser');

// --- BaseParser utility tests ---

test('parseDate: DD/MM/YYYY → YYYY-MM-DD', () => {
	assert.equal(BaseParser.parseDate('04/02/2026'), '2026-02-04');
	assert.equal(BaseParser.parseDate('31/12/2025'), '2025-12-31');
	assert.equal(BaseParser.parseDate('01/01/2020'), '2020-01-01');
});

test('parseDate: already ISO format', () => {
	assert.equal(BaseParser.parseDate('2026-02-04'), '2026-02-04');
	assert.equal(BaseParser.parseDate('2025-12-31T00:00:00Z'), '2025-12-31');
});

test('parseDate: handles null/empty/dash', () => {
	assert.equal(BaseParser.parseDate(null), null);
	assert.equal(BaseParser.parseDate(''), null);
	assert.equal(BaseParser.parseDate('-'), null);
	assert.equal(BaseParser.parseDate(undefined), null);
});

test('extractTicker: product string with name', () => {
	assert.equal(
		BaseParser.extractTicker('KNCR11 - KINEA RENDIMENTOS IMOBILIÁRIOS FDO INV IMOB - FII'),
		'KNCR11'
	);
	assert.equal(
		BaseParser.extractTicker('ALZR11 - ALIANZA TRUST RENDA IMOBILIARIA FII RESP LIM'),
		'ALZR11'
	);
	assert.equal(
		BaseParser.extractTicker('PETR4 - PETROLEO BRASILEIRO S.A. PETROBRAS'),
		'PETR4'
	);
});

test('extractTicker: strips trailing L suffix (ex-rights)', () => {
	assert.equal(
		BaseParser.extractTicker('ALZR11L - ALIANZA TRUST RENDA IMOBILIARIA FDO INV IMOB'),
		'ALZR11'
	);
	assert.equal(
		BaseParser.extractTicker('HGLG12 - PÁTRIA LOG - FDO INV IMOB - RESPONSABILIDADE LTDA.'),
		'HGLG12'
	);
});

test('extractTicker: standalone ticker', () => {
	assert.equal(BaseParser.extractTicker('PETR4'), 'PETR4');
	assert.equal(BaseParser.extractTicker('BCFF11'), 'BCFF11');
	assert.equal(BaseParser.extractTicker('AURE3'), 'AURE3');
});

test('extractTicker: returns null for non-tickers', () => {
	assert.equal(BaseParser.extractTicker(''), null);
	assert.equal(BaseParser.extractTicker(null), null);
	assert.equal(BaseParser.extractTicker('Some random text'), null);
});

test('extractProductName: extracts name after ticker', () => {
	assert.equal(
		BaseParser.extractProductName('KNCR11 - KINEA RENDIMENTOS IMOBILIÁRIOS FDO INV IMOB - FII'),
		'KINEA RENDIMENTOS IMOBILIÁRIOS FDO INV IMOB - FII'
	);
	assert.equal(
		BaseParser.extractProductName('PETR4 - PETROLEO BRASILEIRO S.A. PETROBRAS'),
		'PETROLEO BRASILEIRO S.A. PETROBRAS'
	);
});

test('extractProductName: returns null for no dash', () => {
	assert.equal(BaseParser.extractProductName('PETR4'), null);
	assert.equal(BaseParser.extractProductName(''), null);
	assert.equal(BaseParser.extractProductName(null), null);
});

test('parseNumber: handles various B3 formats', () => {
	assert.equal(BaseParser.parseNumber(97.03), 97.03);
	assert.equal(BaseParser.parseNumber('582.18'), 582.18);
	assert.equal(BaseParser.parseNumber('-'), 0);
	assert.equal(BaseParser.parseNumber(''), 0);
	assert.equal(BaseParser.parseNumber(null), 0);
	assert.equal(BaseParser.parseNumber(undefined), 0);
	assert.equal(BaseParser.parseNumber(0), 0);
});

test('inferAssetClass: FII tickers by pattern', () => {
	assert.equal(BaseParser.inferAssetClass('HGLG11'), 'fii');
	assert.equal(BaseParser.inferAssetClass('KNCR11'), 'fii');
	assert.equal(BaseParser.inferAssetClass('ALZR11'), 'fii');
});

test('inferAssetClass: FII subscription receipts (12, 13, 14)', () => {
	assert.equal(BaseParser.inferAssetClass('HGLG12'), 'fii');
	assert.equal(BaseParser.inferAssetClass('ALZR12'), 'fii');
	assert.equal(BaseParser.inferAssetClass('BTLG13'), 'fii');
	assert.equal(BaseParser.inferAssetClass('XPML14'), 'fii');
	assert.equal(BaseParser.inferAssetClass('XPML18'), 'fii');
});

test('inferAssetClass: FII from product name', () => {
	assert.equal(BaseParser.inferAssetClass('BCFF12', 'FDO INV IMOB - FII BTG PACTUAL FUNDO DE FUNDOS'), 'fii');
	assert.equal(BaseParser.inferAssetClass('XPLG12', 'XP LOG FUNDO DE INVESTIMENTO IMOBILIARIO FII'), 'fii');
	assert.equal(BaseParser.inferAssetClass('GARE12', 'FUND. DE INVEST. IMOBILIÁRIO GUARDIAN REAL ESTATE'), 'fii');
});

test('inferAssetClass: stock tickers', () => {
	assert.equal(BaseParser.inferAssetClass('PETR4'), 'stock');
	assert.equal(BaseParser.inferAssetClass('BBAS3'), 'stock');
	assert.equal(BaseParser.inferAssetClass('VALE3'), 'stock');
});

test('inferAssetClass: derivatives', () => {
	assert.equal(BaseParser.inferAssetClass('WINM23'), 'derivative');
	assert.equal(BaseParser.inferAssetClass('WDOG23'), 'derivative');
	assert.equal(BaseParser.inferAssetClass('DOLZ22'), 'derivative');
});

test('transactionKey: creates consistent dedup key', () => {
	const key = BaseParser.transactionKey({
		ticker: 'HGLG11',
		date: '2026-02-04',
		type: 'buy',
		amount: 582.18,
		quantity: 6,
	});
	assert.equal(key, 'HGLG11|2026-02-04|buy|582.18|6');
});

// --- B3 Negociação parser tests ---

test('b3-negociacao: detects negociacao files', () => {
	const parser = require('./b3-negociacao');
	assert.equal(parser.detect('negociacao-2026-02-14.xlsx', ['Negociação'], []), true);
	assert.equal(parser.detect('other-file.xlsx', ['Negociação'], []), true);
	assert.equal(parser.detect('negociacao.xlsx', ['SomeSheet'], []), false);
	assert.equal(parser.detect('movimentacao.xlsx', ['Movimentação'], []), false);
});

test('b3-negociacao: parses trade data', () => {
	const XLSX = require('xlsx');
	const parser = require('./b3-negociacao');

	// Create a mock workbook
	const data = [
		['Data do Negócio', 'Tipo de Movimentação', 'Mercado', 'Prazo/Vencimento', 'Instituição', 'Código de Negociação', 'Quantidade', 'Preço', 'Valor'],
		['04/02/2026', 'Compra', 'Mercado à Vista', '-', 'XP INVESTIMENTOS', 'HGCR11', 6, 97.03, 582.18],
		['04/02/2026', 'Venda', 'Mercado à Vista', '-', 'NU INVEST', 'PETR4', 100, 38.5, 3850],
	];
	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Negociação');

	const result = parser.parse(wb);

	assert.equal(result.transactions.length, 2);
	assert.equal(result.transactions[0].ticker, 'HGCR11');
	assert.equal(result.transactions[0].type, 'buy');
	assert.equal(result.transactions[0].date, '2026-02-04');
	assert.equal(result.transactions[0].price, 97.03);
	assert.equal(result.transactions[0].amount, 582.18);
	assert.equal(result.transactions[0].quantity, 6);

	assert.equal(result.transactions[1].ticker, 'PETR4');
	assert.equal(result.transactions[1].type, 'sell');

	assert.equal(result.assets.length, 2);
	assert.equal(result.assets[0].ticker, 'HGCR11');
	assert.equal(result.assets[0].assetClass, 'fii');
	assert.equal(result.assets[1].ticker, 'PETR4');
	assert.equal(result.assets[1].assetClass, 'stock');
});

// --- B3 Movimentação parser tests ---

test('b3-movimentacao: detects movimentacao files', () => {
	const parser = require('./b3-movimentacao');
	assert.equal(parser.detect('movimentacao-2026.xlsx', ['Movimentação'], []), true);
	assert.equal(parser.detect('other.xlsx', ['Movimentação'], []), true);
	assert.equal(parser.detect('negociacao.xlsx', ['Negociação'], []), false);
});

test('b3-movimentacao: parses dividend and subscription data', () => {
	const XLSX = require('xlsx');
	const parser = require('./b3-movimentacao');

	const data = [
		['Entrada/Saída', 'Data', 'Movimentação', 'Produto', 'Instituição', 'Quantidade', 'Preço unitário', 'Valor da Operação'],
		['Credito', '12/02/2026', 'Rendimento', 'KNCR11 - KINEA RENDIMENTOS IMOBILIÁRIOS FDO INV IMOB - FII', 'NU INVEST', 130, 1.2, 156],
		['Credito', '12/02/2026', 'Direito de Subscrição', 'HGLG12 - PÁTRIA LOG FDO INV IMOB', 'NU INVEST', 32, '-', '-'],
		['Credito', '10/02/2026', 'Juros Sobre Capital Próprio', 'BBDC4 - BCO BRADESCO S.A.', 'XP', 46, 0.5, 23],
	];
	const ws = XLSX.utils.aoa_to_sheet(data);
	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, ws, 'Movimentação');

	const result = parser.parse(wb);

	assert.equal(result.transactions.length, 3);

	// Dividend
	assert.equal(result.transactions[0].ticker, 'KNCR11');
	assert.equal(result.transactions[0].type, 'dividend');
	assert.equal(result.transactions[0].amount, 156);

	// Subscription
	assert.equal(result.transactions[1].ticker, 'HGLG12');
	assert.equal(result.transactions[1].type, 'subscription');

	// JCP
	assert.equal(result.transactions[2].ticker, 'BBDC4');
	assert.equal(result.transactions[2].type, 'jcp');

	// Aliases created
	assert.ok(result.aliases.length >= 2);
	const kncr = result.aliases.find(a => a.ticker === 'KNCR11');
	assert.ok(kncr);
	assert.equal(kncr.source, 'b3');
});

// --- B3 Posição parser tests ---

test('b3-posicao: detects posicao files', () => {
	const parser = require('./b3-posicao');
	assert.equal(parser.detect('posicao-2026.xlsx', ['COE', 'Fundo de Investimento', 'Renda Fixa', 'Tesouro Direto'], []), true);
	assert.equal(parser.detect('relatorio-consolidado.xlsx', ['Proventos Recebidos'], []), false);
	assert.equal(parser.detect('negociacao.xlsx', ['Negociação'], []), false);
});

test('b3-posicao: parses FII holdings and aggregates across institutions', () => {
	const XLSX = require('xlsx');
	const parser = require('./b3-posicao');

	const fundoData = [
		['Produto', 'Instituição', 'Conta', 'Código de Negociação', 'CNPJ do Fundo', 'Código ISIN / Distribuição', 'Tipo', 'Administrador', 'Quantidade', 'Quantidade Disponível', 'Quantidade Indisponível', 'Motivo', 'Preço de Fechamento', 'Valor Atualizado'],
		['ALZR11 - ALIANZA TRUST', 'NU INVEST', '123', 'ALZR11', '123', 'BR123', 'Cotas', 'BTG', 2000, 2000, '-', '-', 10.81, 21620],
		['ALZR11 - ALIANZA TRUST', 'XP INVEST', '456', 'ALZR11', '123', 'BR123', 'Cotas', 'BTG', 73, 73, '-', '-', 10.81, 789.13],
	];

	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(fundoData), 'Fundo de Investimento');
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['h']]), 'COE');
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['h']]), 'Renda Fixa');
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['h']]), 'Tesouro Direto');

	const result = parser.parse(wb);

	// Should aggregate: 2000 + 73 = 2073
	const alzr = result.assets.find(a => a.ticker === 'ALZR11');
	assert.ok(alzr);
	assert.equal(alzr.quantity, 2073);
	assert.equal(alzr.assetClass, 'fii');
	assert.ok(Math.abs(alzr.value - 22409.13) < 0.01);
});

// --- B3 Relatório parser tests ---

test('b3-relatorio: detects relatorio files', () => {
	const parser = require('./b3-relatorio');
	assert.equal(parser.detect('relatorio-consolidado-anual-2024.xlsx', ['Proventos Recebidos', 'Posição - Fundos'], []), true);
	assert.equal(parser.detect('relatorio-consolidado-mensal-2026.xlsx', ['Negociações', 'Proventos Recebidos'], []), true);
	assert.equal(parser.detect('posicao-2026.xlsx', ['COE', 'Fundo de Investimento'], []), false);
});

test('b3-relatorio: parses proventos (annual format)', () => {
	const XLSX = require('xlsx');
	const parser = require('./b3-relatorio');

	const proventosData = [
		['Produto', 'Tipo de Evento', 'Valor líquido'],
		['PETR4', 'Dividendo', 78.74],
		['BCFF11', 'Rendimento', 2.21],
		['BBDC3', 'Juros Sobre Capital Próprio', 1.53],
	];

	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(proventosData), 'Proventos Recebidos');
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['h']]), 'Posição - Fundos');

	const result = parser.parse(wb, { sourceFile: 'relatorio-consolidado-anual-2024.xlsx' });

	assert.equal(result.transactions.length, 3);
	assert.equal(result.transactions[0].ticker, 'PETR4');
	assert.equal(result.transactions[0].type, 'dividend');
	assert.equal(result.transactions[0].amount, 78.74);
	assert.equal(result.transactions[0].date, '2024-12-31');

	assert.equal(result.transactions[2].type, 'jcp');
});

test('b3-relatorio: parses negociacoes (monthly format)', () => {
	const XLSX = require('xlsx');
	const parser = require('./b3-relatorio');

	const negData = [
		['Código de Negociação', 'Período (Inicial)', 'Período (Final)', 'Instituição', 'Quantidade (Compra)', 'Quantidade (Venda)', 'Quantidade (Líquida)', 'Preço Médio (Compra)', 'Preço Médio (Venda)'],
		['ALZR11', '06/01/2026', '26/01/2026', 'XP INVEST', 73, 0, 73, 10.71, 0],
		['GGRC11', '02/01/2026', '-', 'XP INVEST', 1, 0, 1, 9.9, 0],
	];

	const wb = XLSX.utils.book_new();
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['h']]), 'Proventos Recebidos');
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(negData), 'Negociações');
	XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['h']]), 'Posição - Fundos');

	const result = parser.parse(wb, { sourceFile: 'relatorio-consolidado-mensal-2026-janeiro.xlsx' });

	const buys = result.transactions.filter(t => t.type === 'buy');
	assert.equal(buys.length, 2);
	assert.equal(buys[0].ticker, 'ALZR11');
	assert.equal(buys[0].quantity, 73);
	assert.equal(buys[0].price, 10.71);
});

// --- Parser registry tests ---

test('parser registry: detectProvider matches correct parser', () => {
	const fs = require('fs');
	const dataDir = path.resolve(__dirname, '../../../.data/B3');

	// Skip if no data files available (CI environment)
	if (!fs.existsSync(dataDir)) return;

	const { detectProvider } = require('../index');

	// Test with actual files if available
	const negFile = findFile(dataDir, /negociacao.*\.xlsx$/);
	if (negFile) {
		const result = detectProvider(negFile);
		assert.ok(result, 'Should detect negociacao parser');
		assert.equal(result.parser.id, 'b3-negociacao');
	}

	const movFile = findFile(dataDir, /movimentacao.*\.xlsx$/);
	if (movFile) {
		const result = detectProvider(movFile);
		assert.ok(result, 'Should detect movimentacao parser');
		assert.equal(result.parser.id, 'b3-movimentacao');
	}

	const posFile = findFile(dataDir, /posicao.*\.xlsx$/);
	if (posFile) {
		const result = detectProvider(posFile);
		assert.ok(result, 'Should detect posicao parser');
		assert.equal(result.parser.id, 'b3-posicao');
	}

	const relFile = findFile(dataDir, /relatorio.*\.xlsx$/);
	if (relFile) {
		const result = detectProvider(relFile);
		assert.ok(result, 'Should detect relatorio parser');
		assert.equal(result.parser.id, 'b3-relatorio');
	}
});

test('parser registry: listParsers returns all parsers', () => {
	const { listParsers } = require('../index');
	const list = listParsers();
	assert.ok(list.length >= 4);
	assert.ok(list.some(p => p.id === 'b3-negociacao'));
	assert.ok(list.some(p => p.id === 'b3-movimentacao'));
	assert.ok(list.some(p => p.id === 'b3-posicao'));
	assert.ok(list.some(p => p.id === 'b3-relatorio'));
});

// --- Integration tests with real files ---

test('integration: parse all B3 files without errors', () => {
	const fs = require('fs');
	const dataDir = path.resolve(__dirname, '../../../.data/B3');
	if (!fs.existsSync(dataDir)) return;

	const { detectProvider } = require('../index');
	const files = findAllXlsx(dataDir);

	let totalAssets = 0;
	let totalTransactions = 0;
	let totalAliases = 0;

	for (const file of files) {
		const result = detectProvider(file);
		if (!result) continue;

		const parsed = result.parser.parse(result.workbook, { sourceFile: path.basename(file) });
		totalAssets += parsed.assets.length;
		totalTransactions += parsed.transactions.length;
		totalAliases += parsed.aliases.length;

		// Verify all transactions have required fields
		for (const t of parsed.transactions) {
			assert.ok(t.ticker, `Missing ticker in ${path.basename(file)}`);
			assert.ok(t.date, `Missing date in ${path.basename(file)} for ${t.ticker}`);
			assert.ok(t.type, `Missing type in ${path.basename(file)} for ${t.ticker}`);
		}

		// Verify all assets have required fields
		for (const a of parsed.assets) {
			assert.ok(a.ticker, `Missing ticker in asset from ${path.basename(file)}`);
			assert.ok(a.assetClass, `Missing assetClass in asset from ${path.basename(file)}`);
		}
	}

	assert.ok(totalAssets > 0, 'Should have parsed some assets');
	assert.ok(totalTransactions > 0, 'Should have parsed some transactions');
});

// --- Helpers ---

function findFile(dir, pattern) {
	const fs = require('fs');
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			const found = findFile(full, pattern);
			if (found) return found;
		} else if (pattern.test(entry.name)) {
			return full;
		}
	}
	return null;
}

function findAllXlsx(dir) {
	const fs = require('fs');
	const results = [];
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			results.push(...findAllXlsx(full));
		} else if (entry.name.endsWith('.xlsx') && !entry.name.startsWith('~')) {
			results.push(full);
		}
	}
	return results;
}
