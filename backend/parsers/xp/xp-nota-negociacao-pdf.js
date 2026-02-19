/**
 * XP Nota de Negociacao PDF parser.
 *
 * Parses "Negocios realizados" rows and emits buy/sell transactions.
 */
const BaseParser = require('../base-parser');

const parser = new BaseParser({ id: 'xp-nota-negociacao-pdf', provider: 'xp' });

const TYPE_BY_SIDE = {
	C: 'buy',
	V: 'sell',
};

const normalizeText = (value) => (
	String(value || '')
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
);

const toIsoDate = (value) => BaseParser.parseDate(String(value || '').trim());

const parseTradeDate = (text, lines = []) => {
	const normalizedLines = (lines || []).map((line) => normalizeText(line));
	const headerIndex = normalizedLines.findIndex((line) => line.includes('data pregao'));

	if (headerIndex >= 0) {
		for (let index = headerIndex; index <= Math.min(lines.length - 1, headerIndex + 4); index += 1) {
			const match = String(lines[index] || '').match(/(\d{2}\/\d{2}\/\d{4})/);
			if (match) return toIsoDate(match[1]);
		}
	}

	const broadMatch = String(text || '').match(/Data preg[aã]o[\s\S]{0,2000}?(\d{2}\/\d{2}\/\d{4})/i);
	if (broadMatch) return toIsoDate(broadMatch[1]);

	const firstDate = String(text || '').match(/(\d{2}\/\d{2}\/\d{4})/);
	if (firstDate) return toIsoDate(firstDate[1]);
	return null;
};

const parseTickerFromSegment = (segment) => {
	const tickers = String(segment || '').match(/\b[A-Z]{4}\d{1,2}[A-Z]?\b/g);
	if (!Array.isArray(tickers) || tickers.length === 0) return null;
	return BaseParser.normalizeTicker(tickers[tickers.length - 1]) || tickers[tickers.length - 1];
};

const parseTradeRow = (line, tradeDate) => {
	const compactLine = String(line || '').replace(/\s+/g, ' ').trim();
	if (!compactLine) return null;

	// Row tail pattern:
	// "<quantidade> <preco> <valor> <D/C>"
	const tailMatch = compactLine.match(/(\d[\d.,]*)\s+(\d[\d.,]*)\s+(\d[\d.,]*)\s+([DC])$/);
	if (!tailMatch) return null;

	const sideMatch = compactLine.match(/^\S+\s+([CV])\s+/);
	if (!sideMatch) return null;
	const side = sideMatch[1];
	const type = TYPE_BY_SIDE[side];
	if (!type) return null;

	const prefix = compactLine.slice(0, tailMatch.index).trim();
	const remainderMatch = prefix.match(/^\S+\s+[CV]\s+(.+)$/);
	if (!remainderMatch) return null;
	const tradeSegment = remainderMatch[1].trim();
	const ticker = parseTickerFromSegment(tradeSegment);
	if (!ticker) return null;

	const tickerIndex = tradeSegment.lastIndexOf(ticker);
	const beforeTicker = tickerIndex >= 0
		? tradeSegment.slice(0, tickerIndex).trim()
		: tradeSegment;
	const beforeTickerTokens = beforeTicker.split(/\s+/).filter(Boolean);
	const productTokens = beforeTickerTokens.length > 1
		? beforeTickerTokens.slice(1)
		: beforeTickerTokens;
	const productName = productTokens.join(' ').trim() || ticker;
	const market = beforeTickerTokens[0] || '';

	const quantity = BaseParser.parseNumber(tailMatch[1]);
	const price = BaseParser.parseNumber(tailMatch[2]);
	const amount = BaseParser.parseNumber(tailMatch[3]);

	if (!tradeDate || !Number.isFinite(quantity) || quantity <= 0) return null;

	return {
		ticker,
		type,
		date: tradeDate,
		quantity,
		price,
		amount,
		currency: 'BRL',
		institution: 'XP Investimentos',
		market,
		source: 'xp-nota-negociacao-pdf',
		productName,
	};
};

parser.detect = function (fileName, _sheetNames, sampleRows) {
	if (!/\.pdf$/i.test(String(fileName || ''))) return false;

	const sampleText = (sampleRows || [])
		.map((row) => (Array.isArray(row) ? String(row[0] || '') : ''))
		.join('\n');
	const normalized = normalizeText(sampleText);

	return (
		normalized.includes('nota de negociacao')
		&& normalized.includes('negocios realizados')
		&& normalized.includes('xp investimentos')
	);
};

parser.parse = function (workbook) {
	const text = String(workbook?.__pdfText || '').trim();
	if (!text) return { assets: [], transactions: [], aliases: [] };

	const lines = Array.isArray(workbook?.__pdfLines)
		? workbook.__pdfLines
		: text.split(/\r?\n/);
	const normalizedLines = lines.map((line) => normalizeText(line));
	const tradeDate = parseTradeDate(text, lines);

	const startIndex = normalizedLines.findIndex((line) => line.includes('negocios realizados'));
	if (startIndex < 0) return { assets: [], transactions: [], aliases: [] };

	const transactions = [];
	const assetsByTicker = new Map();
	const aliasKeys = new Set();
	const aliases = [];

	for (let index = startIndex + 1; index < lines.length; index += 1) {
		const normalizedLine = normalizedLines[index] || '';
		if (
			normalizedLine.includes('resumo dos negocios')
			|| normalizedLine.includes('resumo financeiro')
		) {
			break;
		}

		const transaction = parseTradeRow(lines[index], tradeDate);
		if (!transaction) continue;
		transactions.push(transaction);

		if (!assetsByTicker.has(transaction.ticker)) {
			assetsByTicker.set(transaction.ticker, {
				ticker: transaction.ticker,
				name: transaction.productName || transaction.ticker,
				assetClass: BaseParser.inferAssetClass(transaction.ticker, transaction.productName || ''),
				country: 'BR',
				currency: 'BRL',
			});
		}

		const normalizedName = String(transaction.productName || '')
			.trim()
			.toLowerCase();
		if (normalizedName && normalizedName !== transaction.ticker.toLowerCase()) {
			const key = `${normalizedName}|${transaction.ticker}`;
			if (!aliasKeys.has(key)) {
				aliasKeys.add(key);
				aliases.push({
					normalizedName,
					ticker: transaction.ticker,
					source: 'xp',
				});
			}
		}
	}

	return {
		assets: Array.from(assetsByTicker.values()),
		transactions: transactions.map(({ productName, ...item }) => item),
		aliases,
	};
};

module.exports = parser;
