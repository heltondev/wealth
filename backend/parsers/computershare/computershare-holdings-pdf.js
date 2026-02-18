/**
 * Computershare plan holdings PDF parser.
 *
 * Parses the "Purchases" table exported from plan holdings statements and
 * emits buy transactions compatible with the existing import pipeline.
 */
const BaseParser = require('../base-parser');

const parser = new BaseParser({ id: 'computershare-holdings-pdf', provider: 'computershare' });

const MONTHS = {
	jan: '01',
	feb: '02',
	mar: '03',
	apr: '04',
	may: '05',
	jun: '06',
	jul: '07',
	aug: '08',
	sep: '09',
	oct: '10',
	nov: '11',
	dec: '12',
};

const PURCHASE_ROW_REGEX = /^\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+([\d\s.,]+)\s+CAD\s+([\d\s.,]+)\s+CAD\s+([\d\s.,]+)\s+CAD\s+([\d\s.,]+)\s+CAD\s+([\d\s.,]+)\s+CAD\s+([\d.,]+)\s+([\d\s.,]+)\s+CAD\s*$/;

const normalizeHeaderRow = (sampleRows) => (
	(sampleRows || [])
		.map((row) => {
			if (!Array.isArray(row)) return '';
			return String(row[0] || '').trim().toLowerCase();
		})
		.filter(Boolean)
);

const parseShortDate = (value) => {
	const text = String(value || '').trim();
	const match = text.match(/^(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})$/);
	if (!match) return null;
	const day = String(match[1]).padStart(2, '0');
	const month = MONTHS[String(match[2]).toLowerCase()];
	if (!month) return null;
	return `${match[3]}-${month}-${day}`;
};

const parseMoney = (value) => {
	const text = String(value || '').trim();
	if (!text) return 0;
	const compact = text.replace(/\s+/g, '').replace(/,/g, '.');
	const numeric = Number(compact);
	return Number.isFinite(numeric) ? numeric : 0;
};

const parseShares = (value) => {
	const numeric = Number(String(value || '').trim().replace(/,/g, '.'));
	return Number.isFinite(numeric) ? numeric : 0;
};

const normalizeTicker = (value) => (
	String(value || '')
		.trim()
		.toUpperCase()
		.replace(/[^A-Z0-9.\-]/g, '')
);

const extractTicker = (text) => {
	const fromLabel = text.match(/-\s*([A-Z]{1,6}\.[A-Z]{1,4})\s*:/);
	if (fromLabel) return normalizeTicker(fromLabel[1]);
	const fallback = text.match(/\b([A-Z]{1,6}\.[A-Z]{1,4})\b/);
	return fallback ? normalizeTicker(fallback[1]) : null;
};

const extractAssetName = (text, ticker) => {
	if (!ticker) return null;
	const escapedTicker = ticker.replace('.', '\\.');
	const regex = new RegExp(`([^\\n]+?)\\s*-\\s*${escapedTicker}\\s*:`, 'i');
	const match = text.match(regex);
	if (!match) return null;
	return String(match[1] || '').trim() || null;
};

parser.detect = function (fileName, _sheetNames, sampleRows) {
	if (!/\.pdf$/i.test(String(fileName || ''))) return false;
	const lines = normalizeHeaderRow(sampleRows);
	if (lines.length === 0) return false;
	const text = lines.join('\n');
	return (
		text.includes('plan holdings statement')
		&& text.includes('computershare')
		&& text.includes('purchases')
	);
};

parser.parse = function (workbook) {
	const text = String(workbook?.__pdfText || '').trim();
	if (!text) return { assets: [], transactions: [], aliases: [] };

	const lines = Array.isArray(workbook?.__pdfLines)
		? workbook.__pdfLines
		: text.split(/\r?\n/);

	const ticker = extractTicker(text) || 'UNKNOWN';
	const assetName = extractAssetName(text, ticker) || ticker;

	const transactions = [];
	let quantitySum = 0;
	let latestPrice = null;
	let latestTradeDate = null;

	for (let index = 0; index < lines.length; index += 1) {
		const line = String(lines[index] || '');
		const match = line.match(PURCHASE_ROW_REGEX);
		if (!match) continue;

		const allocationDate = parseShortDate(match[1]);
		const contributionDate = parseShortDate(match[2]);
		const tradeDate = parseShortDate(match[3]);
		const settlementDate = parseShortDate(match[4]);
		const contributionAmount = parseMoney(match[5]);
		const previousResidualAmount = parseMoney(match[6]);
		const fees = parseMoney(match[7]);
		const fairMarketValue = parseMoney(match[8]);
		const purchasePrice = parseMoney(match[9]);
		const purchasedShares = parseShares(match[10]);
		const residualAmount = parseMoney(match[11]);

		let source = null;
		for (let lookAhead = index + 1; lookAhead < Math.min(lines.length, index + 8); lookAhead += 1) {
			const sourceLine = String(lines[lookAhead] || '').trim().toLowerCase();
			if (!sourceLine) continue;
			if (PURCHASE_ROW_REGEX.test(lines[lookAhead])) break;
			if (sourceLine === 'company' || sourceLine === 'participant') {
				source = sourceLine;
				break;
			}
		}

		transactions.push({
			ticker,
			type: 'buy',
			date: tradeDate || settlementDate || contributionDate || allocationDate,
			quantity: purchasedShares,
			price: purchasePrice,
			amount: contributionAmount,
			currency: 'CAD',
			institution: 'Computershare',
			direction: source,
			market: 'CA',
			source: 'computershare-holdings-pdf',
			metadata: {
				allocationDate,
				contributionDate,
				settlementDate,
				previousResidualAmount,
				fairMarketValue,
				fees,
				residualAmount,
			},
		});

		quantitySum += purchasedShares;
		if (purchasePrice > 0 && (!latestTradeDate || (tradeDate && tradeDate >= latestTradeDate))) {
			latestPrice = purchasePrice;
			latestTradeDate = tradeDate || latestTradeDate;
		}
	}

	const aliases = [];
	const normalizedName = String(assetName || '').trim().toLowerCase();
	if (normalizedName && normalizedName !== String(ticker || '').toLowerCase()) {
		aliases.push({
			normalizedName,
			ticker,
			source: 'computershare',
		});
	}

	const assets = [];
	if (ticker && quantitySum > 0) {
		const price = latestPrice && Number.isFinite(latestPrice) ? Number(latestPrice.toFixed(6)) : null;
		const quantity = Number(quantitySum.toFixed(6));
		assets.push({
			ticker,
			name: assetName || ticker,
			assetClass: 'stock',
			country: 'CA',
			currency: 'CAD',
			quantity,
			price,
			value: (price !== null ? Number((quantity * price).toFixed(6)) : null),
		});
	}

	return {
		assets,
		transactions,
		aliases,
	};
};

module.exports = parser;
