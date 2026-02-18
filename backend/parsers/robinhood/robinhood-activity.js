/**
 * Robinhood account activity parser.
 *
 * Expected columns (CSV export):
 * - Activity Date
 * - Process Date
 * - Settle Date
 * - Instrument
 * - Description
 * - Trans Code
 * - Quantity
 * - Price
 * - Amount
 */
const XLSX = require('xlsx');
const BaseParser = require('../base-parser');

const parser = new BaseParser({ id: 'robinhood-activity', provider: 'robinhood' });

const REQUIRED_HEADERS = new Set([
	'activity date',
	'process date',
	'settle date',
	'instrument',
	'description',
	'trans code',
	'quantity',
	'price',
	'amount',
]);

const EPSILON = 1e-9;
const POSITION_QUANTITY_EPSILON = 1e-5;

const QUANTITY_AFFECTING_CODES = new Set([
	'BUY',
	'SELL',
	'REC',
	'SPR',
	'SDIV',
]);

const SUPPORTED_TRANSACTION_CODES = new Set([
	'BUY',
	'SELL',
	'CDIV',
	'CIL',
	'SLIP',
	'DTAX',
	'DFEE',
	'AFEE',
]);

const NON_NAME_DESCRIPTION_PREFIXES = [
	'CASH DIV',
	'STOCK LENDING',
	'ADR FEE',
	'SPONSORED ADR',
	'FOREIGN TAX',
	'INTEREST PAYMENT',
	'GOLD',
	'ACH',
	'OPTION EXPIRATION',
	'CIL ON',
	'EXTERNAL DEBIT CARD TRANSFER',
];

const toText = (value) => String(value || '').trim();

const normalizeHeader = (value) => toText(value).toLowerCase();

const normalizeTicker = (value) => (
	toText(value)
		.toUpperCase()
		.replace(/[^A-Z0-9.\-]/g, '')
);

const pad = (value) => String(value).padStart(2, '0');

const toIsoDate = (year, month, day) => `${year}-${pad(month)}-${pad(day)}`;

const parseDateValue = (value) => {
	if (typeof value === 'number' && Number.isFinite(value)) {
		const parsed = XLSX.SSF.parse_date_code(value);
		if (parsed?.y && parsed?.m && parsed?.d) {
			return toIsoDate(parsed.y, parsed.m, parsed.d);
		}
	}

	if (value instanceof Date && !Number.isNaN(value.getTime())) {
		return toIsoDate(value.getUTCFullYear(), value.getUTCMonth() + 1, value.getUTCDate());
	}

	const text = toText(value);
	if (!text) return null;

	const iso = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
	if (iso) return toIsoDate(iso[1], iso[2], iso[3]);

	const us = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
	if (us) return toIsoDate(us[3], us[1], us[2]);

	return null;
};

const parseSignedNumber = (value) => {
	if (typeof value === 'number' && Number.isFinite(value)) return value;
	const text = toText(value);
	if (!text) return null;

	const negativeByParens = /^\(.*\)$/.test(text);
	const sanitized = text
		.replace(/\$/g, '')
		.replace(/,/g, '')
		.replace(/[()]/g, '')
		.trim();

	if (!sanitized) return null;
	const numeric = Number(sanitized);
	if (!Number.isFinite(numeric)) return null;
	return negativeByParens ? -Math.abs(numeric) : numeric;
};

const parseShareQuantity = (value) => {
	if (typeof value === 'number' && Number.isFinite(value)) return value;
	const text = toText(value);
	if (!text) return null;

	const isNegativeSuffix = /S$/i.test(text);
	const sanitized = text
		.replace(/S$/i, '')
		.replace(/,/g, '')
		.trim();
	if (!sanitized) return null;

	const numeric = Number(sanitized);
	if (!Number.isFinite(numeric)) return null;
	return isNegativeSuffix ? -Math.abs(numeric) : numeric;
};

const normalizeAliasName = (value) => (
	toText(value)
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, ' ')
		.trim()
);

const extractInstrumentName = (description, ticker) => {
	const lines = String(description || '')
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter(Boolean);
	if (lines.length === 0) return ticker;

	const firstLine = lines[0];
	const upper = firstLine.toUpperCase();
	if (NON_NAME_DESCRIPTION_PREFIXES.some((prefix) => upper.startsWith(prefix))) {
		return ticker;
	}

	// Ignore option-like descriptions as asset names.
	if (/\b(CALL|PUT)\b/i.test(firstLine) && /\d{1,2}\/\d{1,2}\/\d{4}/.test(firstLine)) {
		return ticker;
	}

	return firstLine || ticker;
};

const resolveQuantityDelta = (code, quantity) => {
	if (!QUANTITY_AFFECTING_CODES.has(code)) return null;
	if (quantity === null || !Number.isFinite(quantity)) return null;

	if (code === 'BUY') return Math.abs(quantity);
	if (code === 'SELL') return -Math.abs(quantity);
	return quantity;
};

const buildTransactionFromRow = ({
	code,
	ticker,
	date,
	quantity,
	price,
	amount,
}) => {
	if (!SUPPORTED_TRANSACTION_CODES.has(code)) return null;
	if (!ticker || !date) return null;

	if (code === 'BUY' || code === 'SELL') {
		if (quantity === null || !Number.isFinite(quantity) || Math.abs(quantity) <= EPSILON) {
			return null;
		}
		const resolvedQuantity = Math.abs(quantity);
		const resolvedPrice = (
			price !== null && Number.isFinite(price)
				? Math.abs(price)
				: (amount !== null && Number.isFinite(amount) ? Math.abs(amount) / resolvedQuantity : 0)
		);
		const resolvedAmount = (
			amount !== null && Number.isFinite(amount)
				? Math.abs(amount)
				: Math.abs(resolvedQuantity * resolvedPrice)
		);
		return {
			ticker,
			type: code === 'BUY' ? 'buy' : 'sell',
			date,
			quantity: resolvedQuantity,
			price: resolvedPrice,
			amount: resolvedAmount,
			currency: 'USD',
			institution: 'Robinhood',
			market: 'US',
			source: 'robinhood-activity',
		};
	}

	if (code === 'CDIV' || code === 'CIL' || code === 'SLIP') {
		if (amount === null || !Number.isFinite(amount)) return null;
		return {
			ticker,
			type: 'dividend',
			date,
			quantity: 0,
			price: 0,
			amount,
			currency: 'USD',
			institution: 'Robinhood',
			market: 'US',
			source: 'robinhood-activity',
		};
	}

	if (code === 'DTAX' || code === 'DFEE' || code === 'AFEE') {
		if (amount === null || !Number.isFinite(amount)) return null;
		return {
			ticker,
			type: 'tax',
			date,
			quantity: 0,
			price: 0,
			amount,
			currency: 'USD',
			institution: 'Robinhood',
			market: 'US',
			source: 'robinhood-activity',
		};
	}

	return null;
};

parser.detect = function (_fileName, _sheetNames, sampleRows) {
	if (!Array.isArray(sampleRows) || sampleRows.length === 0) return false;
	const headerRow = Array.isArray(sampleRows[0]) ? sampleRows[0] : [];
	const headers = new Set(headerRow.map(normalizeHeader).filter(Boolean));
	return Array.from(REQUIRED_HEADERS).every((header) => headers.has(header));
};

parser.parse = function (workbook) {
	const firstSheetName = workbook?.SheetNames?.[0];
	if (!firstSheetName) {
		return { assets: [], transactions: [], aliases: [] };
	}

	const rows = BaseParser.sheetToRows(workbook, firstSheetName);
	const assetsByTicker = new Map();
	const aliasesByKey = new Map();
	const transactions = [];

	for (const row of rows) {
		const code = toText(row['Trans Code']).toUpperCase();
		const ticker = normalizeTicker(row.Instrument);
		const description = toText(row.Description);
		const date = (
			parseDateValue(row['Activity Date'])
			|| parseDateValue(row['Process Date'])
			|| parseDateValue(row['Settle Date'])
		);
		const quantity = parseShareQuantity(row.Quantity);
		const price = parseSignedNumber(row.Price);
		const amount = parseSignedNumber(row.Amount);

		if (!code && !ticker && !description) continue;
		if (!ticker) continue;

		if (!assetsByTicker.has(ticker)) {
			assetsByTicker.set(ticker, {
				ticker,
				name: ticker,
				assetClass: 'stock',
				country: 'US',
				currency: 'USD',
				quantity: 0,
				latestPrice: null,
				latestPriceDate: null,
			});
		}

		const asset = assetsByTicker.get(ticker);
		const nameCandidate = extractInstrumentName(description, ticker);
		if (nameCandidate && asset.name === ticker) {
			asset.name = nameCandidate;
		}

		const normalizedAlias = normalizeAliasName(nameCandidate);
		if (normalizedAlias && normalizedAlias !== ticker.toLowerCase()) {
			const aliasKey = `${normalizedAlias}|${ticker}`;
			aliasesByKey.set(aliasKey, {
				normalizedName: normalizedAlias,
				ticker,
				source: 'robinhood',
			});
		}

		const quantityDelta = resolveQuantityDelta(code, quantity);
		if (quantityDelta !== null) {
			asset.quantity += quantityDelta;
			if (price !== null && Number.isFinite(price) && price > 0 && date) {
				if (!asset.latestPriceDate || date >= asset.latestPriceDate) {
					asset.latestPrice = price;
					asset.latestPriceDate = date;
				}
			}
		}

		const transaction = buildTransactionFromRow({
			code,
			ticker,
			date,
			quantity,
			price,
			amount,
		});
		if (transaction) transactions.push(transaction);
	}

	const assets = Array.from(assetsByTicker.values())
		.map((asset) => {
			const roundedQuantity = Number(asset.quantity.toFixed(6));
			const roundedPrice = Number.isFinite(asset.latestPrice)
				? Number(asset.latestPrice.toFixed(6))
				: null;
			const currentValue = (
				roundedPrice !== null
				&& Math.abs(roundedQuantity) > EPSILON
					? Number((roundedQuantity * roundedPrice).toFixed(6))
					: null
			);
			return {
				ticker: asset.ticker,
				name: asset.name || asset.ticker,
				assetClass: asset.assetClass,
				country: asset.country,
				currency: asset.currency,
				quantity: Math.abs(roundedQuantity) <= POSITION_QUANTITY_EPSILON ? 0 : roundedQuantity,
				price: roundedPrice,
				value: currentValue,
			};
		})
		.sort((left, right) => left.ticker.localeCompare(right.ticker));

	return {
		assets,
		transactions,
		aliases: Array.from(aliasesByKey.values()),
	};
};

module.exports = parser;
