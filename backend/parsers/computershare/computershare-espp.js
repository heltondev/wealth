/**
 * Computershare ESPP CSV parser.
 *
 * Expected columns:
 * - ticker
 * - event_type
 * - allocation_date
 * - contribution_date
 * - trade_date
 * - settlement_date
 * - source
 * - contribution_amount_cad
 * - fees_cad
 * - fair_market_value_cad
 * - purchase_price_cad
 * - purchased_shares
 * - residual_amount_cad
 * - currency
 * - custodian
 */
const XLSX = require('xlsx');
const BaseParser = require('../base-parser');

const parser = new BaseParser({ id: 'computershare-espp', provider: 'computershare' });

const FIELD_ALIASES = {
	ticker: ['ticker', 'symbol', 'instrument'],
	eventType: ['event_type', 'type', 'transaction_type', 'event'],
	allocationDate: ['allocation_date', 'allocation date'],
	contributionDate: ['contribution_date', 'contribution date'],
	tradeDate: ['trade_date', 'trade date', 'activity date'],
	settlementDate: ['settlement_date', 'settlement date', 'settle date'],
	source: ['source', 'funding_source'],
	contributionAmount: ['contribution_amount_cad', 'contribution_amount', 'amount_cad', 'amount'],
	fees: ['fees_cad', 'fees'],
	fairMarketValue: ['fair_market_value_cad', 'fair_market_value', 'fmv_cad', 'fmv'],
	purchasePrice: ['purchase_price_cad', 'purchase_price', 'price_cad', 'price'],
	purchasedShares: ['purchased_shares', 'shares', 'quantity'],
	residualAmount: ['residual_amount_cad', 'residual_amount', 'residual'],
	currency: ['currency'],
	custodian: ['custodian', 'institution', 'broker'],
};

const DETECT_REQUIRED_FIELDS = [
	'ticker',
	'eventType',
	'tradeDate',
	'purchasePrice',
	'purchasedShares',
];

const EPSILON = 1e-9;
const POSITION_QUANTITY_EPSILON = 1e-5;

const toText = (value) => String(value || '').trim();

const normalizeHeader = (value) => (
	toText(value)
		.replace(/^\uFEFF/, '')
		.replace(/^["']+|["']+$/g, '')
		.replace(/\s+/g, ' ')
		.trim()
		.toLowerCase()
);

const normalizeHeaderRow = (rowValues) => (
	(rowValues || [])
		.map((value) => normalizeHeader(value))
		.filter(Boolean)
);

const hasAlias = (headersSet, fieldName) => (
	(FIELD_ALIASES[fieldName] || []).some((alias) => headersSet.has(alias))
);

const getRowFieldValue = (row, fieldName) => {
	const aliases = FIELD_ALIASES[fieldName] || [];
	for (const [key, value] of Object.entries(row || {})) {
		if (aliases.includes(normalizeHeader(key))) return value;
	}
	return undefined;
};

const normalizeTicker = (value) => (
	toText(value)
		.toUpperCase()
		.replace(/[^A-Z0-9.\-]/g, '')
);

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

	return BaseParser.parseDate(text);
};

const resolveCountryFromTicker = (ticker) => {
	if (!ticker) return 'CA';
	if (ticker.endsWith('.TO') || ticker.endsWith('.V') || ticker.endsWith('.NE')) return 'CA';
	return 'CA';
};

const normalizeEventType = (value) => toText(value).toLowerCase();

const mapEventTypeToTransactionType = (eventType) => {
	if (['buy', 'purchase', 'espp_purchase'].includes(eventType)) return 'buy';
	if (['sell', 'sale'].includes(eventType)) return 'sell';
	if (['dividend'].includes(eventType)) return 'dividend';
	if (['tax', 'withholding', 'fee'].includes(eventType)) return 'tax';
	return null;
};

const resolveAmountFromRow = ({ contributionAmount, fees, purchasedShares, purchasePrice }) => {
	if (contributionAmount !== null && Number.isFinite(contributionAmount)) return Math.abs(contributionAmount);
	if (
		purchasedShares !== null
		&& Number.isFinite(purchasedShares)
		&& purchasePrice !== null
		&& Number.isFinite(purchasePrice)
	) {
		const gross = Math.abs(purchasedShares * purchasePrice);
		if (fees !== null && Number.isFinite(fees)) return Math.max(gross + fees, 0);
		return gross;
	}
	return null;
};

parser.detect = function (_fileName, _sheetNames, sampleRows) {
	if (!Array.isArray(sampleRows) || sampleRows.length === 0) return false;
	for (const candidate of sampleRows) {
		if (!Array.isArray(candidate) || candidate.length === 0) continue;
		const headers = new Set(normalizeHeaderRow(candidate));
		if (headers.size === 0) continue;
		const isMatch = DETECT_REQUIRED_FIELDS.every((fieldName) => hasAlias(headers, fieldName));
		if (isMatch) return true;
	}
	return false;
};

parser.parse = function (workbook) {
	const firstSheetName = workbook?.SheetNames?.[0];
	if (!firstSheetName) return { assets: [], transactions: [], aliases: [] };

	const rows = BaseParser.sheetToRows(workbook, firstSheetName);
	const assetsByTicker = new Map();
	const transactions = [];

	for (const row of rows) {
		const ticker = normalizeTicker(getRowFieldValue(row, 'ticker'));
		if (!ticker) continue;

		const eventType = normalizeEventType(getRowFieldValue(row, 'eventType'));
		const transactionType = mapEventTypeToTransactionType(eventType);
		const tradeDate = (
			parseDateValue(getRowFieldValue(row, 'tradeDate'))
			|| parseDateValue(getRowFieldValue(row, 'settlementDate'))
			|| parseDateValue(getRowFieldValue(row, 'contributionDate'))
			|| parseDateValue(getRowFieldValue(row, 'allocationDate'))
		);
		if (!tradeDate) continue;

		const purchasedShares = parseSignedNumber(getRowFieldValue(row, 'purchasedShares'));
		const purchasePrice = parseSignedNumber(getRowFieldValue(row, 'purchasePrice'));
		const contributionAmount = parseSignedNumber(getRowFieldValue(row, 'contributionAmount'));
		const fees = parseSignedNumber(getRowFieldValue(row, 'fees'));
		const currency = toText(getRowFieldValue(row, 'currency')).toUpperCase() || 'CAD';
		const custodian = toText(getRowFieldValue(row, 'custodian')) || 'Computershare';
		const contributionSource = toText(getRowFieldValue(row, 'source')).toLowerCase() || null;

		if (!assetsByTicker.has(ticker)) {
			assetsByTicker.set(ticker, {
				ticker,
				name: ticker,
				assetClass: 'stock',
				country: resolveCountryFromTicker(ticker),
				currency,
				quantity: 0,
				latestPrice: null,
				latestPriceDate: null,
			});
		}

		const asset = assetsByTicker.get(ticker);
		if (transactionType === 'buy' || transactionType === 'sell') {
			if (purchasedShares !== null && Number.isFinite(purchasedShares)) {
				const shareDelta = transactionType === 'sell'
					? -Math.abs(purchasedShares)
					: Math.abs(purchasedShares);
				asset.quantity += shareDelta;
			}
			if (
				purchasePrice !== null
				&& Number.isFinite(purchasePrice)
				&& purchasePrice > 0
				&& (!asset.latestPriceDate || tradeDate >= asset.latestPriceDate)
			) {
				asset.latestPrice = Math.abs(purchasePrice);
				asset.latestPriceDate = tradeDate;
			}
		}

		if (!transactionType) continue;

		const amount = resolveAmountFromRow({
			contributionAmount,
			fees,
			purchasedShares,
			purchasePrice,
		});

		const transaction = {
			ticker,
			type: transactionType,
			date: tradeDate,
			quantity: (
				purchasedShares !== null && Number.isFinite(purchasedShares)
					? Math.abs(purchasedShares)
					: 0
			),
			price: (
				purchasePrice !== null && Number.isFinite(purchasePrice)
					? Math.abs(purchasePrice)
					: 0
			),
			amount: (
				amount !== null && Number.isFinite(amount)
					? Math.abs(amount)
					: 0
			),
			currency,
			institution: custodian,
			direction: contributionSource,
			market: 'CA',
			source: 'computershare-espp',
		};

		transactions.push(transaction);
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
		aliases: [],
	};
};

module.exports = parser;
