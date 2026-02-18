/**
 * Cold wallet crypto CSV parser.
 *
 * Expected columns:
 * - date
 * - event_type
 * - ticker
 * - quantity
 * - value_usd
 * - from_address
 * - notes
 */
const XLSX = require('xlsx');
const BaseParser = require('../base-parser');

const parser = new BaseParser({ id: 'cold-wallet-crypto', provider: 'cold-wallet' });

const FIELD_ALIASES = {
	date: ['date', 'event_date', 'tx_date'],
	eventType: ['event_type', 'type', 'action'],
	ticker: ['ticker', 'symbol', 'asset'],
	quantity: ['quantity', 'qty', 'amount'],
	valueUsd: ['value_usd', 'usd_value', 'value'],
	fromAddress: ['from_address', 'address', 'wallet_address'],
	notes: ['notes', 'note', 'memo', 'description'],
};

const DETECT_REQUIRED_FIELDS = ['date', 'eventType', 'ticker', 'quantity'];
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

const normalizeTicker = (value) => (
	toText(value)
		.toUpperCase()
		.replace(/[^A-Z0-9.\-]/g, '')
);

const parseNumber = (value) => {
	if (typeof value === 'number' && Number.isFinite(value)) return value;
	const text = toText(value);
	if (!text) return null;
	const sanitized = text
		.replace(/\$/g, '')
		.replace(/,/g, '')
		.replace(/[()]/g, '')
		.trim();
	if (!sanitized) return null;
	const numeric = Number(sanitized);
	return Number.isFinite(numeric) ? numeric : null;
};

const normalizeEventType = (value) => toText(value).toLowerCase();

const mapEventTypeToTransactionType = (eventType) => {
	if (['receive', 'mint', 'buy', 'deposit', 'airdrop', 'reward', 'staking_reward', 'transfer_in'].includes(eventType)) {
		return 'buy';
	}
	if (['send', 'sell', 'withdraw', 'transfer_out'].includes(eventType)) {
		return 'sell';
	}
	if (['tax', 'fee', 'withholding'].includes(eventType)) {
		return 'tax';
	}
	if (['dividend'].includes(eventType)) {
		return 'dividend';
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

		const date = parseDateValue(getRowFieldValue(row, 'date'));
		if (!date) continue;

		const eventType = normalizeEventType(getRowFieldValue(row, 'eventType'));
		const transactionType = mapEventTypeToTransactionType(eventType);
		const quantityRaw = parseNumber(getRowFieldValue(row, 'quantity'));
		const quantity = quantityRaw !== null ? Math.abs(quantityRaw) : 0;
		const valueUsd = parseNumber(getRowFieldValue(row, 'valueUsd'));
		const fromAddress = toText(getRowFieldValue(row, 'fromAddress')) || null;
		const notes = toText(getRowFieldValue(row, 'notes')) || null;

		if (!assetsByTicker.has(ticker)) {
			assetsByTicker.set(ticker, {
				ticker,
				name: ticker,
				assetClass: 'crypto',
				country: 'GLOBAL',
				currency: 'USD',
				quantity: 0,
				latestPrice: null,
				latestPriceDate: null,
			});
		}

		const asset = assetsByTicker.get(ticker);
		if (transactionType === 'buy' || transactionType === 'sell') {
			const delta = transactionType === 'sell' ? -quantity : quantity;
			asset.quantity += delta;
		}

		const price = (
			valueUsd !== null
			&& Number.isFinite(valueUsd)
			&& quantity > EPSILON
				? Math.abs(valueUsd) / quantity
				: 0
		);
		if (price > 0 && (!asset.latestPriceDate || date >= asset.latestPriceDate)) {
			asset.latestPrice = price;
			asset.latestPriceDate = date;
		}

		if (!transactionType) continue;

		transactions.push({
			ticker,
			type: transactionType,
			date,
			quantity,
			price,
			amount: valueUsd !== null && Number.isFinite(valueUsd) ? Math.abs(valueUsd) : 0,
			currency: 'USD',
			institution: 'Cold Wallet',
			direction: fromAddress,
			market: 'CRYPTO',
			source: 'cold-wallet-crypto',
			notes,
		});
	}

	const assets = Array.from(assetsByTicker.values())
		.map((asset) => {
			const roundedQuantity = Number(asset.quantity.toFixed(8));
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
		.filter((asset) => Math.abs(Number(asset.quantity || 0)) > POSITION_QUANTITY_EPSILON)
		.sort((left, right) => left.ticker.localeCompare(right.ticker));

	return {
		assets,
		transactions,
		aliases: [],
	};
};

module.exports = parser;
