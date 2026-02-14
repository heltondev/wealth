/**
 * Base parser class with shared utilities for document parsing.
 * All provider parsers extend this class.
 */
class BaseParser {
	constructor({ id, provider }) {
		this.id = id;
		this.provider = provider;
	}

	/**
	 * Detect if this parser can handle the given file.
	 * @param {string} fileName - Original filename
	 * @param {string[]} sheetNames - Worksheet names in the workbook
	 * @param {Array} sampleRows - First 3 data rows for header sniffing
	 * @returns {boolean}
	 */
	detect(fileName, sheetNames, sampleRows) {
		throw new Error(`${this.id}: detect() not implemented`);
	}

	/**
	 * Parse the workbook and extract structured data.
	 * @param {object} workbook - XLSX workbook object
	 * @param {object} options - Additional options (e.g. sourceFile)
	 * @returns {{ assets: Array, transactions: Array, aliases: Array }}
	 */
	parse(workbook, options = {}) {
		throw new Error(`${this.id}: parse() not implemented`);
	}

	// --- Shared Utilities ---

	/**
	 * Parse DD/MM/YYYY date string to YYYY-MM-DD.
	 */
	static parseDate(dateStr) {
		if (!dateStr || dateStr === '-') return null;
		const str = dateStr.toString().trim();

		// Already ISO format
		if (/^\d{4}-\d{2}-\d{2}/.test(str)) return str.slice(0, 10);

		// DD/MM/YYYY
		const match = str.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
		if (match) return `${match[3]}-${match[2]}-${match[1]}`;

		return null;
	}

	/**
	 * Extract ticker from B3 product string.
	 * e.g. "KNCR11 - KINEA RENDIMENTOS IMOBILIÁRIOS FDO INV IMOB - FII" → "KNCR11"
	 * e.g. "ALZR11L - ALIANZA TRUST..." → "ALZR11" (strip trailing L suffix)
	 */
	static extractTicker(productStr) {
		if (!productStr) return null;
		const str = productStr.toString().trim();

		// Pattern: "TICKER - Full Name" or just "TICKER"
		const match = str.match(/^([A-Z0-9]{4,8})\s*-/);
		if (match) {
			return BaseParser.normalizeTicker(match[1]);
		}

		// If it's just a ticker (e.g. "PETR4" in proventos)
		if (/^[A-Z]{2,6}\d{1,2}[A-Z]?$/.test(str)) {
			return BaseParser.normalizeTicker(str);
		}

		return null;
	}

	/**
	 * Normalize ticker - strip trailing L suffix (used for ex-rights tickers).
	 * e.g. "ALZR11L" → "ALZR11"
	 */
	static normalizeTicker(ticker) {
		if (!ticker) return null;
		// Remove trailing L (ex-rights suffix) but only if base looks like a valid ticker
		return ticker.replace(/^([A-Z]{4}\d{2})L$/, '$1');
	}

	/**
	 * Extract full product name for alias creation.
	 * e.g. "KNCR11 - KINEA RENDIMENTOS..." → "KINEA RENDIMENTOS..."
	 */
	static extractProductName(productStr) {
		if (!productStr) return null;
		const str = productStr.toString().trim();
		const dashIndex = str.indexOf(' - ');
		if (dashIndex === -1) return null;
		return str.slice(dashIndex + 3).trim();
	}

	/**
	 * Parse a numeric value, handling B3 formats like "-", "R$ -", empty strings.
	 */
	static parseNumber(value) {
		if (value === null || value === undefined || value === '' || value === '-') return 0;
		if (typeof value === 'number') return value;
		let str = value.toString().replace(/[R$\s]/g, '');
		// Brazilian format: 1.234,56 → 1234.56
		// If both . and , exist, . is thousands separator
		if (str.includes(',') && str.includes('.')) {
			str = str.replace(/\./g, '').replace(',', '.');
		} else if (str.includes(',')) {
			str = str.replace(',', '.');
		}
		// If string is just digits and dots (e.g. "582.18"), keep as-is (decimal dot)
		const num = parseFloat(str);
		return isNaN(num) ? 0 : num;
	}

	/**
	 * Determine asset class from ticker pattern.
	 */
	static inferAssetClass(ticker) {
		if (!ticker) return 'stock';
		if (/^\d{2}[A-Z]{1,4}\d{2}$/.test(ticker)) return 'stock'; // options
		if (/^[A-Z]{4}11[A-Z]?$/.test(ticker)) return 'fii';
		if (/^Tesouro/i.test(ticker)) return 'bond';
		if (/^CDB/i.test(ticker)) return 'bond';
		return 'stock';
	}

	/**
	 * Create a dedup key for transactions to prevent duplicate imports.
	 */
	static transactionKey(trans) {
		return `${trans.ticker}|${trans.date}|${trans.type}|${trans.amount}|${trans.quantity}`;
	}

	/**
	 * Read sheet data as array of objects using first row as headers.
	 */
	static sheetToRows(workbook, sheetName) {
		const XLSX = require('xlsx');
		const sheet = workbook.Sheets[sheetName];
		if (!sheet) return [];
		const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
		// Filter out completely empty rows
		return rows.filter(row =>
			Object.values(row).some(v => v !== '' && v !== '-' && v !== null && v !== undefined)
		);
	}
}

module.exports = BaseParser;
