/**
 * Parser registry - detects provider/format and returns the appropriate parser.
 */
const XLSX = require('xlsx');

const parsers = [
	require('./b3/b3-negociacao'),
	require('./b3/b3-movimentacao'),
	require('./b3/b3-posicao'),
	require('./b3/b3-relatorio'),
];

/**
 * Detect the appropriate parser for a given file.
 * @param {string} filePath - Path to the file
 * @returns {{ parser: BaseParser, workbook: object } | null}
 */
function detectProvider(filePath) {
	const fileName = require('path').basename(filePath);
	const workbook = XLSX.readFile(filePath);
	const sheetNames = workbook.SheetNames;

	// Get sample rows from first sheet for header sniffing
	const firstSheet = workbook.Sheets[sheetNames[0]];
	const sampleRows = XLSX.utils.sheet_to_json(firstSheet, { header: 1, defval: '' }).slice(0, 4);

	for (const parser of parsers) {
		if (parser.detect(fileName, sheetNames, sampleRows)) {
			return { parser, workbook };
		}
	}

	return null;
}

/**
 * Get a parser by its ID.
 * @param {string} parserId
 * @returns {BaseParser | null}
 */
function getParser(parserId) {
	return parsers.find(p => p.id === parserId) || null;
}

/**
 * List all registered parsers.
 * @returns {Array<{ id: string, provider: string }>}
 */
function listParsers() {
	return parsers.map(p => ({ id: p.id, provider: p.provider }));
}

module.exports = { detectProvider, getParser, listParsers };
