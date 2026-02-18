/**
 * Parser registry - detects provider/format and returns the appropriate parser.
 */
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const XLSX = require('xlsx');

const parsers = [
	require('./b3/b3-negociacao'),
	require('./b3/b3-movimentacao'),
	require('./b3/b3-posicao'),
	require('./b3/b3-relatorio'),
	require('./robinhood/robinhood-activity'),
	require('./crypto/cold-wallet-crypto'),
	require('./computershare/computershare-espp'),
	require('./computershare/computershare-holdings-pdf'),
];

const PDF_SAMPLE_LINE_LIMIT = 220;
const PDF_MAX_BUFFER_BYTES = 32 * 1024 * 1024;

/**
 * Build a pseudo-workbook from PDF bytes.
 * We keep parser contracts stable by exposing sheet-like rows plus raw PDF text.
 */
function buildPdfWorkbookFromBuffer(fileName, pdfBuffer) {
	if (!pdfBuffer || pdfBuffer.length === 0) return null;

	const extraction = spawnSync('pdftotext', ['-layout', '-', '-'], {
		input: pdfBuffer,
		encoding: 'utf8',
		maxBuffer: PDF_MAX_BUFFER_BYTES,
	});

	if (extraction.error || extraction.status !== 0) return null;
	const text = String(extraction.stdout || '').replace(/\u0000/g, '').trim();
	if (!text) return null;

	const lines = text
		.split(/\r?\n/)
		.map(line => line.replace(/\s+$/g, ''))
		.filter(Boolean);

	const sampleRows = lines.slice(0, PDF_SAMPLE_LINE_LIMIT).map(line => [line]);
	const sheetName = 'PDF_TEXT';
	const pseudoSheet = XLSX.utils.aoa_to_sheet(lines.map(line => [line]));

	return {
		SheetNames: [sheetName],
		Sheets: {
			[sheetName]: pseudoSheet,
		},
		__fileName: path.basename(fileName || 'upload.pdf'),
		__fileType: 'pdf',
		__pdfText: text,
		__pdfLines: lines,
		__sampleRows: sampleRows,
	};
}

/**
 * Build a pseudo-workbook from PDF file path.
 */
function buildPdfWorkbookFromFile(filePath) {
	if (!filePath) return null;
	const pdfBuffer = fs.readFileSync(filePath);
	return buildPdfWorkbookFromBuffer(path.basename(filePath), pdfBuffer);
}

function getSampleRows(workbook, sheetNames) {
	if (Array.isArray(workbook?.__sampleRows) && workbook.__sampleRows.length > 0) {
		return workbook.__sampleRows;
	}

	if (!sheetNames || sheetNames.length === 0) return [];
	const firstSheet = workbook.Sheets[sheetNames[0]];
	if (!firstSheet) return [];
	return XLSX.utils
		.sheet_to_json(firstSheet, { header: 1, defval: '' })
		.slice(0, 12);
}

/**
 * Detect parser by workbook metadata and sample rows.
 * @param {string} fileName
 * @param {object} workbook
 * @returns {{ parser: BaseParser, workbook: object } | null}
 */
function detectProviderFromWorkbook(fileName, workbook) {
	if (!workbook || !Array.isArray(workbook.SheetNames)) return null;
	const safeFileName = path.basename(fileName || 'upload.xlsx');
	const sheetNames = workbook.SheetNames;
	const sampleRows = getSampleRows(workbook, sheetNames);
	if (sampleRows.length === 0) return null;

	for (const parser of parsers) {
		if (parser.detect(safeFileName, sheetNames, sampleRows)) {
			return { parser, workbook };
		}
	}

	return null;
}

/**
 * Detect the appropriate parser for a given file.
 * @param {string} filePath - Path to the file
 * @returns {{ parser: BaseParser, workbook: object } | null}
 */
function detectProvider(filePath) {
	const fileName = path.basename(filePath);
	let workbook;
	if (/\.pdf$/i.test(fileName)) {
		workbook = buildPdfWorkbookFromFile(filePath);
	} else {
		workbook = XLSX.readFile(filePath);
	}
	if (!workbook) return null;
	return detectProviderFromWorkbook(fileName, workbook);
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

module.exports = {
	detectProvider,
	detectProviderFromWorkbook,
	getParser,
	listParsers,
	buildPdfWorkbookFromBuffer,
	buildPdfWorkbookFromFile,
};
