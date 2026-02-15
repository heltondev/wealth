const {
	fetchWithTimeout,
	parseCsv,
	withRetry,
	normalizeWhitespace,
} = require('../../market-data/utils');
const {
	ProviderUnavailableError,
	DataIncompleteError,
} = require('../../market-data/errors');
const {
	normalizeTesouroType,
	normalizeTesouroMaturity,
} = require('../../market-data/symbol-resolver');

const DEFAULT_CSV_URLS = {
	ALL: (process.env.TESOURO_CSV_URL_ALL || '')
		.split(',')
		.map((value) => value.trim())
		.filter(Boolean),
	'NTN-B': (process.env.TESOURO_CSV_URL_NTNB || '')
		.split(',')
		.map((value) => value.trim())
		.filter(Boolean),
	'NTN-F': (process.env.TESOURO_CSV_URL_NTNF || '')
		.split(',')
		.map((value) => value.trim())
		.filter(Boolean),
	LTN: (process.env.TESOURO_CSV_URL_LTN || '')
		.split(',')
		.map((value) => value.trim())
		.filter(Boolean),
	LFT: (process.env.TESOURO_CSV_URL_LFT || '')
		.split(',')
		.map((value) => value.trim())
		.filter(Boolean),
};

const normalizeColumnName = (value) =>
	String(value || '')
		.normalize('NFD')
		.replace(/\p{Diacritic}/gu, '')
		.toLowerCase()
		.trim();

const parseLocaleNumber = (value) => {
	if (value === undefined || value === null || value === '') return null;
	const cleaned = String(value)
		.replace(/\./g, '')
		.replace(',', '.')
		.replace(/[^\d.-]/g, '');
	const numeric = Number(cleaned);
	return Number.isFinite(numeric) ? numeric : null;
};

const normalizeDate = (value) => {
	const raw = normalizeWhitespace(value);
	if (!raw) return null;
	const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
	if (iso) return raw;
	const br = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
	if (br) return `${br[3]}-${br[2]}-${br[1]}`;
	return null;
};

const findFirstColumn = (row, aliases) => {
	const keys = Object.keys(row || {});
	const normalizedMap = new Map(keys.map((key) => [normalizeColumnName(key), key]));
	for (const alias of aliases) {
		const key = normalizedMap.get(normalizeColumnName(alias));
		if (key) return key;
	}
	return null;
};

const isCsvResponse = (contentType, bodyPreview = '') => {
	const normalizedType = String(contentType || '').toLowerCase();
	if (
		normalizedType.includes('text/csv') ||
		normalizedType.includes('application/csv')
	) {
		return true;
	}
	const preview = String(bodyPreview).slice(0, 1024).toLowerCase();
	return preview.includes(',') && preview.includes('\n') && !preview.includes('<html');
};

class TesouroHistoryProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(
			options.timeoutMs || process.env.MARKET_DATA_TESOURO_TIMEOUT_MS || 25000
		);
		this.csvUrlsByType = this.#resolveCsvUrls(options.csvUrlsByType);
	}

	async fetchHistory(ticker, options = {}) {
		const tesouroType = normalizeTesouroType(ticker);
		const maturity = normalizeTesouroMaturity(ticker);
		const urls = [
			...(this.csvUrlsByType[tesouroType] || []),
			...(this.csvUrlsByType.ALL || []),
		];

		if (!urls.length) {
			throw new ProviderUnavailableError(
				'No Tesouro CSV URLs configured. Set TESOURO_CSV_URL_* env vars.',
				{
					ticker,
					tesouroType,
				}
			);
		}

		const errors = [];
		for (const url of urls) {
			try {
				const rows = await this.#fetchAndParseCsv(url);
				const normalizedRows = this.#normalizeRows(rows, ticker, tesouroType, maturity);
				const incrementalRows = options.startDate
					? normalizedRows.filter((row) => row.date >= options.startDate)
					: normalizedRows;

				if (!incrementalRows.length) continue;
				return {
					data_source: 'tesouro_api',
					is_scraped: false,
					currency: 'BRL',
					rows: incrementalRows,
					raw: {
						sourceUrl: url,
						totalRows: rows.length,
					},
				};
			} catch (error) {
				errors.push({
					url,
					error: error.message,
				});
			}
		}

		if (options.allowEmpty) {
			return {
				data_source: 'tesouro_api',
				is_scraped: false,
				currency: 'BRL',
				rows: [],
				raw: { attempts: errors },
			};
		}

		throw new DataIncompleteError(
			'Tesouro history provider could not resolve rows for ticker',
			{
				ticker,
				tesouroType,
				maturity,
				errors,
			}
		);
	}

	#resolveCsvUrls(overrideMap) {
		if (!overrideMap) {
			const fromJson = process.env.TESOURO_CSV_URLS_JSON;
			if (fromJson) {
				try {
					return this.#sanitizeMap(JSON.parse(fromJson));
				} catch {
					return this.#sanitizeMap(DEFAULT_CSV_URLS);
				}
			}
			return this.#sanitizeMap(DEFAULT_CSV_URLS);
		}
		return this.#sanitizeMap(overrideMap);
	}

	#sanitizeMap(map) {
		const sanitized = {};
		for (const [key, value] of Object.entries(map || {})) {
			sanitized[key] = Array.isArray(value)
				? value.map((entry) => String(entry).trim()).filter(Boolean)
				: [];
		}
		return sanitized;
	}

	async #fetchAndParseCsv(url) {
		const response = await withRetry(
			() => fetchWithTimeout(url, { timeoutMs: this.timeoutMs }),
			{
				retries: 2,
				baseDelayMs: 500,
				factor: 2,
			}
		);

		if (!response.ok) {
			throw new ProviderUnavailableError(`Tesouro endpoint responded with ${response.status}`, {
				url,
			});
		}

		const body = await response.text();
		const contentType = response.headers.get('content-type');
		if (!isCsvResponse(contentType, body)) {
			throw new ProviderUnavailableError('Tesouro endpoint did not return CSV content', {
				url,
				contentType,
			});
		}

		return parseCsv(body);
	}

	#normalizeRows(rows, ticker, tesouroType, maturity) {
		if (!rows.length) return [];

		const titleColumn = findFirstColumn(rows[0], [
			'Titulo',
			'Nome Titulo',
			'Tipo Titulo',
			'Titulo Publico',
		]);
		const typeColumn = findFirstColumn(rows[0], ['Tipo', 'Tipo Titulo', 'Indexador']);
		const maturityColumn = findFirstColumn(rows[0], [
			'Data Vencimento',
			'Vencimento',
			'Data de Vencimento',
		]);
		const dateColumn = findFirstColumn(rows[0], [
			'Data Base',
			'Data',
			'Data de Referencia',
			'Data Referencia',
		]);
		const buyPuColumn = findFirstColumn(rows[0], ['PU Compra', 'Preco Unitario Compra', 'Preco Compra']);
		const sellPuColumn = findFirstColumn(rows[0], ['PU Venda', 'Preco Unitario Venda', 'Preco Venda']);
		const buyRateColumn = findFirstColumn(rows[0], ['Taxa Compra', 'Taxa de Compra']);
		const sellRateColumn = findFirstColumn(rows[0], ['Taxa Venda', 'Taxa de Venda']);

		const normalizedTicker = normalizeWhitespace(ticker).toUpperCase();
		const normalizedType = normalizeWhitespace(tesouroType).toUpperCase();
		const maturityYear = String(maturity || '').slice(0, 4);

		const filtered = rows.filter((row) => {
			const title = normalizeWhitespace(row[titleColumn]).toUpperCase();
			const type = normalizeWhitespace(row[typeColumn]).toUpperCase();
			const rowMaturity = normalizeWhitespace(row[maturityColumn]).toUpperCase();

			const byTicker = title && title.includes(normalizedTicker);
			const byType = normalizedType ? type.includes(normalizedType) : true;
			const byMaturity = maturityYear ? rowMaturity.includes(maturityYear) : true;

			return byTicker || (byType && byMaturity);
		});

		const candidates = filtered.length ? filtered : rows;
		const byDate = new Map();

		for (const row of candidates) {
			const date = normalizeDate(row[dateColumn]);
			if (!date) continue;
			const normalizedRow = {
				date,
				open: null,
				high: null,
				low: null,
				close:
					parseLocaleNumber(row[sellPuColumn]) ??
					parseLocaleNumber(row[buyPuColumn]),
				adjusted_close:
					parseLocaleNumber(row[sellPuColumn]) ??
					parseLocaleNumber(row[buyPuColumn]),
				volume: null,
				dividends: 0,
				stock_splits: 0,
				pu_compra: parseLocaleNumber(row[buyPuColumn]),
				pu_venda: parseLocaleNumber(row[sellPuColumn]),
				taxa_compra: parseLocaleNumber(row[buyRateColumn]),
				taxa_venda: parseLocaleNumber(row[sellRateColumn]),
			};
			byDate.set(date, normalizedRow);
		}

		return Array.from(byDate.values()).sort((left, right) =>
			left.date.localeCompare(right.date)
		);
	}
}

module.exports = {
	TesouroHistoryProvider,
};
