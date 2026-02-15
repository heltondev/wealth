const {
	DataIncompleteError,
	ProviderUnavailableError,
} = require('../errors');
const {
	fetchWithTimeout,
	parseCsv,
	toNumberOrNull,
	withRetry,
	normalizeWhitespace,
} = require('../utils');
const { normalizeTesouroMaturity, normalizeTesouroType } = require('../symbol-resolver');

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
	const cleaned = String(value).replace(/\./g, '').replace(',', '.').replace(/[^\d.-]/g, '');
	return toNumberOrNull(cleaned);
};

const findFirstColumn = (row, aliases) => {
	const keys = Object.keys(row || {});
	const normalized = new Map(keys.map((key) => [normalizeColumnName(key), key]));

	for (const alias of aliases) {
		const key = normalized.get(normalizeColumnName(alias));
		if (key) return key;
	}

	return null;
};

const isCsvResponse = (contentType, bodyPreview = '') => {
	const normalizedType = String(contentType || '').toLowerCase();
	if (normalizedType.includes('text/csv') || normalizedType.includes('application/csv')) {
		return true;
	}

	// Some endpoints return text/plain for CSV.
	const preview = bodyPreview.slice(0, 1024).toLowerCase();
	return preview.includes(',') && preview.includes('\n') && !preview.includes('<html');
};

class TesouroProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_TESOURO_TIMEOUT_MS || 25000);
		this.csvUrlsByType = this.#resolveCsvUrls(options.csvUrlsByType);
	}

	async fetch(ticker) {
		const tesouroType = normalizeTesouroType(ticker);
		const maturity = normalizeTesouroMaturity(ticker);
		const urls = this.#urlsForType(tesouroType);

		if (urls.length === 0) {
			throw new ProviderUnavailableError(
				'No Tesouro CSV URLs configured. Set TESOURO_CSV_URL_* env vars.',
				{ tesouroType, ticker }
			);
		}

		const fetchErrors = [];

		for (const url of urls) {
			try {
				const parsed = await this.#fetchCsv(url);
				const row = this.#findBestRow(parsed, ticker, tesouroType, maturity);
				if (!row) {
					fetchErrors.push({
						url,
						error: 'ticker_not_found_in_csv',
					});
					continue;
				}

				const normalized = this.#normalizeTesouroRow(row);
				const currentPrice = normalized.sellPrice ?? normalized.buyPrice;

				if (!currentPrice) {
					throw new DataIncompleteError('Tesouro row does not have sell/buy PU', {
						ticker,
						url,
						row,
					});
				}

				return {
					data_source: 'tesouro_api',
					is_scraped: false,
					quote: {
						currentPrice,
						currency: 'BRL',
						change: null,
						changePercent: null,
						previousClose: null,
						marketCap: null,
						volume: null,
					},
					fundamentals: {
						tesouro: normalized,
					},
					historical: {
						history_30d: [],
						dividends: [],
					},
					raw: {
						row,
						sourceUrl: url,
					},
				};
			} catch (error) {
				fetchErrors.push({
					url,
					error: error.message,
				});
			}
		}

		throw new ProviderUnavailableError('Unable to fetch Tesouro data from configured CSV URLs', {
			ticker,
			tesouroType,
			maturity,
			attempts: fetchErrors,
		});
	}

	#resolveCsvUrls(overrideMap) {
		if (!overrideMap) {
			const fromJson = process.env.TESOURO_CSV_URLS_JSON;
			if (fromJson) {
				try {
					const parsed = JSON.parse(fromJson);
					return this.#sanitizeMap(parsed);
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

	#urlsForType(tesouroType) {
		return [
			...(this.csvUrlsByType[tesouroType] || []),
			...(this.csvUrlsByType.ALL || []),
		];
	}

	async #fetchCsv(url) {
		const response = await withRetry(
			() =>
				fetchWithTimeout(url, {
					timeoutMs: this.timeoutMs,
				}),
			{
				retries: 2,
				baseDelayMs: 500,
				factor: 2,
			}
		);

		if (!response.ok) {
			throw new ProviderUnavailableError(`Tesouro endpoint responded with ${response.status}`, {
				url,
				status: response.status,
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

	#findBestRow(rows, ticker, tesouroType, maturity) {
		const normalizedTicker = normalizeWhitespace(ticker).toUpperCase();
		const normalizedType = normalizeWhitespace(tesouroType).toUpperCase();
		const normalizedMaturity = normalizeWhitespace(maturity || '').toUpperCase();

		const titleColumn = findFirstColumn(rows[0] || {}, [
			'Titulo',
			'Tipo Titulo',
			'Nome Titulo',
			'Titulo Publico',
		]);
		const typeColumn = findFirstColumn(rows[0] || {}, ['Tipo', 'Tipo Titulo', 'Indexador']);
		const maturityColumn = findFirstColumn(rows[0] || {}, [
			'Data Vencimento',
			'Vencimento',
			'Data de Vencimento',
		]);

		const byTicker = rows.filter((row) => {
			const title = normalizeWhitespace(row[titleColumn] || '').toUpperCase();
			return title && normalizedTicker && title.includes(normalizedTicker);
		});
		if (byTicker.length > 0) return byTicker[0];

		const byTypeAndMaturity = rows.filter((row) => {
			const typeValue = normalizeWhitespace(row[typeColumn] || '').toUpperCase();
			const maturityValue = normalizeWhitespace(row[maturityColumn] || '').toUpperCase();
			const sameType = normalizedType ? typeValue.includes(normalizedType) : true;
			const sameMaturity = normalizedMaturity
				? maturityValue.includes(normalizedMaturity.slice(0, 4))
				: true;
			return sameType && sameMaturity;
		});
		if (byTypeAndMaturity.length > 0) return byTypeAndMaturity[0];

		return rows[0] || null;
	}

	#normalizeTesouroRow(row) {
		const titleColumn = findFirstColumn(row, ['Titulo', 'Tipo Titulo', 'Nome Titulo']);
		const maturityColumn = findFirstColumn(row, ['Data Vencimento', 'Vencimento']);
		const buyRateColumn = findFirstColumn(row, ['Taxa Compra', 'Taxa de Compra', 'Taxa Compra Manha']);
		const sellRateColumn = findFirstColumn(row, ['Taxa Venda', 'Taxa de Venda', 'Taxa Venda Manha']);
		const buyPriceColumn = findFirstColumn(row, ['PU Compra', 'Preco Unitario Compra', 'Preco Compra']);
		const sellPriceColumn = findFirstColumn(row, ['PU Venda', 'Preco Unitario Venda', 'Preco Venda']);
		const baseDateColumn = findFirstColumn(row, ['Data Base', 'Data de Referencia', 'Data']);

		return {
			title: row[titleColumn] || null,
			maturityDate: row[maturityColumn] || null,
			buyRate: parseLocaleNumber(row[buyRateColumn]),
			sellRate: parseLocaleNumber(row[sellRateColumn]),
			buyPrice: parseLocaleNumber(row[buyPriceColumn]),
			sellPrice: parseLocaleNumber(row[sellPriceColumn]),
			baseDate: row[baseDateColumn] || null,
		};
	}
}

module.exports = {
	TesouroProvider,
};
