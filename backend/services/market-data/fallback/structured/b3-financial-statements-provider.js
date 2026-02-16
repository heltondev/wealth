const {
	fetchWithTimeout,
	nowIso,
	toNumberOrNull,
	withRetry,
} = require('../../utils');

const B3_FII_BASE_URL = 'https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/';
const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_START_YEAR = 2016;

const REPORT_META_KEYS = new Set([
	'id',
	'idmain',
	'idfnet',
	'idcem',
	'type',
	'typename',
	'describletype',
	'referencedate',
	'referencedateformat',
	'deliverydate',
	'deliverydateformat',
	'status',
	'version',
	'url',
	'urldownload',
	'urlviewerfundosnet',
	'urlfundosnet',
	'label',
	'arialabel',
	'acronym',
	'fundname',
	'tradingname',
	'keyword',
	'dateinitial',
	'datefinal',
	'pagenumber',
	'pagesize',
	'totalrecords',
	'totalpages',
]);

const STATEMENT_TYPE_HINTS = {
	income: [
		'dre',
		'resultado',
		'lucro',
		'receita',
		'income',
		'earnings',
		'profit',
	],
	balance: [
		'balanco',
		'balance',
		'patrimonio',
		'ativo',
		'passivo',
		'asset',
		'liabil',
		'equity',
	],
	cashflow: [
		'fluxodecaixa',
		'fluxocaixa',
		'cashflow',
		'cash',
		'caixa',
	],
};

const isObjectRecord = (value) =>
	value !== null && typeof value === 'object' && !Array.isArray(value);

const normalizeTextKey = (value) =>
	String(value || '')
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
		.replace(/[^a-z0-9]/g, '');

const normalizeTicker = (value) =>
	String(value || '')
		.trim()
		.toUpperCase()
		.replace(/\.SA$/i, '');

const normalizeFiiAcronym = (value) =>
	normalizeTicker(value).replace(/11$/, '');

const toIsoDate = (value) => {
	if (!value) return null;
	const text = String(value).trim();
	if (!text) return null;
	if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;

	const brazilianMatch = text.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
	if (brazilianMatch) {
		return `${brazilianMatch[3]}-${brazilianMatch[2]}-${brazilianMatch[1]}`;
	}

	const parsed = new Date(text);
	if (!Number.isFinite(parsed.getTime())) return null;
	return parsed.toISOString().slice(0, 10);
};

const parseLocalizedNumber = (value) => {
	if (value === null || value === undefined || value === '') return null;
	if (typeof value === 'number') return Number.isFinite(value) ? value : null;

	const directNumeric = toNumberOrNull(value);
	if (directNumeric !== null) return directNumeric;

	let text = String(value).trim();
	if (!text) return null;
	text = text.replace(/[^\d,.-]/g, '');
	if (!text) return null;

	const commaMatches = text.match(/,/g) || [];
	const dotMatches = text.match(/\./g) || [];

	if (commaMatches.length > 0 && dotMatches.length > 0) {
		if (text.lastIndexOf(',') > text.lastIndexOf('.')) {
			text = text.replace(/\./g, '').replace(',', '.');
		} else {
			text = text.replace(/,/g, '');
		}
	} else if (commaMatches.length > 0) {
		if (/,\d{1,4}$/.test(text)) {
			text = text.replace(/\./g, '').replace(',', '.');
		} else {
			text = text.replace(/,/g, '');
		}
	} else if (dotMatches.length > 1) {
		text = text.replace(/\./g, '');
	}

	const parsed = Number(text);
	return Number.isFinite(parsed) ? parsed : null;
};

const encodePayload = (payload) =>
	Buffer.from(JSON.stringify(payload || {}), 'utf8').toString('base64');

const inferStatementTypeFromText = (value) => {
	const normalized = normalizeTextKey(value);
	if (!normalized) return null;

	for (const [statementType, hints] of Object.entries(STATEMENT_TYPE_HINTS)) {
		if (hints.some((hint) => normalized.includes(hint))) {
			return statementType;
		}
	}
	return null;
};

const inferFrequencyFromLabel = (label) => {
	const normalized = normalizeTextKey(label);
	if (!normalized) return null;
	if (
		normalized.includes('trimestral') ||
		normalized.includes('quarter') ||
		normalized.includes('mensal') ||
		normalized.includes('month')
	) {
		return 'quarterly';
	}
	if (normalized.includes('anual') || normalized.includes('annual')) {
		return 'annual';
	}
	return null;
};

const inferFrequencyFromPeriod = (period) => {
	if (!period) return 'annual';
	return period.endsWith('-12-31') ? 'annual' : 'quarterly';
};

const initializeStatementBucket = () => ({
	income: {
		annual: new Map(),
		quarterly: new Map(),
	},
	balance: {
		annual: new Map(),
		quarterly: new Map(),
	},
	cashflow: {
		annual: new Map(),
		quarterly: new Map(),
	},
});

const appendMetricRowValue = (bucket, period, metricKey, metricValue) => {
	if (!bucket || !period || !metricKey || metricValue === null) return;
	const current = bucket.get(period) || { period };
	if (current[metricKey] === undefined || current[metricKey] === null) {
		current[metricKey] = metricValue;
	}
	bucket.set(period, current);
};

const mapStatementBucketToRows = (bucket) => {
	if (!(bucket instanceof Map) || bucket.size === 0) return null;
	const rows = Array.from(bucket.values())
		.filter((row) => isObjectRecord(row) && Object.keys(row).length > 1)
		.sort((left, right) => String(left.period).localeCompare(String(right.period)));
	return rows.length ? rows : null;
};

const parseStructuredReportsStatements = (reports) => {
	const buckets = initializeStatementBucket();

	for (const report of reports || []) {
		if (!isObjectRecord(report)) continue;
		const reportLabel = String(report.typeLabel || '');
		const reportStatementType = inferStatementTypeFromText(reportLabel) || 'income';
		const reportFrequency = inferFrequencyFromLabel(reportLabel);
		const rows = Array.isArray(report.rows) ? report.rows : [];

		for (const row of rows) {
			if (!isObjectRecord(row)) continue;
			const period =
				toIsoDate(row.referenceDate) ||
				toIsoDate(row.referenceDateFormat) ||
				toIsoDate(row.deliveryDate) ||
				toIsoDate(row.deliveryDateFormat) ||
				toIsoDate(row.date) ||
				toIsoDate(row.period) ||
				null;
			if (!period) continue;

			for (const [rawKey, rawValue] of Object.entries(row)) {
				const normalizedKey = normalizeTextKey(rawKey);
				if (!normalizedKey || REPORT_META_KEYS.has(normalizedKey)) continue;

				const numericValue = parseLocalizedNumber(rawValue);
				if (numericValue === null) continue;

				const metricKey = normalizedKey;
				const metricStatementType =
					inferStatementTypeFromText(rawKey) ||
					inferStatementTypeFromText(metricKey) ||
					reportStatementType;
				const metricFrequency = reportFrequency || inferFrequencyFromPeriod(period);

				appendMetricRowValue(
					buckets[metricStatementType]?.[metricFrequency],
					period,
					metricKey,
					numericValue
				);
			}
		}
	}

	return {
		financials: mapStatementBucketToRows(buckets.income.annual),
		quarterly_financials: mapStatementBucketToRows(buckets.income.quarterly),
		balance_sheet: mapStatementBucketToRows(buckets.balance.annual),
		quarterly_balance_sheet: mapStatementBucketToRows(buckets.balance.quarterly),
		cashflow: mapStatementBucketToRows(buckets.cashflow.annual),
		quarterly_cashflow: mapStatementBucketToRows(buckets.cashflow.quarterly),
	};
};

class B3FinancialStatementsProvider {
	constructor(options = {}) {
		this.baseUrl = options.baseUrl || B3_FII_BASE_URL;
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_B3_TIMEOUT_MS || DEFAULT_TIMEOUT_MS);
		this.language = options.language || 'pt-br';
		this.startYear = Number(options.startYear || process.env.MARKET_DATA_B3_REPORTS_START_YEAR || DEFAULT_START_YEAR);
		this.pageSize = Number(options.pageSize || process.env.MARKET_DATA_B3_PAGE_SIZE || 50);
		this.maxReportTypes = Number(options.maxReportTypes || process.env.MARKET_DATA_B3_MAX_REPORT_TYPES || 8);
	}

	canHandle(asset) {
		const market = String(asset?.market || '').toUpperCase();
		const assetClass = String(asset?.assetClass || '').toLowerCase();
		const ticker = normalizeTicker(asset?.ticker);
		if (market !== 'BR') return false;
		if (assetClass && assetClass !== 'fii') return false;
		return assetClass === 'fii' || ticker.endsWith('11');
	}

	async fetch(asset) {
		if (!this.canHandle(asset)) return null;

		const normalizedTicker = normalizeTicker(asset.ticker);
		const fundDescriptor = await this.#resolveFundDescriptor(normalizedTicker);
		if (!fundDescriptor) return null;

		const detailPayload = await this.#requestJson('GetDetailFund', {
			language: this.language,
			idFNET: fundDescriptor.idFNET,
			idCEM: fundDescriptor.idCEM,
			typeFund: 'FII',
		});

		const dateInitial = `${this.startYear}-01-01`;
		const dateFinal = nowIso().slice(0, 10);
		const reportFilterBase = {
			language: this.language,
			idFNET: fundDescriptor.idFNET,
			dateInitial,
			dateFinal,
			pageNumber: 1,
			pageSize: this.pageSize,
		};

		const reportTypesPayload = await this.#requestJson('GetTypesReport', reportFilterBase);
		const reportTypes = Array.isArray(reportTypesPayload) ? reportTypesPayload : [];
		const reports = [];

		for (const reportType of reportTypes.slice(0, this.maxReportTypes)) {
			const typeId = toNumberOrNull(reportType?.inputId);
			if (typeId === null) continue;
			const reportPayload = await this.#requestJson('GetStructuredReports', {
				...reportFilterBase,
				type: typeId,
			});
			const reportRows = Array.isArray(reportPayload?.results) ? reportPayload.results : [];
			if (reportRows.length === 0) continue;

			reports.push({
				typeId,
				typeLabel: reportType?.label || reportType?.ariaLabel || null,
				rows: reportRows,
				page: reportPayload?.page || null,
			});
		}

		const statements = parseStructuredReportsStatements(reports);
		const detail = isObjectRecord(detailPayload) ? detailPayload : {};
		const currentPrice = parseLocalizedNumber(detail.quote);
		const marketCap = parseLocalizedNumber(detail.equity);

		return {
			data_source: 'b3_direct_financials',
			is_scraped: false,
			quote: {
				currentPrice,
				currency: 'BRL',
				change: null,
				changePercent: null,
				previousClose: null,
				marketCap,
				volume: null,
			},
			fundamentals: {
				info: detail,
				financials: statements.financials,
				quarterly_financials: statements.quarterly_financials,
				balance_sheet: statements.balance_sheet,
				quarterly_balance_sheet: statements.quarterly_balance_sheet,
				cashflow: statements.cashflow,
				quarterly_cashflow: statements.quarterly_cashflow,
				b3: {
					fund: fundDescriptor,
					report_types: reportTypes,
					reports_total: reports.reduce(
						(total, report) => total + (Array.isArray(report.rows) ? report.rows.length : 0),
						0
					),
				},
			},
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				ticker: normalizedTicker,
				fund: fundDescriptor,
				detail,
				report_types: reportTypes,
				reports,
				financials: statements.financials,
				quarterly_financials: statements.quarterly_financials,
				balance_sheet: statements.balance_sheet,
				quarterly_balance_sheet: statements.quarterly_balance_sheet,
				cashflow: statements.cashflow,
				quarterly_cashflow: statements.quarterly_cashflow,
			},
		};
	}

	async #resolveFundDescriptor(ticker) {
		const normalizedTicker = normalizeTicker(ticker);
		const normalizedAcronym = normalizeFiiAcronym(normalizedTicker);
		const candidateKeywords = Array.from(
			new Set(
				[
					normalizedTicker,
					normalizedTicker.endsWith('11') ? normalizedAcronym : `${normalizedTicker}11`,
					normalizedAcronym,
				].filter(Boolean)
			)
		);

		for (const keyword of candidateKeywords) {
			const listingPayload = await this.#requestJson('GetListFunds', {
				language: this.language,
				typeFund: 'FII',
				pageNumber: 1,
				pageSize: 20,
				keyword,
			});
			const results = Array.isArray(listingPayload?.results) ? listingPayload.results : [];
			if (results.length === 0) continue;

			const selected =
				results.find((result) => normalizeFiiAcronym(result?.acronym) === normalizedAcronym) ||
				results[0];

			const idFNET = toNumberOrNull(selected?.id);
			const idCEM = String(selected?.acronym || normalizedAcronym || '')
				.trim()
				.toUpperCase();
			if (idFNET === null || !idCEM) continue;

			return {
				idFNET,
				idCEM,
				acronym: idCEM,
				fundName: selected?.fundName || null,
				tradingName: selected?.tradingName || null,
			};
		}

		return null;
	}

	async #requestJson(path, payload) {
		const encodedPayload = encodePayload(payload);
		if (!encodedPayload) return null;
		const url = `${this.baseUrl}${path}/${encodedPayload}`;

		try {
			const response = await withRetry(
				() =>
					fetchWithTimeout(url, {
						timeoutMs: this.timeoutMs,
						headers: {
							Accept: 'application/json,text/plain,*/*',
						},
					}),
				{
					retries: 1,
					baseDelayMs: 300,
					factor: 2,
				}
			);
			if (!response.ok) return null;

			const text = await response.text();
			if (!text || text.trim().length === 0) return null;
			return JSON.parse(text);
		} catch {
			return null;
		}
	}
}

module.exports = {
	B3FinancialStatementsProvider,
};
