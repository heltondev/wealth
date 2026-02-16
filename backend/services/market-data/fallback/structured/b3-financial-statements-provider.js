const {
	fetchWithTimeout,
	nowIso,
	toNumberOrNull,
	withRetry,
} = require('../../utils');

const B3_FII_BASE_URL = 'https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/';
const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_START_YEAR = 2016;
const DEFAULT_MAX_PAGES = 8;
const DEFAULT_RELEVANT_CATEGORIES = [1, 2, 3, 4, 5, 6, 7, 8];

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

const MONTHLY_STRUCTURED_REPORT_HINTS = [
	'informemensalestruturado',
	'informemensal',
	'estruturado',
];

const FUND_PORTFOLIO_SECTION_TAG = 'TotalInvestido';
const FUND_PORTFOLIO_SECTION_LABEL = 'Total Investido';
const FUND_PORTFOLIO_MAX_ITEMS = 30;

const FUND_PORTFOLIO_LABEL_OVERRIDES = {
	direitosbensimoveis: 'Direitos e bens imóveis',
	terrenos: 'Terrenos',
	imoveisrendaacabados: 'Imóveis para renda (acabados)',
	imoveisrendaconstrucao: 'Imóveis para renda (construção)',
	imoveisvendaacabados: 'Imóveis para venda (acabados)',
	imoveisvendaconstrucao: 'Imóveis para venda (construção)',
	outrosdireitosreais: 'Outros direitos reais',
	acoes: 'Ações',
	debentures: 'Debêntures',
	bonussubscricao: 'Bônus de subscrição',
	certificadosdepositovalmob: 'Certificados de Depósito de Valores Mobiliários',
	fia: 'FIA',
	fip: 'FIP',
	fii: 'FII',
	fdic: 'FIDC',
	outrascotasfi: 'Outras cotas de FI',
	notaspromissorias: 'Notas promissórias',
	notascomerciais: 'Notas comerciais',
	acoessociedadesativfii: 'Ações de sociedades de ativo FII',
	cotassociedadesativfii: 'Cotas de sociedades de ativo FII',
	cepac: 'CEPAC',
	cricra: 'CRI/CRA',
	letrashipotecarias: 'Letras hipotecárias',
	lcilca: 'LCI/LCA',
	lig: 'LIG',
	outrosvaloresmobiliarios: 'Outros valores mobiliários',
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

	const brazilianDateTimeMatch = text.match(
		/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+\d{2}:\d{2}(?::\d{2})?)?$/
	);
	if (brazilianDateTimeMatch) {
		return `${brazilianDateTimeMatch[3]}-${brazilianDateTimeMatch[2]}-${brazilianDateTimeMatch[1]}`;
	}

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

const readXmlAttribute = (rawAttributes, attributeName) => {
	const normalizedName = String(attributeName || '').trim();
	if (!normalizedName) return null;
	const serialized = String(rawAttributes || '');
	const regex = new RegExp(`${normalizedName}\\s*=\\s*"([^"]+)"`, 'i');
	const match = serialized.match(regex);
	if (!match) return null;
	return String(match[1] || '').trim() || null;
};

const extractXmlFirstLevelElements = (xmlFragment) => {
	const rows = [];
	const fragment = String(xmlFragment || '');
	if (!fragment) return rows;

	const regex = /<([A-Za-z0-9_]+)\b([^>]*)>([\s\S]*?)<\/\1>/g;
	let match = regex.exec(fragment);
	while (match) {
		rows.push({
			tagName: String(match[1] || '').trim(),
			attributes: String(match[2] || ''),
			inner: String(match[3] || ''),
		});
		match = regex.exec(fragment);
	}

	return rows;
};

const toFundPortfolioLabel = (rawTagName) => {
	const normalizedTag = normalizeTextKey(rawTagName);
	if (!normalizedTag) return null;
	if (FUND_PORTFOLIO_LABEL_OVERRIDES[normalizedTag]) {
		return FUND_PORTFOLIO_LABEL_OVERRIDES[normalizedTag];
	}

	const text = String(rawTagName || '')
		.replace(/_/g, ' ')
		.replace(/([a-z])([A-Z0-9])/g, '$1 $2')
		.replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
		.trim();
	if (!text) return null;
	return text;
};

const decodeFundosNetPayloadToXml = (payloadText) => {
	let serialized = String(payloadText || '').trim();
	if (!serialized) return null;

	if (
		(serialized.startsWith('"') && serialized.endsWith('"')) ||
		(serialized.startsWith('\'') && serialized.endsWith('\''))
	) {
		try {
			serialized = JSON.parse(serialized);
		} catch {
			serialized = serialized.slice(1, -1);
		}
	}

	const trimmed = String(serialized || '').trim();
	if (!trimmed) return null;
	if (trimmed.startsWith('<')) return trimmed;

	const base64Payload = trimmed.replace(/\s+/g, '');
	if (!/^[A-Za-z0-9+/=]+$/.test(base64Payload)) return null;
	try {
		const decoded = Buffer.from(base64Payload, 'base64').toString('utf8');
		if (!decoded || !decoded.includes('<')) return null;
		return decoded;
	} catch {
		return null;
	}
};

const normalizeFundosNetDownloadUrl = (value) => {
	const normalized = normalizeDocumentUrl(value);
	if (!normalized) return null;
	if (normalized.includes('/downloadDocumento?')) return normalized;
	if (normalized.includes('/visualizarDocumento?')) {
		return normalized.replace('/visualizarDocumento?', '/downloadDocumento?');
	}
	const idMatch = normalized.match(/[?&]id=(\d+)/i);
	if (!idMatch) return normalized;
	return `https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id=${idMatch[1]}`;
};

const normalizeReportRowReferenceDate = (row) => (
	toIsoDate(row?.referenceDate) ||
	toIsoDate(row?.referenceDateFormat) ||
	toIsoDate(row?.deliveryDate) ||
	toIsoDate(row?.deliveryDateFormat) ||
	null
);

const looksLikeMonthlyStructuredReport = (reportTypeLabel, row) => {
	const candidateTexts = [
		reportTypeLabel,
		row?.describleType,
		row?.typeName,
		row?.label,
		row?.ariaLabel,
	];
	const normalizedText = candidateTexts
		.map((value) => normalizeTextKey(value))
		.filter(Boolean)
		.join(' ');
	if (!normalizedText) return false;
	return MONTHLY_STRUCTURED_REPORT_HINTS.some((hint) => normalizedText.includes(hint));
};

const extractFundPortfolioFromMonthlyStructuredXml = (xmlText, source) => {
	const xml = String(xmlText || '');
	if (!xml) return [];

	const sectionRegex = new RegExp(`<${FUND_PORTFOLIO_SECTION_TAG}\\b([^>]*)>([\\s\\S]*?)<\\/${FUND_PORTFOLIO_SECTION_TAG}>`, 'i');
	const sectionMatch = xml.match(sectionRegex);
	if (!sectionMatch) return [];

	const sectionAttributes = String(sectionMatch[1] || '');
	const sectionBody = String(sectionMatch[2] || '');
	const totalFromAttribute = parseLocalizedNumber(readXmlAttribute(sectionAttributes, 'total'));

	const rawRows = extractXmlFirstLevelElements(sectionBody)
		.map((entry) => {
			const label = toFundPortfolioLabel(entry.tagName);
			if (!label) return null;

			let value = parseLocalizedNumber(readXmlAttribute(entry.attributes, 'total'));
			if (value === null) {
				const innerText = String(entry.inner || '').trim();
				if (innerText && !innerText.includes('<')) {
					value = parseLocalizedNumber(innerText);
				}
			}
			if (value === null || !Number.isFinite(value) || value <= 0) return null;

			return {
				label,
				value,
				category: FUND_PORTFOLIO_SECTION_LABEL,
			};
		})
		.filter(Boolean);

	if (rawRows.length === 0) return [];

	const denominator = (Number.isFinite(totalFromAttribute) && totalFromAttribute > 0)
		? totalFromAttribute
		: rawRows.reduce((sum, entry) => sum + entry.value, 0);
	if (!Number.isFinite(denominator) || denominator <= 0) return [];

	const dedupe = new Set();
	const rows = [];
	for (const entry of rawRows) {
		const allocation = (entry.value / denominator) * 100;
		if (!Number.isFinite(allocation) || allocation <= 0 || allocation > 100.5) continue;

		const key = normalizeTextKey(entry.label);
		if (!key || dedupe.has(key)) continue;
		dedupe.add(key);

		rows.push({
			label: entry.label,
			allocation_pct: Number(allocation.toFixed(4)),
			category: entry.category,
			source: source || 'b3_informe_mensal_estruturado',
		});
	}

	return rows
		.sort((left, right) => right.allocation_pct - left.allocation_pct)
		.slice(0, FUND_PORTFOLIO_MAX_ITEMS);
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

const normalizeDocumentUrl = (value) => {
	const text = String(value || '').trim();
	if (!text) return null;
	if (/^https?:\/\//i.test(text)) return text;
	if (text.startsWith('//')) return `https:${text}`;
	if (text.startsWith('/')) return `https://fnet.bmfbovespa.com.br${text}`;
	if (/^[a-z0-9.-]+\.[a-z]{2,}\/?/i.test(text)) return `https://${text}`;
	return null;
};

const normalizeFilingDocumentRow = (row, source) => {
	if (!isObjectRecord(row)) return null;
	const viewerUrl = normalizeDocumentUrl(
		row.urlViewerFundosNet ?? row.urlviewerfundosnet ?? row.urlViewer ?? null
	);
	const downloadUrl = normalizeDocumentUrl(
		row.urlFundosNet ??
		row.urlfundosnet ??
		row.urlDownload ??
		row.urldownload ??
		null
	);
	const primaryUrl = viewerUrl || downloadUrl;
	if (!primaryUrl) return null;

	const title =
		String(
			row.subjects ||
			row.subject ||
			row.describleType ||
			row.describleKind ||
			row.describleCategory ||
			row.typeName ||
			row.type ||
			''
		).trim() || 'Documento B3';
	const category = String(row.describleCategory || row.category || '').trim() || null;
	const documentType = String(row.describleType || row.describleKind || row.typeName || '').trim() || null;
	const referenceDate =
		toIsoDate(row.referenceDate) ||
		toIsoDate(row.referenceDateFormat) ||
		toIsoDate(row.competencia) ||
		null;
	const deliveryDate =
		toIsoDate(row.deliveryDate) ||
		toIsoDate(row.deliveryDateFormat) ||
		null;

	return {
		id: String(row.idMain || row.id || '').trim() || null,
		source: source || 'b3_fundosnet',
		title,
		category,
		document_type: documentType,
		reference_date: referenceDate,
		delivery_date: deliveryDate,
		status: String(row.status || '').trim() || null,
		url: primaryUrl,
		url_viewer: viewerUrl,
		url_download: downloadUrl,
		url_alternatives: [],
	};
};

const dedupeDocuments = (documents) => {
	const dedupe = new Set();
	const rows = [];
	for (const document of Array.isArray(documents) ? documents : []) {
		if (!isObjectRecord(document)) continue;
		const url = String(document.url || '').trim();
		if (!url) continue;
		const key = [
			url,
			String(document.reference_date || ''),
			String(document.delivery_date || ''),
			String(document.title || ''),
		].join('|');
		if (dedupe.has(key)) continue;
		dedupe.add(key);
		rows.push(document);
	}

	return rows.sort((left, right) => {
		const leftDate = left.reference_date || left.delivery_date || '';
		const rightDate = right.reference_date || right.delivery_date || '';
		return String(rightDate).localeCompare(String(leftDate));
	});
};

const collectNumericIdCandidates = (value, output = new Set(), path = []) => {
	if (Array.isArray(value)) {
		for (let index = 0; index < value.length; index += 1) {
			collectNumericIdCandidates(value[index], output, [...path, String(index)]);
		}
		return output;
	}
	if (!isObjectRecord(value)) return output;

	for (const [key, entry] of Object.entries(value)) {
		const normalizedKey = normalizeTextKey(key);
		const numericValue = toNumberOrNull(entry);
		const keyLooksLikeId =
			normalizedKey.startsWith('id') ||
			normalizedKey.includes('idfnet') ||
			normalizedKey.includes('idmain');

		if (
			keyLooksLikeId &&
			numericValue !== null &&
			Number.isFinite(numericValue) &&
			numericValue > 0 &&
			numericValue < 1_000_000_000
		) {
			output.add(Math.trunc(numericValue));
		}

		if (entry && typeof entry === 'object') {
			collectNumericIdCandidates(entry, output, [...path, key]);
		}
	}

	return output;
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
		this.maxPages = Number(options.maxPages || process.env.MARKET_DATA_B3_MAX_PAGES || DEFAULT_MAX_PAGES);
		const categories =
			options.relevantCategories ||
			process.env.MARKET_DATA_B3_RELEVANT_CATEGORIES ||
			DEFAULT_RELEVANT_CATEGORIES;
		this.relevantCategories = Array.isArray(categories)
			? categories.map((value) => toNumberOrNull(value)).filter((value) => value !== null)
			: String(categories)
				.split(',')
				.map((value) => toNumberOrNull(value.trim()))
				.filter((value) => value !== null);
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
		const baseReportFilter = {
			language: this.language,
			dateInitial,
			dateFinal,
			pageNumber: 1,
			pageSize: this.pageSize,
		};
		const detail = isObjectRecord(detailPayload) ? detailPayload : {};
		const reportIdCandidates = Array.from(
			new Set([
				fundDescriptor.idFNET,
				...Array.from(collectNumericIdCandidates(detail)),
			].filter((value) => Number.isFinite(value) && value > 0))
		);

		const reportCollection = await this.#fetchStructuredReports(
			reportIdCandidates,
			baseReportFilter
		);
		const reports = reportCollection.reports;
		const reportTypes = reportCollection.reportTypes;
		const resolvedStructuredReportId = reportCollection.resolvedReportId;
		const relevantReports = await this.#fetchRelevantReports(
			reportIdCandidates,
			baseReportFilter
		);

		const structuredDocuments = reports.flatMap((report) => (
			Array.isArray(report.rows)
				? report.rows
					.map((row) => normalizeFilingDocumentRow(row, 'b3_structured_reports'))
					.filter(Boolean)
				: []
		));
		const relevantDocuments = relevantReports.rows
			.map((row) => normalizeFilingDocumentRow(row, 'b3_reports_relevants'))
			.filter(Boolean);
		const documents = dedupeDocuments([...structuredDocuments, ...relevantDocuments]);
		const fundPortfolioPayload = await this.#fetchFundPortfolioFromStructuredReports(reports);
		const fundPortfolioRows = Array.isArray(fundPortfolioPayload?.rows)
			? fundPortfolioPayload.rows
			: null;

		const statements = parseStructuredReportsStatements(reports);
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
				fund_portfolio: fundPortfolioRows,
				portfolio_composition: fundPortfolioRows,
				b3: {
					fund: fundDescriptor,
					report_types: reportTypes,
					relevant_report_categories: relevantReports.categories,
					resolved_report_id: resolvedStructuredReportId || relevantReports.resolvedReportId || null,
					reports_total: reports.reduce(
						(total, report) => total + (Array.isArray(report.rows) ? report.rows.length : 0),
						0
					),
					relevant_reports_total: relevantReports.rows.length,
					fund_portfolio_reference_date: fundPortfolioPayload?.reference_date || null,
					fund_portfolio_source_url: fundPortfolioPayload?.source_url || null,
				},
			},
			fund_portfolio: fundPortfolioRows,
			portfolio_composition: fundPortfolioRows,
			portfolio_composition_meta: fundPortfolioPayload
				? {
					reference_date: fundPortfolioPayload.reference_date || null,
					source_url: fundPortfolioPayload.source_url || null,
				}
				: null,
			documents,
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				ticker: normalizedTicker,
				fund: fundDescriptor,
				detail,
				report_id_candidates: reportIdCandidates,
				report_types: reportTypes,
				reports,
				relevant_reports: relevantReports.rows,
				financials: statements.financials,
				quarterly_financials: statements.quarterly_financials,
				balance_sheet: statements.balance_sheet,
				quarterly_balance_sheet: statements.quarterly_balance_sheet,
				cashflow: statements.cashflow,
				quarterly_cashflow: statements.quarterly_cashflow,
				documents,
				fund_portfolio: fundPortfolioRows,
				fund_portfolio_meta: fundPortfolioPayload
					? {
						reference_date: fundPortfolioPayload.reference_date || null,
						source_url: fundPortfolioPayload.source_url || null,
					}
					: null,
			},
		};
	}

	async #fetchFundPortfolioFromStructuredReports(reports) {
		const candidates = [];
		for (const report of Array.isArray(reports) ? reports : []) {
			const reportLabel = report?.typeLabel || report?.label || null;
			for (const row of Array.isArray(report?.rows) ? report.rows : []) {
				if (!isObjectRecord(row)) continue;
				if (!looksLikeMonthlyStructuredReport(reportLabel, row)) continue;

				const downloadUrl = normalizeFundosNetDownloadUrl(
					row.urlFundosNet ||
					row.urlfundosnet ||
					row.urlViewerFundosNet ||
					row.urlviewerfundosnet ||
					null
				);
				if (!downloadUrl) continue;

				candidates.push({
					download_url: downloadUrl,
					reference_date: normalizeReportRowReferenceDate(row),
				});
			}
		}

		if (candidates.length === 0) return null;

		const dedupeUrls = new Set();
		const orderedCandidates = candidates
			.filter((entry) => {
				const key = String(entry.download_url || '').trim();
				if (!key || dedupeUrls.has(key)) return false;
				dedupeUrls.add(key);
				return true;
			})
			.sort((left, right) => {
				const leftDate = String(left.reference_date || '');
				const rightDate = String(right.reference_date || '');
				return rightDate.localeCompare(leftDate);
			});

		for (const candidate of orderedCandidates) {
			const xml = await this.#downloadFundosNetXml(candidate.download_url);
			if (!xml) continue;
			const rows = extractFundPortfolioFromMonthlyStructuredXml(
				xml,
				'b3_informe_mensal_estruturado'
			);
			if (!Array.isArray(rows) || rows.length === 0) continue;

			return {
				rows,
				reference_date: candidate.reference_date || null,
				source_url: candidate.download_url,
			};
		}

		return null;
	}

	async #downloadFundosNetXml(downloadUrl) {
		if (!downloadUrl) return null;
		try {
			const response = await withRetry(
				() => fetchWithTimeout(downloadUrl, {
					timeoutMs: this.timeoutMs,
					headers: {
						Accept: 'text/plain,application/xml,text/xml,*/*',
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
			return decodeFundosNetPayloadToXml(text);
		} catch {
			return null;
		}
	}

	async #fetchStructuredReports(reportIdCandidates, baseFilter) {
		const reportIds = Array.isArray(reportIdCandidates) ? reportIdCandidates : [];
		let resolvedReportId = null;
		let reportTypes = [];
		const reports = [];

		for (const reportId of reportIds) {
			const reportFilterBase = {
				...baseFilter,
				idFNET: reportId,
			};
			const reportTypesPayload = await this.#requestJson('GetTypesReport', reportFilterBase);
			const candidateTypes = Array.isArray(reportTypesPayload) ? reportTypesPayload : [];
			if (candidateTypes.length === 0) continue;

			const candidateReports = [];
			for (const reportType of candidateTypes.slice(0, this.maxReportTypes)) {
				const typeId = toNumberOrNull(reportType?.inputId);
				if (typeId === null) continue;
				const reportRows = await this.#listStructuredReportsPages({
					...reportFilterBase,
					type: typeId,
				});
				if (reportRows.length === 0) continue;
				candidateReports.push({
					typeId,
					typeLabel: reportType?.label || reportType?.ariaLabel || null,
					rows: reportRows,
				});
			}

			if (candidateReports.length === 0) continue;
			resolvedReportId = reportId;
			reportTypes = candidateTypes;
			reports.push(...candidateReports);
			break;
		}

		return {
			resolvedReportId,
			reportTypes,
			reports,
		};
	}

	async #fetchRelevantReports(reportIdCandidates, baseFilter) {
		const reportIds = Array.isArray(reportIdCandidates) ? reportIdCandidates : [];
		const categories = this.relevantCategories.length > 0
			? this.relevantCategories
			: DEFAULT_RELEVANT_CATEGORIES;
		let resolvedReportId = null;
		const rows = [];

		for (const reportId of reportIds) {
			const candidateRows = [];
			for (const category of categories) {
				const pageRows = await this.#listRelevantReportsPages({
					...baseFilter,
					idFNET: reportId,
					category,
				});
				if (pageRows.length === 0) continue;
				for (const row of pageRows) {
					candidateRows.push({
						...row,
						__category: category,
					});
				}
			}
			if (candidateRows.length === 0) continue;
			resolvedReportId = reportId;
			rows.push(...candidateRows);
			break;
		}

		return {
			resolvedReportId,
			categories,
			rows,
		};
	}

	async #listStructuredReportsPages(basePayload) {
		const rows = [];
		let pageNumber = 1;
		let totalPages = 1;

		while (pageNumber <= totalPages && pageNumber <= this.maxPages) {
			const payload = {
				...basePayload,
				pageNumber,
				pageSize: basePayload.pageSize || this.pageSize,
			};
			const response = await this.#requestJson('GetStructuredReports', payload);
			const pageRows = Array.isArray(response?.results) ? response.results : [];
			rows.push(...pageRows);
			totalPages = toNumberOrNull(response?.page?.totalPages) || 1;
			if (pageRows.length === 0) break;
			pageNumber += 1;
		}

		return rows;
	}

	async #listRelevantReportsPages(basePayload) {
		const rows = [];
		let pageNumber = 1;
		let totalPages = 1;

		while (pageNumber <= totalPages && pageNumber <= this.maxPages) {
			const payload = {
				...basePayload,
				pageNumber,
				pageSize: basePayload.pageSize || this.pageSize,
			};
			const response = await this.#requestJson('GetReportsRelevants', payload);
			const pageRows = Array.isArray(response?.results) ? response.results : [];
			rows.push(...pageRows);
			totalPages = toNumberOrNull(response?.page?.totalPages) || 1;
			if (pageRows.length === 0) break;
			pageNumber += 1;
		}

		return rows;
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
