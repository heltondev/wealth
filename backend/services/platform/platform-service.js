const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const {
	QueryCommand,
	PutCommand,
	UpdateCommand,
	DeleteCommand,
	GetCommand,
	ScanCommand,
} = require('@aws-sdk/lib-dynamodb');

const {
	fetchWithTimeout,
	withRetry,
	toNumberOrNull,
	nowIso,
} = require('../market-data/utils');
const { extractJsonScriptContent } = require('../market-data/fallback/extractors');
const { resolveAssetMarket, resolveYahooSymbol } = require('../market-data/symbol-resolver');
const { YahooApiProvider } = require('../market-data/providers/yahoo-api-provider');
const { YahooFinanceScraper } = require('../market-data/fallback/scrapers/yahoo-finance-scraper');
const { GoogleFinanceScraper } = require('../market-data/fallback/scrapers/google-finance-scraper');
const { StatusInvestScraper } = require('../market-data/fallback/scrapers/status-invest-scraper');
const { StatusInvestStructuredProvider } = require('../market-data/fallback/structured/status-invest-provider');
const {
	B3FinancialStatementsProvider,
} = require('../market-data/fallback/structured/b3-financial-statements-provider');
const {
	FundsExplorerProvider,
} = require('../market-data/fallback/structured/fundsexplorer-provider');
const {
	buildAwsClientConfig,
	resolveS3BucketName,
	resolveRuntimeEnvironment,
} = require('../../config/aws');

const PERIOD_TO_DAYS = {
	'1M': 30,
	'3M': 90,
	'6M': 180,
	'1A': 365,
	'1Y': 365,
	'2A': 730,
	'2Y': 730,
	'5A': 1825,
	'5Y': 1825,
	MAX: null,
};

const BENCHMARK_SYMBOLS = {
	IBOV: '^BVSP',
	SNP500: '^GSPC',
	SP500: '^GSPC',
	SNP: '^GSPC',
	IFIX: 'IFIX.SA',
	TSX: '^GSPTSE',
};

const INDICATOR_SERIES = {
	CDI: '12',
	SELIC: '11',
	SELIC_META: '432',
	IPCA: '433',
	IGPM: '189',
	POUPANCA: '195',
	USD_BRL_ALT: '1',
};

const TAX_RATE_BY_CLASS = {
	stock: 0.15,
	fii: 0.2,
	etf: 0.15,
	bond: 0.15,
	crypto: 0.15,
	rsu: 0.15,
};

const numeric = (value, fallback = 0) => {
	const parsed = toNumberOrNull(value);
	return parsed === null ? fallback : parsed;
};

const FINANCIAL_STATEMENT_KEYS = [
	'financials',
	'quarterly_financials',
	'balance_sheet',
	'quarterly_balance_sheet',
	'cashflow',
	'quarterly_cashflow',
];

const FINANCIAL_DOCUMENT_LINK_KEY_HINTS = [
	'url',
	'link',
	'href',
	'download',
	'viewer',
];

const FINANCIAL_DOCUMENT_TITLE_KEY_HINTS = [
	'describletype',
	'typename',
	'label',
	'arialabel',
	'title',
	'name',
	'eventtitle',
	'documenttype',
	'reporttype',
	'type',
];

const FINANCIAL_DOCUMENT_REFERENCE_DATE_KEY_HINTS = [
	'referencedate',
	'period',
	'date',
	'competencia',
];

const FINANCIAL_DOCUMENT_DELIVERY_DATE_KEY_HINTS = [
	'deliverydate',
	'publishdate',
	'publishedat',
	'createdat',
	'updatedat',
];

const FINANCIAL_DOCUMENT_STATUS_KEY_HINTS = [
	'status',
	'situacao',
];

const FINANCIAL_DOCUMENT_TYPE_KEY_HINTS = [
	'tipo',
	'type',
	'documenttype',
	'reporttype',
	'kind',
	'classe',
];

const FINANCIAL_DOCUMENT_ID_KEY_HINTS = [
	'id',
	'idmain',
	'idfnet',
	'idcem',
	'documentid',
	'reportid',
];

const FINANCIAL_DOCUMENT_TITLE_KEYWORDS = [
	'comunicado',
	'relatorio',
	'informe',
	'fato relevante',
	'assembleia',
	'ata',
	'document',
	'filing',
	'demonstrativo',
	'demonstracao',
	'resultado',
];

const FUND_PORTFOLIO_LABEL_KEY_HINTS = [
	'nome',
	'name',
	'segmento',
	'segment',
	'setor',
	'sector',
	'categoria',
	'category',
	'tipo',
	'type',
	'ativo',
	'asset',
	'ticker',
	'titulo',
	'title',
	'descricao',
	'description',
	'classe',
	'class',
];

const FUND_PORTFOLIO_ALLOCATION_KEY_HINTS = [
	'percent',
	'perc',
	'particip',
	'allocation',
	'aloc',
	'weight',
	'peso',
	'represent',
	'compos',
];

const FUND_PORTFOLIO_CATEGORY_KEY_HINTS = [
	'segmento',
	'segment',
	'setor',
	'sector',
	'categoria',
	'category',
	'tipo',
	'type',
	'classe',
	'class',
];

const createEmptyFinancialStatements = () => ({
	financials: null,
	quarterly_financials: null,
	balance_sheet: null,
	quarterly_balance_sheet: null,
	cashflow: null,
	quarterly_cashflow: null,
});

const isPopulatedStatement = (value) => {
	if (Array.isArray(value)) return value.length > 0;
	if (value && typeof value === 'object') return Object.keys(value).length > 0;
	return false;
};

const hasAnyFinancialStatements = (statements) =>
	FINANCIAL_STATEMENT_KEYS.some((key) => isPopulatedStatement(statements?.[key]));

const mergeFinancialStatements = (base, incoming) => {
	const merged = {
		...createEmptyFinancialStatements(),
		...(base || {}),
	};
	for (const key of FINANCIAL_STATEMENT_KEYS) {
		if (!isPopulatedStatement(merged[key]) && isPopulatedStatement(incoming?.[key])) {
			merged[key] = incoming[key];
		}
	}
	return merged;
};

const normalizeRecordKey = (value) =>
	String(value || '')
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
		.replace(/[^a-z0-9]/g, '');

const isObjectRecord = (value) =>
	value !== null && typeof value === 'object' && !Array.isArray(value);

const normalizeDocumentUrl = (value) => {
	const text = String(value || '').trim();
	if (!text) return null;
	if (/^https?:\/\//i.test(text)) return text;
	if (text.startsWith('//')) return `https:${text}`;
	if (text.startsWith('/')) return `https://sistemaswebb3-listados.b3.com.br${text}`;
	if (/^[a-z0-9.-]+\.[a-z]{2,}\/?/i.test(text)) return `https://${text}`;
	return null;
};

const looksLikeFinancialDocumentUrl = (url) => {
	const normalized = String(url || '').toLowerCase();
	if (!normalized) return false;
	if (normalized.includes('.pdf')) return true;
	if (normalized.includes('fundosnet')) return true;
	if (normalized.includes('rad.cvm.gov.br')) return true;
	if (normalized.includes('/download')) return true;
	if (normalized.includes('document') || normalized.includes('comunicado')) return true;
	return false;
};

const inferFinancialDocumentType = (title, fallback = null) => {
	const normalized = String(title || fallback || '').toLowerCase();
	if (!normalized) return null;
	if (normalized.includes('comunicado')) return 'comunicado';
	if (normalized.includes('fato relevante')) return 'fato_relevante';
	if (normalized.includes('relatorio')) return 'relatorio';
	if (normalized.includes('informe')) return 'informe';
	if (normalized.includes('ata')) return 'ata';
	if (normalized.includes('assembleia')) return 'assembleia';
	if (normalized.includes('resultado')) return 'resultado';
	if (normalized.includes('balanc') || normalized.includes('dre')) return 'demonstrativo';
	return null;
};

const findRecordValueByHints = (record, hints) => {
	if (!isObjectRecord(record)) return null;
	for (const [key, value] of Object.entries(record)) {
		const normalized = normalizeRecordKey(key);
		if (!normalized) continue;
		if (!hints.some((hint) => normalized.includes(hint))) continue;
		if (value === null || value === undefined || String(value).trim() === '') continue;
		return value;
	}
	return null;
};

const readFinancialDocumentLinksFromRecord = (record) => {
	if (!isObjectRecord(record)) {
		return {
			primary: null,
			viewer: null,
			download: null,
			all: [],
		};
	}

	const urls = [];
	for (const [key, value] of Object.entries(record)) {
		const normalized = normalizeRecordKey(key);
		if (!normalized) continue;
		if (!FINANCIAL_DOCUMENT_LINK_KEY_HINTS.some((hint) => normalized.includes(hint))) continue;
		const normalizedUrl = normalizeDocumentUrl(value);
		if (!normalizedUrl) continue;
		urls.push({ key: normalized, url: normalizedUrl });
	}

	const viewer = urls.find((item) => item.key.includes('viewer'))?.url || null;
	const download = urls.find((item) => item.key.includes('download'))?.url || null;
	const primary = viewer || download || urls[0]?.url || null;
	const all = Array.from(new Set(urls.map((item) => item.url)));

	return {
		primary,
		viewer,
		download,
		all,
	};
};

const readFinancialDocumentsFromPayload = (payload, sourceHint) => {
	const root = isObjectRecord(payload) ? payload : {};
	const fundamentals = isObjectRecord(root.fundamentals) ? root.fundamentals : {};
	const raw = isObjectRecord(root.raw) ? root.raw : {};

	const sourceValue =
		String(
			root.data_source ||
				root.source ||
				fundamentals.data_source ||
				raw.data_source ||
				sourceHint ||
				'financial_source'
		)
			.trim()
			.toLowerCase() || 'financial_source';

	const dedupe = new Set();
	const documents = [];

	const pushDocument = (record, context = {}) => {
		if (!isObjectRecord(record)) return;

		const links = readFinancialDocumentLinksFromRecord(record);
		if (!links.primary) return;

		const titleCandidateRaw =
			context.title ||
			findRecordValueByHints(record, FINANCIAL_DOCUMENT_TITLE_KEY_HINTS) ||
			null;
		const title = String(titleCandidateRaw || '').trim() || 'Financial filing';

		const referenceDate =
			normalizeDate(context.referenceDate) ||
			normalizeDate(findRecordValueByHints(record, FINANCIAL_DOCUMENT_REFERENCE_DATE_KEY_HINTS)) ||
			null;
		const deliveryDate =
			normalizeDate(context.deliveryDate) ||
			normalizeDate(findRecordValueByHints(record, FINANCIAL_DOCUMENT_DELIVERY_DATE_KEY_HINTS)) ||
			null;
		const statusRaw =
			context.status ||
			findRecordValueByHints(record, FINANCIAL_DOCUMENT_STATUS_KEY_HINTS) ||
			null;
		const status = statusRaw ? String(statusRaw).trim() : null;

		const categoryRaw =
			context.category ||
			findRecordValueByHints(record, ['category', 'tipo']) ||
			null;
		const category = categoryRaw ? String(categoryRaw).trim() : null;

		const documentTypeRaw =
			context.documentType ||
			findRecordValueByHints(record, FINANCIAL_DOCUMENT_TYPE_KEY_HINTS) ||
			inferFinancialDocumentType(title, category) ||
			null;
		const documentType = documentTypeRaw ? String(documentTypeRaw).trim() : null;

		const titleKey = normalizeRecordKey(titleCandidateRaw || '');
		const categoryKey = normalizeRecordKey(category || '');
		const isFilingLike =
			Boolean(context.forceInclude) ||
			looksLikeFinancialDocumentUrl(links.primary) ||
			FINANCIAL_DOCUMENT_TITLE_KEYWORDS.some((keyword) => {
				const normalizedKeyword = normalizeRecordKey(keyword);
				return titleKey.includes(normalizedKeyword) || categoryKey.includes(normalizedKeyword);
			});
		if (!isFilingLike) return;

		const idRaw =
			context.documentId ||
			findRecordValueByHints(record, FINANCIAL_DOCUMENT_ID_KEY_HINTS) ||
			null;
		const id = idRaw ? String(idRaw).trim() : null;

		const dedupeKey = [
			sourceValue,
			links.primary,
			links.viewer || '',
			links.download || '',
			referenceDate || '',
			deliveryDate || '',
			title,
		].join('|');
		if (dedupe.has(dedupeKey)) return;
		dedupe.add(dedupeKey);

		documents.push({
			id,
			source: sourceValue,
			title,
			category,
			document_type: documentType,
			reference_date: referenceDate,
			delivery_date: deliveryDate,
			status,
			url: links.primary,
			url_viewer: links.viewer,
			url_download: links.download,
			url_alternatives: links.all.filter((url) => url !== links.primary),
		});
	};

	const reportRows = Array.isArray(raw.reports) ? raw.reports : [];
	for (const report of reportRows) {
		if (!isObjectRecord(report)) continue;
		const reportTitle =
			report.typeLabel ||
			report.label ||
			report.typeName ||
			findRecordValueByHints(report, FINANCIAL_DOCUMENT_TITLE_KEY_HINTS) ||
			null;
		const reportStatus = findRecordValueByHints(report, FINANCIAL_DOCUMENT_STATUS_KEY_HINTS);
		const reportId = findRecordValueByHints(report, FINANCIAL_DOCUMENT_ID_KEY_HINTS);
		const rowEntries = Array.isArray(report.rows) ? report.rows : [];

		if (rowEntries.length === 0) {
			pushDocument(report, {
				title: reportTitle,
				documentType: inferFinancialDocumentType(reportTitle),
				status: reportStatus,
				documentId: reportId,
				forceInclude: true,
			});
			continue;
		}

		for (const row of rowEntries) {
			pushDocument(row, {
				title: reportTitle,
				documentType: inferFinancialDocumentType(reportTitle),
				status: reportStatus,
				documentId: reportId,
				forceInclude: true,
			});
		}
	}

	const directCandidates = [
		root,
		fundamentals,
		raw,
		raw.detail,
		fundamentals.b3,
	];

	for (const candidate of directCandidates) {
		pushDocument(candidate);
	}

	const listCandidates = [
		root.documents,
		root.communications,
		root.comunicados,
		root.notices,
		root.filings,
		root.reports,
		fundamentals.documents,
		fundamentals.communications,
		fundamentals.comunicados,
		fundamentals.notices,
		fundamentals.filings,
		fundamentals.reports,
		raw.documents,
		raw.communications,
		raw.comunicados,
		raw.notices,
		raw.filings,
	];

	for (const candidate of listCandidates) {
		if (!Array.isArray(candidate)) continue;
		for (const row of candidate) {
			pushDocument(row);
		}
	}

	return documents;
};

const mergeFinancialDocuments = (base, incoming) => {
	const dedupe = new Set();
	const merged = [];
	const append = (entry) => {
		if (!isObjectRecord(entry)) return;
		const url = String(entry.url || '').trim();
		if (!url) return;
		const key = [
			url,
			String(entry.reference_date || ''),
			String(entry.delivery_date || ''),
			String(entry.title || ''),
		].join('|');
		if (dedupe.has(key)) return;
		dedupe.add(key);
		merged.push(entry);
	};

	for (const entry of Array.isArray(base) ? base : []) append(entry);
	for (const entry of Array.isArray(incoming) ? incoming : []) append(entry);

	return merged.sort((left, right) => {
		const leftDate = normalizeDate(left.reference_date || left.delivery_date || null) || '0000-00-00';
		const rightDate = normalizeDate(right.reference_date || right.delivery_date || null) || '0000-00-00';
		return rightDate.localeCompare(leftDate);
	});
};

const readFinancialStatementsFromPayload = (payload) => {
	const root = payload && typeof payload === 'object' ? payload : {};
	const fundamentals = root.fundamentals && typeof root.fundamentals === 'object' ? root.fundamentals : {};
	const raw = root.raw && typeof root.raw === 'object' ? root.raw : {};
	const quoteSummary = raw.quote_summary && typeof raw.quote_summary === 'object' ? raw.quote_summary : {};
	const rawFinal = raw.final_payload && typeof raw.final_payload === 'object' ? raw.final_payload : {};
	const rawPrimary = raw.primary_payload && typeof raw.primary_payload === 'object' ? raw.primary_payload : {};

	return {
		financials:
			root.financials ??
			fundamentals.financials ??
			raw.financials ??
			rawFinal.financials ??
			rawPrimary.financials ??
			quoteSummary.financials ??
			null,
		quarterly_financials:
			root.quarterly_financials ??
			fundamentals.quarterly_financials ??
			raw.quarterly_financials ??
			rawFinal.quarterly_financials ??
			rawPrimary.quarterly_financials ??
			quoteSummary.quarterly_financials ??
			null,
		balance_sheet:
			root.balance_sheet ??
			fundamentals.balance_sheet ??
			raw.balance_sheet ??
			rawFinal.balance_sheet ??
			rawPrimary.balance_sheet ??
			quoteSummary.balance_sheet ??
			null,
		quarterly_balance_sheet:
			root.quarterly_balance_sheet ??
			fundamentals.quarterly_balance_sheet ??
			raw.quarterly_balance_sheet ??
			rawFinal.quarterly_balance_sheet ??
			rawPrimary.quarterly_balance_sheet ??
			quoteSummary.quarterly_balance_sheet ??
			null,
		cashflow:
			root.cashflow ??
			fundamentals.cashflow ??
			raw.cashflow ??
			rawFinal.cashflow ??
			rawPrimary.cashflow ??
			quoteSummary.cashflow ??
			null,
		quarterly_cashflow:
			root.quarterly_cashflow ??
			fundamentals.quarterly_cashflow ??
			raw.quarterly_cashflow ??
			rawFinal.quarterly_cashflow ??
			rawPrimary.quarterly_cashflow ??
			quoteSummary.quarterly_cashflow ??
			null,
	};
};

const hasMeaningfulValue = (value) => {
	if (value === null || value === undefined) return false;
	if (typeof value === 'number') return Number.isFinite(value);
	if (typeof value === 'string') return value.trim().length > 0;
	return String(value).trim().length > 0;
};

const toTrimmedStringOrNull = (value) => {
	if (!hasMeaningfulValue(value)) return null;
	const text = String(value).trim();
	return text || null;
};

const parseLocalizedNumber = (value) => {
	if (value === null || value === undefined || value === '') return null;
	if (typeof value === 'number') return Number.isFinite(value) ? value : null;
	const direct = toNumberOrNull(value);
	if (direct !== null) return direct;

	let text = String(value).trim();
	if (!text) return null;
	text = text.replace(/[^\d,.-]/g, '');
	if (!text) return null;

	const hasComma = text.includes(',');
	const hasDot = text.includes('.');
	if (hasComma && hasDot) {
		if (text.lastIndexOf(',') > text.lastIndexOf('.')) {
			text = text.replace(/\./g, '').replace(',', '.');
		} else {
			text = text.replace(/,/g, '');
		}
	} else if (hasComma) {
		if (/,\d{1,4}$/.test(text)) {
			text = text.replace(/\./g, '').replace(',', '.');
		} else {
			text = text.replace(/,/g, '');
		}
	} else if ((text.match(/\./g) || []).length > 1) {
		text = text.replace(/\./g, '');
	}

	const parsed = Number(text);
	return Number.isFinite(parsed) ? parsed : null;
};

const readValueFromRecordHints = (records, hints) => {
	for (const record of records) {
		const value = findRecordValueByHints(record, hints);
		if (!hasMeaningfulValue(value)) continue;
		return value;
	}
	return null;
};

const readValueFromRecordKeys = (records, keys) => {
	const normalizedKeys = (Array.isArray(keys) ? keys : [])
		.map((key) => normalizeRecordKey(key))
		.filter(Boolean);
	if (normalizedKeys.length === 0) return null;

	for (const record of records) {
		if (!isObjectRecord(record)) continue;
		for (const [key, value] of Object.entries(record)) {
			const normalizedKey = normalizeRecordKey(key);
			if (!normalizedKeys.includes(normalizedKey)) continue;
			if (!hasMeaningfulValue(value)) continue;
			return value;
		}
	}

	return null;
};

const toUrlOrNull = (value) => normalizeDocumentUrl(toTrimmedStringOrNull(value));

const buildFundPhone = (...values) => {
	const parts = values
		.map((value) => toTrimmedStringOrNull(value))
		.filter(Boolean)
		.map((value) => value.replace(/\s+/g, ' '));
	if (parts.length === 0) return null;
	return parts.join(' ');
};

const hasFundGeneralInfo = (value) => {
	if (!isObjectRecord(value)) return false;
	return Object.entries(value).some(([key, entry]) => {
		if (key === 'source') return false;
		return hasMeaningfulValue(entry);
	});
};

const mergeFundGeneralInfo = (base, incoming) => {
	if (!hasFundGeneralInfo(incoming)) {
		return hasFundGeneralInfo(base) ? { ...base } : null;
	}

	const merged = isObjectRecord(base) ? { ...base } : {};
	for (const [key, value] of Object.entries(incoming || {})) {
		if (!hasMeaningfulValue(value)) continue;
		if (!hasMeaningfulValue(merged[key])) {
			merged[key] = value;
		}
	}

	return hasFundGeneralInfo(merged) ? merged : null;
};

const isFundPortfolioRow = (value) => (
	isObjectRecord(value) &&
	toTrimmedStringOrNull(value.label) &&
	Number.isFinite(toNumberOrNull(value.allocation_pct))
);

const normalizeFundPortfolioRows = (rows, sourceHint = null) => {
	if (!Array.isArray(rows)) return [];

	const normalized = [];
	for (const rawRow of rows) {
		if (!isObjectRecord(rawRow)) continue;

		const label =
			toTrimmedStringOrNull(
				findRecordValueByHints(rawRow, FUND_PORTFOLIO_LABEL_KEY_HINTS)
			) ||
			Object.values(rawRow)
				.map((value) => toTrimmedStringOrNull(value))
				.find((value) => value && !/^https?:\/\//i.test(value)) ||
			null;
		if (!label) continue;

		let allocationPct = parseLocalizedNumber(
			findRecordValueByHints(rawRow, FUND_PORTFOLIO_ALLOCATION_KEY_HINTS)
		);
		if (allocationPct === null) continue;
		if (Math.abs(allocationPct) <= 1 && allocationPct !== 0) {
			allocationPct *= 100;
		}
		if (!Number.isFinite(allocationPct) || allocationPct <= 0 || allocationPct > 100) continue;

		const categoryRaw = toTrimmedStringOrNull(
			findRecordValueByHints(rawRow, FUND_PORTFOLIO_CATEGORY_KEY_HINTS)
		);
		const category = categoryRaw && categoryRaw !== label ? categoryRaw : null;

		normalized.push({
			label,
			allocation_pct: allocationPct,
			category,
			source:
				toTrimmedStringOrNull(rawRow.source) ||
				toTrimmedStringOrNull(sourceHint) ||
				null,
		});
	}

	return normalized.sort((left, right) => right.allocation_pct - left.allocation_pct);
};

const hasFundPortfolio = (value) =>
	Array.isArray(value) && value.some((entry) => isFundPortfolioRow(entry));

const mergeFundPortfolio = (base, incoming) => {
	const dedupe = new Set();
	const merged = [];

	const append = (entry) => {
		if (!isFundPortfolioRow(entry)) return;
		const label = toTrimmedStringOrNull(entry.label);
		const allocationPct = toNumberOrNull(entry.allocation_pct);
		if (!label || allocationPct === null) return;
		const normalizedPct = Number(allocationPct.toFixed(4));
		const key = [
			label.toLowerCase(),
			normalizedPct.toFixed(4),
			toTrimmedStringOrNull(entry.category || ''),
		].join('|');
		if (dedupe.has(key)) return;
		dedupe.add(key);
		merged.push({
			label,
			allocation_pct: normalizedPct,
			category: toTrimmedStringOrNull(entry.category) || null,
			source: toTrimmedStringOrNull(entry.source) || null,
		});
	};

	for (const entry of Array.isArray(base) ? base : []) append(entry);
	for (const entry of Array.isArray(incoming) ? incoming : []) append(entry);

	return merged.sort((left, right) => right.allocation_pct - left.allocation_pct);
};

const readFundPortfolioFromPayload = (payload, sourceHint) => {
	const root = isObjectRecord(payload) ? payload : {};
	const fundamentals = isObjectRecord(root.fundamentals) ? root.fundamentals : {};
	const raw = isObjectRecord(root.raw) ? root.raw : {};
	const rawDetail = isObjectRecord(raw.detail) ? raw.detail : {};
	const rawFund = isObjectRecord(raw.fund) ? raw.fund : {};
	const fundamentalsInfo = isObjectRecord(fundamentals.info) ? fundamentals.info : {};
	const rootFundInfo = isObjectRecord(root.fund_info) ? root.fund_info : {};
	const fundamentalsFundInfo = isObjectRecord(fundamentals.fund_info) ? fundamentals.fund_info : {};

	const listCandidates = [
		root.fund_portfolio,
		root.portfolio_composition,
		root.portfolio,
		fundamentals.fund_portfolio,
		fundamentals.portfolio_composition,
		raw.fund_portfolio,
		raw.portfolio_composition,
		rawDetail.classes,
		rawFund.classes,
		fundamentalsInfo.classes,
		rootFundInfo.classes,
		fundamentalsFundInfo.classes,
	];

	let merged = [];
	for (const candidate of listCandidates) {
		const normalized = normalizeFundPortfolioRows(candidate, sourceHint);
		if (normalized.length === 0) continue;
		merged = mergeFundPortfolio(merged, normalized);
	}

	return merged;
};

const readFundGeneralInfoFromPayload = (payload, sourceHint) => {
	const root = isObjectRecord(payload) ? payload : {};
	const fundamentals = isObjectRecord(root.fundamentals) ? root.fundamentals : {};
	const raw = isObjectRecord(root.raw) ? root.raw : {};
	const rawDetail = isObjectRecord(raw.detail) ? raw.detail : {};
	const rawFund = isObjectRecord(raw.fund) ? raw.fund : {};
	const fundamentalsInfo = isObjectRecord(fundamentals.info) ? fundamentals.info : {};
	const quoteSummary = isObjectRecord(raw.quote_summary) ? raw.quote_summary : {};
	const rawFinal = isObjectRecord(raw.final_payload) ? raw.final_payload : {};
	const rawPrimary = isObjectRecord(raw.primary_payload) ? raw.primary_payload : {};
	const finalInfo = isObjectRecord(rawFinal.info) ? rawFinal.info : {};
	const primaryInfo = isObjectRecord(rawPrimary.info) ? rawPrimary.info : {};
	const b3Meta = isObjectRecord(fundamentals.b3) ? fundamentals.b3 : {};
	const b3MetaFund = isObjectRecord(b3Meta.fund) ? b3Meta.fund : {};
	const rootFundInfo = isObjectRecord(root.fund_info) ? root.fund_info : {};
	const fundamentalsFundInfo = isObjectRecord(fundamentals.fund_info) ? fundamentals.fund_info : {};

	const candidateRecords = [
		rootFundInfo,
		fundamentalsFundInfo,
		rawDetail,
		rawFund,
		fundamentalsInfo,
		b3MetaFund,
		finalInfo,
		primaryInfo,
		quoteSummary,
		root,
		fundamentals,
		raw,
	];

	const shareHolderRecords = candidateRecords
		.map((record) => {
			if (!isObjectRecord(record)) return null;
			return (
				record.shareHolder ||
				record.shareholder ||
				record.share_holder ||
				record.escriturador ||
				null
			);
		})
		.filter((value) => isObjectRecord(value));

	const allRecords = [
		...candidateRecords.filter((record) => isObjectRecord(record)),
		...shareHolderRecords,
	];
	const readString = (hints) => toTrimmedStringOrNull(readValueFromRecordHints(allRecords, hints));

	const fundPhone = buildFundPhone(
		readValueFromRecordKeys(allRecords, ['fundphonenumberddd']),
		readValueFromRecordKeys(allRecords, ['fundphonenumber'])
	);
	const shareHolderPhone = buildFundPhone(
		readValueFromRecordKeys(shareHolderRecords, ['shareholderphonenumberddd']),
		readValueFromRecordKeys(shareHolderRecords, ['shareholderphonenumber'])
	);
	const fallbackPhone = buildFundPhone(
		readValueFromRecordKeys(allRecords, ['phonenumberddd']),
		readValueFromRecordKeys(allRecords, ['phonenumber'])
	);

	const fundId = parseLocalizedNumber(
		readValueFromRecordHints(allRecords, ['idfnet'])
	);
	const acronym = readString(['acronym', 'idcem']);
	const b3DetailsUrl =
		Number.isFinite(fundId) && acronym
			? `https://sistemaswebb3-listados.b3.com.br/fundsListedPage/funds-main/${Math.trunc(
				fundId
			)}/${encodeURIComponent(acronym.toUpperCase())}/FII/funds-details`
			: toUrlOrNull(readValueFromRecordHints(allRecords, ['fundsdetailsurl', 'fundslistpage']));

	const source =
		toTrimmedStringOrNull(
			root.data_source ||
				root.source ||
				fundamentals.data_source ||
				raw.data_source ||
				sourceHint ||
				null
		) || null;

	const fundInfo = {
		legal_name: readString(['fundname', 'legalname']),
		trading_name: readString(['tradingname', 'pregao']),
		acronym,
		cnpj: readString(['cnpj']),
		description: readString(['tudosobre', 'descricao', 'description', 'about', 'funddescription']),
		description_html: toTrimmedStringOrNull(
			readValueFromRecordKeys(allRecords, [
				'description_html',
				'descriptionHtml',
				'summary_html',
				'summaryHtml',
				'descricao_html',
				'descricaoHtml',
			])
		),
		dividends_resume:
			readValueFromRecordKeys(allRecords, [
				'dividends_resume',
				'dividendsResume',
				'dividend_resume',
				'dividendResume',
			]) || null,
		dividend_yield_comparator:
			readValueFromRecordKeys(allRecords, [
				'dividend_yield_comparator',
				'dividendYieldComparator',
				'yield_comparator',
				'yieldComparator',
			]) || null,
		classification: readString(['classification', 'classificacao']),
		segment: readString(['segment']),
		administrator: readString(['positionmanager', 'administrator', 'administrador']),
		manager_name: readString(['managername', 'gestor']),
		bookkeeper: readString(['shareholdername', 'escriturador', 'bookkeeper']),
		website: toUrlOrNull(readValueFromRecordHints(allRecords, ['website', 'site'])),
		address: readString(['fundaddress', 'address', 'endereco']),
		phone: fundPhone || shareHolderPhone || fallbackPhone,
		quota_count: parseLocalizedNumber(readValueFromRecordHints(allRecords, ['quotacount', 'quotas'])),
		quota_date_approved: normalizeDate(readValueFromRecordHints(allRecords, ['quotadateapproved'])),
		trading_code: readString(['tradingcode']),
		trading_code_others: readString(['tradingcodeothers']),
		b3_details_url: b3DetailsUrl,
		source,
	};

	return hasFundGeneralInfo(fundInfo) ? fundInfo : null;
};

const normalizeDate = (value) => {
	if (!value) return null;
	const input = String(value).trim();
	if (/^\d{4}-\d{2}-\d{2}$/.test(input)) return input;
	const br = input.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
	if (br) return `${br[3]}-${br[2]}-${br[1]}`;
	const parsed = new Date(input);
	if (Number.isNaN(parsed.getTime())) return null;
	return parsed.toISOString().slice(0, 10);
};

const toBrDate = (isoDate) => {
	const normalized = normalizeDate(isoDate);
	if (!normalized) return null;
	const [yyyy, mm, dd] = normalized.split('-');
	return `${dd}/${mm}/${yyyy}`;
};

const formatMonthDayYear = (date) => {
	const yyyy = date.getUTCFullYear();
	const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
	const dd = String(date.getUTCDate()).padStart(2, '0');
	return `${mm}-${dd}-${yyyy}`;
};

const addDays = (isoDate, days) => {
	const base = new Date(`${isoDate}T00:00:00Z`);
	if (Number.isNaN(base.getTime())) return isoDate;
	base.setUTCDate(base.getUTCDate() + days);
	return base.toISOString().slice(0, 10);
};

const monthKey = (date) => {
	const normalized = normalizeDate(date);
	return normalized ? normalized.slice(0, 7) : null;
};

const escapePdf = (value) =>
	String(value || '')
		.replace(/\\/g, '\\\\')
		.replace(/\(/g, '\\(')
		.replace(/\)/g, '\\)');

const createSimplePdfBuffer = (lines) => {
	const safeLines = Array.isArray(lines) ? lines : [];
	let y = 800;
	const textCommands = [];
	for (const line of safeLines.slice(0, 45)) {
		textCommands.push(`BT /F1 11 Tf 40 ${y} Td (${escapePdf(line)}) Tj ET`);
		y -= 16;
	}

	const content = `${textCommands.join('\n')}\n`;
	const objects = [
		'1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n',
		'2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n',
		'3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n',
		'4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n',
		`5 0 obj << /Length ${Buffer.byteLength(content, 'utf8')} >> stream\n${content}endstream\nendobj\n`,
	];

	let output = '%PDF-1.4\n';
	const offsets = [0];
	for (const object of objects) {
		offsets.push(Buffer.byteLength(output, 'utf8'));
		output += object;
	}
	const xrefOffset = Buffer.byteLength(output, 'utf8');
	output += `xref\n0 ${objects.length + 1}\n`;
	output += '0000000000 65535 f \n';
	for (let index = 1; index <= objects.length; index += 1) {
		output += `${String(offsets[index]).padStart(10, '0')} 00000 n \n`;
	}
	output += `trailer << /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF\n`;
	return Buffer.from(output, 'utf8');
};

const stdDev = (values) => {
	if (!Array.isArray(values) || values.length < 2) return 0;
	const avg = values.reduce((sum, value) => sum + value, 0) / values.length;
	const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / (values.length - 1);
	return Math.sqrt(Math.max(variance, 0));
};

const correlation = (left, right) => {
	if (!Array.isArray(left) || !Array.isArray(right) || left.length < 2 || right.length < 2) {
		return null;
	}
	const size = Math.min(left.length, right.length);
	const xs = left.slice(left.length - size);
	const ys = right.slice(right.length - size);
	const meanX = xs.reduce((sum, value) => sum + value, 0) / size;
	const meanY = ys.reduce((sum, value) => sum + value, 0) / size;
	let numerator = 0;
	let denX = 0;
	let denY = 0;
	for (let index = 0; index < size; index += 1) {
		const dx = xs[index] - meanX;
		const dy = ys[index] - meanY;
		numerator += dx * dy;
		denX += dx * dx;
		denY += dy * dy;
	}
	if (denX <= 0 || denY <= 0) return null;
	return numerator / Math.sqrt(denX * denY);
};

const hashId = (value) =>
	crypto.createHash('sha1').update(String(value || ''), 'utf8').digest('hex').slice(0, 16);

const parseBrDatetime = (value) => {
	if (!value) return null;
	const text = String(value).trim();
	const match = text.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2}))?/);
	if (!match) return null;
	const [, dd, mm, yyyy, hh, mi] = match;
	const iso = `${yyyy}-${mm}-${dd}`;
	if (hh && mi) return `${iso}T${hh}:${mi}:00`;
	return iso;
};

const decodeHtmlEntities = (value) =>
	String(value || '')
		.replace(/&nbsp;/gi, ' ')
		.replace(/&amp;/gi, '&')
		.replace(/&quot;/gi, '"')
		.replace(/&#39;/gi, '\'')
		.replace(/&lt;/gi, '<')
		.replace(/&gt;/gi, '>');

const stripHtmlTags = (value) =>
	decodeHtmlEntities(String(value || '').replace(/<[^>]*>/g, ' '))
		.replace(/\s+/g, ' ')
		.trim();

const parseFirstDateInText = (value) => {
	const text = String(value || '');
	if (!text) return null;
	const brDatetime = text.match(/\b\d{2}\/\d{2}\/\d{4}(?:\s+\d{2}:\d{2})?\b/)?.[0] || null;
	if (brDatetime) return parseBrDatetime(brDatetime);
	const isoDate = text.match(/\b\d{4}-\d{2}-\d{2}\b/)?.[0] || null;
	return normalizeDate(isoDate);
};

const mergeFiiUpdateItems = (primaryItems, secondaryItems) => {
	const merged = [];
	const dedupe = new Set();
	const append = (item) => {
		if (!item || typeof item !== 'object') return;
		const normalizedUrl = String(item.url || '').trim();
		const normalizedTitle = String(item.title || '').trim();
		const normalizedDate = normalizeDate(item.deliveryDate || item.referenceDate || null) || '';
		if (!normalizedUrl && !normalizedTitle) return;
		const key = `${normalizedUrl}|${normalizedTitle.toLowerCase()}|${normalizedDate}`;
		if (dedupe.has(key)) return;
		dedupe.add(key);
		merged.push(item);
	};

	for (const item of Array.isArray(primaryItems) ? primaryItems : []) append(item);
	for (const item of Array.isArray(secondaryItems) ? secondaryItems : []) append(item);

	return merged.sort((left, right) => {
		const leftDate = normalizeDate(left.deliveryDate || left.referenceDate || null) || '';
		const rightDate = normalizeDate(right.deliveryDate || right.referenceDate || null) || '';
		return rightDate.localeCompare(leftDate);
	});
};

const parseRssTag = (block, tag) => {
	const regex = new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, 'i');
	const match = block.match(regex);
	if (!match) return null;
	return match[1]
		.replace(/<!\[CDATA\[/g, '')
		.replace(/\]\]>/g, '')
		.trim();
};

const parseRssItems = (xml) => {
	const items = [];
	const blocks = String(xml || '').match(/<item>([\s\S]*?)<\/item>/gi) || [];
	for (const block of blocks) {
		const title = parseRssTag(block, 'title');
		const link = parseRssTag(block, 'link');
		const description = parseRssTag(block, 'description');
		const pubDateRaw = parseRssTag(block, 'pubDate');
		const publishedAt = pubDateRaw ? new Date(pubDateRaw).toISOString() : nowIso();
		items.push({
			title: title || 'Untitled',
			link,
			description,
			publishedAt,
		});
	}
	return items;
};

class PlatformService {
	constructor(options = {}) {
		this.dynamo = options.dynamo;
		this.tableName = options.tableName || process.env.TABLE_NAME || 'wealth-main';
		this.logger = options.logger || console;
		this.marketDataService = options.marketDataService;
		this.priceHistoryService = options.priceHistoryService;
		this.yahooApiProvider = options.yahooApiProvider || new YahooApiProvider(options);
		this.yahooFinanceScraper = options.yahooFinanceScraper || new YahooFinanceScraper(options);
		this.googleFinanceScraper = options.googleFinanceScraper || new GoogleFinanceScraper(options);
		this.statusInvestStructuredProvider =
			options.statusInvestStructuredProvider || new StatusInvestStructuredProvider(options);
		this.statusInvestScraper = options.statusInvestScraper || new StatusInvestScraper(options);
		this.b3FinancialStatementsProvider =
			options.b3FinancialStatementsProvider || new B3FinancialStatementsProvider(options);
		this.fundsExplorerProvider =
			options.fundsExplorerProvider || new FundsExplorerProvider(options);
		this.runtimeEnv = resolveRuntimeEnvironment();
		this.reportsLocalDir =
			options.reportsLocalDir ||
			process.env.REPORTS_LOCAL_DIR ||
			path.resolve(__dirname, '../../../.data/reports');
		this.s3Bucket = options.s3Bucket || process.env.S3_BUCKET || resolveS3BucketName();
		this.useS3 = this.runtimeEnv === 'aws' || Boolean(process.env.S3_ENDPOINT);
		this.s3 = this.useS3
			? new S3Client(buildAwsClientConfig({ service: 's3' }))
			: null;
	}

	async fetchEconomicIndicators() {
		const seriesIds = [
			INDICATOR_SERIES.CDI,
			INDICATOR_SERIES.SELIC,
			INDICATOR_SERIES.SELIC_META,
			INDICATOR_SERIES.IPCA,
			INDICATOR_SERIES.IGPM,
			INDICATOR_SERIES.POUPANCA,
		];

		const results = [];
		for (const seriesId of seriesIds) {
			const cursor = await this.#getCursor('economic-indicators', `sgs-${seriesId}`);
			try {
				const data = await this.#fetchSgsSeries(seriesId, cursor?.lastDate || null);
				let persisted = 0;
				let latestDate = cursor?.lastDate || null;
				for (const point of data) {
					await this.dynamo.send(
						new PutCommand({
							TableName: this.tableName,
							Item: {
								PK: `ECON#${seriesId}`,
								SK: `DATE#${point.date}`,
								entityType: 'ECON_INDICATOR',
								seriesId,
								date: point.date,
								value: point.value,
								currency: 'BRL',
								data_source: 'bcb_sgs',
								fetched_at: nowIso(),
								is_scraped: false,
								updatedAt: nowIso(),
							},
						})
					);
					persisted += 1;
					latestDate = point.date;
				}

				if (latestDate) {
					await this.#setCursor('economic-indicators', `sgs-${seriesId}`, {
						lastDate: latestDate,
						updatedAt: nowIso(),
					});
				}

				results.push({ seriesId, fetched: data.length, persisted, latestDate });
			} catch (error) {
				this.logger.warn(
					JSON.stringify({
						event: 'economic_series_fetch_failed',
						seriesId,
						error: error.message,
						fetched_at: nowIso(),
					})
				);
				results.push({
					seriesId,
					fetched: 0,
					persisted: 0,
					latestDate: cursor?.lastDate || null,
					error: error.message,
				});
			}
		}

		const fx = await this.#refreshFxRates();
		await this.#recordJobRun('economic-indicators', {
			status: 'success',
			series: results,
			fx,
		});

		return {
			job: 'economic-indicators',
			series: results,
			fx,
			fetched_at: nowIso(),
		};
	}

	async fetchCorporateEvents(ticker, options = {}) {
		const assets = await this.#resolveAssetsForTickerOrPortfolio(ticker, options.portfolioId);
		const activeAssets = assets.filter((asset) =>
			String(asset.status || 'active').toLowerCase() === 'active'
		);
		const prioritizeFiiForPortfolio = !ticker && Boolean(options.portfolioId);
		const persistedEvents = [];

		for (const asset of activeAssets) {
			try {
				const isBrazilianFii =
					String(asset.assetClass || '').toLowerCase() === 'fii'
					&& String(asset.country || 'BR').toUpperCase() === 'BR';
				if (prioritizeFiiForPortfolio && !isBrazilianFii) continue;
				let normalizedEvents = [];

				if (isBrazilianFii) {
					const statusInvestEvents = await this.#fetchStatusInvestDividendEvents(asset.ticker, 'fii');
					const fundsExplorerEvents = await this.#fetchFundsExplorerDividendEvents(asset.ticker);
					normalizedEvents = this.#mergeDividendEvents(statusInvestEvents, fundsExplorerEvents);
				}

				if (normalizedEvents.length === 0 && !isBrazilianFii) {
					const market = resolveAssetMarket(asset);
					const payload = await this.marketDataService.fetchAssetData(asset.ticker, market, asset);
					const calendar =
						payload?.raw?.final_payload?.calendar ||
						payload?.raw?.primary_payload?.calendar ||
						payload?.raw?.final_payload?.info?.calendarEvents ||
						null;
					normalizedEvents = this.#normalizeCalendarEvents(asset.ticker, calendar, payload.data_source);
				}

				for (const event of normalizedEvents) {
					await this.dynamo.send(
						new PutCommand({
							TableName: this.tableName,
							Item: {
								PK: `ASSET_EVENT#${asset.ticker}`,
								SK: `DATE#${event.date}#${event.eventId}`,
								entityType: 'ASSET_EVENT',
								ticker: asset.ticker,
								portfolioId: asset.portfolioId,
								eventType: event.eventType,
								eventTitle: event.title,
								eventDate: event.date,
								details: event.details,
								data_source: event.data_source,
								fetched_at: nowIso(),
								is_scraped: Boolean(event.is_scraped),
								updatedAt: nowIso(),
							},
						})
					);
					persistedEvents.push(event);
				}
			} catch (error) {
				this.logger.error(
					JSON.stringify({
						event: 'corporate_events_fetch_failed',
						ticker: asset.ticker,
						error: error.message,
						fetched_at: nowIso(),
					})
				);
			}
		}

		await this.#recordJobRun('corporate-events', {
			status: 'success',
			tickers: activeAssets.map((asset) => asset.ticker),
			persisted: persistedEvents.length,
		});

		return {
			tickers: activeAssets.map((asset) => asset.ticker),
			persisted: persistedEvents.length,
			events: persistedEvents,
			fetched_at: nowIso(),
		};
	}

	async fetchNews(ticker, options = {}) {
		const assets = await this.#resolveAssetsForTickerOrPortfolio(ticker, options.portfolioId);
		const persisted = [];

		for (const asset of assets) {
			try {
				const url = `https://news.google.com/rss/search?q=${encodeURIComponent(asset.ticker)}&hl=pt-BR&gl=BR&ceid=BR:pt-419`;
				const response = await withRetry(
					() => fetchWithTimeout(url, { timeoutMs: 15000 }),
					{ retries: 2, baseDelayMs: 400, factor: 2 }
				);
				if (!response.ok) continue;
				const xml = await response.text();
				const items = parseRssItems(xml).slice(0, 20);
				for (const item of items) {
					const date = normalizeDate(item.publishedAt) || nowIso().slice(0, 10);
					const itemId = hashId(`${asset.ticker}:${item.title}:${item.link}:${item.publishedAt}`);
					const payload = {
						PK: `NEWS#${asset.ticker}`,
						SK: `DATE#${date}#${itemId}`,
						entityType: 'NEWS_ITEM',
						ticker: asset.ticker,
						portfolioId: asset.portfolioId,
						title: item.title,
						link: item.link,
						description: item.description,
						publishedAt: item.publishedAt,
						read: false,
						data_source: 'google_news_rss',
						fetched_at: nowIso(),
						is_scraped: false,
						updatedAt: nowIso(),
					};
					await this.dynamo.send(
						new PutCommand({
							TableName: this.tableName,
							Item: payload,
						})
					);
					persisted.push(payload);
				}
			} catch (error) {
				this.logger.error(
					JSON.stringify({
						event: 'news_fetch_failed',
						ticker: asset.ticker,
						error: error.message,
						fetched_at: nowIso(),
					})
				);
			}
		}

		await this.#recordJobRun('news-refresh', {
			status: 'success',
			tickers: assets.map((asset) => asset.ticker),
			persisted: persisted.length,
		});

		return {
			tickers: assets.map((asset) => asset.ticker),
			persisted: persisted.length,
			items: persisted,
			fetched_at: nowIso(),
		};
	}

	async getFiiUpdates(ticker, options = {}) {
		const assets = await this.#resolveAssetsForTickerOrPortfolio(ticker, options.portfolioId);
		const asset = assets[0];
		if (!asset) return { ticker, items: [], fetched_at: nowIso() };

		const normalizedClass = String(asset.assetClass || '').toLowerCase();
		const normalizedTicker = String(asset.ticker || '').trim().toUpperCase();
		if (normalizedClass !== 'fii' && !normalizedTicker.endsWith('11')) {
			return { ticker: normalizedTicker, items: [], fetched_at: nowIso() };
		}

		const fiisFallbackPromise = this.#fetchFiisUpdates(normalizedTicker);

		try {
			const cnpj = await this.#resolveFiiCnpj(normalizedTicker);
			if (!cnpj) {
				const fiisItems = await fiisFallbackPromise.catch(() => []);
				return {
					ticker: normalizedTicker,
					items: fiisItems,
					total: fiisItems.length,
					sources: fiisItems.length > 0 ? ['fiis'] : [],
					error: 'Could not resolve CNPJ',
					fetched_at: nowIso(),
				};
			}

			const now = new Date();
			const oneYearAgo = new Date(now);
			oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
			const formatBrDate = (d) => `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}/${d.getFullYear()}`;

			const params = new URLSearchParams({
				d: '0',
				s: '0',
				l: '100',
				'o[0][0]': 'dataEntrega',
				'o[0][1]': 'desc',
				cnpjFundo: cnpj,
				dataInicial: formatBrDate(oneYearAgo),
				dataFinal: formatBrDate(now),
			});

			const url = `https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?${params.toString()}`;
			const response = await withRetry(
				() => fetchWithTimeout(url, {
					timeoutMs: 15000,
					headers: {
						Accept: 'application/json',
						'X-Requested-With': 'XMLHttpRequest',
					},
				}),
				{ retries: 2, baseDelayMs: 400, factor: 2 }
			);
			if (!response.ok) {
				const fiisItems = await fiisFallbackPromise.catch(() => []);
				return {
					ticker: normalizedTicker,
					items: fiisItems,
					total: fiisItems.length,
					sources: fiisItems.length > 0 ? ['fiis'] : [],
					error: `FNET responded with ${response.status}`,
					fetched_at: nowIso(),
				};
			}

			const payload = await response.json();
			const rawItems = Array.isArray(payload?.data) ? payload.data : [];

			const fnetItems = rawItems
				.filter((item) => item.id && item.status === 'AC')
				.map((item) => {
					const detailParts = [item.tipoDocumento, item.especieDocumento].filter(Boolean);
					const title = detailParts.length > 0
						? detailParts.join(', ')
						: (item.categoriaDocumento || 'Documento');
					return {
						id: item.id,
						category: item.categoriaDocumento || null,
						title,
						deliveryDate: parseBrDatetime(item.dataEntrega),
						referenceDate: parseBrDatetime(item.dataReferencia),
						url: `https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id=${item.id}&cvm=true`,
						source: 'fnet',
					};
				});
			const fiisItems = await fiisFallbackPromise.catch(() => []);
			const items = mergeFiiUpdateItems(fnetItems, fiisItems);
			const sources = [];
			if (fnetItems.length > 0) sources.push('fnet');
			if (fiisItems.length > 0) sources.push('fiis');

			return {
				ticker: normalizedTicker,
				total: items.length,
				items,
				sources,
				fetched_at: nowIso(),
			};
		} catch (error) {
			this.logger.error(JSON.stringify({
				event: 'fii_updates_fetch_failed',
				ticker: normalizedTicker,
				error: error.message,
				fetched_at: nowIso(),
			}));
			const fiisItems = await fiisFallbackPromise.catch(() => []);
			return {
				ticker: normalizedTicker,
				items: fiisItems,
				total: fiisItems.length,
				sources: fiisItems.length > 0 ? ['fiis'] : [],
				error: error.message,
				fetched_at: nowIso(),
			};
		}
	}

	async getFiiEmissions(ticker, options = {}) {
		const assets = await this.#resolveAssetsForTickerOrPortfolio(ticker, options.portfolioId);
		const asset = assets[0];
		if (!asset) return { ticker, emissions: [], fetched_at: nowIso() };

		const normalizedClass = String(asset.assetClass || '').toLowerCase();
		const normalizedTicker = String(asset.ticker || '').trim().toUpperCase();
		if (normalizedClass !== 'fii' && !normalizedTicker.endsWith('11')) {
			return { ticker: normalizedTicker, emissions: [], fetched_at: nowIso() };
		}

		try {
			const result = await this.fundsExplorerProvider.fetchEmissions(asset);
			if (!result) {
				return { ticker: normalizedTicker, emissions: [], fetched_at: nowIso() };
			}
			return result;
		} catch (error) {
			console.log('[getFiiEmissions] error: %s', error.message);
			this.logger.error(JSON.stringify({
				event: 'fii_emissions_fetch_failed',
				ticker: normalizedTicker,
				error: error.message,
				fetched_at: nowIso(),
			}));
			return { ticker: normalizedTicker, emissions: [], error: error.message, fetched_at: nowIso() };
		}
	}

	async #resolveFiiCnpj(ticker) {
		const normalizedTicker = String(ticker).trim().toUpperCase();
		const acronym = normalizedTicker.endsWith('11') ? normalizedTicker.slice(0, -2) : normalizedTicker;

		const candidateKeywords = Array.from(new Set([normalizedTicker, acronym].filter(Boolean)));
		for (const keyword of candidateKeywords) {
			const encodedPayload = Buffer.from(JSON.stringify({
				language: 'pt-br',
				typeFund: 'FII',
				pageNumber: 1,
				pageSize: 5,
				keyword,
			})).toString('base64');
			const listUrl = `https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/GetListFunds/${encodedPayload}`;

			try {
				const listResp = await withRetry(
					() => fetchWithTimeout(listUrl, {
						timeoutMs: 15000,
						headers: { Accept: 'application/json,text/plain,*/*' },
					}),
					{ retries: 1, baseDelayMs: 300, factor: 2 }
				);
				if (!listResp.ok) continue;
				const listPayload = await listResp.json();
				const results = Array.isArray(listPayload?.results) ? listPayload.results : [];
				if (results.length === 0) continue;

				const normalizedAcronym = acronym.toUpperCase();
				const match = results.find((r) => String(r.acronym || '').toUpperCase() === normalizedAcronym) || results[0];
				const idFNET = toNumberOrNull(match?.id);
				const idCEM = String(match?.acronym || acronym).trim().toUpperCase();
				if (idFNET === null) continue;

				const detailEncoded = Buffer.from(JSON.stringify({
					language: 'pt-br',
					idFNET,
					idCEM,
					typeFund: 'FII',
				})).toString('base64');
				const detailUrl = `https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/GetDetailFund/${detailEncoded}`;

				const detailResp = await withRetry(
					() => fetchWithTimeout(detailUrl, {
						timeoutMs: 15000,
						headers: { Accept: 'application/json,text/plain,*/*' },
					}),
					{ retries: 1, baseDelayMs: 300, factor: 2 }
				);
				if (!detailResp.ok) continue;
				const detail = await detailResp.json();
				const cnpj = String(detail?.cnpj || '').replace(/\D/g, '');
				if (cnpj.length === 14) return cnpj;
			} catch {
				continue;
			}
		}
		return null;
	}

	async getDashboard(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const activeAssets = assets.filter((asset) =>
			String(asset.status || 'active').toLowerCase() === 'active'
		);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, {
			portfolioId,
			method: options.method || 'fifo',
			includeBenchmarkComparison: false,
		});
		const fxRates = await this.#getLatestFxMap();
		const assetById = new Map(activeAssets.map((asset) => [asset.assetId, asset]));
		const activeMetrics = metrics.assets.filter((metric) => assetById.has(metric.assetId));
		const periodKey = String(options.period || 'MAX').toUpperCase();
		const evolutionDays = Object.prototype.hasOwnProperty.call(PERIOD_TO_DAYS, periodKey)
			? PERIOD_TO_DAYS[periodKey]
			: PERIOD_TO_DAYS.MAX;

		const detailEntries = await Promise.all(
			activeMetrics.map(async (metric) => {
				const detail = await this.#getLatestAssetDetail(portfolioId, metric.assetId);
				return [metric.assetId, detail];
			})
		);
		const detailByAssetId = new Map(detailEntries);

		let totalBrl = 0;
		let totalCostBrl = 0;
		const allocationByClass = {};
		const allocationByCurrency = {};
		const allocationBySector = {};
		const fxRateByAssetId = {};
		const fallbackBrlByAssetId = {};

		for (const metric of activeMetrics) {
			const asset = assetById.get(metric.assetId) || {};
			const currency = metric.currency || asset.currency || 'BRL';
			const fxKey = `${currency}/BRL`;
			const fxRate = currency === 'BRL' ? 1 : numeric(fxRates[fxKey], 0);
			const metricMarketValue = toNumberOrNull(metric.market_value);
			const metricQuantity = toNumberOrNull(metric.quantity_current);
			const metricCurrentPrice = toNumberOrNull(metric.current_price);
			const assetSnapshotCurrentValue = toNumberOrNull(asset.currentValue);
			const assetSnapshotCurrentPrice = toNumberOrNull(asset.currentPrice);
			const hasOpenQuantity =
				metricQuantity !== null && Math.abs(metricQuantity) > Number.EPSILON;
			const usableMetricMarketValue =
				metricMarketValue !== null &&
				(!hasOpenQuantity || Math.abs(metricMarketValue) > Number.EPSILON)
					? metricMarketValue
					: null;
			const fallbackPrice = metricCurrentPrice ?? assetSnapshotCurrentPrice;
			const derivedMarketValue =
				(fallbackPrice !== null && metricQuantity !== null)
					? fallbackPrice * metricQuantity
					: null;
			const marketValue =
				usableMetricMarketValue ??
				assetSnapshotCurrentValue ??
				derivedMarketValue ??
				0;
			const costTotal = toNumberOrNull(metric.cost_total) ?? 0;
			const marketValueBrl = fxRate > 0 ? marketValue * fxRate : 0;
			const costTotalBrl = fxRate > 0 ? costTotal * fxRate : 0;
			totalBrl += marketValueBrl;
			totalCostBrl += costTotalBrl;
			fxRateByAssetId[metric.assetId] = fxRate > 0 ? fxRate : 1;
			fallbackBrlByAssetId[metric.assetId] = marketValueBrl;

			const assetClass = String(asset.assetClass || 'unknown').toLowerCase();
			allocationByClass[assetClass] = (allocationByClass[assetClass] || 0) + marketValueBrl;
			allocationByCurrency[currency] = (allocationByCurrency[currency] || 0) + marketValueBrl;

			const detail = detailByAssetId.get(metric.assetId) || null;
			const sector =
				detail?.fundamentals?.sector ||
				detail?.raw?.final_payload?.info?.sector ||
				detail?.raw?.primary_payload?.info?.sector ||
				(assetClass === 'bond'
					? 'fixed_income'
					: assetClass === 'fii'
						? 'real_estate'
						: 'unknown');
			allocationBySector[sector] = (allocationBySector[sector] || 0) + marketValueBrl;
		}

		const historySeries = await this.#buildPortfolioValueSeries(
			portfolioId,
			activeMetrics,
			evolutionDays,
			{
				fxRateByAssetId,
				fallbackBrlByAssetId,
			}
		);
		const today = nowIso().slice(0, 10);
		if (historySeries.length) {
			const lastPoint = historySeries[historySeries.length - 1];
			if (lastPoint.date === today) {
				historySeries[historySeries.length - 1] = {
					...lastPoint,
					value: totalBrl,
				};
			} else {
				historySeries.push({ date: today, value: totalBrl });
			}
		} else {
			historySeries.push({ date: today, value: totalBrl });
		}
		const absoluteReturn = totalBrl - totalCostBrl;
		const percentReturn =
			totalCostBrl > Number.EPSILON
				? (absoluteReturn / totalCostBrl) * 100
				: 0;

		return {
			portfolioId,
			currency: 'BRL',
			fx_rates: fxRates,
			total_value_brl: totalBrl,
			allocation_by_class: this.#toAllocationArray(allocationByClass, totalBrl),
			allocation_by_currency: this.#toAllocationArray(allocationByCurrency, totalBrl),
			allocation_by_sector: this.#toAllocationArray(allocationBySector, totalBrl),
			evolution: historySeries,
			evolution_period: periodKey,
			return_absolute: absoluteReturn,
			return_percent: percentReturn,
			fetched_at: nowIso(),
		};
	}

	async getDividendAnalytics(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const activeAssets = assets.filter((asset) =>
			String(asset.status || 'active').toLowerCase() === 'active'
		);
		const activeAssetIds = new Set(activeAssets.map((asset) => asset.assetId));
		const activeAssetById = new Map(activeAssets.map((asset) => [asset.assetId, asset]));
		const activeAssetByTicker = new Map(
			activeAssets.map((asset) => [String(asset.ticker || '').toUpperCase(), asset])
		);
		const activeIncomeTickers = new Set(
			activeAssets
				.filter((asset) => {
					const quantity = toNumberOrNull(asset.quantity);
					const currentValue = toNumberOrNull(asset.currentValue);
					if (quantity === null && currentValue === null) return true;
					return (quantity ?? 0) > 0 || (currentValue ?? 0) > 0;
				})
				.map((asset) => String(asset.ticker || '').toUpperCase())
				.filter(Boolean)
		);
		const fxRates = await this.#getLatestFxMap();
		const transactions = await this.#listPortfolioTransactions(portfolioId);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, {
			portfolioId,
			method: options.method || 'fifo',
			includeBenchmarkComparison: false,
		});

		const today = nowIso().slice(0, 10);
		const normalizedFromDate = normalizeDate(options.fromDate);
		const requestedPeriodMonths = Math.min(
			Math.max(Math.round(numeric(options.periodMonths, 12)), 1),
			120
		);
		const periodStartDate = (() => {
			const baseDate = new Date(`${today}T00:00:00Z`);
			if (Number.isNaN(baseDate.getTime())) return normalizedFromDate || addDays(today, -365);

			if (normalizedFromDate) {
				const from = new Date(`${normalizedFromDate}T00:00:00Z`);
				if (!Number.isNaN(from.getTime())) {
					from.setUTCDate(1);
					return from.toISOString().slice(0, 10);
				}
			}

			baseDate.setUTCDate(1);
			baseDate.setUTCMonth(baseDate.getUTCMonth() - requestedPeriodMonths + 1);
			return baseDate.toISOString().slice(0, 10);
		})();
		const dividendTransactions = transactions.filter((tx) => {
			const txType = String(tx.type || '').toLowerCase();
			const txDate = normalizeDate(tx.date);
			return ['dividend', 'jcp'].includes(txType)
				&& txDate
				&& txDate >= periodStartDate
				&& txDate <= today;
		});

		const monthly = {};
		for (const tx of dividendTransactions) {
			const key = monthKey(tx.date);
			if (!key) continue;
			const asset =
				activeAssetById.get(tx.assetId)
				|| activeAssetByTicker.get(String(tx.ticker || '').toUpperCase())
				|| {};
			const currency = String(tx.currency || asset.currency || 'BRL').toUpperCase();
			const fxRate = currency === 'BRL' ? 1 : numeric(fxRates[`${currency}/BRL`], 0);
			const amount = numeric(tx.amount, 0);
			const amountBrl = fxRate > 0 ? amount * fxRate : amount;
			monthly[key] = (monthly[key] || 0) + amountBrl;
		}

		const monthsInPeriod = (() => {
			const start = new Date(`${periodStartDate.slice(0, 7)}-01T00:00:00Z`);
			const end = new Date(`${today.slice(0, 7)}-01T00:00:00Z`);
			if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) return [];
			const range = [];
			const cursor = new Date(start.getTime());
			while (cursor <= end) {
				const year = cursor.getUTCFullYear();
				const month = String(cursor.getUTCMonth() + 1).padStart(2, '0');
				range.push(`${year}-${month}`);
				cursor.setUTCMonth(cursor.getUTCMonth() + 1);
			}
			return range;
		})();

		const monthlySeries = monthsInPeriod.map((period) => ({
			period,
			amount: numeric(monthly[period], 0),
		}));
		const totalInPeriod = monthlySeries.reduce((sum, item) => sum + numeric(item.amount, 0), 0);
		const totalLast12 = monthlySeries
			.slice(-12)
			.reduce((sum, item) => sum + numeric(item.amount, 0), 0);
		const averageMonthly = monthlySeries.length > 0 ? totalInPeriod / monthlySeries.length : 0;
		const projectedMonthly = averageMonthly;
		const projectedAnnual = averageMonthly * 12;
		const activeMetrics = metrics.assets.filter((metric) => activeAssetIds.has(metric.assetId));
		let costTotalBrl = 0;
		let currentValueBrl = 0;
		for (const metric of activeMetrics) {
			const asset = activeAssetById.get(metric.assetId) || {};
			const currency = String(metric.currency || asset.currency || 'BRL').toUpperCase();
			const fxRate = currency === 'BRL' ? 1 : numeric(fxRates[`${currency}/BRL`], 0);
			const metricMarketValue = toNumberOrNull(metric.market_value);
			const metricCostTotal = toNumberOrNull(metric.cost_total);
			const metricQuantity = toNumberOrNull(metric.quantity_current);
			const metricCurrentPrice = toNumberOrNull(metric.current_price);
			const snapshotCurrentValue = toNumberOrNull(asset.currentValue);
			const snapshotCurrentPrice = toNumberOrNull(asset.currentPrice);
			const fallbackPrice = metricCurrentPrice ?? snapshotCurrentPrice;
			const derivedMarketValue =
				(fallbackPrice !== null && metricQuantity !== null)
					? fallbackPrice * metricQuantity
					: null;
			const marketValue = metricMarketValue ?? snapshotCurrentValue ?? derivedMarketValue ?? 0;
			const costTotal = metricCostTotal ?? 0;
			currentValueBrl += fxRate > 0 ? marketValue * fxRate : marketValue;
			costTotalBrl += fxRate > 0 ? costTotal * fxRate : costTotal;
		}
		const realizedYield = costTotalBrl > 0 ? (totalInPeriod / costTotalBrl) * 100 : 0;
		const currentDividendYield = currentValueBrl > 0 ? (totalInPeriod / currentValueBrl) * 100 : 0;

			const calendarByTicker = await Promise.all(
				Array.from(activeIncomeTickers).map(async (ticker) => {
				const events = await this.#queryAll({
					TableName: this.tableName,
					KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
					ExpressionAttributeValues: {
						':pk': `ASSET_EVENT#${ticker}`,
						':sk': 'DATE#',
					},
				});
					return events
						.filter((event) => this.#isDividendCalendarEventType(event.eventType))
						.map((event) => ({
							...event,
							ticker: String(event.ticker || ticker).toUpperCase(),
							eventDate: normalizeDate(event.eventDate || event.date) || normalizeDate(event.fetched_at) || null,
						}));
				})
			);
			const classifyDividendFamily = (eventType) => {
				const normalizedType = String(eventType || '').toLowerCase();
				if (normalizedType.includes('jcp') || normalizedType.includes('juros')) return 'jcp';
				if (normalizedType.includes('amort')) return 'amortization';
				return 'income';
			};
			const sourceWeight = (source) => {
				const normalized = String(source || '').toLowerCase();
				if (normalized.includes('statusinvest')) return 3;
				if (normalized.includes('fundsexplorer')) return 2;
				if (normalized) return 1;
				return 0;
			};
			const readDetails = (event) => (
				event?.details && typeof event.details === 'object' ? { ...event.details } : {}
			);
			const readDetailValue = (event) => toNumberOrNull(readDetails(event).value);
			const eventQualityScore = (event) => {
				const details = readDetails(event);
				const value = toNumberOrNull(details.value);
				let score = 0;
				if (value !== null) score += value > 0 ? 200 : 90;
				if (normalizeDate(details.paymentDate)) score += 20;
				if (normalizeDate(details.exDate)) score += 10;
				if (normalizeDate(details.recordDate || details.comDate || details.dataCom)) score += 6;
				if (normalizeDate(details.announcementDate || details.declarationDate)) score += 4;
				if (details.value_source) score += 15;
				score += sourceWeight(event.data_source) * 20;
				const type = String(event.eventType || '').toLowerCase();
				if (type.includes('payment') || type.includes('dividend') || type.includes('jcp') || type.includes('rend')) {
					score += 8;
				}
				return score;
			};

			const flattenedCalendars = calendarByTicker
				.flat()
				.filter((event) =>
					event.eventDate
					&& activeIncomeTickers.has(String(event.ticker || '').toUpperCase())
				);
			const dedupedCalendars = new Map();
			for (const event of flattenedCalendars) {
				const ticker = String(event.ticker || '').toUpperCase();
				const eventDate = normalizeDate(event.eventDate || event.date);
				if (!ticker || !eventDate) continue;
				const key = `${ticker}|${eventDate}|${classifyDividendFamily(event.eventType)}`;
				const existing = dedupedCalendars.get(key);
				const eventDetails = readDetails(event);

				if (!existing) {
					const sourceCandidates = Array.from(new Set([String(event.data_source || '').trim()].filter(Boolean)));
					dedupedCalendars.set(key, {
						...event,
						eventDate,
						details: {
							...eventDetails,
							source_candidates: sourceCandidates,
						},
					});
					continue;
				}

				const existingDetails = readDetails(existing);
				const existingValue = readDetailValue(existing);
				const candidateValue = readDetailValue(event);
				const existingSources = Array.isArray(existingDetails.source_candidates)
					? existingDetails.source_candidates.map((value) => String(value || '').trim()).filter(Boolean)
					: [];
				const mergedSources = Array.from(new Set([
					...existingSources,
					String(existing.data_source || '').trim(),
					String(event.data_source || '').trim(),
				].filter(Boolean)));

				const existingScore = eventQualityScore(existing);
				const candidateScore = eventQualityScore(event);
				const selected = candidateScore > existingScore ? event : existing;
				const selectedDetails = readDetails(selected);
				const selectedValue = selected === event ? candidateValue : existingValue;
				const otherValue = selected === event ? existingValue : candidateValue;
				const valueCandidates = Array.from(new Set(
					[selectedValue, otherValue]
						.filter((value) => value !== null && Number.isFinite(value))
						.map((value) => Number(value).toFixed(8))
				));

				dedupedCalendars.set(key, {
					...selected,
					eventDate,
					details: {
						...selectedDetails,
						source_candidates: mergedSources,
						value_candidates: valueCandidates.length > 0 ? valueCandidates : undefined,
						revised: valueCandidates.length > 1 || mergedSources.length > 1,
					},
				});
			}

			const calendars = Array.from(dedupedCalendars.values())
				.sort((left, right) =>
					String(left.eventDate || '').localeCompare(String(right.eventDate || ''))
					|| String(left.ticker || '').localeCompare(String(right.ticker || ''))
				);
			const upcoming = calendars.filter((event) => String(event.eventDate || '') >= today);

		return {
			portfolioId,
			monthly_dividends: monthlySeries,
			total_last_12_months: totalLast12,
			total_in_period: totalInPeriod,
			average_monthly_income: averageMonthly,
			annualized_income: projectedAnnual,
			period_months: monthlySeries.length,
			period_from: periodStartDate,
			period_to: today,
			projected_monthly_income: projectedMonthly,
			projected_annual_income: projectedAnnual,
			yield_on_cost_realized: realizedYield,
			dividend_yield_current: currentDividendYield,
			calendar: calendars,
			calendar_upcoming: upcoming,
			fetched_at: nowIso(),
		};
	}

	async getTaxReport(userId, year, options = {}) {
		const selectedYear = Number(year);
		if (!Number.isFinite(selectedYear) || selectedYear < 2000) {
			throw new Error('year must be a valid number');
		}
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const assetById = new Map(assets.map((asset) => [asset.assetId, asset]));
		const transactions = await this.#listPortfolioTransactions(portfolioId);

		const sorted = [...transactions]
			.map((tx) => ({
				...tx,
				date: normalizeDate(tx.date),
				createdAt: String(tx.createdAt || ''),
				transId: String(tx.transId || ''),
			}))
			.filter((tx) => tx.date)
			.sort((left, right) =>
				left.date.localeCompare(right.date)
				|| left.createdAt.localeCompare(right.createdAt)
				|| left.transId.localeCompare(right.transId)
			);

		const lotsByAsset = new Map();
		const monthly = new Map();
		const carryLossByClass = {};

		const getMonth = (date) => date.slice(0, 7);
		const ensureMonth = (key) => {
			if (!monthly.has(key)) {
				monthly.set(key, {
					month: key,
					gross_sales: {},
					realized_gain: {},
					tax_due: {},
					dividends: 0,
					jcp: 0,
				});
			}
			return monthly.get(key);
		};
		const addRealizedGain = (monthData, assetClass, gain) => {
			const normalizedGain = numeric(gain, 0);
			if (Math.abs(normalizedGain) <= Number.EPSILON) return;
			monthData.realized_gain[assetClass] = (monthData.realized_gain[assetClass] || 0) + normalizedGain;
		};

		for (const tx of sorted) {
			const txYear = Number(tx.date.slice(0, 4));
			const type = String(tx.type || '').toLowerCase();
			const quantity = Math.abs(numeric(tx.quantity, 0));
			const price = numeric(tx.price, 0);
			const amount = tx.amount !== undefined ? numeric(tx.amount, quantity * price) : quantity * price;
			const fees = numeric(tx.fees, 0);
			const asset = assetById.get(tx.assetId) || {};
			const assetClass = String(asset.assetClass || 'stock').toLowerCase();
			const month = getMonth(tx.date);

			if (!lotsByAsset.has(tx.assetId)) lotsByAsset.set(tx.assetId, []);
			const lots = lotsByAsset.get(tx.assetId);

			if (type === 'buy' || type === 'subscription') {
				const totalCost = amount + fees;
				const costPerUnit = quantity > 0 ? totalCost / quantity : 0;
				let remaining = quantity;
				let realizedGain = 0;

				// Close existing short lots (sell then buy) before opening a new long lot.
				while (remaining > 0 && lots.length > 0 && numeric(lots[0].quantity, 0) < 0) {
					const lot = lots[0];
					const shortQty = Math.abs(numeric(lot.quantity, 0));
					if (shortQty <= 0) {
						lots.shift();
						continue;
					}
					const consumed = Math.min(remaining, shortQty);
					realizedGain += consumed * (numeric(lot.costPerUnit, 0) - costPerUnit);
					lot.quantity += consumed;
					remaining -= consumed;
					if (Math.abs(numeric(lot.quantity, 0)) <= Number.EPSILON) lots.shift();
				}

				if (remaining > 0) {
					lots.push({ quantity: remaining, costPerUnit, date: tx.date });
				}

				if (txYear === selectedYear && Math.abs(realizedGain) > Number.EPSILON) {
					const monthData = ensureMonth(month);
					addRealizedGain(monthData, assetClass, realizedGain);
				}
				continue;
			}

			if (type === 'sell') {
				const proceedsPerUnit = quantity > 0 ? amount / quantity : 0;
				const feesPerUnit = quantity > 0 ? fees / quantity : 0;
				let remaining = quantity;
				let costBasis = 0;
				let closedLongQty = 0;

				// Close existing long lots first (FIFO).
				while (remaining > 0 && lots.length > 0 && numeric(lots[0].quantity, 0) > 0) {
					const lot = lots[0];
					const lotQty = numeric(lot.quantity, 0);
					if (lotQty <= 0) {
						lots.shift();
						continue;
					}
					const consumed = Math.min(remaining, lotQty);
					costBasis += consumed * numeric(lot.costPerUnit, 0);
					lot.quantity -= consumed;
					remaining -= consumed;
					closedLongQty += consumed;
					if (numeric(lot.quantity, 0) <= Number.EPSILON) lots.shift();
				}

				// If sell quantity exceeds available long lots, open a short lot instead of treating
				// unmatched quantity as immediate profit. This avoids inflated gains for intraday shorts.
				if (remaining > 0) {
					const shortOpenPrice = proceedsPerUnit - feesPerUnit;
					lots.push({ quantity: -remaining, costPerUnit: shortOpenPrice, date: tx.date });
				}

				const realizedFromClosedLong = (closedLongQty * (proceedsPerUnit - feesPerUnit)) - costBasis;

				if (txYear === selectedYear) {
					const monthData = ensureMonth(month);
					monthData.gross_sales[assetClass] = (monthData.gross_sales[assetClass] || 0) + amount;
					addRealizedGain(monthData, assetClass, realizedFromClosedLong);
				}
				continue;
			}

			if (txYear === selectedYear && (type === 'dividend' || type === 'jcp')) {
				const monthData = ensureMonth(month);
				if (type === 'dividend') monthData.dividends += amount;
				if (type === 'jcp') monthData.jcp += amount;
			}
		}

		const monthKeys = Array.from(monthly.keys()).sort();
		const monthlyOutput = [];
		let totalTaxDue = 0;
		let totalDividends = 0;
		let totalJcp = 0;

		for (const month of monthKeys) {
			const record = monthly.get(month);
			for (const [assetClass, rawGain] of Object.entries(record.realized_gain)) {
				const grossSales = numeric(record.gross_sales[assetClass], 0);
				const carried = numeric(carryLossByClass[assetClass], 0);
				const adjustedGain = rawGain + carried;

				let taxableGain = Math.max(0, adjustedGain);
				if (assetClass === 'stock' && grossSales < 20000) {
					taxableGain = 0;
				}

				const taxRate = TAX_RATE_BY_CLASS[assetClass] || 0.15;
				const taxDue = taxableGain * taxRate;
				record.tax_due[assetClass] = taxDue;
				totalTaxDue += taxDue;

				carryLossByClass[assetClass] = adjustedGain - taxableGain;
			}

			totalDividends += record.dividends;
			totalJcp += record.jcp;
			monthlyOutput.push(record);

			await this.dynamo.send(
				new PutCommand({
					TableName: this.tableName,
					Item: {
						PK: `PORTFOLIO#${portfolioId}`,
						SK: `TAX#${record.month}`,
						entityType: 'TAX_MONTHLY',
						portfolioId,
						year: selectedYear,
						month: record.month,
						gross_sales: record.gross_sales,
						realized_gain: record.realized_gain,
						tax_due: record.tax_due,
						dividends: record.dividends,
						jcp: record.jcp,
						data_source: 'internal_calc',
						fetched_at: nowIso(),
						is_scraped: false,
						updatedAt: nowIso(),
					},
				})
			);
		}

		const summary = {
			portfolioId,
			year: selectedYear,
			monthly: monthlyOutput,
			total_tax_due: totalTaxDue,
			total_dividends_isentos: totalDividends,
			total_jcp_tributavel: totalJcp,
			carry_loss_by_class: carryLossByClass,
			data_source: 'internal_calc',
			fetched_at: nowIso(),
			is_scraped: false,
		};

		await this.dynamo.send(
			new PutCommand({
				TableName: this.tableName,
				Item: {
					PK: `PORTFOLIO#${portfolioId}`,
					SK: `TAX_ANNUAL#${selectedYear}`,
					entityType: 'TAX_ANNUAL',
					...summary,
					updatedAt: nowIso(),
				},
			})
		);

		return summary;
	}

	async setRebalanceTargets(userId, payload, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const targets = Array.isArray(payload?.targets) ? payload.targets : [];
		const deduped = new Map();

		for (const target of targets) {
			const scopeRaw = String(target.scope || 'assetClass').trim().toLowerCase();
			const scope = scopeRaw === 'asset' ? 'asset' : 'assetClass';
			const value = String(target.value || '').trim();
			if (!value) continue;
			const percent = numeric(target.percent, 0);
			if (percent <= 0) continue;
			const targetId = String(target.targetId || `target-${hashId(`${scope}:${value}`)}`);
			const dedupeKey = `${scope}:${value.toLowerCase()}`;

			deduped.set(dedupeKey, {
				PK: `PORTFOLIO#${portfolioId}`,
				SK: `TARGET_ALLOC#${targetId}`,
				entityType: 'TARGET_ALLOCATION',
				portfolioId,
				targetId,
				scope,
				value,
				percent,
				data_source: 'user_input',
				fetched_at: nowIso(),
				is_scraped: false,
				updatedAt: nowIso(),
			});
		}

		const normalized = Array.from(deduped.values());
		const existing = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TARGET_ALLOC#',
			},
		});

		for (const item of existing) {
			await this.dynamo.send(
				new DeleteCommand({
					TableName: this.tableName,
					Key: {
						PK: item.PK,
						SK: item.SK,
					},
				})
			);
		}

		for (const item of normalized) {
			await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
		}

		return {
			portfolioId,
			targets: normalized,
		};
	}

	async getRebalanceTargets(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const items = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TARGET_ALLOC#',
			},
		});

		const targets = items
			.map((item) => ({
				targetId: item.targetId || String(item.SK || '').replace(/^TARGET_ALLOC#/, ''),
				scope: String(item.scope || 'assetClass').trim().toLowerCase() === 'asset' ? 'asset' : 'assetClass',
				value: String(item.value || '').trim(),
				percent: numeric(item.percent, 0),
				updatedAt: item.updatedAt || null,
			}))
			.filter((item) => item.value && item.percent > 0)
			.sort((left, right) =>
				String(left.scope).localeCompare(String(right.scope))
				|| String(left.value).localeCompare(String(right.value))
			);

		return {
			portfolioId,
			targets,
			fetched_at: nowIso(),
		};
	}

	async getRebalancingSuggestion(userId, amount, options = {}) {
		const contribution = numeric(amount, 0);
		if (contribution <= 0) {
			throw new Error('amount must be greater than zero');
		}
		const scopeRaw = String(options.scope || 'assetClass').trim().toLowerCase();
		const suggestionScope = scopeRaw === 'asset' ? 'asset' : 'assetClass';

		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const activeAssets = assets.filter((asset) =>
			String(asset.status || 'active').toLowerCase() === 'active'
		);
		const assetById = new Map(activeAssets.map((asset) => [String(asset.assetId || ''), asset]));
		const assetIdByTicker = new Map(
			activeAssets.map((asset) => [String(asset.ticker || '').toUpperCase(), String(asset.assetId || '')])
		);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, {
			portfolioId,
			method: options.method || 'fifo',
		});
		const fxRates = await this.#getLatestFxMap();
		const metricsByAssetId = new Map(
			(metrics.assets || [])
				.filter((metric) => assetById.has(String(metric.assetId || '')))
				.map((metric) => [String(metric.assetId || ''), metric])
		);

		const currentByClass = {};
		const currentByAsset = {};
		const assetByClass = {};
		for (const asset of activeAssets) {
			const assetId = String(asset.assetId || '');
			if (!assetId) continue;
			const metric = metricsByAssetId.get(assetId) || {};
			const currency = String(metric.currency || asset.currency || 'BRL').toUpperCase();
			const fxRate = currency === 'BRL' ? 1 : numeric(fxRates[`${currency}/BRL`], 0);
			const metricMarketValue = toNumberOrNull(metric.market_value);
			const metricQuantity = toNumberOrNull(metric.quantity_current);
			const snapshotQuantity = toNumberOrNull(asset.quantity);
			const metricCurrentPrice = toNumberOrNull(metric.current_price);
			const snapshotCurrentValue = toNumberOrNull(asset.currentValue);
			const snapshotCurrentPrice = toNumberOrNull(asset.currentPrice);
			const resolvedQuantity =
				(metricQuantity !== null && Math.abs(metricQuantity) > Number.EPSILON)
					? metricQuantity
					: snapshotQuantity;
			const hasOpenQuantity =
				resolvedQuantity !== null && Math.abs(resolvedQuantity) > Number.EPSILON;
			const usableMetricMarketValue =
				metricMarketValue !== null &&
				(!hasOpenQuantity || Math.abs(metricMarketValue) > Number.EPSILON)
					? metricMarketValue
					: null;
			const fallbackPrice = metricCurrentPrice ?? snapshotCurrentPrice;
			const derivedMarketValue =
				(fallbackPrice !== null && resolvedQuantity !== null)
					? fallbackPrice * resolvedQuantity
					: null;
			const marketValue =
				usableMetricMarketValue ??
				snapshotCurrentValue ??
				derivedMarketValue ??
				0;
			const marketValueBrl = fxRate > 0 ? marketValue * fxRate : marketValue;
			const assetClass = String(asset.assetClass || 'unknown').toLowerCase();
			currentByClass[assetClass] = (currentByClass[assetClass] || 0) + marketValueBrl;
			currentByAsset[assetId] = marketValueBrl;
			if (!assetByClass[assetClass]) assetByClass[assetClass] = [];
			assetByClass[assetClass].push({
				asset,
				metric,
				assetId,
				market_value_brl: marketValueBrl,
			});
		}

		const targets = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TARGET_ALLOC#',
			},
		});

		const targetByClass = {};
		const targetByAsset = {};
		for (const target of targets) {
			const scope = String(target.scope || '').trim().toLowerCase();
			const value = String(target.value || '').trim();
			const weight = numeric(target.percent, 0) / 100;
			if (!value || weight <= 0) continue;
			if (scope === 'assetclass') {
				targetByClass[value.toLowerCase()] = weight;
				continue;
			}
			if (scope === 'asset') {
				const directAssetId = assetById.has(value) ? value : null;
				const tickerAssetId = assetIdByTicker.get(value.toUpperCase()) || null;
				const assetId = directAssetId || tickerAssetId;
				if (!assetId) continue;
				targetByAsset[assetId] = weight;
			}
		}

		let targetByScope = {};
		let currentByScope = {};
		if (suggestionScope === 'asset') {
			targetByScope = { ...targetByAsset };
			currentByScope = currentByAsset;
			if (Object.keys(targetByScope).length === 0) {
				const assetsWithValue = Object.entries(currentByAsset)
					.filter(([, value]) => Math.abs(numeric(value, 0)) > Number.EPSILON)
					.map(([assetId]) => assetId);
				const base = assetsWithValue.length > 0
					? assetsWithValue
					: Object.keys(currentByAsset);
				const equalWeight = base.length ? 1 / base.length : 0;
				for (const assetId of base) targetByScope[assetId] = equalWeight;
			}
		} else {
			targetByScope = { ...targetByClass };
			currentByScope = currentByClass;
			if (Object.keys(targetByScope).length === 0) {
				const classes = Object.entries(currentByClass)
					.filter(([, value]) => Math.abs(numeric(value, 0)) > Number.EPSILON)
					.map(([cls]) => cls);
				const equalWeight = classes.length ? 1 / classes.length : 0;
				for (const cls of classes) targetByScope[cls] = equalWeight;
			}
		}

		const currentTotal = Object.values(currentByScope).reduce((sum, value) => sum + value, 0);
		const targetTotal = currentTotal + contribution;
		const deficits = {};
		const drift = [];
		let positiveDeficitSum = 0;
		for (const [key, weight] of Object.entries(targetByScope)) {
			const desired = targetTotal * weight;
			const current = numeric(currentByScope[key], 0);
			const deficit = Math.max(0, desired - current);
			deficits[key] = deficit;
			positiveDeficitSum += deficit;
			const targetWeight = numeric(weight, 0);
			const currentWeight = currentTotal > 0 ? current / currentTotal : 0;
			const driftPct = (currentWeight - targetWeight) * 100;
			const driftValue = current - desired;

			if (suggestionScope === 'asset') {
				const asset = assetById.get(key) || {};
				drift.push({
					scope: 'asset',
					scope_key: key,
					assetId: key,
					ticker: asset.ticker || null,
					assetClass: String(asset.assetClass || 'unknown').toLowerCase(),
					current_value: current,
					target_value: desired,
					target_weight_pct: targetWeight * 100,
					current_weight_pct: currentWeight * 100,
					drift_value: driftValue,
					drift_pct: driftPct,
				});
			} else {
				drift.push({
					scope: 'assetClass',
					scope_key: key,
					assetClass: key,
					current_value: current,
					target_value: desired,
					target_weight_pct: targetWeight * 100,
					current_weight_pct: currentWeight * 100,
					drift_value: driftValue,
					drift_pct: driftPct,
				});
			}
		}

		const suggestions = [];
		for (const [key, deficit] of Object.entries(deficits)) {
			if (deficit <= 0) continue;
			const allocation = positiveDeficitSum > 0
				? (deficit / positiveDeficitSum) * contribution
				: 0;
			if (suggestionScope === 'asset') {
				const asset = assetById.get(key) || {};
				suggestions.push({
					scope: 'asset',
					assetId: key,
					ticker: asset.ticker || null,
					assetClass: String(asset.assetClass || 'unknown').toLowerCase(),
					recommended_amount: allocation,
					current_value: numeric(currentByScope[key], 0),
					target_value: targetTotal * numeric(targetByScope[key], 0),
				});
				continue;
			}

			const cls = key;
			const bucket = assetByClass[cls] || [];
			const selected = bucket
				.sort((left, right) => numeric(right.market_value_brl, 0) - numeric(left.market_value_brl, 0))[0];
			suggestions.push({
				scope: 'assetClass',
				assetClass: cls,
				recommended_amount: allocation,
				assetId: selected?.asset?.assetId || null,
				ticker: selected?.asset?.ticker || null,
				current_value: numeric(currentByScope[cls], 0),
				target_value: targetTotal * numeric(targetByScope[cls], 0),
			});
		}

		return {
			portfolioId,
			scope: suggestionScope,
			contribution,
			current_total: currentTotal,
			target_total_after_contribution: targetTotal,
			targets: targetByScope,
			drift,
			suggestions,
			fetched_at: nowIso(),
		};
	}

	async recordContribution(userId, payload, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId || payload.portfolioId);
		const contributionId = payload.contributionId || `contrib-${hashId(`${nowIso()}:${Math.random()}`)}`;
		const date = normalizeDate(payload.date) || nowIso().slice(0, 10);
		const item = {
			PK: `PORTFOLIO#${portfolioId}`,
			SK: `CONTRIB#${date}#${contributionId}`,
			entityType: 'PORTFOLIO_CONTRIBUTION',
			portfolioId,
			contributionId,
			date,
			amount: numeric(payload.amount, 0),
			currency: payload.currency || 'BRL',
			destination: payload.destination || null,
			notes: payload.notes || null,
			data_source: 'user_input',
			fetched_at: nowIso(),
			is_scraped: false,
			updatedAt: nowIso(),
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
		return item;
	}

	async getContributionProgress(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const items = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'CONTRIB#',
			},
		});

		const monthly = {};
		let total = 0;
		for (const item of items) {
			const amount = numeric(item.amount, 0);
			total += amount;
			const month = monthKey(item.date);
			if (!month) continue;
			monthly[month] = (monthly[month] || 0) + amount;
		}

		const monthlySeries = Object.entries(monthly)
			.sort(([left], [right]) => left.localeCompare(right))
			.map(([month, amount]) => ({ month, amount }));
		const avgMonthly = monthlySeries.length
			? monthlySeries.reduce((sum, entry) => sum + entry.amount, 0) / monthlySeries.length
			: 0;

		return {
			portfolioId,
			total_contributions: total,
			average_monthly: avgMonthly,
			monthly: monthlySeries,
			fetched_at: nowIso(),
		};
	}

	async createAlertRule(userId, rule) {
		const ruleId = rule.ruleId || `alert-${hashId(`${nowIso()}:${Math.random()}`)}`;
		const item = {
			PK: `USER#${userId}`,
			SK: `ALERT_RULE#${ruleId}`,
			entityType: 'ALERT_RULE',
			ruleId,
			type: String(rule.type || 'price_target'),
			enabled: rule.enabled !== false,
			portfolioId: rule.portfolioId || null,
			params: rule.params || {},
			description: rule.description || null,
			data_source: 'user_input',
			fetched_at: nowIso(),
			is_scraped: false,
			createdAt: nowIso(),
			updatedAt: nowIso(),
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
		return item;
	}

	async updateAlertRule(userId, ruleId, rule) {
		const key = { PK: `USER#${userId}`, SK: `ALERT_RULE#${ruleId}` };
		const existing = await this.dynamo.send(new GetCommand({ TableName: this.tableName, Key: key }));
		if (!existing.Item) throw new Error('Alert rule not found');
		const next = {
			...existing.Item,
			...rule,
			ruleId,
			updatedAt: nowIso(),
			fetched_at: nowIso(),
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: next }));
		return next;
	}

	async deleteAlertRule(userId, ruleId) {
		await this.dynamo.send(
			new DeleteCommand({
				TableName: this.tableName,
				Key: { PK: `USER#${userId}`, SK: `ALERT_RULE#${ruleId}` },
			})
		);
		return { deleted: true, ruleId };
	}

	async getAlerts(userId, options = {}) {
		const rules = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'ALERT_RULE#',
			},
		});

		const events = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'ALERT_EVENT#',
			},
		});

		const recentEvents = events
			.sort((left, right) => String(right.eventAt || '').localeCompare(String(left.eventAt || '')))
			.slice(0, options.limit ? Number(options.limit) : 100);

		return {
			rules,
			events: recentEvents,
		};
	}

	async evaluateAlerts(userId, portfolioId, options = {}) {
		const rulesResult = await this.getAlerts(userId, { limit: 0 });
		const rules = rulesResult.rules.filter((rule) => rule.enabled !== false);
		const hasConcentrationRule = rules.some((rule) => String(rule.type || '').toLowerCase() === 'concentration');
		const hasRebalanceRule = rules.some((rule) => String(rule.type || '').toLowerCase() === 'rebalance_drift');
		const [dashboard, risk] = await Promise.all([
			hasRebalanceRule ? this.getDashboard(userId, { portfolioId: portfolioId || options.portfolioId }) : null,
			hasConcentrationRule ? this.getPortfolioRisk(userId, { portfolioId: portfolioId || options.portfolioId }) : null,
		]);
		const triggered = [];
		const existingDedupe = new Set(
			rulesResult.events
				.map((event) => `${event.ruleId || ''}::${event.dedupeKey || ''}`)
				.filter((entry) => !entry.endsWith('::'))
		);
		const eventCacheByTicker = new Map();

		const createAlertEvent = async (rule, type, message, dedupeKey = null, metadata = null) => {
			if (dedupeKey) {
				const dedupeRef = `${rule.ruleId || ''}::${dedupeKey}`;
				if (existingDedupe.has(dedupeRef)) return null;
				existingDedupe.add(dedupeRef);
			}

			const eventId = `alert-event-${hashId(`${rule.ruleId}:${nowIso()}:${Math.random()}`)}`;
			const item = {
				PK: `USER#${userId}`,
				SK: `ALERT_EVENT#${nowIso()}#${eventId}`,
				entityType: 'ALERT_EVENT',
				eventId,
				ruleId: rule.ruleId,
				type,
				message,
				dedupeKey: dedupeKey || null,
				metadata: metadata || null,
				eventAt: nowIso(),
				read: false,
				data_source: 'internal_calc',
				fetched_at: nowIso(),
				is_scraped: false,
			};
			await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
			triggered.push(item);
			return item;
		};

		for (const rule of rules) {
			try {
				const type = String(rule.type || '').toLowerCase();
				let shouldTrigger = false;
				let message = null;

				if (type === 'concentration') {
					const threshold = numeric(rule.params?.thresholdPct, 15);
					const top = (risk?.concentration || []).find((item) => item.weight_pct > threshold);
					if (top) {
						shouldTrigger = true;
						message = `Concentration alert: ${top.ticker} at ${top.weight_pct.toFixed(2)}%`;
					}
				}

				if (type === 'price_target') {
					const ticker = String(rule.params?.ticker || '').toUpperCase();
					const target = numeric(rule.params?.target, 0);
					const direction = String(rule.params?.direction || 'above').toLowerCase();
					if (ticker && target > 0) {
						const price = await this.priceHistoryService.getPriceAtDate(ticker, nowIso().slice(0, 10), {
							userId,
							portfolioId: portfolioId || options.portfolioId,
						});
						const close = numeric(price.close, 0);
						if ((direction === 'above' && close >= target) || (direction === 'below' && close <= target)) {
							shouldTrigger = true;
							message = `Price target hit for ${ticker}: ${close}`;
						}
					}
				}

				if (type === 'rebalance_drift') {
					const threshold = numeric(rule.params?.thresholdPct, 5);
					const worst = (dashboard?.allocation_by_class || [])
						.map((item) => ({ ...item, drift_pct: Math.abs(numeric(item.weight_pct, 0) - numeric(rule.params?.targetByClass?.[item.key], 0)) }))
						.sort((left, right) => right.drift_pct - left.drift_pct)[0];
					if (worst && worst.drift_pct > threshold) {
						shouldTrigger = true;
						message = `Rebalance drift above threshold on ${worst.key}`;
					}
				}

				if (type === 'dividend_announcement') {
					const resolvedPortfolioId = String(rule.portfolioId || portfolioId || options.portfolioId || '');
					const tickerFilter = String(rule.params?.ticker || '').trim().toUpperCase();
					const lookaheadDays = Math.min(Math.max(Math.round(numeric(rule.params?.lookaheadDays, 30)), 1), 365);
					const today = nowIso().slice(0, 10);
					const untilDate = addDays(today, lookaheadDays);

					if (resolvedPortfolioId) {
						const assets = await this.#listPortfolioAssets(resolvedPortfolioId);
						const activeAssets = assets.filter((asset) =>
							String(asset.status || 'active').toLowerCase() === 'active'
						);
						const trackedAssets = tickerFilter
							? activeAssets.filter((asset) => String(asset.ticker || '').toUpperCase() === tickerFilter)
							: activeAssets;

						for (const asset of trackedAssets) {
							const ticker = String(asset.ticker || '').toUpperCase();
							if (!ticker) continue;

							if (!eventCacheByTicker.has(ticker)) {
								const rows = await this.#queryAll({
									TableName: this.tableName,
									KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
									ExpressionAttributeValues: {
										':pk': `ASSET_EVENT#${ticker}`,
										':sk': 'DATE#',
									},
								});
								eventCacheByTicker.set(ticker, rows);
							}

							const rows = eventCacheByTicker.get(ticker) || [];
							for (const event of rows) {
								if (!this.#isDividendEventType(event.eventType)) continue;
								const eventDate = normalizeDate(event.eventDate || event.date);
								if (!eventDate) continue;
								if (eventDate < today || eventDate > untilDate) continue;
								const eventType = String(event.eventType || 'dividend');
								const dedupeKey = `dividend_announcement:${ticker}:${eventDate}:${eventType.toLowerCase()}`;
								const eventMessage = `Dividend announcement: ${ticker} ${eventType} on ${eventDate}`;
								await createAlertEvent(
									rule,
									type,
									eventMessage,
									dedupeKey,
									{
										ticker,
										eventDate,
										eventType,
										eventTitle: event.eventTitle || null,
									}
								);
							}
						}
					}
				}

				if (shouldTrigger) {
					await createAlertEvent(rule, type, message);
				}
			} catch (error) {
				this.logger.error(
					JSON.stringify({
						event: 'alert_evaluation_failed',
						ruleId: rule.ruleId,
						error: error.message,
						fetched_at: nowIso(),
					})
				);
			}
		}

		return {
			triggered_count: triggered.length,
			triggered,
		};
	}

	async createGoal(userId, goal) {
		const goalId = goal.goalId || `goal-${hashId(`${nowIso()}:${Math.random()}`)}`;
		const item = {
			PK: `USER#${userId}`,
			SK: `GOAL#${goalId}`,
			entityType: 'USER_GOAL',
			goalId,
			type: goal.type || 'net_worth',
			targetAmount: numeric(goal.targetAmount, 0),
			targetDate: normalizeDate(goal.targetDate) || null,
			currency: goal.currency || 'BRL',
			label: goal.label || null,
			status: goal.status || 'active',
			data_source: 'user_input',
			fetched_at: nowIso(),
			is_scraped: false,
			createdAt: nowIso(),
			updatedAt: nowIso(),
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
		return item;
	}

	async updateGoal(userId, goalId, goal) {
		const key = { PK: `USER#${userId}`, SK: `GOAL#${goalId}` };
		const existing = await this.dynamo.send(new GetCommand({ TableName: this.tableName, Key: key }));
		if (!existing.Item) throw new Error('Goal not found');
		const item = {
			...existing.Item,
			...goal,
			goalId,
			targetAmount: goal.targetAmount !== undefined ? numeric(goal.targetAmount, 0) : existing.Item.targetAmount,
			targetDate: goal.targetDate !== undefined ? normalizeDate(goal.targetDate) : existing.Item.targetDate,
			updatedAt: nowIso(),
			fetched_at: nowIso(),
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
		return item;
	}

	async deleteGoal(userId, goalId) {
		await this.dynamo.send(
			new DeleteCommand({
				TableName: this.tableName,
				Key: { PK: `USER#${userId}`, SK: `GOAL#${goalId}` },
			})
		);
		return { deleted: true, goalId };
	}

	async listGoals(userId) {
		return this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'GOAL#',
			},
		});
	}

	async getGoalProgress(userId, goalId, options = {}) {
		const key = { PK: `USER#${userId}`, SK: `GOAL#${goalId}` };
		const response = await this.dynamo.send(new GetCommand({ TableName: this.tableName, Key: key }));
		if (!response.Item) throw new Error('Goal not found');
		const goal = response.Item;

		let currentValue = 0;
		if (goal.type === 'passive_income') {
			const div = await this.getDividendAnalytics(userId, { portfolioId: options.portfolioId });
			currentValue = numeric(div.projected_monthly_income, 0);
		} else {
			const dashboard = await this.getDashboard(userId, { portfolioId: options.portfolioId });
			currentValue = numeric(dashboard.total_value_brl, 0);
		}

		const targetValue = numeric(goal.targetAmount, 0);
		const progressPct = targetValue > 0 ? (currentValue / targetValue) * 100 : 0;
		const remaining = Math.max(0, targetValue - currentValue);

		let projectedDate = null;
		if (remaining > 0) {
			const contributions = await this.getContributionProgress(userId, { portfolioId: options.portfolioId });
			const avgMonthly = Math.max(numeric(contributions.average_monthly, 0), 1);
			const months = Math.ceil(remaining / avgMonthly);
			const base = new Date();
			base.setUTCMonth(base.getUTCMonth() + months);
			projectedDate = base.toISOString().slice(0, 10);
		}

		return {
			goal,
			current_value: currentValue,
			target_value: targetValue,
			progress_pct: progressPct,
			remaining,
			projected_completion_date: projectedDate,
			fetched_at: nowIso(),
		};
	}

	async getAssetDetails(ticker, options = {}) {
		if (!ticker) throw new Error('ticker is required');
		const context = await this.#resolveAssetContext(ticker, options.userId, options.portfolioId);
		const detail = await this.#getLatestAssetDetail(context.portfolioId, context.asset.assetId);
		const prices = await this.#listAssetPriceRows(context.portfolioId, context.asset.assetId);
		const averageCost = await this.priceHistoryService.getAverageCost(context.asset.ticker, options.userId, {
			portfolioId: context.portfolioId,
			method: options.method || 'fifo',
		});
		const financialStatements = this.#extractFinancialStatementsFromDetail(detail);

		return {
			asset: context.asset,
			detail: detail || null,
			latest_price: prices.length ? prices[prices.length - 1] : null,
			average_cost: averageCost,
			financial_statements: financialStatements,
			fetched_at: nowIso(),
		};
	}

	async getAssetFinancialStatements(ticker, options = {}) {
		const details = await this.getAssetDetails(ticker, options);
		const current = mergeFinancialStatements(createEmptyFinancialStatements(), details.financial_statements);
		let documents = readFinancialDocumentsFromPayload(details.detail, 'cached_asset_detail');
		let fundInfo = readFundGeneralInfoFromPayload(details.detail, 'cached_asset_detail');
		let fundPortfolio = readFundPortfolioFromPayload(details.detail, 'cached_asset_detail');
		const sourcesWithData = new Set();
		const attemptedSources = [];
		const errors = [];

		if (hasAnyFinancialStatements(current)) {
			sourcesWithData.add('cached_asset_detail');
		}
		if (hasFundPortfolio(fundPortfolio)) {
			sourcesWithData.add('cached_asset_detail');
		}

		const market = resolveAssetMarket(details.asset);
		const sourceAsset = {
			...details.asset,
			market,
		};
		const yahooSymbol = resolveYahooSymbol(details.asset.ticker, market);

		const candidates = [];
		if (yahooSymbol && market !== 'TESOURO') {
			candidates.push({
				source: 'yahoo_quote_summary_api',
				load: async () => {
					const payload = await this.yahooApiProvider.fetch(yahooSymbol, { historyDays: 5 });
					return {
						statements: readFinancialStatementsFromPayload(payload),
						documents: readFinancialDocumentsFromPayload(payload, 'yahoo_quote_summary_api'),
						fund_info: readFundGeneralInfoFromPayload(payload, 'yahoo_quote_summary_api'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'yahoo_quote_summary_api'),
						error: payload?.raw?.quote_summary_error || null,
					};
				},
			});
			candidates.push({
				source: 'yahoo_finance_scraper',
				load: async () => {
					const payload = await this.yahooFinanceScraper.scrape(sourceAsset);
					return {
						statements: readFinancialStatementsFromPayload(payload),
						documents: readFinancialDocumentsFromPayload(payload, 'yahoo_finance_scraper'),
						fund_info: readFundGeneralInfoFromPayload(payload, 'yahoo_finance_scraper'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'yahoo_finance_scraper'),
						error: null,
					};
				},
			});
		}

		if (market === 'BR') {
			candidates.push({
				source: 'b3_direct_financials',
				load: async () => {
					const payload = await this.b3FinancialStatementsProvider.fetch(sourceAsset);
					return {
						statements: readFinancialStatementsFromPayload(payload),
						documents: readFinancialDocumentsFromPayload(payload, 'b3_direct_financials'),
						fund_info: readFundGeneralInfoFromPayload(payload, 'b3_direct_financials'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'b3_direct_financials'),
						error: payload?.raw?.error || null,
					};
				},
			});
			candidates.push({
				source: 'fundsexplorer',
				load: async () => {
					const payload = await this.fundsExplorerProvider.fetch(sourceAsset);
					return {
						statements: createEmptyFinancialStatements(),
						documents: [],
						fund_info: readFundGeneralInfoFromPayload(payload, 'fundsexplorer'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'fundsexplorer'),
						error: null,
					};
				},
			});
			candidates.push({
				source: 'statusinvest_structured',
				load: async () => {
					const payload = await this.statusInvestStructuredProvider.fetch(sourceAsset);
					return {
						statements: readFinancialStatementsFromPayload(payload),
						documents: readFinancialDocumentsFromPayload(payload, 'statusinvest_structured'),
						fund_info: readFundGeneralInfoFromPayload(payload, 'statusinvest_structured'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'statusinvest_structured'),
						error: null,
					};
				},
			});
			candidates.push({
				source: 'statusinvest_scraper',
				load: async () => {
					const payload = await this.statusInvestScraper.scrape(sourceAsset);
					return {
						statements: readFinancialStatementsFromPayload(payload),
						documents: readFinancialDocumentsFromPayload(payload, 'statusinvest_scraper'),
						fund_info: readFundGeneralInfoFromPayload(payload, 'statusinvest_scraper'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'statusinvest_scraper'),
						error: null,
					};
				},
			});
		}

		if (['BR', 'US', 'CA'].includes(market)) {
			candidates.push({
				source: 'google_finance_scraper',
				load: async () => {
					const payload = await this.googleFinanceScraper.scrape(sourceAsset);
					return {
						statements: readFinancialStatementsFromPayload(payload),
						documents: readFinancialDocumentsFromPayload(payload, 'google_finance_scraper'),
						fund_info: readFundGeneralInfoFromPayload(payload, 'google_finance_scraper'),
						fund_portfolio: readFundPortfolioFromPayload(payload, 'google_finance_scraper'),
						error: null,
					};
				},
			});
		}

		let merged = current;
		for (const candidate of candidates) {
			attemptedSources.push(candidate.source);
			try {
				const loaded = await candidate.load();
				const candidateStatements = loaded?.statements || createEmptyFinancialStatements();
				documents = mergeFinancialDocuments(documents, loaded?.documents);
				fundInfo = mergeFundGeneralInfo(fundInfo, loaded?.fund_info);
				fundPortfolio = mergeFundPortfolio(fundPortfolio, loaded?.fund_portfolio);
				if (loaded?.error?.message) {
					errors.push({
						source: candidate.source,
						message: loaded.error.message,
					});
				}
				if (hasAnyFinancialStatements(candidateStatements)) {
					sourcesWithData.add(candidate.source);
				}
				if (hasFundGeneralInfo(loaded?.fund_info)) {
					sourcesWithData.add(candidate.source);
				}
				if (hasFundPortfolio(loaded?.fund_portfolio)) {
					sourcesWithData.add(candidate.source);
				}

				const nextMerged = mergeFinancialStatements(merged, candidateStatements);
				merged = nextMerged;
				if (FINANCIAL_STATEMENT_KEYS.every((key) => isPopulatedStatement(merged[key]))) {
					break;
				}
			} catch (error) {
				errors.push({
					source: candidate.source,
					message: error.message,
				});
			}
		}

		const normalizedAssetClass = String(details.asset.assetClass || '').toLowerCase();
		if (
			documents.length === 0 &&
			market === 'BR' &&
			['fii', 'stock'].includes(normalizedAssetClass)
		) {
			attemptedSources.push('statusinvest_communications');
			try {
				const communications = await this.#fetchStatusInvestFilingDocuments(
					details.asset.ticker,
					details.asset.assetClass
				);
				documents = mergeFinancialDocuments(documents, communications);
				if (Array.isArray(communications) && communications.length > 0) {
					sourcesWithData.add('statusinvest_communications');
				}
			} catch (error) {
				errors.push({
					source: 'statusinvest_communications',
					message: error.message,
				});
			}
		}

		return {
			...merged,
			ticker: details.asset.ticker,
			portfolioId: details.asset.portfolioId,
				sources: Array.from(sourcesWithData),
				attempted_sources: attemptedSources,
				documents,
				fund_info: fundInfo,
				fund_portfolio: fundPortfolio,
				errors,
				fetched_at: nowIso(),
			};
		}

	async getFairPrice(ticker, options = {}) {
		const details = await this.getAssetDetails(ticker, options);
		const info =
			details.detail?.raw?.final_payload?.info ||
			details.detail?.raw?.primary_payload?.info ||
			details.detail?.fundamentals ||
			{};
		const lpa =
			numeric(info.trailingEps, 0) ||
			numeric(info.epsTrailingTwelveMonths, 0) ||
			numeric(info.lpa, 0);
		const vpa = numeric(info.bookValue, 0) || numeric(info.vpa, 0);
		const graham = lpa > 0 && vpa > 0 ? Math.sqrt(22.5 * lpa * vpa) : null;

		const dividends = this.#extractDividendAmounts(details.detail?.historical?.dividends || []);
		const annualDividend = dividends.slice(-12).reduce((sum, value) => sum + value, 0);
		const bazin = annualDividend > 0 ? annualDividend / 0.06 : null;

		const currentPrice = numeric(details.latest_price?.close, numeric(details.average_cost.current_price, 0));
		const fairValues = [graham, bazin].filter((value) => value !== null);
		const fairPrice = fairValues.length
			? fairValues.reduce((sum, value) => sum + value, 0) / fairValues.length
			: null;
		const marginOfSafety = fairPrice && fairPrice > 0
			? ((fairPrice - currentPrice) / fairPrice) * 100
			: null;

		return {
			ticker: details.asset.ticker,
			current_price: currentPrice,
			graham,
			bazin,
			fair_price: fairPrice,
			margin_of_safety_pct: marginOfSafety,
			fundamentals: {
				pe: info.trailingPE ?? null,
				pb: info.priceToBook ?? null,
				roe: info.returnOnEquity ?? null,
				roa: info.returnOnAssets ?? null,
				roic: info.returnOnInvestedCapital ?? null,
				netDebtEbitda: info.netDebtToEbitda ?? null,
				payout: info.payoutRatio ?? null,
				evEbitda: info.enterpriseToEbitda ?? null,
				lpa,
				vpa,
				netMargin: info.profitMargins ?? null,
				ebitMargin: info.operatingMargins ?? null,
			},
			fetched_at: nowIso(),
		};
	}

	async screenAssets(filters = {}, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(options.userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const results = [];

		for (const asset of assets) {
			const details = await this.getAssetDetails(asset.ticker, {
				userId: options.userId,
				portfolioId,
			});
			const fair = await this.getFairPrice(asset.ticker, {
				userId: options.userId,
				portfolioId,
			});
			const info =
				details.detail?.raw?.final_payload?.info ||
				details.detail?.raw?.primary_payload?.info ||
				details.detail?.fundamentals ||
				{};

			const pe = numeric(info.trailingPE, null);
			const dy = numeric(info.dividendYield, null);
			const roe = numeric(info.returnOnEquity, null);
			const payout = numeric(info.payoutRatio, null);
			const netDebtEbitda = numeric(info.netDebtToEbitda, null);
			const revenueGrowth = numeric(info.revenueGrowth, null);
			const sector = String(info.sector || 'unknown');

			if (filters.assetClass && String(filters.assetClass).toLowerCase() !== String(asset.assetClass || '').toLowerCase()) {
				continue;
			}
			if (filters.sector && String(filters.sector).toLowerCase() !== sector.toLowerCase()) {
				continue;
			}
			if (filters.peMax !== undefined && pe !== null && pe > numeric(filters.peMax, pe)) continue;
			if (filters.dyMin !== undefined && dy !== null && dy * 100 < numeric(filters.dyMin, 0)) continue;
			if (filters.roeMin !== undefined && roe !== null && roe * 100 < numeric(filters.roeMin, 0)) continue;

			let score = 0;
			if (dy !== null && dy * 100 >= numeric(filters.dyTarget || 5, 5)) score += 2;
			if (roe !== null && roe * 100 >= numeric(filters.roeTarget || 15, 15)) score += 2;
			if (netDebtEbitda !== null && netDebtEbitda < 3) score += 2;
			if (payout !== null && payout < 0.8) score += 2;
			if (revenueGrowth !== null && revenueGrowth > 0) score += 2;

			results.push({
				assetId: asset.assetId,
				ticker: asset.ticker,
				name: asset.name,
				assetClass: asset.assetClass,
				sector,
				pe,
				dy: dy !== null ? dy * 100 : null,
				roe: roe !== null ? roe * 100 : null,
				fair_price: fair.fair_price,
				margin_of_safety_pct: fair.margin_of_safety_pct,
				buy_and_hold_score: score,
			});
		}

		results.sort((left, right) => right.buy_and_hold_score - left.buy_and_hold_score);
		return {
			portfolioId,
			filters,
			count: results.length,
			results,
			fetched_at: nowIso(),
		};
	}

	async compareAssets(tickers = [], options = {}) {
		if (!Array.isArray(tickers) || tickers.length < 2) {
			throw new Error('tickers[] requires at least 2 assets');
		}
		const rows = [];
		for (const ticker of tickers) {
			const details = await this.getAssetDetails(ticker, options);
			const fair = await this.getFairPrice(ticker, options);
			const risk = await this.#getAssetRiskSnapshot(details.asset.portfolioId, details.asset.assetId);
			rows.push({
				ticker: details.asset.ticker,
				name: details.asset.name,
				assetClass: details.asset.assetClass,
				currency: details.asset.currency,
				current_price: fair.current_price,
				fair_price: fair.fair_price,
				margin_of_safety_pct: fair.margin_of_safety_pct,
				fundamentals: fair.fundamentals,
				risk,
			});
		}
		return {
			tickers,
			comparison: rows,
			fetched_at: nowIso(),
		};
	}

	async getPortfolioRisk(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const activeAssets = assets.filter((asset) =>
			String(asset.status || 'active').toLowerCase() === 'active'
		);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, { portfolioId });
		const activeAssetById = new Map(activeAssets.map((asset) => [asset.assetId, asset]));
		const activeMetrics = metrics.assets.filter((metric) => activeAssetById.has(metric.assetId));
		const fxRates = await this.#getLatestFxMap();
		const fxRateByAssetId = {};
		const fallbackBrlByAssetId = {};
		const normalizedActiveMetrics = activeMetrics.map((metric) => {
			const asset = activeAssetById.get(metric.assetId) || {};
			const currency = String(metric.currency || asset.currency || 'BRL').toUpperCase();
			const fxRate = currency === 'BRL' ? 1 : numeric(fxRates[`${currency}/BRL`], 0);
			const metricMarketValue = toNumberOrNull(metric.market_value);
			const metricQuantity = toNumberOrNull(metric.quantity_current);
			const metricCurrentPrice = toNumberOrNull(metric.current_price);
			const snapshotCurrentValue = toNumberOrNull(asset.currentValue);
			const snapshotCurrentPrice = toNumberOrNull(asset.currentPrice);
			const hasOpenQuantity =
				metricQuantity !== null && Math.abs(metricQuantity) > Number.EPSILON;
			const usableMetricMarketValue =
				metricMarketValue !== null &&
				(!hasOpenQuantity || Math.abs(metricMarketValue) > Number.EPSILON)
					? metricMarketValue
					: null;
			const fallbackPrice = metricCurrentPrice ?? snapshotCurrentPrice;
			const derivedMarketValue =
				(fallbackPrice !== null && metricQuantity !== null)
					? fallbackPrice * metricQuantity
					: null;
			const marketValue =
				usableMetricMarketValue ??
				snapshotCurrentValue ??
				derivedMarketValue ??
				0;
			const marketValueBrl = fxRate > 0 ? marketValue * fxRate : marketValue;
			fxRateByAssetId[metric.assetId] = fxRate > 0 ? fxRate : 1;
			fallbackBrlByAssetId[metric.assetId] = marketValueBrl;
			return {
				...metric,
				ticker: String(metric.ticker || asset.ticker || '').toUpperCase(),
				currency,
				market_value: marketValue,
				market_value_brl: marketValueBrl,
			};
		});
		const totalValue = Math.max(
			normalizedActiveMetrics.reduce((sum, metric) => sum + numeric(metric.market_value_brl, 0), 0),
			1
		);

		const concentration = normalizedActiveMetrics
			.map((metric) => ({
				assetId: metric.assetId,
				ticker: String(metric.ticker || activeAssetById.get(metric.assetId)?.ticker || '').toUpperCase(),
				market_value: numeric(metric.market_value_brl, 0),
				weight_pct: (numeric(metric.market_value_brl, 0) / totalValue) * 100,
			}))
			.sort((left, right) => right.weight_pct - left.weight_pct);

		const byAssetReturns = {};
		const volatilityByAsset = {};
		const drawdownByAsset = {};

		for (const asset of activeAssets) {
			const ticker = String(asset.ticker || '').toUpperCase();
			if (!ticker) continue;
			const rows = await this.#listAssetPriceRows(portfolioId, asset.assetId);
			const returns = this.#toReturns(rows);
			byAssetReturns[ticker] = returns;
			const onlyReturns = returns.map((item) => item.returnPct / 100);
			volatilityByAsset[ticker] = stdDev(onlyReturns) * Math.sqrt(252) * 100;
			drawdownByAsset[ticker] = this.#maxDrawdown(rows.map((row) => numeric(row.close, 0)));
		}

		const correlationMatrix = [];
		const tickers = Object.keys(byAssetReturns);
		for (const leftTicker of tickers) {
			for (const rightTicker of tickers) {
				if (leftTicker >= rightTicker) continue;
				const aligned = this.#alignReturns(byAssetReturns[leftTicker], byAssetReturns[rightTicker]);
				correlationMatrix.push({
					left: leftTicker,
					right: rightTicker,
					correlation: correlation(aligned.left, aligned.right),
				});
			}
		}

		const series = await this.#buildPortfolioValueSeries(
			portfolioId,
			normalizedActiveMetrics,
			365,
			{
				fxRateByAssetId,
				fallbackBrlByAssetId,
			}
		);
		const portfolioValues = series.map((point) => numeric(point.value, 0));
		const portfolioDrawdown = this.#maxDrawdown(portfolioValues);
		const portfolioReturns = [];
		for (let index = 1; index < portfolioValues.length; index += 1) {
			const prev = portfolioValues[index - 1];
			const curr = portfolioValues[index];
			if (prev > 0) portfolioReturns.push((curr / prev) - 1);
		}

		const fxExposure = this.#buildFxExposure(normalizedActiveMetrics);
		const ipcaDeflatedSeries = await this.#buildIpcaDeflatedSeries(series);

		return {
			portfolioId,
			concentration,
			concentration_alerts: concentration.filter((item) => item.weight_pct > numeric(options.concentrationThreshold, 15)),
			volatility_by_asset: volatilityByAsset,
			drawdown_by_asset: drawdownByAsset,
			portfolio_drawdown: portfolioDrawdown,
			portfolio_volatility: stdDev(portfolioReturns) * Math.sqrt(252) * 100,
			correlation_matrix: correlationMatrix,
			risk_return_scatter: normalizedActiveMetrics
				.map((asset) => {
					const ticker = String(asset.ticker || activeAssetById.get(asset.assetId)?.ticker || '').toUpperCase();
					if (!ticker) return null;
					return {
						ticker,
						volatility: volatilityByAsset[ticker] || 0,
						return_pct: numeric(asset.percent_return, 0),
					};
				})
				.filter(Boolean),
			fx_exposure: fxExposure,
			inflation_adjusted_value: ipcaDeflatedSeries,
			fetched_at: nowIso(),
		};
	}

	async getBenchmarkComparison(userId, benchmark, period = '1A', options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, { portfolioId });
		const portfolioSeries = await this.#buildPortfolioValueSeries(
			portfolioId,
			metrics.assets,
			PERIOD_TO_DAYS[String(period || '1A').toUpperCase()] || 365
		);
		const portfolioReturn = this.#seriesReturnPct(portfolioSeries.map((point) => ({ ...point, value: numeric(point.value, 0) })));
		const fromDate = portfolioSeries[0]?.date || addDays(nowIso().slice(0, 10), -365);
		const toDate = portfolioSeries[portfolioSeries.length - 1]?.date || nowIso().slice(0, 10);

		const selected = String(benchmark || 'IBOV').toUpperCase();
		const symbols = [selected, 'CDI', 'IPCA', 'IBOV', 'SNP500', 'IFIX', 'POUPANCA'];
		const seen = new Set();
		const benchmarkResults = [];

		for (const key of symbols) {
			const normalizedKey = String(key).toUpperCase();
			if (seen.has(normalizedKey)) continue;
			seen.add(normalizedKey);

			if (normalizedKey in INDICATOR_SERIES) {
				const value = await this.#computeIndicatorReturn(INDICATOR_SERIES[normalizedKey], fromDate, toDate);
				benchmarkResults.push({ benchmark: normalizedKey, return_pct: value });
				continue;
			}

			const symbol = BENCHMARK_SYMBOLS[normalizedKey] || normalizedKey;
			const rows = await this.#fetchBenchmarkHistory(symbol, fromDate);
			const returnPct = this.#seriesReturnPct(rows.map((row) => ({ date: row.date, value: numeric(row.close, 0) })));
			benchmarkResults.push({ benchmark: normalizedKey, symbol, return_pct: returnPct });
		}

		const selectedBenchmark = benchmarkResults.find((item) => item.benchmark === selected || item.symbol === selected) || null;
		const alpha = selectedBenchmark ? portfolioReturn - numeric(selectedBenchmark.return_pct, 0) : null;

		const normalizedSeries = {
			portfolio: this.#normalizeSeries(portfolioSeries),
			benchmarks: {},
		};
		for (const item of benchmarkResults) {
			if (!item.symbol) continue;
			const rows = await this.#fetchBenchmarkHistory(item.symbol, fromDate);
			normalizedSeries.benchmarks[item.benchmark] = this.#normalizeSeries(rows.map((row) => ({ date: row.date, value: numeric(row.close, 0) })));
		}

		return {
			portfolioId,
			period,
			from: fromDate,
			to: toDate,
			portfolio_return_pct: portfolioReturn,
			benchmarks: benchmarkResults,
			selected_benchmark: selectedBenchmark,
			alpha,
			normalized_series: normalizedSeries,
			fetched_at: nowIso(),
		};
	}

	async getMultiCurrencyAnalytics(userId, period = '1Y', options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const activeAssets = assets.filter((asset) =>
			String(asset.status || 'active').toLowerCase() === 'active'
		);
		const latestFxMap = await this.#getLatestFxMap();
		const periodKey = String(period || '1Y').toUpperCase();
		const periodDays = Object.prototype.hasOwnProperty.call(PERIOD_TO_DAYS, periodKey)
			? PERIOD_TO_DAYS[periodKey]
			: PERIOD_TO_DAYS['1Y'];
		const today = nowIso().slice(0, 10);

		const normalizedAssets = activeAssets
			.map((asset) => {
				const ticker = String(asset.ticker || '').toUpperCase();
				const currency = String(asset.currency || 'BRL').toUpperCase();
				const quantity = toNumberOrNull(asset.quantity) ?? 0;
				const snapshotCurrentPrice = toNumberOrNull(asset.currentPrice);
				const fallbackPrice = snapshotCurrentPrice;
				const snapshotCurrentValue = toNumberOrNull(asset.currentValue);
				const derivedMarketValue =
					(fallbackPrice !== null && Number.isFinite(quantity))
						? fallbackPrice * quantity
						: null;
				const marketValueNative =
					snapshotCurrentValue ??
					derivedMarketValue ??
					0;
				const hasExposure =
					Math.abs(quantity) > Number.EPSILON
					|| Math.abs(marketValueNative) > Number.EPSILON;
				if (!hasExposure) return null;

				return {
					assetId: asset.assetId,
					ticker,
					name: String(asset.name || '').trim() || ticker,
					assetClass: String(asset.assetClass || 'unknown').toLowerCase(),
					currency,
					quantity,
					marketValueNative,
					fallbackPrice,
				};
			})
			.filter(Boolean);

		if (normalizedAssets.length === 0) {
			return {
				portfolioId,
				period: periodKey,
				from: today,
				to: today,
				portfolio: {
					start_value_brl: 0,
					end_value_brl: 0,
					start_value_original_brl: 0,
					end_value_original_brl: 0,
					return_brl_pct: 0,
					return_original_pct: 0,
					fx_impact_pct: 0,
					fx_impact_brl: 0,
					foreign_exposure_pct: 0,
				},
				evolution: [{ date: today, value_brl: 0, value_original_brl: 0, fx_impact_brl: 0 }],
				by_currency: [],
				by_asset: [],
				fx_rates: {
					latest: latestFxMap,
					start: { BRL: 1 },
					end: { BRL: 1 },
				},
				fetched_at: nowIso(),
			};
		}

		const priceRowsByAssetId = new Map(
			await Promise.all(
				normalizedAssets.map(async (asset) => [
					asset.assetId,
					await this.#listAssetPriceRows(portfolioId, asset.assetId),
				])
			)
		);

		const allDates = new Set();
		for (const rows of priceRowsByAssetId.values()) {
			for (const row of rows) {
				if (row?.date) allDates.add(row.date);
			}
		}
		let dates = Array.from(allDates).sort();
		if (Number.isFinite(periodDays) && periodDays !== null && periodDays > 0 && dates.length > 0) {
			const latestDate = dates[dates.length - 1];
			const cutoffDate = addDays(latestDate, -(periodDays - 1));
			dates = dates.filter((date) => date >= cutoffDate);
		}
		if (dates.length === 0) dates = [today];
		if (dates[dates.length - 1] !== today) dates.push(today);
		const fromDate = dates[0];
		const toDate = dates[dates.length - 1];

		const currencies = Array.from(
			new Set(normalizedAssets.map((asset) => String(asset.currency || 'BRL').toUpperCase()))
		).sort();
		const fxRowsByCurrency = new Map(
			await Promise.all(
				currencies.map(async (currency) => [
					currency,
					await this.#listFxHistory(currency),
				])
			)
		);

		const startFxByCurrency = {};
		const endFxByCurrency = {};
		const fxTrackByCurrency = new Map();
		for (const currency of currencies) {
			const fallback =
				currency === 'BRL'
					? 1
					: numeric(latestFxMap[`${currency}/BRL`], 0) || 1;
			const rows = fxRowsByCurrency.get(currency) || [];
			const startRate = this.#resolveFxRateAtDate(rows, fromDate, fallback, currency);
			const endRate = this.#resolveFxRateAtDate(rows, toDate, fallback, currency);
			startFxByCurrency[currency] = startRate;
			endFxByCurrency[currency] = endRate;
			fxTrackByCurrency.set(currency, {
				rows,
				index: 0,
				lastRate: startRate,
				fallback,
			});
		}

		const priceTrackByAssetId = new Map();
		for (const asset of normalizedAssets) {
			priceTrackByAssetId.set(asset.assetId, {
				rows: priceRowsByAssetId.get(asset.assetId) || [],
				index: 0,
				lastClose: null,
			});
		}

		const byAssetState = new Map(
			normalizedAssets.map((asset) => [
				asset.assetId,
				{
					assetId: asset.assetId,
					ticker: asset.ticker,
					name: asset.name,
					asset_class: asset.assetClass,
					currency: asset.currency,
					quantity: asset.quantity,
					fx_start: startFxByCurrency[asset.currency] || 1,
					fx_current: endFxByCurrency[asset.currency] || 1,
					start_value_native: null,
					end_value_native: null,
					start_value_brl: null,
					end_value_brl: null,
					start_value_original_brl: null,
					end_value_original_brl: null,
				},
			])
		);

		const evolution = [];
		for (const [dateIndex, date] of dates.entries()) {
			for (const tracker of fxTrackByCurrency.values()) {
				while (tracker.index < tracker.rows.length && tracker.rows[tracker.index].date <= date) {
					const candidate = numeric(tracker.rows[tracker.index].rate, 0);
					if (candidate > 0) tracker.lastRate = candidate;
					tracker.index += 1;
				}
			}

			let valueBrl = 0;
			let valueOriginalBrl = 0;
			for (const asset of normalizedAssets) {
				const tracker = priceTrackByAssetId.get(asset.assetId);
				while (tracker.index < tracker.rows.length && tracker.rows[tracker.index].date <= date) {
					const close = toNumberOrNull(tracker.rows[tracker.index].close);
					if (close !== null && Math.abs(close) > Number.EPSILON) {
						tracker.lastClose = close;
					}
					tracker.index += 1;
				}

				const quantity = Number.isFinite(asset.quantity) ? asset.quantity : 0;
				const nativeValue = (
					Math.abs(quantity) > Number.EPSILON
					&& tracker.lastClose !== null
					&& Math.abs(tracker.lastClose) > Number.EPSILON
				)
					? quantity * tracker.lastClose
					: asset.marketValueNative;
				const currency = asset.currency;
				const fxStart = startFxByCurrency[currency] || 1;
				const fxTracker = fxTrackByCurrency.get(currency);
				const fxCurrent = currency === 'BRL'
					? 1
					: (fxTracker?.lastRate && fxTracker.lastRate > 0
						? fxTracker.lastRate
						: (fxTracker?.fallback || 1));
				const assetValueBrl = nativeValue * fxCurrent;
				const assetValueOriginalBrl = nativeValue * fxStart;

				valueBrl += assetValueBrl;
				valueOriginalBrl += assetValueOriginalBrl;

				const snapshot = byAssetState.get(asset.assetId);
				if (snapshot) {
					if (dateIndex === 0) {
						snapshot.start_value_native = nativeValue;
						snapshot.start_value_brl = assetValueBrl;
						snapshot.start_value_original_brl = assetValueOriginalBrl;
					}
					snapshot.end_value_native = nativeValue;
					snapshot.end_value_brl = assetValueBrl;
					snapshot.end_value_original_brl = assetValueOriginalBrl;
					snapshot.fx_current = fxCurrent;
				}
			}

			evolution.push({
				date,
				value_brl: valueBrl,
				value_original_brl: valueOriginalBrl,
				fx_impact_brl: valueBrl - valueOriginalBrl,
			});
		}

		const portfolioSeriesBrl = evolution.map((row) => ({ date: row.date, value: numeric(row.value_brl, 0) }));
		const portfolioSeriesOriginal = evolution.map((row) => ({ date: row.date, value: numeric(row.value_original_brl, 0) }));
		const portfolioReturnBrl = this.#seriesReturnPct(portfolioSeriesBrl);
		const portfolioReturnOriginal = this.#seriesReturnPct(portfolioSeriesOriginal);
		const portfolioStartBrl = numeric(evolution[0]?.value_brl, 0);
		const portfolioEndBrl = numeric(evolution[evolution.length - 1]?.value_brl, 0);
		const portfolioStartOriginal = numeric(evolution[0]?.value_original_brl, 0);
		const portfolioEndOriginal = numeric(evolution[evolution.length - 1]?.value_original_brl, 0);
		const portfolioFxImpactBrl = portfolioEndBrl - portfolioEndOriginal;
		const foreignEndValue = normalizedAssets.reduce((sum, asset) => {
			if (asset.currency === 'BRL') return sum;
			const snapshot = byAssetState.get(asset.assetId);
			return sum + numeric(snapshot?.end_value_brl, 0);
		}, 0);
		const foreignExposurePct = portfolioEndBrl > 0
			? (foreignEndValue / portfolioEndBrl) * 100
			: 0;

		const byAsset = Array.from(byAssetState.values())
			.map((asset) => {
				const startValueBrl = numeric(asset.start_value_brl, 0);
				const endValueBrl = numeric(asset.end_value_brl, 0);
				const startOriginal = numeric(asset.start_value_original_brl, 0);
				const endOriginal = numeric(asset.end_value_original_brl, 0);
				return {
					...asset,
					start_value_native: numeric(asset.start_value_native, 0),
					end_value_native: numeric(asset.end_value_native, 0),
					start_value_brl: startValueBrl,
					end_value_brl: endValueBrl,
					start_value_original_brl: startOriginal,
					end_value_original_brl: endOriginal,
					return_brl_pct: startValueBrl > 0 ? ((endValueBrl / startValueBrl) - 1) * 100 : 0,
					return_original_pct: startOriginal > 0 ? ((endOriginal / startOriginal) - 1) * 100 : 0,
					fx_impact_pct:
						(startValueBrl > 0 && startOriginal > 0)
							? ((((endValueBrl / startValueBrl) - 1) - ((endOriginal / startOriginal) - 1)) * 100)
							: 0,
					fx_impact_brl: endValueBrl - endOriginal,
				};
			})
			.sort((left, right) => Math.abs(right.fx_impact_brl) - Math.abs(left.fx_impact_brl));

		const byCurrencyMap = new Map();
		for (const asset of byAsset) {
			if (!byCurrencyMap.has(asset.currency)) {
				byCurrencyMap.set(asset.currency, {
					currency: asset.currency,
					start_value_brl: 0,
					end_value_brl: 0,
					start_value_original_brl: 0,
					end_value_original_brl: 0,
					fx_start: startFxByCurrency[asset.currency] || 1,
					fx_current: endFxByCurrency[asset.currency] || 1,
				});
			}
			const row = byCurrencyMap.get(asset.currency);
			row.start_value_brl += numeric(asset.start_value_brl, 0);
			row.end_value_brl += numeric(asset.end_value_brl, 0);
			row.start_value_original_brl += numeric(asset.start_value_original_brl, 0);
			row.end_value_original_brl += numeric(asset.end_value_original_brl, 0);
		}
		const byCurrency = Array.from(byCurrencyMap.values())
			.map((row) => {
				const startBrl = numeric(row.start_value_brl, 0);
				const endBrl = numeric(row.end_value_brl, 0);
				const startOriginal = numeric(row.start_value_original_brl, 0);
				const endOriginal = numeric(row.end_value_original_brl, 0);
				const returnBrlPct = startBrl > 0 ? ((endBrl / startBrl) - 1) * 100 : 0;
				const returnOriginalPct = startOriginal > 0 ? ((endOriginal / startOriginal) - 1) * 100 : 0;
				return {
					...row,
					weight_pct: portfolioEndBrl > 0 ? (endBrl / portfolioEndBrl) * 100 : 0,
					return_brl_pct: returnBrlPct,
					return_original_pct: returnOriginalPct,
					fx_impact_pct: returnBrlPct - returnOriginalPct,
					fx_impact_brl: endBrl - endOriginal,
				};
			})
			.sort((left, right) => right.end_value_brl - left.end_value_brl);

		return {
			portfolioId,
			period: periodKey,
			from: fromDate,
			to: toDate,
			portfolio: {
				start_value_brl: portfolioStartBrl,
				end_value_brl: portfolioEndBrl,
				start_value_original_brl: portfolioStartOriginal,
				end_value_original_brl: portfolioEndOriginal,
				return_brl_pct: portfolioReturnBrl,
				return_original_pct: portfolioReturnOriginal,
				fx_impact_pct: portfolioReturnBrl - portfolioReturnOriginal,
				fx_impact_brl: portfolioFxImpactBrl,
				foreign_exposure_pct: foreignExposurePct,
			},
			evolution,
			by_currency: byCurrency,
			by_asset: byAsset,
			fx_rates: {
				latest: latestFxMap,
				start: startFxByCurrency,
				end: endFxByCurrency,
			},
			fetched_at: nowIso(),
		};
	}

	async getCostAnalysis(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const transactions = await this.#listPortfolioTransactions(portfolioId);
		const byBroker = {};
		let totalFees = 0;
		let operationCount = 0;

		for (const tx of transactions) {
			const fees = numeric(tx.fees, 0) + numeric(tx.b3Fees, 0) + numeric(tx.spreadFx, 0) + numeric(tx.iof, 0);
			totalFees += fees;
			if (fees > 0) operationCount += 1;
			const broker = String(tx.institution || 'unknown');
			if (!byBroker[broker]) byBroker[broker] = { broker, total_fees: 0, operations: 0, avg_fee: 0 };
			byBroker[broker].total_fees += fees;
			byBroker[broker].operations += 1;
		}

		for (const broker of Object.values(byBroker)) {
			broker.avg_fee = broker.operations > 0 ? broker.total_fees / broker.operations : 0;
		}

		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, { portfolioId });
		const grossReturn = numeric(metrics.consolidated.percent_return, 0);
		const costImpactPct = numeric(metrics.consolidated.total_market_value, 0) > 0
			? (totalFees / numeric(metrics.consolidated.total_market_value, 1)) * 100
			: 0;

		return {
			portfolioId,
			total_fees: totalFees,
			operation_count_with_fees: operationCount,
			by_broker: Object.values(byBroker).sort((left, right) => right.total_fees - left.total_fees),
			gross_return_pct: grossReturn,
			net_return_pct_after_costs: grossReturn - costImpactPct,
			cost_impact_pct: costImpactPct,
			fetched_at: nowIso(),
		};
	}

	async calculatePrivateFixedIncomePosition(payload = {}) {
		const principal = numeric(payload.principal, 0);
		const cdiPct = numeric(payload.cdiPct, 100) / 100;
		const startDate = normalizeDate(payload.startDate);
		const endDate = normalizeDate(payload.endDate) || nowIso().slice(0, 10);
		if (!startDate || principal <= 0) {
			throw new Error('principal and startDate are required');
		}
		const cdiAccum = await this.#computeIndicatorAccumulation(INDICATOR_SERIES.CDI, startDate, endDate);
		const grossFactor = 1 + (cdiAccum * cdiPct);
		const currentValue = principal * grossFactor;
		return {
			principal,
			cdi_pct: cdiPct * 100,
			start_date: startDate,
			end_date: endDate,
			cdi_accumulated_pct: cdiAccum * 100,
			current_value: currentValue,
			currency: payload.currency || 'BRL',
			fetched_at: nowIso(),
		};
	}

	async getFixedIncomeComparison(userId, options = {}) {
		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, { portfolioId });
		const fixedIncome = metrics.assets.filter((asset) => String(asset.market || '').toUpperCase() === 'TESOURO' || String(asset.ticker || '').toUpperCase().startsWith('TESOURO'));
		const fromDate = options.fromDate || addDays(nowIso().slice(0, 10), -365);
		const toDate = options.toDate || nowIso().slice(0, 10);

		const cdiReturn = await this.#computeIndicatorReturn(INDICATOR_SERIES.CDI, fromDate, toDate);
		const ipcaReturn = await this.#computeIndicatorReturn(INDICATOR_SERIES.IPCA, fromDate, toDate);
		const poupancaReturn = await this.#computeIndicatorReturn(INDICATOR_SERIES.POUPANCA, fromDate, toDate);

		return {
			portfolioId,
			period: { from: fromDate, to: toDate },
			tesouro_assets: fixedIncome,
			benchmarks: {
				cdi_return_pct: cdiReturn,
				ipca_return_pct: ipcaReturn,
				poupanca_return_pct: poupancaReturn,
			},
			fetched_at: nowIso(),
		};
	}

	async simulate(monthlyAmount, rate, years, options = {}) {
		const monthlyContribution = numeric(monthlyAmount, 0);
		const annualRate = numeric(rate, 0) / 100;
		const totalYears = numeric(years, 0);
		if (monthlyContribution <= 0 || totalYears <= 0) {
			throw new Error('monthlyAmount and years must be greater than zero');
		}

		const months = Math.round(totalYears * 12);
		const buildScenario = (annualRatePct) => {
			const monthlyRate = annualRatePct / 12;
			let balance = 0;
			const series = [];
			for (let month = 1; month <= months; month += 1) {
				balance = balance * (1 + monthlyRate) + monthlyContribution;
				series.push({
					month,
					value: balance,
				});
			}
			return {
				annual_rate_pct: annualRatePct * 100,
				final_value: balance,
				series,
			};
		};

		const base = buildScenario(annualRate);
		const optimistic = buildScenario(annualRate + 0.02);
		const pessimistic = buildScenario(Math.max(annualRate - 0.02, 0));

		let backtest = null;
		if (options.ticker && options.initialAmount) {
			const context = await this.#resolveAssetContext(options.ticker, options.userId, options.portfolioId);
			const rows = await this.#listAssetPriceRows(context.portfolioId, context.asset.assetId);
			const fromDate = addDays(nowIso().slice(0, 10), -Math.round(totalYears * 365));
			const relevant = rows.filter((row) => row.date >= fromDate);
			if (relevant.length > 0) {
				const start = numeric(relevant[0].close, 0);
				const end = numeric(relevant[relevant.length - 1].close, 0);
				const shares = start > 0 ? numeric(options.initialAmount, 0) / start : 0;
				backtest = {
					ticker: options.ticker,
					from: relevant[0].date,
					to: relevant[relevant.length - 1].date,
					initial_amount: numeric(options.initialAmount, 0),
					final_value: shares * end,
				};
			}
		}

		return {
			inputs: {
				monthly_amount: monthlyContribution,
				rate_pct: annualRate * 100,
				years: totalYears,
			},
			scenarios: { optimistic, base, pessimistic },
			backtest,
			fetched_at: nowIso(),
		};
	}

	async generatePDF(userId, reportType, period, options = {}) {
		const normalizedType = String(reportType || 'portfolio').toLowerCase();
		const reportId = `report-${hashId(`${userId}:${normalizedType}:${period || 'current'}:${nowIso()}`)}`;
		let payload;

		if (normalizedType === 'tax') {
			const year = Number(period) || new Date().getUTCFullYear();
			payload = await this.getTaxReport(userId, year, options);
		} else if (normalizedType === 'dividends') {
			payload = await this.getDividendAnalytics(userId, options);
		} else if (normalizedType === 'performance') {
			payload = await this.getBenchmarkComparison(userId, 'IBOV', period || '1A', options);
		} else {
			payload = await this.getDashboard(userId, options);
		}

		const lines = [
			`WealthHub Report`,
			`Type: ${normalizedType}`,
			`User: ${userId}`,
			`Generated at: ${nowIso()}`,
			`Period: ${period || 'current'}`,
			'',
			JSON.stringify(payload, null, 2).slice(0, 3500),
		];
		const pdfBuffer = createSimplePdfBuffer(lines);
		const yearFolder = String(new Date().getUTCFullYear());

		let storage;
		if (this.useS3 && this.s3) {
			const key = `reports/pdf/${userId}/${yearFolder}/${normalizedType}/${reportId}.pdf`;
			await this.s3.send(
				new PutObjectCommand({
					Bucket: this.s3Bucket,
					Key: key,
					Body: pdfBuffer,
					ContentType: 'application/pdf',
				})
			);
			storage = {
				type: 's3',
				bucket: this.s3Bucket,
				key,
				uri: `s3://${this.s3Bucket}/${key}`,
			};
		} else {
			const dir = path.join(this.reportsLocalDir, userId, yearFolder, normalizedType);
			fs.mkdirSync(dir, { recursive: true });
			const filePath = path.join(dir, `${reportId}.pdf`);
			fs.writeFileSync(filePath, pdfBuffer);
			storage = {
				type: 'local',
				path: filePath,
			};
		}

		const item = {
			PK: `USER#${userId}`,
			SK: `REPORT#${reportId}`,
			entityType: 'REPORT_PDF',
			reportId,
			reportType: normalizedType,
			period: period || null,
			storage,
			data_source: 'internal_calc',
			fetched_at: nowIso(),
			is_scraped: false,
			createdAt: nowIso(),
			updatedAt: nowIso(),
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));

		return item;
	}

	async listReports(userId) {
		return this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'REPORT#',
			},
		});
	}

	async publishIdea(userId, payload = {}) {
		const ideaId = payload.ideaId || `idea-${hashId(`${nowIso()}:${Math.random()}`)}`;
		const item = {
			PK: 'COMMUNITY#IDEA',
			SK: `DATE#${nowIso()}#${ideaId}`,
			entityType: 'COMMUNITY_IDEA',
			ideaId,
			userId,
			title: payload.title || 'Untitled idea',
			content: payload.content || '',
			tags: Array.isArray(payload.tags) ? payload.tags : [],
			createdAt: nowIso(),
			updatedAt: nowIso(),
			likes: 0,
			data_source: 'user_input',
			fetched_at: nowIso(),
			is_scraped: false,
		};
		await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
		return item;
	}

	async listIdeas(options = {}) {
		const items = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': 'COMMUNITY#IDEA',
				':sk': 'DATE#',
			},
		});
		const limit = options.limit ? Number(options.limit) : 100;
		return items
			.sort((left, right) => String(right.createdAt || '').localeCompare(String(left.createdAt || '')))
			.slice(0, limit);
	}

	async getLeagueRanking(options = {}) {
		const portfolios = await this.#scanAll({
			TableName: this.tableName,
			FilterExpression: 'begins_with(SK, :portfolioPrefix)',
			ExpressionAttributeValues: {
				':portfolioPrefix': 'PORTFOLIO#',
			},
		});
		const ranking = [];
		for (const portfolio of portfolios) {
			try {
				const userId = String(portfolio.PK || '').replace('USER#', '') || options.userId || 'anonymous';
				const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, {
					portfolioId: portfolio.portfolioId,
				});
				ranking.push({
					portfolioId: portfolio.portfolioId,
					name: portfolio.name,
					return_pct: numeric(metrics.consolidated.percent_return, 0),
					total_value: numeric(metrics.consolidated.total_market_value, 0),
				});
			} catch {
				// Ignore inaccessible portfolios in ranking.
			}
		}
		return ranking.sort((left, right) => right.return_pct - left.return_pct);
	}

	async #fetchSgsSeries(seriesId, startDate = null) {
		const fallbackStartDate = addDays(nowIso().slice(0, 10), -3650);
		const effectiveStartDate = startDate || fallbackStartDate;
		const endpoints = [];

		const rangedUrl = new URL(`https://api.bcb.gov.br/dados/serie/bcdata.sgs.${seriesId}/dados`);
		rangedUrl.searchParams.set('formato', 'json');
		const brDate = toBrDate(effectiveStartDate);
		if (brDate) rangedUrl.searchParams.set('dataInicial', brDate);
		endpoints.push(rangedUrl.toString());

		const latestUrl = new URL(`https://api.bcb.gov.br/dados/serie/bcdata.sgs.${seriesId}/dados/ultimos/180`);
		latestUrl.searchParams.set('formato', 'json');
		endpoints.push(latestUrl.toString());

		let lastError = null;
		for (const endpoint of endpoints) {
			try {
				const response = await withRetry(
					() => fetchWithTimeout(endpoint, { timeoutMs: 20000 }),
					{ retries: 2, baseDelayMs: 500, factor: 2 }
				);
				if (!response.ok) {
					const body = await response.text().catch(() => '');
					lastError = new Error(
						`BCB SGS series ${seriesId} responded with ${response.status}${
							body ? ` (${String(body).slice(0, 200)})` : ''
						}`
					);
					continue;
				}
				const rows = await response.json();
				if (!Array.isArray(rows)) return [];

				return rows
					.map((row) => ({
						date: normalizeDate(row.data),
						value: numeric(String(row.valor || '').replace(',', '.'), null),
					}))
					.filter((row) => row.date && row.value !== null)
					.sort((left, right) => left.date.localeCompare(right.date));
			} catch (error) {
				lastError = error;
			}
		}

		throw lastError || new Error(`Unable to fetch BCB SGS series ${seriesId}`);
	}

	async #refreshFxRates() {
		const rates = [];
		for (const currency of ['USD', 'CAD']) {
			const latest = await this.#fetchPtaxRate(currency);
			if (!latest) continue;
			const item = {
				PK: `FX#${currency}#BRL`,
				SK: `RATE#${latest.date}`,
				entityType: 'FX_RATE',
				base: currency,
				quote: 'BRL',
				date: latest.date,
				rate: latest.rate,
				data_source: latest.source,
				fetched_at: nowIso(),
				is_scraped: false,
				updatedAt: nowIso(),
			};
			await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
			rates.push(item);
		}
		return rates;
	}

	async #fetchPtaxRate(currency) {
		const today = new Date();
		const start = new Date(today.getTime() - 8 * 86400000);
		const url =
			`https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo` +
			`(moeda='${currency}',dataInicial='${formatMonthDayYear(start)}',dataFinalCotacao='${formatMonthDayYear(today)}')` +
			`?$top=1&$orderby=dataHoraCotacao%20desc&$format=json`;

		try {
			const response = await withRetry(
				() => fetchWithTimeout(url, { timeoutMs: 20000 }),
				{ retries: 2, baseDelayMs: 500, factor: 2 }
			);
			if (response.ok) {
				const json = await response.json();
				const item = Array.isArray(json.value) && json.value.length ? json.value[0] : null;
				if (item && item.cotacaoVenda) {
					return {
						date: normalizeDate(item.dataHoraCotacao),
						rate: numeric(item.cotacaoVenda, 0),
						source: 'bcb_ptax',
					};
				}
			}
		} catch {
			// fallback below
		}

		if (currency === 'USD') {
			const fallbackRows = await this.#fetchSgsSeries(INDICATOR_SERIES.USD_BRL_ALT);
			const latest = fallbackRows[fallbackRows.length - 1];
			if (latest) {
				return {
					date: latest.date,
					rate: latest.value,
					source: 'bcb_sgs_1',
				};
			}
		}

		return null;
	}

	async #getLatestFxMap() {
		const map = {};
		for (const currency of ['USD', 'CAD']) {
			const result = await this.dynamo.send(
				new QueryCommand({
					TableName: this.tableName,
					KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
					ExpressionAttributeValues: {
						':pk': `FX#${currency}#BRL`,
						':sk': 'RATE#',
					},
					ScanIndexForward: false,
					Limit: 1,
				})
			);
			const latest = Array.isArray(result.Items) && result.Items.length ? result.Items[0] : null;
			if (latest) map[`${currency}/BRL`] = numeric(latest.rate, 0);
		}
		map['BRL/BRL'] = 1;
		return map;
	}

	async #listFxHistory(currency) {
		const normalizedCurrency = String(currency || '').toUpperCase();
		if (!normalizedCurrency || normalizedCurrency === 'BRL') return [];

		const rows = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `FX#${normalizedCurrency}#BRL`,
				':sk': 'RATE#',
			},
		});

		return rows
			.map((row) => ({
				date: normalizeDate(row.date || String(row.SK || '').replace('RATE#', '')),
				rate: numeric(row.rate, 0),
			}))
			.filter((row) => row.date && row.rate > 0)
			.sort((left, right) => left.date.localeCompare(right.date));
	}

	#resolveFxRateAtDate(rows, date, fallbackRate = 1, currency = 'BRL') {
		const normalizedCurrency = String(currency || 'BRL').toUpperCase();
		if (normalizedCurrency === 'BRL') return 1;
		const normalizedDate = normalizeDate(date);
		const fallback = fallbackRate > 0 ? fallbackRate : 1;
		if (!normalizedDate || !Array.isArray(rows) || rows.length === 0) return fallback;

		for (let index = rows.length - 1; index >= 0; index -= 1) {
			const row = rows[index];
			if (String(row.date || '') > normalizedDate) continue;
			const rate = numeric(row.rate, 0);
			if (rate > 0) return rate;
		}
		return fallback;
	}

	#buildStatusInvestAssetUrl(ticker, assetClass = 'fii') {
		const rawTicker = String(ticker || '').toLowerCase().replace(/\.sa$/i, '');
		const slug = rawTicker.replace(/[^a-z0-9]/g, '');
		if (!slug) return null;
		const category = String(assetClass || '').toLowerCase() === 'fii' ? 'fundos-imobiliarios' : 'acoes';
		return `https://statusinvest.com.br/${category}/${slug}`;
	}

	async #fetchStatusInvestDividendEvents(ticker, assetClass = 'fii') {
		const sourceUrl = this.#buildStatusInvestAssetUrl(ticker, assetClass);
		if (!sourceUrl) return [];
		const timeoutMs = Number(process.env.MARKET_DATA_STATUSINVEST_TIMEOUT_MS || 9000);
		let response;
		try {
			response = await withRetry(
				() =>
					fetchWithTimeout(sourceUrl, {
						timeoutMs,
						headers: { Accept: 'text/html,*/*' },
					}),
				{ retries: 0, baseDelayMs: 400, factor: 2 }
			);
		} catch {
			return [];
		}
		if (!response?.ok) return [];

		const html = await response.text();
		const parsed = this.#extractStatusInvestDividendRows(html);
		const normalizedTicker = String(ticker || '').toUpperCase();
		return parsed.map((row) => ({
			eventId: hashId(`${normalizedTicker}:statusinvest:${row.eventDate}:${row.type || ''}:${row.value ?? ''}`),
			title: `${row.type || 'Dividend'} - ${normalizedTicker}`,
			eventType: row.eventType,
			date: row.eventDate,
			details: {
				ticker: normalizedTicker,
				exDate: row.exDate || null,
				recordDate: row.recordDate || null,
				announcementDate: row.announcementDate || null,
				paymentDate: row.paymentDate || row.eventDate,
				value: row.value ?? null,
				valueText: row.valueText || null,
				rawType: row.type || null,
				url: sourceUrl,
			},
			data_source: 'statusinvest_proventos',
			is_scraped: true,
		}));
	}

	async #fetchStatusInvestFilingDocuments(ticker, assetClass = 'fii') {
		const sourceUrl = this.#buildStatusInvestAssetUrl(ticker, assetClass);
		if (!sourceUrl) return [];
		const timeoutMs = Number(process.env.MARKET_DATA_STATUSINVEST_TIMEOUT_MS || 9000);

		let response;
		try {
			response = await withRetry(
				() =>
					fetchWithTimeout(sourceUrl, {
						timeoutMs,
						headers: { Accept: 'text/html,*/*' },
					}),
				{ retries: 0, baseDelayMs: 400, factor: 2 }
			);
		} catch {
			return [];
		}
		if (!response?.ok) return [];

		const html = await response.text();
		const htmlRows = this.#extractStatusInvestFilingsFromHtml(html, sourceUrl);
		const jsonRows = this.#extractStatusInvestFilingsFromNextData(html, sourceUrl);
		return mergeFinancialDocuments(htmlRows, jsonRows).slice(0, 200);
	}

	#extractStatusInvestFilingsFromHtml(html, sourceUrl) {
		const content = String(html || '');
		if (!content) return [];

		const rows = content.match(/<tr[\s\S]*?<\/tr>/gi) || [];
		const extracted = [];

		for (const row of rows) {
			const linkMatches = Array.from(
				row.matchAll(/<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)
			);
			if (linkMatches.length === 0) continue;

			const links = linkMatches
				.map((match) => ({
					url: this.#resolveAbsoluteUrl(sourceUrl, match[1]),
					label: this.#htmlToPlainText(match[2] || ''),
				}))
				.filter((entry) => Boolean(entry.url));
			if (links.length === 0) continue;

			const selectedLink = links.find((entry) => looksLikeFinancialDocumentUrl(entry.url)) || links[0];
			if (!selectedLink?.url) continue;

			const cells = [];
			const cellRegex = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
			let cellMatch;
			while ((cellMatch = cellRegex.exec(row)) !== null) {
				const text = this.#htmlToPlainText(cellMatch[1] || '');
				if (text) cells.push(text);
			}

			const rowText = this.#htmlToPlainText(row);
			const rowTextLower = rowText.toLowerCase();
			const hasFilingKeyword = FINANCIAL_DOCUMENT_TITLE_KEYWORDS.some((keyword) =>
				rowTextLower.includes(keyword)
			);
			if (!hasFilingKeyword && !looksLikeFinancialDocumentUrl(selectedLink.url)) continue;

			const dateMatches = rowText.match(/\b\d{2}\/\d{2}\/\d{4}\b/g) || [];
			const firstDate = normalizeDate(dateMatches[0] || null);
			const secondDate = normalizeDate(dateMatches[1] || null);

			const nonDateCells = cells.filter((cell) => !/\b\d{2}\/\d{2}\/\d{4}\b/.test(cell));
			const title =
				selectedLink.label ||
				nonDateCells.find((cell) => cell.length >= 3) ||
				'Comunicado';
			const categoryCandidate = nonDateCells.find(
				(cell) => cell !== title && FINANCIAL_DOCUMENT_TITLE_KEYWORDS.some((keyword) => cell.toLowerCase().includes(keyword))
			);
			const category = categoryCandidate || null;
			const documentType = inferFinancialDocumentType(title, category);

			extracted.push({
				id: null,
				source: 'statusinvest_communications',
				title,
				category,
				document_type: documentType,
				reference_date: secondDate || firstDate || null,
				delivery_date: firstDate || secondDate || null,
				status: null,
				url: selectedLink.url,
				url_viewer: null,
				url_download: selectedLink.url,
				url_alternatives: links
					.map((entry) => entry.url)
					.filter((url) => url && url !== selectedLink.url),
			});
		}

		return extracted;
	}

	#extractStatusInvestFilingsFromNextData(html, sourceUrl) {
		const content = String(html || '');
		if (!content) return [];

		const script = extractJsonScriptContent(content, '__NEXT_DATA__');
		if (!script) return [];

		let payload;
		try {
			payload = JSON.parse(script);
		} catch {
			return [];
		}

		const rows = [];
		const visited = new Set();
		const dedupe = new Set();

		const pushCandidate = (entry, path) => {
			if (!isObjectRecord(entry)) return;
			const urls = [];
			for (const [rawKey, rawValue] of Object.entries(entry)) {
				const normalizedKey = normalizeRecordKey(rawKey);
				if (!normalizedKey) continue;
				if (!FINANCIAL_DOCUMENT_LINK_KEY_HINTS.some((hint) => normalizedKey.includes(hint))) continue;
				const normalizedUrl = this.#resolveAbsoluteUrl(sourceUrl, rawValue);
				if (!normalizedUrl) continue;
				urls.push(normalizedUrl);
			}
			if (urls.length === 0) return;

			const titleRaw =
				findRecordValueByHints(entry, FINANCIAL_DOCUMENT_TITLE_KEY_HINTS) ||
				findRecordValueByHints(entry, ['description', 'descricao', 'subject']) ||
				null;
			const categoryRaw =
				findRecordValueByHints(entry, ['category', 'categoria', 'classe']) ||
				null;
			const keysText = Object.keys(entry).map((key) => normalizeRecordKey(key)).join(' ');
			const pathText = path.map((segment) => normalizeRecordKey(segment)).join(' ');
			const contextText = normalizeRecordKey(
				`${keysText} ${pathText} ${String(titleRaw || '')} ${String(categoryRaw || '')}`
			);
			const hasFilingKeyword = FINANCIAL_DOCUMENT_TITLE_KEYWORDS.some((keyword) =>
				contextText.includes(normalizeRecordKey(keyword))
			);

			const primaryUrl = urls.find((url) => looksLikeFinancialDocumentUrl(url)) || urls[0];
			if (!primaryUrl) return;
			if (!hasFilingKeyword && !looksLikeFinancialDocumentUrl(primaryUrl)) return;

			const title = String(titleRaw || '').trim() || 'Comunicado';
			const category = categoryRaw ? String(categoryRaw).trim() : null;
			const documentTypeRaw =
				findRecordValueByHints(entry, FINANCIAL_DOCUMENT_TYPE_KEY_HINTS) ||
				inferFinancialDocumentType(title, category);
			const documentType = documentTypeRaw ? String(documentTypeRaw).trim() : null;
			const statusRaw = findRecordValueByHints(entry, FINANCIAL_DOCUMENT_STATUS_KEY_HINTS);
			const status = statusRaw ? String(statusRaw).trim() : null;
			const idRaw = findRecordValueByHints(entry, FINANCIAL_DOCUMENT_ID_KEY_HINTS);
			const id = idRaw ? String(idRaw).trim() : null;
			const referenceDate = normalizeDate(
				findRecordValueByHints(entry, FINANCIAL_DOCUMENT_REFERENCE_DATE_KEY_HINTS)
			);
			const deliveryDate = normalizeDate(
				findRecordValueByHints(entry, FINANCIAL_DOCUMENT_DELIVERY_DATE_KEY_HINTS)
			);

			const key = `${primaryUrl}|${referenceDate || ''}|${deliveryDate || ''}|${title}`;
			if (dedupe.has(key)) return;
			dedupe.add(key);

			rows.push({
				id,
				source: 'statusinvest_communications',
				title,
				category,
				document_type: documentType,
				reference_date: referenceDate || null,
				delivery_date: deliveryDate || null,
				status,
				url: primaryUrl,
				url_viewer: primaryUrl,
				url_download: urls.find((url) => url !== primaryUrl) || null,
				url_alternatives: urls.filter((url) => url !== primaryUrl),
			});
		};

		const walk = (node, path = []) => {
			if (node === null || node === undefined) return;
			if (Array.isArray(node)) {
				for (let index = 0; index < node.length; index += 1) {
					walk(node[index], [...path, String(index)]);
				}
				return;
			}
			if (!isObjectRecord(node)) return;

			if (visited.has(node)) return;
			visited.add(node);

			pushCandidate(node, path);
			for (const [key, value] of Object.entries(node)) {
				if (value && typeof value === 'object') {
					walk(value, [...path, key]);
				}
			}
		};

		walk(payload, []);
		return rows;
	}

	#resolveAbsoluteUrl(baseUrl, value) {
		const text = String(value || '').trim();
		if (!text) return null;
		if (/^https?:\/\//i.test(text)) return text;
		if (text.startsWith('//')) return `https:${text}`;
		if (text.startsWith('/')) {
			try {
				return new URL(text, baseUrl).toString();
			} catch {
				return null;
			}
		}
		if (/^[a-z0-9.-]+\.[a-z]{2,}\/?/i.test(text)) return `https://${text}`;
		return null;
	}

	async #fetchFiisUpdates(ticker) {
		const normalizedTicker = String(ticker || '').toUpperCase().replace(/\.SA$/i, '');
		const slug = normalizedTicker.toLowerCase().replace(/[^a-z0-9]/g, '');
		if (!slug) return [];

		const sourceUrl = `https://fiis.com.br/${slug}/`;
		const timeoutMs = Number(process.env.MARKET_DATA_FIIS_TIMEOUT_MS || 9000);
		let response;
		try {
			response = await withRetry(
				() =>
					fetchWithTimeout(sourceUrl, {
						timeoutMs,
						headers: {
							Accept: 'text/html,*/*',
							'User-Agent': 'Mozilla/5.0 (compatible; WealthBot/1.0)',
						},
					}),
				{ retries: 1, baseDelayMs: 350, factor: 2 }
			);
		} catch {
			return [];
		}
		if (!response?.ok) return [];

		const html = await response.text();
		return this.#extractFiisUpdatesFromHtml(html, sourceUrl, normalizedTicker);
	}

	#extractFiisUpdatesFromHtml(html, sourceUrl, normalizedTicker) {
		const content = String(html || '');
		if (!content) return [];

		const sectionMatch = content.match(
			/<section[^>]*>[\s\S]*?Atualiza(?:ç|c)(?:[oõ]|oe)s?\s+do\s+[A-Z0-9]{4,12}[\s\S]*?<\/section>/i
		);
		const scope = sectionMatch?.[0] || content;
		const anchorPattern = /<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
		const dedupe = new Set();
		const items = [];
		let match = anchorPattern.exec(scope);
		while (match) {
			const href = this.#resolveAbsoluteUrl(sourceUrl, match[1]);
			const title = stripHtmlTags(match[2]);
			if (!href || !title || title.length < 8) {
				match = anchorPattern.exec(scope);
				continue;
			}
			if (!/^https?:\/\//i.test(href) || !href.includes('fiis.com.br')) {
				match = anchorPattern.exec(scope);
				continue;
			}
			if (/atualiza(?:ç|c)(?:[oõ]|oe)s?\s+do/i.test(title)) {
				match = anchorPattern.exec(scope);
				continue;
			}

			const contextStart = Math.max(0, (match.index || 0) - 220);
			const contextEnd = Math.min(scope.length, (match.index || 0) + match[0].length + 220);
			const context = scope.slice(contextStart, contextEnd);
			const deliveryDate = parseFirstDateInText(context);

			const key = `${href}|${title.toLowerCase()}|${deliveryDate || ''}`;
			if (dedupe.has(key)) {
				match = anchorPattern.exec(scope);
				continue;
			}
			dedupe.add(key);

			const categoryMatch = stripHtmlTags(context).match(
				/\b(Fato Relevante|Assembleia|Relat[oó]rio(?:s)?|Informes?|Comunicado(?:s)?|Aviso(?:s)?)\b/i
			);
			const category = categoryMatch ? categoryMatch[1] : null;

			items.push({
				id: hashId(`${normalizedTicker}:fiis:${href}:${deliveryDate || ''}:${title}`),
				category,
				title,
				deliveryDate,
				referenceDate: deliveryDate,
				url: href,
				source: 'fiis',
			});

			match = anchorPattern.exec(scope);
		}

		return items.sort((left, right) => {
			const leftDate = normalizeDate(left.deliveryDate || left.referenceDate || null) || '';
			const rightDate = normalizeDate(right.deliveryDate || right.referenceDate || null) || '';
			return rightDate.localeCompare(leftDate);
		});
	}

	async #fetchFundsExplorerDividendEvents(ticker) {
		const normalizedTicker = String(ticker || '').toUpperCase().replace(/\.SA$/i, '');
		const slug = normalizedTicker.toLowerCase().replace(/[^a-z0-9]/g, '');
		if (!slug) return [];

		const sourceUrl = `https://www.fundsexplorer.com.br/funds/${slug}`;
		const timeoutMs = Number(process.env.MARKET_DATA_FUNDSEXPLORER_TIMEOUT_MS || 9000);
		let response;
		try {
			response = await withRetry(
				() =>
					fetchWithTimeout(sourceUrl, {
						timeoutMs,
						headers: {
							Accept: 'text/html,*/*',
							'User-Agent': 'Mozilla/5.0 (compatible; WealthBot/1.0)',
						},
					}),
				{ retries: 0, baseDelayMs: 400, factor: 2 }
			);
		} catch {
			return [];
		}
		if (!response?.ok) return [];

		const html = await response.text();
		const meta = this.#extractFundsExplorerMeta(html);
		if (!meta) return [];

		const paymentDate = normalizeDate(meta.pr_datapagamento || meta.ur_data_pagamento);
		const baseDate = normalizeDate(meta.pr_database || meta.ur_data_base);
		const value = toNumberOrNull(meta.pr_valor ?? meta.ur_valor);
		if (!paymentDate && !baseDate) return [];

		const events = [];
		const pushEvent = (eventType, eventDate, details = {}) => {
			if (!eventDate) return;
			events.push({
				eventId: hashId(`${normalizedTicker}:fundsexplorer:${eventType}:${eventDate}:${value ?? ''}`),
				title: `${eventType.replace(/_/g, ' ')} - ${normalizedTicker}`,
				eventType,
				date: eventDate,
				details: {
					ticker: normalizedTicker,
					value,
					url: sourceUrl,
					...details,
				},
				data_source: 'fundsexplorer_fii',
				is_scraped: true,
			});
		};

		pushEvent('dividend_payment', paymentDate, {
			exDate: baseDate || null,
			paymentDate: paymentDate || null,
		});
		if (baseDate && baseDate !== paymentDate) {
			pushEvent('dividend_base_date', baseDate, {
				exDate: baseDate,
				paymentDate: paymentDate || null,
			});
		}

		return events.sort((left, right) => String(left.date || '').localeCompare(String(right.date || '')));
	}

	#extractFundsExplorerMeta(html) {
		const content = String(html || '');
		if (!content) return null;
		const match = content.match(/var\s+dataLayer_content\s*=\s*(\{[\s\S]*?\});/);
		if (!match) return null;

		try {
			const parsed = JSON.parse(match[1]);
			return parsed?.pagePostTerms?.meta || null;
		} catch {
			return null;
		}
	}

	#mergeDividendEvents(primaryEvents, secondaryEvents) {
		const primary = Array.isArray(primaryEvents) ? primaryEvents : [];
		const secondary = Array.isArray(secondaryEvents) ? secondaryEvents : [];
		if (primary.length === 0) return secondary;
		if (secondary.length === 0) return primary;

		const merged = primary.map((event) => ({
			...event,
			details: event?.details && typeof event.details === 'object'
				? { ...event.details }
				: event.details,
		}));
		const eventByDate = new Map();
		for (const event of merged) {
			const key = normalizeDate(event?.date || event?.eventDate);
			if (key && !eventByDate.has(key)) eventByDate.set(key, event);
		}

		for (const extra of secondary) {
			const key = normalizeDate(extra?.date || extra?.eventDate);
			if (!key) continue;
			const target = eventByDate.get(key);
			if (!target) {
				merged.push(extra);
				eventByDate.set(key, extra);
				continue;
			}

			const targetDetails =
				target?.details && typeof target.details === 'object'
					? target.details
					: {};
			const extraDetails =
				extra?.details && typeof extra.details === 'object'
					? extra.details
					: {};
			const targetValue = toNumberOrNull(targetDetails.value);
			const extraValue = toNumberOrNull(extraDetails.value);
			if ((targetValue === null || Math.abs(targetValue) === 0) && extraValue !== null && Math.abs(extraValue) > 0) {
				targetDetails.value = extraValue;
				if (extraDetails.valueText && !targetDetails.valueText) {
					targetDetails.valueText = extraDetails.valueText;
				}
				targetDetails.value_source = extra.data_source || target.data_source;
			}
			if (!targetDetails.exDate && extraDetails.exDate) targetDetails.exDate = extraDetails.exDate;
			if (!targetDetails.recordDate && extraDetails.recordDate) targetDetails.recordDate = extraDetails.recordDate;
			if (!targetDetails.announcementDate && extraDetails.announcementDate) {
				targetDetails.announcementDate = extraDetails.announcementDate;
			}
			if (!targetDetails.paymentDate && extraDetails.paymentDate) targetDetails.paymentDate = extraDetails.paymentDate;
			target.details = targetDetails;
		}

		return merged.sort((left, right) => String(left.date || '').localeCompare(String(right.date || '')));
	}

	#extractStatusInvestDividendRows(html) {
		const content = String(html || '');
		if (!content) return [];
		const rows = content.match(/<tr[\s\S]*?<\/tr>/gi) || [];
		const parsed = [];

		for (const row of rows) {
			const cells = [];
			const cellRegex = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
			let match;
			while ((match = cellRegex.exec(row)) !== null) {
				const text = this.#htmlToPlainText(match[1]);
				if (text) cells.push(text);
			}
			if (cells.length < 3) continue;

				const dateMatches = cells
					.map((cell) => cell.match(/\b\d{2}\/\d{2}\/\d{4}\b/)?.[0] || null)
					.filter(Boolean);
				if (dateMatches.length < 1) continue;

				const findDateByKeyword = (keywords) => {
					const loweredKeywords = Array.isArray(keywords) ? keywords : [];
					for (const cell of cells) {
						const lowered = String(cell || '').toLowerCase();
						if (!loweredKeywords.some((keyword) => lowered.includes(keyword))) continue;
						const matched = lowered.match(/\b\d{2}\/\d{2}\/\d{4}\b/)?.[0];
						if (matched) return normalizeDate(matched);
					}
					return null;
				};

				const exDate =
					findDateByKeyword(['ex', 'base'])
					|| normalizeDate(dateMatches[0]);
				const recordDate =
					findDateByKeyword(['record', 'com', 'data com'])
					|| null;
				const announcementDate =
					findDateByKeyword(['anunc', 'declar', 'aprova'])
					|| null;
				const paymentDate =
					findDateByKeyword(['pag', 'payment', 'credito'])
					|| normalizeDate(dateMatches[dateMatches.length - 1])
					|| exDate;
				const eventDate = paymentDate || exDate;
				if (!eventDate) continue;

				const valueText =
					cells.find(
						(cell) =>
						(/[R$]|\d+[.,]\d+/.test(cell))
						&& !/\b\d{2}\/\d{2}\/\d{4}\b/.test(cell)
				) || null;
			const value = valueText
				? toNumberOrNull(
					String(valueText)
						.replace(/[^\d,.-]/g, '')
						.replace(/\./g, '')
						.replace(',', '.')
				)
				: null;
				const type = cells[0] || 'Dividend';
				const normalizedType = String(type)
					.toLowerCase()
					.normalize('NFD')
					.replace(/[\u0300-\u036f]/g, '');
				const eventType =
					normalizedType.includes('jcp') || normalizedType.includes('juros')
						? 'jcp'
						: normalizedType.includes('amort')
							? 'amortization'
							: normalizedType.includes('rend')
								? 'rendimento'
								: normalizedType.includes('subscr') || normalizedType.includes('subscription')
									? 'subscription'
									: 'dividend';

				if (!valueText && !/divid|rend|juro|provent|amort|subscr|subscription|preferenc/i.test(normalizedType)) continue;

				parsed.push({
					type,
					eventType,
					exDate,
					recordDate,
					announcementDate,
					paymentDate,
					eventDate,
					value,
					valueText,
				});
		}

		const dedupe = new Map();
		for (const row of parsed) {
			const key = `${row.eventType}:${row.eventDate}:${row.value ?? ''}:${row.type || ''}`;
			if (!dedupe.has(key)) dedupe.set(key, row);
		}
		return Array.from(dedupe.values()).sort((left, right) =>
			String(left.eventDate || '').localeCompare(String(right.eventDate || ''))
		);
	}

	#htmlToPlainText(value) {
		return String(value || '')
			.replace(/<[^>]*>/g, ' ')
			.replace(/&nbsp;/gi, ' ')
			.replace(/&#160;/g, ' ')
			.replace(/&amp;/gi, '&')
			.replace(/&quot;/gi, '"')
			.replace(/&#39;/g, '\'')
			.replace(/&apos;/gi, '\'')
			.replace(/&lt;/gi, '<')
			.replace(/&gt;/gi, '>')
			.replace(/\s+/g, ' ')
			.trim();
	}

	#normalizeCalendarEvents(ticker, calendar, source) {
		if (!calendar) return [];
		const events = [];
		const pushEvent = (type, rawDateLike, details = null) => {
			if (rawDateLike === undefined || rawDateLike === null || rawDateLike === '') return;
			const rawDate = normalizeDate(rawDateLike) || nowIso().slice(0, 10);
			events.push({
				eventId: hashId(`${ticker}:${type}:${rawDate}:${JSON.stringify(details ?? rawDateLike)}`),
				title: `${type} - ${ticker}`,
				eventType: type,
				date: rawDate,
				details: details ?? rawDateLike,
				data_source: source || 'yahoo_quote_api',
				is_scraped: false,
			});
		};

		if (Array.isArray(calendar)) {
			for (const entry of calendar) {
				const eventType =
					entry?.eventType
					|| entry?.type
					|| entry?.title
					|| 'calendar';
				const eventDate =
					entry?.date
					|| entry?.eventDate
					|| entry?.paymentDate
					|| entry?.exDate
					|| JSON.stringify(entry);
				pushEvent(eventType, eventDate, entry);
			}
			return events;
		}

		if (typeof calendar === 'object') {
			for (const [key, value] of Object.entries(calendar)) {
				if (Array.isArray(value)) {
					for (const row of value) {
						const rowDate =
							row?.date
							|| row?.eventDate
							|| row?.paymentDate
							|| row?.exDate
							|| row?.value
							|| JSON.stringify(row);
						pushEvent(key, rowDate, row);
					}
				} else if (typeof value === 'object') {
					for (const nested of Object.values(value)) {
						const nestedDate =
							nested?.date
							|| nested?.eventDate
							|| nested?.paymentDate
							|| nested?.exDate
							|| nested;
						pushEvent(key, nestedDate, nested);
					}
				} else {
					pushEvent(key, value, value);
				}
			}
		}
		return events;
	}

	#isDividendEventType(value) {
		const type = String(value || '').toLowerCase();
		return type.includes('dividend')
			|| type.includes('provento')
			|| type.includes('jcp')
			|| type.includes('juros')
			|| type.includes('rendimento')
			|| type.includes('amort');
	}

	#isDividendCalendarEventType(value) {
		const type = String(value || '')
			.toLowerCase()
			.normalize('NFD')
			.replace(/[\u0300-\u036f]/g, '');
		return this.#isDividendEventType(type)
			|| type.includes('subscr')
			|| type.includes('subscription')
			|| (type.includes('direito') && type.includes('preferenc'));
	}

	async #resolveAssetsForTickerOrPortfolio(ticker, portfolioId) {
		if (ticker) {
			if (portfolioId) {
				const assets = await this.#listPortfolioAssets(portfolioId);
				const matched = assets.filter((asset) => String(asset.ticker || '').toUpperCase() === String(ticker).toUpperCase());
				if (matched.length) return matched;
			}

			const allAssets = await this.#scanAll({
				TableName: this.tableName,
				FilterExpression: 'begins_with(SK, :assetPrefix) AND ticker = :ticker',
				ExpressionAttributeValues: {
					':assetPrefix': 'ASSET#',
					':ticker': String(ticker).toUpperCase(),
				},
			});
			if (allAssets.length) return allAssets;

			return [{
				ticker: String(ticker).toUpperCase(),
				portfolioId: portfolioId || null,
				assetId: `virtual-${String(ticker).toLowerCase()}`,
				assetClass: 'stock',
				country: 'US',
				currency: 'USD',
			}];
		}

		if (!portfolioId) throw new Error('portfolioId or ticker is required');
		return this.#listPortfolioAssets(portfolioId);
	}

	async #resolveAssetContext(ticker, userId, explicitPortfolioId) {
		const portfolioId = await this.#resolvePortfolioId(userId, explicitPortfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const normalized = String(ticker || '').toUpperCase().replace(/\.SA$|\.TO$/g, '');
		const asset = assets.find((candidate) =>
			String(candidate.ticker || '').toUpperCase().replace(/\.SA$|\.TO$/g, '') === normalized
		);
		if (!asset) throw new Error(`Asset '${ticker}' not found in portfolio`);
		return { portfolioId, asset };
	}

	async #resolvePortfolioId(userId, explicitPortfolioId) {
		if (explicitPortfolioId) return explicitPortfolioId;
		const portfolios = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `USER#${userId}`,
				':sk': 'PORTFOLIO#',
			},
		});
		if (!portfolios.length) throw new Error(`No portfolio found for user '${userId}'`);
		portfolios.sort((left, right) =>
			String(right.updatedAt || right.createdAt || '').localeCompare(String(left.updatedAt || left.createdAt || ''))
		);
		return portfolios[0].portfolioId;
	}

	async #listPortfolioAssets(portfolioId) {
		return this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'ASSET#',
			},
		});
	}

	async #listPortfolioTransactions(portfolioId) {
		return this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TRANS#',
			},
		});
	}

	async #getLatestAssetDetail(portfolioId, assetId) {
		const response = await this.dynamo.send(
			new GetCommand({
				TableName: this.tableName,
				Key: {
					PK: `PORTFOLIO#${portfolioId}`,
					SK: `ASSET_DETAIL_LATEST#${assetId}`,
				},
			})
		);
		return response.Item || null;
	}

	#extractFinancialStatementsFromDetail(detail) {
		if (!detail) return createEmptyFinancialStatements();
		const extracted = readFinancialStatementsFromPayload(detail);
		return mergeFinancialStatements(createEmptyFinancialStatements(), extracted);
	}

	async #listAssetPriceRows(portfolioId, assetId) {
		const rows = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': `ASSET_PRICE#${assetId}#`,
			},
		});
		return rows
			.map((item) => ({
				date: item.date,
				close: numeric(item.close, null),
				adjusted_close: numeric(item.adjustedClose, null),
				volume: numeric(item.volume, null),
				dividends: numeric(item.dividends, 0),
				stock_splits: numeric(item.stockSplits, 0),
			}))
			.filter((item) => item.date && item.close !== null)
			.sort((left, right) => left.date.localeCompare(right.date));
	}

	#toReturns(rows) {
		const result = [];
		for (let index = 1; index < rows.length; index += 1) {
			const prev = numeric(rows[index - 1].close, 0);
			const curr = numeric(rows[index].close, 0);
			if (prev <= 0 || curr <= 0) continue;
			result.push({
				date: rows[index].date,
				returnPct: ((curr / prev) - 1) * 100,
			});
		}
		return result;
	}

	#alignReturns(left, right) {
		const leftMap = new Map(left.map((item) => [item.date, item.returnPct / 100]));
		const rightMap = new Map(right.map((item) => [item.date, item.returnPct / 100]));
		const dates = Array.from(leftMap.keys()).filter((date) => rightMap.has(date)).sort();
		return {
			left: dates.map((date) => leftMap.get(date)),
			right: dates.map((date) => rightMap.get(date)),
		};
	}

	#maxDrawdown(values) {
		if (!Array.isArray(values) || values.length === 0) return 0;
		let peak = values[0] || 0;
		let maxDrawdown = 0;
		for (const value of values) {
			if (value > peak) peak = value;
			if (peak > 0) {
				const drawdown = ((value - peak) / peak) * 100;
				if (drawdown < maxDrawdown) maxDrawdown = drawdown;
			}
		}
		return maxDrawdown;
	}

	#buildFxExposure(assetsMetrics) {
		const byCurrency = { BRL: 0, USD: 0, CAD: 0 };
		let total = 0;
		for (const asset of assetsMetrics) {
			const value = numeric(asset.market_value_brl, numeric(asset.market_value, 0));
			const currency = String(asset.currency || 'BRL').toUpperCase();
			if (!(currency in byCurrency)) byCurrency[currency] = 0;
			byCurrency[currency] += value;
			total += value;
		}
		const result = {};
		for (const [currency, value] of Object.entries(byCurrency)) {
			result[currency] = {
				value,
				weight_pct: total > 0 ? (value / total) * 100 : 0,
			};
		}
		return result;
	}

	async #buildIpcaDeflatedSeries(series) {
		if (!Array.isArray(series) || !series.length) return [];
		const fromMonth = String(series[0]?.date || '').slice(0, 7);
		const toMonth = String(series[series.length - 1]?.date || '').slice(0, 7);
		const ipcaRows = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `ECON#${INDICATOR_SERIES.IPCA}`,
				':sk': 'DATE#',
			},
		});
		const ipcaByMonth = new Map(
			ipcaRows
				.map((row) => ({
					month: String(row.date || '').slice(0, 7),
					monthlyRate:
						numeric(
							typeof row.value === 'string'
								? row.value.replace(',', '.')
								: row.value,
							0
						) / 100,
				}))
				.filter((row) => row.month && row.month >= fromMonth && row.month <= toMonth)
				.map((row) => [row.month, row.monthlyRate])
		);
		if (ipcaByMonth.size === 0) return [];
		const seriesMonths = Array.from(
			new Set(
				series
					.map((point) => String(point.date || '').slice(0, 7))
					.filter(Boolean)
			)
		).sort();
		const indexByMonth = new Map();
		let cumulativeIndex = 1;
		for (const month of seriesMonths) {
			cumulativeIndex *= 1 + numeric(ipcaByMonth.get(month), 0);
			indexByMonth.set(month, cumulativeIndex);
		}
		const baseMonth = String(series[0]?.date || '').slice(0, 7);
		const baseIndex = numeric(indexByMonth.get(baseMonth), 1);

		return series.map((point) => {
			const nominal = numeric(point.value, 0);
			const month = String(point.date || '').slice(0, 7);
			const currentIndex = numeric(indexByMonth.get(month), baseIndex);
			return {
				date: point.date,
				real_value: currentIndex > 0 ? nominal * (baseIndex / currentIndex) : nominal,
				nominal_value: nominal,
			};
		});
	}

	async #buildPortfolioValueSeries(portfolioId, metricsAssets, days = 365, options = {}) {
		const assetsSeries = await Promise.all(
			metricsAssets.map(async (asset) => {
				const rows = await this.#listAssetPriceRows(portfolioId, asset.assetId);
				return {
					assetId: asset.assetId,
					rows,
					quantity: numeric(asset.quantity_current, 0),
					fxRate: numeric(options.fxRateByAssetId?.[asset.assetId], 1),
					fallbackBrl: numeric(options.fallbackBrlByAssetId?.[asset.assetId], 0),
				};
			})
		);

		const allDates = new Set();
		for (const assetSeries of assetsSeries) {
			for (const row of assetSeries.rows) allDates.add(row.date);
		}
		let dates = Array.from(allDates).sort();
		if (!dates.length) {
			const today = nowIso().slice(0, 10);
			const totalFallback = assetsSeries.reduce(
				(sum, assetSeries) => sum + numeric(assetSeries.fallbackBrl, 0),
				0
			);
			return [{ date: today, value: totalFallback }];
		}
		if (Number.isFinite(days) && days !== null && days > 0 && dates.length > days) {
			dates = dates.slice(-days);
		}

		const series = [];
		for (const date of dates) {
			let value = 0;
			for (const assetSeries of assetsSeries) {
				if (assetSeries.rows.length > 0) {
					const row = this.#findRowAtOrBefore(assetSeries.rows, date);
					const close = row ? toNumberOrNull(row.close) : null;
					if (close !== null && Math.abs(close) > Number.EPSILON) {
						value += assetSeries.quantity * close * assetSeries.fxRate;
						continue;
					}
				}
				value += assetSeries.fallbackBrl;
			}
			series.push({ date, value });
		}
		return series;
	}

	#findRowAtOrBefore(rows, date) {
		for (let index = rows.length - 1; index >= 0; index -= 1) {
			if (rows[index].date <= date) return rows[index];
		}
		return null;
	}

	#seriesReturnPct(series) {
		if (!Array.isArray(series) || series.length < 2) return 0;
		const first = numeric(series[0].value, 0);
		const last = numeric(series[series.length - 1].value, 0);
		if (first <= 0) return 0;
		return ((last / first) - 1) * 100;
	}

	#normalizeSeries(series) {
		if (!Array.isArray(series) || series.length === 0) return [];
		const first = numeric(series[0].value, 0);
		if (first <= 0) return series.map((point) => ({ date: point.date, value: 100 }));
		return series.map((point) => ({
			date: point.date,
			value: (numeric(point.value, 0) / first) * 100,
		}));
	}

	#toAllocationArray(map, total) {
		return Object.entries(map)
			.map(([key, value]) => ({
				key,
				value,
				weight_pct: total > 0 ? (value / total) * 100 : 0,
			}))
			.sort((left, right) => right.value - left.value);
	}

	async #computeIndicatorReturn(seriesId, fromDate, toDate) {
		const rows = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `ECON#${seriesId}`,
				':sk': 'DATE#',
			},
		});
		const filtered = rows
			.filter((row) => row.date >= fromDate && row.date <= toDate)
			.sort((left, right) => String(left.date).localeCompare(String(right.date)));
		if (!filtered.length) return 0;

		if (seriesId === INDICATOR_SERIES.CDI || seriesId === INDICATOR_SERIES.SELIC || seriesId === INDICATOR_SERIES.POUPANCA) {
			let factor = 1;
			for (const row of filtered) {
				factor *= 1 + (numeric(row.value, 0) / 10000);
			}
			return (factor - 1) * 100;
		}

		const first = numeric(filtered[0].value, 0);
		const last = numeric(filtered[filtered.length - 1].value, 0);
		if (first === 0) return 0;
		return ((last / first) - 1) * 100;
	}

	async #computeIndicatorAccumulation(seriesId, fromDate, toDate) {
		const rows = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `ECON#${seriesId}`,
				':sk': 'DATE#',
			},
		});
		const filtered = rows
			.filter((row) => row.date >= fromDate && row.date <= toDate)
			.sort((left, right) => String(left.date).localeCompare(String(right.date)));
		if (!filtered.length) return 0;

		let factor = 1;
		for (const row of filtered) factor *= 1 + (numeric(row.value, 0) / 10000);
		return factor - 1;
	}

	async #fetchBenchmarkHistory(symbol, fromDate) {
		try {
			const payload = await this.priceHistoryService.yahooHistoryProvider.fetchHistory(symbol, {
				startDate: fromDate,
				period: fromDate ? null : 'max',
				allowEmpty: true,
			});
			return (payload.rows || []).map((row) => ({ date: row.date, close: numeric(row.close, 0) }));
		} catch {
			return [];
		}
	}

	async #getAssetRiskSnapshot(portfolioId, assetId) {
		const rows = await this.#listAssetPriceRows(portfolioId, assetId);
		const returns = this.#toReturns(rows).map((item) => item.returnPct / 100);
		return {
			volatility: stdDev(returns) * Math.sqrt(252) * 100,
			drawdown: this.#maxDrawdown(rows.map((row) => numeric(row.close, 0))),
		};
	}

	#extractDividendAmounts(dividends) {
		if (!Array.isArray(dividends)) return [];
		return dividends
			.map((item) => {
				if (typeof item === 'number') return item;
				if (typeof item === 'object') return numeric(item.value, 0);
				return numeric(item, 0);
			})
			.filter((value) => Number.isFinite(value));
	}

	async #getCursor(jobName, scope) {
		const result = await this.dynamo.send(
			new GetCommand({
				TableName: this.tableName,
				Key: {
					PK: `JOB#${jobName}`,
					SK: `CURSOR#${scope}`,
				},
			})
		);
		return result.Item || null;
	}

	async #setCursor(jobName, scope, payload) {
		await this.dynamo.send(
			new PutCommand({
				TableName: this.tableName,
				Item: {
					PK: `JOB#${jobName}`,
					SK: `CURSOR#${scope}`,
					entityType: 'JOB_CURSOR',
					jobName,
					scope,
					...payload,
					updatedAt: nowIso(),
				},
			})
		);
	}

	async #recordJobRun(jobName, payload) {
		await this.dynamo.send(
			new PutCommand({
				TableName: this.tableName,
				Item: {
					PK: `JOB#${jobName}`,
					SK: `RUN#${nowIso()}#${hashId(Math.random())}`,
					entityType: 'JOB_RUN',
					jobName,
					...payload,
					data_source: 'internal_calc',
					fetched_at: nowIso(),
					is_scraped: false,
					createdAt: nowIso(),
				},
			})
		);
	}

	async #queryAll(queryInput) {
		const items = [];
		let lastEvaluatedKey;
		do {
			const result = await this.dynamo.send(
				new QueryCommand({
					...queryInput,
					ExclusiveStartKey: lastEvaluatedKey,
				})
			);
			if (Array.isArray(result.Items) && result.Items.length > 0) {
				items.push(...result.Items);
			}
			lastEvaluatedKey = result.LastEvaluatedKey;
		} while (lastEvaluatedKey);
		return items;
	}

	async #scanAll(scanInput) {
		const items = [];
		let lastEvaluatedKey;
		do {
			const result = await this.dynamo.send(
				new ScanCommand({
					...scanInput,
					ExclusiveStartKey: lastEvaluatedKey,
				})
			);
			if (Array.isArray(result.Items) && result.Items.length > 0) items.push(...result.Items);
			lastEvaluatedKey = result.LastEvaluatedKey;
		} while (lastEvaluatedKey);
		return items;
	}
}

module.exports = {
	PlatformService,
};
