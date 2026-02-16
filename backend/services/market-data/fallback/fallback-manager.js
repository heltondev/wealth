const { GoogleFinanceStructuredProvider } = require('./structured/google-finance-provider');
const { BcbStructuredProvider } = require('./structured/bcb-provider');
const { StatusInvestStructuredProvider } = require('./structured/status-invest-provider');
const { GoogleFinanceScraper } = require('./scrapers/google-finance-scraper');
const { StatusInvestScraper } = require('./scrapers/status-invest-scraper');
const { FundamentusScraper } = require('./scrapers/fundamentus-scraper');
const { TesouroDiretoScraper } = require('./scrapers/tesouro-direto-scraper');
const { YahooFinanceScraper } = require('./scrapers/yahoo-finance-scraper');
const { nowIso, toNumberOrNull } = require('../utils');

const hasQuotePrice = (payload) => {
	const currentPrice = payload?.quote?.currentPrice;
	return currentPrice !== null
		&& currentPrice !== undefined
		&& currentPrice !== ''
		&& Number.isFinite(Number(currentPrice));
};

const SOURCE_TRUST_WEIGHTS = Object.freeze({
	bcb_sgs: 100,
	scrape_tesouro_direto: 98,
	yahoo_quote_api: 95,
	scrape_yahoo: 90,
	google_finance_structured: 88,
	scrape_google: 86,
	statusinvest_structured: 76,
	scrape_statusinvest: 72,
	scrape_fundamentus: 68,
});

const SOURCE_ALIASES = Object.freeze({
	googlefinancestructuredprovider: 'google_finance_structured',
	bcbstructuredprovider: 'bcb_sgs',
	statusinveststructuredprovider: 'statusinvest_structured',
	googlefinancescraper: 'scrape_google',
	statusinvestscraper: 'scrape_statusinvest',
	fundamentusscraper: 'scrape_fundamentus',
	tesourodiretoscraper: 'scrape_tesouro_direto',
	yahoofinancescraper: 'scrape_yahoo',
});

const HEAVY_FUNDAMENTAL_KEYS = new Set(['info', 'status_invest']);

const DEFAULT_TRUST_WEIGHT = 50;

const isPlainObject = (value) =>
	value !== null && typeof value === 'object' && !Array.isArray(value);

const cloneValue = (value) => {
	if (value === undefined) return undefined;
	try {
		return structuredClone(value);
	} catch {
		try {
			return JSON.parse(JSON.stringify(value));
		} catch {
			return value;
		}
	}
};

const normalizeSourceId = (value) =>
	String(value || '')
		.trim()
		.toLowerCase();

const resolveSourceId = (payload, fallbackSourceName = '') => {
	const preferred = normalizeSourceId(payload?.data_source || fallbackSourceName || 'unknown');
	return SOURCE_ALIASES[preferred] || preferred;
};

const resolveTrustWeight = (sourceId) =>
	SOURCE_TRUST_WEIGHTS[normalizeSourceId(sourceId)] || DEFAULT_TRUST_WEIGHT;

const toEpochMs = (value) => {
	if (value === undefined || value === null) return null;

	const numeric = toNumberOrNull(value);
	if (numeric !== null) {
		if (numeric > 1e12) return numeric;
		if (numeric > 1e9) return numeric * 1000;
	}

	const parsed = Date.parse(String(value));
	return Number.isFinite(parsed) ? parsed : null;
};

const resolveObservationTimestampMs = (payload, collectedAtMs) => {
	const candidates = [
		payload?.quote?.regularMarketTime,
		payload?.quote?.timestamp,
		payload?.quote?.marketTime,
		payload?.raw?.quote?.regularMarketTime,
		payload?.fundamentals?.info?.regularMarketTime,
		payload?.fetched_at,
	];

	for (const candidate of candidates) {
		const epochMs = toEpochMs(candidate);
		if (epochMs !== null) return epochMs;
	}

	return collectedAtMs;
};

const scoreFreshness = (observationTimestampMs, nowMs) => {
	if (!Number.isFinite(observationTimestampMs)) return 0;

	const ageMs = nowMs - observationTimestampMs;
	if (ageMs < -10 * 60 * 1000) return -6;
	if (ageMs <= 15 * 60 * 1000) return 12;
	if (ageMs <= 60 * 60 * 1000) return 10;
	if (ageMs <= 24 * 60 * 60 * 1000) return 6;
	if (ageMs <= 7 * 24 * 60 * 60 * 1000) return 3;
	if (ageMs <= 30 * 24 * 60 * 60 * 1000) return 1;
	return -5;
};

const scoreCompleteness = (payload) => {
	let score = 0;
	const quote = payload?.quote || {};

	if (hasQuotePrice(payload)) {
		score += 28;
	} else {
		score -= 80;
	}

	if (quote.currency) score += 2;
	if (toNumberOrNull(quote.change) !== null) score += 2;
	if (toNumberOrNull(quote.changePercent) !== null) score += 2;
	if (toNumberOrNull(quote.previousClose) !== null) score += 2;
	if (toNumberOrNull(quote.volume) !== null) score += 2;
	if (toNumberOrNull(quote.marketCap) !== null) score += 2;

	const fundamentals = payload?.fundamentals;
	if (isPlainObject(fundamentals)) {
		const populatedKeys = Object.keys(fundamentals).filter((key) => {
			const value = fundamentals[key];
			if (value === null || value === undefined) return false;
			if (isPlainObject(value)) return Object.keys(value).length > 0;
			if (Array.isArray(value)) return value.length > 0;
			return true;
		});
		score += Math.min(10, populatedKeys.length * 2);
	}

	const historyRows = Array.isArray(payload?.historical?.history_30d)
		? payload.historical.history_30d.length
		: 0;
	const dividendRows = Array.isArray(payload?.historical?.dividends)
		? payload.historical.dividends.length
		: 0;
	if (historyRows > 0) {
		score += Math.min(8, Math.max(1, Math.floor(Math.log2(historyRows + 1))));
	}
	if (dividendRows > 0) {
		score += Math.min(6, Math.max(1, Math.floor(Math.log2(dividendRows + 1))));
	}

	if (payload?.is_scraped === false) score += 3;

	return score;
};

const scoreCandidate = (payload, sourceId, collectedAtMs, nowMs = Date.now()) => {
	const trustWeight = resolveTrustWeight(sourceId);
	const observationTimestampMs = resolveObservationTimestampMs(payload, collectedAtMs);
	const freshnessScore = scoreFreshness(observationTimestampMs, nowMs);
	const completenessScore = scoreCompleteness(payload);
	return {
		sourceId,
		trustWeight,
		freshnessScore,
		completenessScore,
		observationTimestampMs,
		totalScore: trustWeight + freshnessScore + completenessScore,
	};
};

const compareCandidates = (left, right) => {
	if (right.score.totalScore !== left.score.totalScore) {
		return right.score.totalScore - left.score.totalScore;
	}
	if (right.score.trustWeight !== left.score.trustWeight) {
		return right.score.trustWeight - left.score.trustWeight;
	}
	if (right.score.freshnessScore !== left.score.freshnessScore) {
		return right.score.freshnessScore - left.score.freshnessScore;
	}
	return left.sourceOrder - right.sourceOrder;
};

const mergeObjectsPreferDefined = (target, source, options = {}) => {
	if (!isPlainObject(source)) return target;
	const { skipHeavyKeys = false } = options;

	for (const [key, value] of Object.entries(source)) {
		if (skipHeavyKeys && HEAVY_FUNDAMENTAL_KEYS.has(key)) continue;
		if (value === null || value === undefined) continue;

		if (Array.isArray(value)) {
			const targetArray = target[key];
			if (!Array.isArray(targetArray) || targetArray.length === 0) {
				target[key] = cloneValue(value);
			}
			continue;
		}

		if (isPlainObject(value)) {
			if (!isPlainObject(target[key])) {
				target[key] = {};
			}
			mergeObjectsPreferDefined(target[key], value, options);
			continue;
		}

		if (target[key] === null || target[key] === undefined || target[key] === '') {
			target[key] = value;
		}
	}

	return target;
};

const hasEnrichment = (payload) => {
	const hasFundamentals = isPlainObject(payload?.fundamentals)
		&& Object.keys(payload.fundamentals).length > 0;
	const hasHistory = Array.isArray(payload?.historical?.history_30d)
		&& payload.historical.history_30d.length > 0;
	const hasDividends = Array.isArray(payload?.historical?.dividends)
		&& payload.historical.dividends.length > 0;
	return hasFundamentals || hasHistory || hasDividends;
};

const dedupeHistoryRows = (entries) => {
	const sortedEntries = [...entries].sort(compareCandidates);
	const byDate = new Map();

	for (const entry of sortedEntries) {
		const rows = Array.isArray(entry?.payload?.historical?.history_30d)
			? entry.payload.historical.history_30d
			: [];
		for (const row of rows) {
			const date = String(row?.date || '').trim();
			if (!date) continue;
			if (!byDate.has(date)) {
				byDate.set(date, cloneValue(row));
			}
		}
	}

	return Array.from(byDate.values()).sort((left, right) =>
		String(left?.date || '').localeCompare(String(right?.date || ''))
	);
};

const buildDividendKey = (row) => {
	const date =
		String(row?.date || row?.paymentDate || row?.eventDate || '')
			.trim();
	if (!date) return null;

	const rawValue = row?.value ?? row?.amount ?? row?.dividend ?? null;
	const numericValue = toNumberOrNull(rawValue);
	const valueKey = numericValue === null
		? 'na'
		: Number(numericValue).toFixed(8);
	const type = String(row?.type || row?.eventType || '').trim().toLowerCase();
	return `${date}|${valueKey}|${type}`;
};

const dedupeDividends = (entries) => {
	const sortedEntries = [...entries].sort(compareCandidates);
	const byKey = new Map();

	for (const entry of sortedEntries) {
		const rows = Array.isArray(entry?.payload?.historical?.dividends)
			? entry.payload.historical.dividends
			: [];
		for (const row of rows) {
			const key = buildDividendKey(row);
			if (!key) continue;
			if (!byKey.has(key)) {
				byKey.set(key, cloneValue(row));
			}
		}
	}

	return Array.from(byKey.values()).sort((left, right) => {
		const leftDate = String(left?.date || left?.paymentDate || left?.eventDate || '');
		const rightDate = String(right?.date || right?.paymentDate || right?.eventDate || '');
		return leftDate.localeCompare(rightDate);
	});
};

const mergeFundamentalsFromEntries = (entries, bestSourceId = null) => {
	const sortedEntries = [...entries].sort(compareCandidates);
	const merged = {};

	for (const entry of sortedEntries) {
		if (!isPlainObject(entry?.payload?.fundamentals)) continue;
		const skipHeavyKeys = bestSourceId !== null && entry.sourceId !== bestSourceId;
		mergeObjectsPreferDefined(merged, entry.payload.fundamentals, { skipHeavyKeys });
	}

	return merged;
};

const mergeHistoricalFromEntries = (entries) => ({
	history_30d: dedupeHistoryRows(entries),
	dividends: dedupeDividends(entries),
});

const buildCandidateSummary = (entry) => ({
	source: entry.sourceId,
	data_source: entry.payload?.data_source || entry.sourceId,
	is_scraped: Boolean(entry.payload?.is_scraped),
	has_price: hasQuotePrice(entry.payload),
	currentPrice: toNumberOrNull(entry.payload?.quote?.currentPrice),
	score: entry.score.totalScore,
	trust_weight: entry.score.trustWeight,
	freshness_score: entry.score.freshnessScore,
	completeness_score: entry.score.completenessScore,
	observed_at: Number.isFinite(entry.score.observationTimestampMs)
		? new Date(entry.score.observationTimestampMs).toISOString()
		: null,
});

class FallbackManager {
	constructor(options = {}) {
		this.logger = options.logger || console;
		this.structuredProviders = options.structuredProviders || [
			new GoogleFinanceStructuredProvider(options),
			new BcbStructuredProvider(options),
			new StatusInvestStructuredProvider(options),
		];
		this.scrapers = options.scrapers || [
			new GoogleFinanceScraper(options),
			new StatusInvestScraper(options),
			new FundamentusScraper(options),
			new TesouroDiretoScraper(options),
			new YahooFinanceScraper(options),
		];
	}

	async fetch(asset) {
		const attempts = [];
		const entries = [];
		let sourceOrder = 0;

		const collectFromSource = async (sourceInstance, fallbackSourceName) => {
			const sourceName = fallbackSourceName
				|| sourceInstance?.name
				|| sourceInstance?.constructor?.name
				|| 'unknown_source';
			try {
				const result = await sourceInstance.fetch(asset);
				if (!result) {
					attempts.push({ source: normalizeSourceId(sourceName), status: 'empty' });
					return;
				}

				const collectedAtMs = Date.now();
				const sourceId = resolveSourceId(result, sourceName);
				const score = scoreCandidate(result, sourceId, collectedAtMs);
				const entry = {
					sourceId,
					sourceName: normalizeSourceId(sourceName),
					payload: result,
					score,
					sourceOrder: sourceOrder += 1,
				};
				entries.push(entry);

				if (!hasQuotePrice(result)) {
					attempts.push({
						source: sourceId,
						status: 'partial',
						message: 'missing_current_price',
						score: score.totalScore,
					});
					return;
				}

				attempts.push({
					source: sourceId,
					status: 'success',
					score: score.totalScore,
				});
			} catch (error) {
				attempts.push({
					source: normalizeSourceId(sourceName),
					status: 'error',
					message: error.message,
				});
			}
		};

		for (const provider of this.structuredProviders) {
			await collectFromSource(provider, provider?.constructor?.name);
		}
		for (const scraper of this.scrapers) {
			await collectFromSource(scraper, scraper?.name || scraper?.constructor?.name);
		}

		const pricedEntries = entries
			.filter((entry) => hasQuotePrice(entry.payload))
			.sort(compareCandidates);
		const enrichmentEntries = entries.filter((entry) => hasEnrichment(entry.payload));

		if (pricedEntries.length > 0) {
			const best = pricedEntries[0];
			const mergedFundamentals = mergeFundamentalsFromEntries(enrichmentEntries, best.sourceId);
			const mergedHistorical = mergeHistoricalFromEntries(enrichmentEntries);
			const mergedSources = [...new Set(enrichmentEntries.map((entry) => entry.sourceId))];

			return {
				...best.payload,
				fundamentals: mergedFundamentals,
				historical: mergedHistorical,
				raw: {
					...(best.payload?.raw || {}),
					fallback_attempts: attempts,
					fallback_candidates: pricedEntries.map(buildCandidateSummary),
					merged_sources: mergedSources,
					source_trust_weights: SOURCE_TRUST_WEIGHTS,
				},
				fallback_trace: attempts,
			};
		}

		const mergedFundamentals = mergeFundamentalsFromEntries(enrichmentEntries);
		const mergedHistorical = mergeHistoricalFromEntries(enrichmentEntries);
		const mergedSources = [...new Set(enrichmentEntries.map((entry) => entry.sourceId))];

		return {
			data_source: 'unavailable',
			is_scraped: false,
			quote: {
				currentPrice: null,
				currency: null,
				change: null,
				changePercent: null,
				previousClose: null,
				marketCap: null,
				volume: null,
			},
			fundamentals: mergedFundamentals,
			historical: mergedHistorical,
			raw: {
				fallback_attempts: attempts,
				fallback_candidates: entries
					.sort(compareCandidates)
					.map(buildCandidateSummary),
				merged_sources: mergedSources,
				source_trust_weights: SOURCE_TRUST_WEIGHTS,
			},
			fetched_at: nowIso(),
		};
	}

	async healthCheckScrapers() {
		const checks = await Promise.all(
			this.scrapers.map(async (scraper) => {
				try {
					return await scraper.healthCheck();
				} catch (error) {
					return {
						scraper: scraper.name,
						ok: false,
						checked_at: nowIso(),
						details: error.message,
					};
				}
			})
		);

		const allHealthy = checks.every((check) => Boolean(check.ok));
		const payload = {
			status: allHealthy ? 'ok' : 'degraded',
			checked_at: nowIso(),
			scrapers: checks,
		};

		this.logger.log(
			JSON.stringify({
				event: 'scraper_health_check',
				...payload,
			})
		);

		return payload;
	}
}

module.exports = {
	FallbackManager,
};
