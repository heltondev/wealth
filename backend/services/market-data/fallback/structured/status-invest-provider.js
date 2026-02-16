const { fetchWithTimeout, withRetry, toNumberOrNull } = require('../../utils');
const { extractJsonScriptContent } = require('../extractors');

const buildStatusInvestUrl = (asset) => {
	const ticker = String(asset.ticker || '').toLowerCase().replace(/\.sa$/i, '');
	const assetClass = String(asset.assetClass || '').toLowerCase();

	if (!ticker) return null;
	if (assetClass === 'fii') {
		return `https://statusinvest.com.br/fundos-imobiliarios/${ticker}`;
	}
	return `https://statusinvest.com.br/acoes/${ticker}`;
};

const deepFindByHints = (value, hints) => {
	if (!value || typeof value !== 'object') return null;
	for (const [key, entry] of Object.entries(value)) {
		const normalized = key.toLowerCase();
		if (hints.some((hint) => normalized.includes(hint))) {
			const numeric = toNumberOrNull(entry);
			if (numeric !== null) return numeric;
		}
	}
	for (const entry of Object.values(value)) {
		if (entry && typeof entry === 'object') {
			const found = deepFindByHints(entry, hints);
			if (found !== null) return found;
		}
	}
	return null;
};

const PORTFOLIO_LABEL_HINTS = [
	'nome',
	'name',
	'segment',
	'segmento',
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
];

const PORTFOLIO_PERCENT_HINTS = [
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

const normalizeKey = (value) =>
	String(value || '')
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
		.replace(/[^a-z0-9]/g, '');

const isMeaningfulText = (value) => {
	const text = String(value || '').trim();
	if (!text) return false;
	if (/^https?:\/\//i.test(text)) return false;
	if (text.length <= 1) return false;
	return true;
};

const collectLeafEntries = (value, path = '', depth = 0, output = []) => {
	if (depth > 4) return output;
	if (Array.isArray(value)) return output;
	if (!value || typeof value !== 'object') return output;

	for (const [key, entry] of Object.entries(value)) {
		const nextPath = path ? `${path}.${key}` : key;
		if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
			collectLeafEntries(entry, nextPath, depth + 1, output);
			continue;
		}
		output.push({
			keyPath: nextPath,
			normalizedKey: normalizeKey(nextPath),
			value: entry,
		});
	}

	return output;
};

const extractPortfolioRowsFromArray = (rows, pathLabel = '') => {
	if (!Array.isArray(rows) || rows.length < 2 || rows.length > 80) return null;

	const normalizedRows = [];
	for (const row of rows) {
		if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
		const leaves = collectLeafEntries(row);
		if (leaves.length === 0) continue;

		const labelEntry =
			leaves.find((entry) => (
				PORTFOLIO_LABEL_HINTS.some((hint) => entry.normalizedKey.includes(hint))
				&& isMeaningfulText(entry.value)
			)) ||
			leaves.find((entry) => typeof entry.value === 'string' && isMeaningfulText(entry.value));

		const pctEntry =
			leaves.find((entry) => (
				PORTFOLIO_PERCENT_HINTS.some((hint) => entry.normalizedKey.includes(hint))
				&& toNumberOrNull(entry.value) !== null
			)) ||
			null;
		if (!labelEntry || !pctEntry) continue;

		const rawPercent = toNumberOrNull(pctEntry.value);
		if (rawPercent === null) continue;

		normalizedRows.push({
			label: String(labelEntry.value).trim(),
			rawPercent,
		});
	}

	if (normalizedRows.length < 2) return null;

	const shouldScaleFraction =
		normalizedRows.every((row) => Math.abs(row.rawPercent) <= 1) &&
		normalizedRows.some((row) => Math.abs(row.rawPercent) > 0);

	const finalRows = normalizedRows
		.map((row) => {
			const allocationPct = shouldScaleFraction
				? row.rawPercent * 100
				: row.rawPercent;
			if (!Number.isFinite(allocationPct)) return null;
			if (allocationPct <= 0 || allocationPct > 100) return null;
			return {
				label: row.label,
				allocation_pct: allocationPct,
			};
		})
		.filter(Boolean);

	if (finalRows.length < 2) return null;

	const uniqueLabels = new Set(finalRows.map((row) => row.label.toLowerCase()));
	if (uniqueLabels.size < 2) return null;

	const totalPct = finalRows.reduce((sum, row) => sum + row.allocation_pct, 0);
	const score =
		(finalRows.length * 3) +
		(totalPct >= 50 && totalPct <= 140 ? 12 : 0) +
		(Math.max(0, 20 - Math.abs(100 - totalPct)) / 10);

	return {
		path: pathLabel,
		score,
		rows: finalRows
			.sort((left, right) => right.allocation_pct - left.allocation_pct)
			.slice(0, 40),
	};
};

const extractPortfolioComposition = (nextData) => {
	const queue = [{
		value: nextData,
		path: 'root',
	}];
	const candidates = [];

	while (queue.length > 0) {
		const current = queue.shift();
		const currentValue = current?.value;
		const currentPath = current?.path || 'root';

		if (Array.isArray(currentValue)) {
			const extracted = extractPortfolioRowsFromArray(currentValue, currentPath);
			if (extracted) candidates.push(extracted);
			for (let index = 0; index < currentValue.length; index += 1) {
				const entry = currentValue[index];
				if (entry && typeof entry === 'object') {
					queue.push({
						value: entry,
						path: `${currentPath}[${index}]`,
					});
				}
			}
			continue;
		}

		if (!currentValue || typeof currentValue !== 'object') continue;
		for (const [key, entry] of Object.entries(currentValue)) {
			if (entry && typeof entry === 'object') {
				queue.push({
					value: entry,
					path: `${currentPath}.${key}`,
				});
			}
		}
	}

	if (candidates.length === 0) return null;
	const best = candidates.sort((left, right) => right.score - left.score)[0];
	return {
		detected_path: best.path,
		rows: best.rows,
	};
};

class StatusInvestStructuredProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_STATUSINVEST_TIMEOUT_MS || 15000);
	}

	async fetch(asset) {
		if (String(asset.market || '').toUpperCase() !== 'BR') return null;

		const url = buildStatusInvestUrl(asset);
		if (!url) return null;

		try {
			const response = await withRetry(
				() =>
					fetchWithTimeout(url, {
						timeoutMs: this.timeoutMs,
						headers: { Accept: 'text/html,*/*' },
					}),
				{ retries: 1, baseDelayMs: 300, factor: 2 }
			);
			if (!response.ok) return null;

			const html = await response.text();
			const nextDataRaw = extractJsonScriptContent(html, '__NEXT_DATA__');
			if (!nextDataRaw) return null;

			const nextData = JSON.parse(nextDataRaw);
			const price = deepFindByHints(nextData, ['price', 'cotacao', 'last']);
			if (!price) return null;
			const portfolioComposition = extractPortfolioComposition(nextData);

			return {
				data_source: 'statusinvest_structured',
				is_scraped: false,
				quote: {
					currentPrice: price,
					currency: 'BRL',
					change: deepFindByHints(nextData, ['change']),
					changePercent: deepFindByHints(nextData, ['percent']),
					previousClose: null,
					marketCap: deepFindByHints(nextData, ['marketcap', 'valorMercado']),
					volume: deepFindByHints(nextData, ['volume']),
				},
				fundamentals: {
					status_invest: nextData,
					portfolio_composition: portfolioComposition?.rows || null,
				},
				portfolio_composition: portfolioComposition?.rows || null,
				portfolio_composition_meta: portfolioComposition
					? { detected_path: portfolioComposition.detected_path }
					: null,
				historical: {
					history_30d: [],
					dividends: [],
				},
				raw: nextData,
			};
		} catch {
			return null;
		}
	}
}

module.exports = {
	StatusInvestStructuredProvider,
};
