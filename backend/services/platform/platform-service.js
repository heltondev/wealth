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
const { resolveAssetMarket, resolveYahooSymbol } = require('../market-data/symbol-resolver');
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
						.filter((event) => this.#isDividendEventType(event.eventType))
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
		const normalized = [];

		for (const target of targets) {
			const targetId = String(target.targetId || `target-${hashId(`${target.scope}:${target.value}`)}`);
			const scope = String(target.scope || 'assetClass');
			const value = String(target.value || '').trim();
			if (!value) continue;
			const percent = numeric(target.percent, 0);
			if (percent <= 0) continue;

			const item = {
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
			};
			await this.dynamo.send(new PutCommand({ TableName: this.tableName, Item: item }));
			normalized.push(item);
		}

		return {
			portfolioId,
			targets: normalized,
		};
	}

	async getRebalancingSuggestion(userId, amount, options = {}) {
		const contribution = numeric(amount, 0);
		if (contribution <= 0) {
			throw new Error('amount must be greater than zero');
		}

		const portfolioId = await this.#resolvePortfolioId(userId, options.portfolioId);
		const assets = await this.#listPortfolioAssets(portfolioId);
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, {
			portfolioId,
			method: options.method || 'fifo',
		});

		const currentByClass = {};
		const assetByClass = {};
		for (const metric of metrics.assets) {
			const asset = assets.find((candidate) => candidate.assetId === metric.assetId) || {};
			const assetClass = String(asset.assetClass || 'unknown').toLowerCase();
			currentByClass[assetClass] = (currentByClass[assetClass] || 0) + numeric(metric.market_value, 0);
			if (!assetByClass[assetClass]) assetByClass[assetClass] = [];
			assetByClass[assetClass].push({ asset, metric });
		}

		const targets = await this.#queryAll({
			TableName: this.tableName,
			KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
			ExpressionAttributeValues: {
				':pk': `PORTFOLIO#${portfolioId}`,
				':sk': 'TARGET_ALLOC#',
			},
		});

		let targetByClass = {};
		if (targets.length > 0) {
			for (const target of targets) {
				if (String(target.scope || '').toLowerCase() !== 'assetclass') continue;
				const cls = String(target.value || '').toLowerCase();
				targetByClass[cls] = numeric(target.percent, 0) / 100;
			}
		}

		if (Object.keys(targetByClass).length === 0) {
			const classes = Object.keys(currentByClass);
			const equalWeight = classes.length ? 1 / classes.length : 0;
			for (const cls of classes) targetByClass[cls] = equalWeight;
		}

		const currentTotal = Object.values(currentByClass).reduce((sum, value) => sum + value, 0);
		const targetTotal = currentTotal + contribution;
		const deficits = {};
		let positiveDeficitSum = 0;
		for (const [cls, weight] of Object.entries(targetByClass)) {
			const desired = targetTotal * weight;
			const current = numeric(currentByClass[cls], 0);
			const deficit = Math.max(0, desired - current);
			deficits[cls] = deficit;
			positiveDeficitSum += deficit;
		}

		const suggestions = [];
		for (const [cls, deficit] of Object.entries(deficits)) {
			if (deficit <= 0) continue;
			const allocation = positiveDeficitSum > 0
				? (deficit / positiveDeficitSum) * contribution
				: 0;
			const bucket = assetByClass[cls] || [];
			const selected = bucket.sort((left, right) => numeric(right.metric.market_value, 0) - numeric(left.metric.market_value, 0))[0];
			suggestions.push({
				assetClass: cls,
				recommended_amount: allocation,
				assetId: selected?.asset?.assetId || null,
				ticker: selected?.asset?.ticker || null,
				current_value: numeric(currentByClass[cls], 0),
				target_value: targetTotal * numeric(targetByClass[cls], 0),
			});
		}

		return {
			portfolioId,
			contribution,
			current_total: currentTotal,
			target_total_after_contribution: targetTotal,
			targets: targetByClass,
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

		return {
			asset: context.asset,
			detail: detail || null,
			latest_price: prices.length ? prices[prices.length - 1] : null,
			average_cost: averageCost,
			financial_statements: {
				financials: detail?.raw?.final_payload?.financials || detail?.raw?.primary_payload?.financials || null,
				quarterly_financials:
					detail?.raw?.final_payload?.quarterly_financials ||
					detail?.raw?.primary_payload?.quarterly_financials ||
					null,
				balance_sheet: detail?.raw?.final_payload?.balance_sheet || detail?.raw?.primary_payload?.balance_sheet || null,
				quarterly_balance_sheet:
					detail?.raw?.final_payload?.quarterly_balance_sheet ||
					detail?.raw?.primary_payload?.quarterly_balance_sheet ||
					null,
				cashflow: detail?.raw?.final_payload?.cashflow || detail?.raw?.primary_payload?.cashflow || null,
				quarterly_cashflow:
					detail?.raw?.final_payload?.quarterly_cashflow ||
					detail?.raw?.primary_payload?.quarterly_cashflow ||
					null,
			},
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
		const metrics = await this.priceHistoryService.getPortfolioMetrics(userId, { portfolioId });
		const totalValue = Math.max(numeric(metrics.consolidated.total_market_value, 0), 1);

		const concentration = metrics.assets
			.map((metric) => ({
				assetId: metric.assetId,
				ticker: metric.ticker,
				market_value: numeric(metric.market_value, 0),
				weight_pct: (numeric(metric.market_value, 0) / totalValue) * 100,
			}))
			.sort((left, right) => right.weight_pct - left.weight_pct);

		const byAssetReturns = {};
		const volatilityByAsset = {};
		const drawdownByAsset = {};

		for (const asset of assets) {
			const rows = await this.#listAssetPriceRows(portfolioId, asset.assetId);
			const returns = this.#toReturns(rows);
			byAssetReturns[asset.ticker] = returns;
			const onlyReturns = returns.map((item) => item.returnPct / 100);
			volatilityByAsset[asset.ticker] = stdDev(onlyReturns) * Math.sqrt(252) * 100;
			drawdownByAsset[asset.ticker] = this.#maxDrawdown(rows.map((row) => numeric(row.close, 0)));
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

		const series = await this.#buildPortfolioValueSeries(portfolioId, metrics.assets, 365);
		const portfolioValues = series.map((point) => numeric(point.value, 0));
		const portfolioDrawdown = this.#maxDrawdown(portfolioValues);
		const portfolioReturns = [];
		for (let index = 1; index < portfolioValues.length; index += 1) {
			const prev = portfolioValues[index - 1];
			const curr = portfolioValues[index];
			if (prev > 0) portfolioReturns.push((curr / prev) - 1);
		}

		const fxExposure = this.#buildFxExposure(metrics.assets);
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
			risk_return_scatter: metrics.assets.map((asset) => ({
				ticker: asset.ticker,
				volatility: volatilityByAsset[asset.ticker] || 0,
				return_pct: numeric(asset.percent_return, 0),
			})),
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
		const url = new URL(`https://api.bcb.gov.br/dados/serie/bcdata.sgs.${seriesId}/dados`);
		url.searchParams.set('formato', 'json');
		if (startDate) {
			const brDate = toBrDate(startDate);
			if (brDate) url.searchParams.set('dataInicial', brDate);
		}

		const response = await withRetry(
			() => fetchWithTimeout(url.toString(), { timeoutMs: 20000 }),
			{ retries: 2, baseDelayMs: 500, factor: 2 }
		);
		if (!response.ok) {
			throw new Error(`BCB SGS series ${seriesId} responded with ${response.status}`);
		}
		const rows = await response.json();
		if (!Array.isArray(rows)) return [];

		return rows
			.map((row) => ({
				date: normalizeDate(row.data),
				value: numeric(row.valor, null),
			}))
			.filter((row) => row.date && row.value !== null)
			.sort((left, right) => left.date.localeCompare(right.date));
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

	async #fetchStatusInvestDividendEvents(ticker, assetClass = 'fii') {
		const rawTicker = String(ticker || '').toLowerCase().replace(/\.sa$/i, '');
		const slug = rawTicker.replace(/[^a-z0-9]/g, '');
		if (!slug) return [];

		const category = String(assetClass || '').toLowerCase() === 'fii' ? 'fundos-imobiliarios' : 'acoes';
		const sourceUrl = `https://statusinvest.com.br/${category}/${slug}`;
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
				if (dateMatches.length < 2) continue;

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
				const normalizedType = String(type).toLowerCase();
				const eventType =
					normalizedType.includes('jcp') || normalizedType.includes('juros')
						? 'jcp'
						: normalizedType.includes('amort')
							? 'amortization'
							: normalizedType.includes('rend')
								? 'rendimento'
						: 'dividend';

			if (!valueText && !/divid|rend|juro|provent|amort/i.test(cells.join(' '))) continue;

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
			const value = numeric(asset.market_value, 0);
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
		const fromDate = series[0].date;
		const toDate = series[series.length - 1].date;
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
				.filter((row) => row.date >= fromDate && row.date <= toDate)
				.map((row) => [String(row.date).slice(0, 7), numeric(row.value, 0)])
		);

		let inflationFactor = 1;
		const output = [];
		for (const point of series) {
			const month = point.date.slice(0, 7);
			const ipca = numeric(ipcaByMonth.get(month), 0) / 100;
			inflationFactor *= 1 + ipca;
			output.push({
				date: point.date,
				real_value: inflationFactor > 0 ? numeric(point.value, 0) / inflationFactor : numeric(point.value, 0),
				nominal_value: numeric(point.value, 0),
			});
		}
		return output;
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
