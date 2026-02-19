const { QueryCommand, UpdateCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { YahooApiProvider } = require('./providers/yahoo-api-provider');
const { TesouroProvider } = require('./providers/tesouro-provider');
const { FallbackManager } = require('./fallback/fallback-manager');
const { createThrottledScheduler } = require('./throttle');
const {
	nowIso,
	truncateForLog,
	toNumberOrNull,
} = require('./utils');
const {
	resolveAssetMarket,
	resolveYahooSymbol,
	TESOURO_MARKET,
} = require('./symbol-resolver');

/**
 * @typedef {Object} AssetRecord
 * @property {string} assetId
 * @property {string} portfolioId
 * @property {string} ticker
 * @property {string} [name]
 * @property {string} [assetClass]
 * @property {string} [country]
 * @property {string} [currency]
 * @property {number} [quantity]
 */

const hasValidCurrentPrice = (payload) =>
	payload?.quote?.currentPrice !== null &&
	payload?.quote?.currentPrice !== undefined &&
	payload?.quote?.currentPrice !== '' &&
	Number.isFinite(Number(payload.quote.currentPrice));

class AssetMarketDataService {
	constructor(options = {}) {
		this.dynamo = options.dynamo;
		this.tableName = options.tableName || process.env.TABLE_NAME || 'wealth-main';
		this.logger = options.logger || console;
		this.yahooProvider =
			options.yahooProvider || new YahooApiProvider(options);
		this.tesouroProvider =
			options.tesouroProvider || new TesouroProvider(options);
		this.fallbackManager =
			options.fallbackManager || new FallbackManager(options);
		this.schedule =
			options.scheduler ||
			createThrottledScheduler({
				minDelayMs: Number(
					options.minDelayMs || process.env.MARKET_DATA_MIN_DELAY_MS || 300
				),
				maxConcurrent: Number(
					options.maxConcurrent || process.env.MARKET_DATA_MAX_CONCURRENT || 2
				),
			});
	}

	/**
	 * Main service interface requested by the product requirements.
	 * Returns normalized sections (quote/fundamentals/historical), while preserving
	 * all provider fields under `raw` to avoid data loss when new fields appear.
	 */
	async fetchAssetData(ticker, market, context = {}, options = {}) {
		const fetchedAt = nowIso();
		const normalizedMarket = String(market || 'US').toUpperCase();
		const historyDays = Number(options.historyDays) || 30;

		let primarySource = null;
		let primaryError = null;

		try {
			if (normalizedMarket === TESOURO_MARKET) {
				primarySource = await this.tesouroProvider.fetch(ticker);
			} else {
				const yahooSymbol = resolveYahooSymbol(ticker, normalizedMarket);
				primarySource = await this.yahooProvider.fetch(yahooSymbol, {
					historyDays,
				});
			}
		} catch (error) {
			primaryError = error;
		}

		let selected = primarySource;
		let usedFallback = false;

		if (!hasValidCurrentPrice(primarySource)) {
			usedFallback = true;
			const fallbackPayload = await this.fallbackManager.fetch({
				ticker,
				market: normalizedMarket,
				assetClass: context.assetClass,
				country: context.country,
			});
			if (hasValidCurrentPrice(fallbackPayload) || fallbackPayload?.data_source === 'unavailable') {
				selected = fallbackPayload;
			}
		}

		const payload = {
			ticker: String(ticker || '').toUpperCase(),
			market: normalizedMarket,
			data_source: selected?.data_source || 'unavailable',
			is_scraped: Boolean(selected?.is_scraped),
			fetched_at: fetchedAt,
			quote: selected?.quote || {
				currentPrice: null,
				currency: null,
				change: null,
				changePercent: null,
				previousClose: null,
				marketCap: null,
				volume: null,
			},
			fundamentals: selected?.fundamentals || {},
			historical: selected?.historical || {
				history_30d: [],
				dividends: [],
			},
			raw: {
				primary_error: primaryError
					? {
						name: primaryError.name,
						message: primaryError.message,
					}
					: null,
				primary_payload: primarySource?.raw || null,
				final_payload: selected?.raw || null,
				fallback_trace: selected?.fallback_trace || selected?.raw?.fallback_attempts || [],
			},
		};

		this.logger.log(
			JSON.stringify({
				event: 'asset_data_fetch',
				status: hasValidCurrentPrice(payload) ? 'success' : 'degraded',
				ticker: payload.ticker,
				market: payload.market,
				data_source: payload.data_source,
				is_scraped: payload.is_scraped,
				used_fallback: usedFallback,
				fetched_at: payload.fetched_at,
				quote: payload.quote,
				raw_excerpt: truncateForLog(payload.raw, 1200),
			})
		);

		return payload;
	}

	async refreshPortfolioAssets(portfolioId, options = {}) {
		const assets = await this.#listPortfolioAssets(portfolioId);
		const filteredAssets = options.assetId
			? assets.filter((asset) => asset.assetId === options.assetId)
			: assets;

		const tasks = filteredAssets.map((asset) =>
			this.schedule(() => this.refreshSingleAsset(asset))
		);
		const settled = await Promise.all(tasks);

		const updated = settled.filter((item) => item.status === 'updated').length;
		const failed = settled.filter((item) => item.status === 'failed').length;

		return {
			portfolioId,
			processed: filteredAssets.length,
			updated,
			failed,
			results: settled,
		};
	}

	/**
	 * Refreshes one asset without throwing, so batch refresh does not break.
	 */
	async refreshSingleAsset(asset) {
		try {
			const market = resolveAssetMarket(asset);
			const payload = await this.fetchAssetData(asset.ticker, market, asset);
			await this.persistAssetData(asset, payload);

			return {
				assetId: asset.assetId,
				ticker: asset.ticker,
				status: 'updated',
				data_source: payload.data_source,
				is_scraped: payload.is_scraped,
				fetched_at: payload.fetched_at,
				currentPrice: payload.quote.currentPrice,
			};
		} catch (error) {
			this.logger.error(
				JSON.stringify({
					event: 'asset_data_refresh_failed',
					assetId: asset.assetId,
					ticker: asset.ticker,
					error: error.message,
					stack: error.stack,
					fetched_at: nowIso(),
				})
			);

			// Persist explicit unavailability to make failure auditable in the DB.
			const unavailablePayload = {
				ticker: asset.ticker,
				market: resolveAssetMarket(asset),
				data_source: 'unavailable',
				is_scraped: false,
				fetched_at: nowIso(),
				quote: {
					currentPrice: null,
					currency: asset.currency || null,
					change: null,
					changePercent: null,
					previousClose: null,
					marketCap: null,
					volume: null,
				},
				fundamentals: {},
				historical: {
					history_30d: [],
					dividends: [],
				},
				raw: {
					error: {
						message: error.message,
						name: error.name,
					},
				},
			};
			await this.persistAssetData(asset, unavailablePayload);

			return {
				assetId: asset.assetId,
				ticker: asset.ticker,
				status: 'failed',
				data_source: 'unavailable',
				error: error.message,
			};
		}
	}

	async persistAssetData(asset, payload) {
		const fetchedAt = payload.fetched_at || nowIso();
		const currentPrice = toNumberOrNull(payload?.quote?.currentPrice);
		const quantity = toNumberOrNull(asset.quantity) || 0;
		const currentValue =
			currentPrice !== null ? Number((currentPrice * quantity).toFixed(2)) : null;
		const snapshotId = `${fetchedAt.replace(/[^\d]/g, '')}-${Math.random()
			.toString(36)
			.slice(2, 8)}`;

		const assetKey = {
			PK: `PORTFOLIO#${asset.portfolioId}`,
			SK: `ASSET#${asset.assetId}`,
		};

		// Update the primary asset record with latest quote info used by portfolio value.
		await this.dynamo.send(
			new UpdateCommand({
				TableName: this.tableName,
				Key: assetKey,
				UpdateExpression:
					'SET currentPrice = :currentPrice, currentValue = :currentValue, lastPriceSource = :source, lastPriceAt = :priceAt, updatedAt = :updatedAt',
				ExpressionAttributeValues: {
					':currentPrice': currentPrice,
					':currentValue': currentValue,
					':source': payload.data_source,
					':priceAt': fetchedAt,
					':updatedAt': nowIso(),
				},
			})
		);

		const detailItem = {
			PK: `PORTFOLIO#${asset.portfolioId}`,
			SK: `ASSET_DETAIL#${asset.assetId}#${snapshotId}`,
			entityType: 'ASSET_DETAIL',
			portfolioId: asset.portfolioId,
			assetId: asset.assetId,
			ticker: asset.ticker,
			market: payload.market,
			data_source: payload.data_source,
			is_scraped: Boolean(payload.is_scraped),
			fetched_at: fetchedAt,
			quote: payload.quote,
			fundamentals: payload.fundamentals,
			historical: payload.historical,
			raw: payload.raw,
			createdAt: fetchedAt,
		};

		const latestDetailItem = {
			...detailItem,
			SK: `ASSET_DETAIL_LATEST#${asset.assetId}`,
			entityType: 'ASSET_DETAIL_LATEST',
		};

		// Keep immutable snapshots + a latest pointer for fast reads.
		await this.dynamo.send(
			new PutCommand({
				TableName: this.tableName,
				Item: detailItem,
			})
		);
		await this.dynamo.send(
			new PutCommand({
				TableName: this.tableName,
				Item: latestDetailItem,
			})
		);
	}

	async runScraperHealthCheck() {
		return this.fallbackManager.healthCheckScrapers();
	}

	async #listPortfolioAssets(portfolioId) {
		const result = await this.dynamo.send(
			new QueryCommand({
				TableName: this.tableName,
				KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
				ExpressionAttributeValues: {
					':pk': `PORTFOLIO#${portfolioId}`,
					':sk': 'ASSET#',
				},
			})
		);
		return Array.isArray(result.Items) ? result.Items : [];
	}
}

module.exports = {
	AssetMarketDataService,
	hasValidCurrentPrice,
};
