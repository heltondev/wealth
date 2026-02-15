const { GoogleFinanceStructuredProvider } = require('./structured/google-finance-provider');
const { BcbStructuredProvider } = require('./structured/bcb-provider');
const { StatusInvestStructuredProvider } = require('./structured/status-invest-provider');
const { GoogleFinanceScraper } = require('./scrapers/google-finance-scraper');
const { StatusInvestScraper } = require('./scrapers/status-invest-scraper');
const { FundamentusScraper } = require('./scrapers/fundamentus-scraper');
const { TesouroDiretoScraper } = require('./scrapers/tesouro-direto-scraper');
const { YahooFinanceScraper } = require('./scrapers/yahoo-finance-scraper');
const { nowIso } = require('../utils');

const hasQuotePrice = (payload) =>
	Number.isFinite(Number(payload?.quote?.currentPrice));

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

		for (const provider of this.structuredProviders) {
			const providerName = provider.constructor.name;
			try {
				const result = await provider.fetch(asset);
				if (!result) {
					attempts.push({ source: providerName, status: 'empty' });
					continue;
				}

				// Structured fallback without quote price is still useful context, but not
				// enough to update the portfolio valuation.
				if (!hasQuotePrice(result)) {
					attempts.push({
						source: providerName,
						status: 'partial',
						message: 'missing_current_price',
					});
					continue;
				}

				return {
					...result,
					fallback_trace: attempts,
				};
			} catch (error) {
				attempts.push({
					source: providerName,
					status: 'error',
					message: error.message,
				});
			}
		}

		for (const scraper of this.scrapers) {
			try {
				const result = await scraper.fetch(asset);
				if (!result) {
					attempts.push({ source: scraper.name, status: 'empty' });
					continue;
				}
				if (!hasQuotePrice(result)) {
					attempts.push({
						source: scraper.name,
						status: 'partial',
						message: 'missing_current_price',
					});
					continue;
				}

				return {
					...result,
					fallback_trace: attempts,
				};
			} catch (error) {
				attempts.push({
					source: scraper.name,
					status: 'error',
					message: error.message,
				});
			}
		}

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
			fundamentals: {},
			historical: {
				history_30d: [],
				dividends: [],
			},
			raw: {
				fallback_attempts: attempts,
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
