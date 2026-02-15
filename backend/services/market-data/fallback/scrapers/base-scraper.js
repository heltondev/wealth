const { withRetry, nowIso } = require('../../utils');

class BaseScraper {
	constructor(options = {}) {
		this.name = options.name || 'base_scraper';
		this.cache = options.cache;
		this.cacheTtlMs = Number(options.cacheTtlMs || 15 * 60 * 1000);
	}

	cacheKey(asset) {
		return `${this.name}:${asset.market}:${asset.ticker}`;
	}

	canHandle() {
		return true;
	}

	async fetch(asset) {
		if (!this.canHandle(asset)) return null;

		const key = this.cacheKey(asset);
		const cached = this.cache?.get(key);
		if (cached) return cached;

		const data = await withRetry(
			() => this.scrape(asset),
			{
				retries: 2,
				baseDelayMs: 600,
				factor: 2,
			}
		);

		if (data && this.cache) {
			this.cache.set(key, data, this.cacheTtlMs);
		}

		return data;
	}

	async scrape() {
		throw new Error(`scrape() not implemented for ${this.name}`);
	}

	async healthCheck() {
		return {
			scraper: this.name,
			ok: true,
			checked_at: nowIso(),
			details: 'No health check implementation',
		};
	}
}

module.exports = {
	BaseScraper,
};
