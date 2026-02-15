const { fetchWithTimeout, withRetry, toNumberOrNull } = require('../../utils');

const BCB_SERIES = {
	SELIC: 432,
	CDI: 12,
	IPCA: 433,
};

class BcbStructuredProvider {
	constructor(options = {}) {
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_BCB_TIMEOUT_MS || 15000);
	}

	async fetch(asset) {
		const market = String(asset.market || '').toUpperCase();
		if (market !== 'TESOURO' && market !== 'BR') return null;

		try {
			const [selic, cdi, ipca] = await Promise.all([
				this.#fetchSeriesLatest(BCB_SERIES.SELIC),
				this.#fetchSeriesLatest(BCB_SERIES.CDI),
				this.#fetchSeriesLatest(BCB_SERIES.IPCA),
			]);

			return {
				data_source: 'bcb_sgs',
				is_scraped: false,
				quote: {
					currentPrice: null,
					currency: 'BRL',
					change: null,
					changePercent: null,
					previousClose: null,
					marketCap: null,
					volume: null,
				},
				fundamentals: {
					bcb_rates: {
						selic,
						cdi,
						ipca,
					},
				},
				historical: {
					history_30d: [],
					dividends: [],
				},
				raw: {
					series: { selic, cdi, ipca },
				},
			};
		} catch {
			return null;
		}
	}

	async #fetchSeriesLatest(code) {
		const url = `https://api.bcb.gov.br/dados/serie/bcdata.sgs.${code}/dados/ultimos/30?formato=json`;
		const response = await withRetry(
			() => fetchWithTimeout(url, { timeoutMs: this.timeoutMs }),
			{ retries: 2, baseDelayMs: 300, factor: 2 }
		);
		if (!response.ok) {
			return null;
		}
		const data = await response.json();
		if (!Array.isArray(data) || data.length === 0) return null;

		const last = data[data.length - 1];
		return {
			date: last?.data || null,
			value: toNumberOrNull(String(last?.valor || '').replace(',', '.')),
		};
	}
}

module.exports = {
	BcbStructuredProvider,
};
