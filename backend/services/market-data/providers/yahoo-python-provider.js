const path = require('path');
const { spawn } = require('child_process');
const { DataIncompleteError, ProviderUnavailableError } = require('../errors');
const { toNumberOrNull } = require('../utils');

class YahooPythonProvider {
	constructor(options = {}) {
		this.pythonBin = options.pythonBin || process.env.MARKET_DATA_PYTHON_BIN || 'python3';
		this.scriptPath =
			options.scriptPath ||
			path.join(__dirname, '..', 'python', 'yfinance_fetcher.py');
		this.timeoutMs = Number(options.timeoutMs || process.env.MARKET_DATA_YFINANCE_TIMEOUT_MS || 30000);
	}

	async fetch(symbol, options = {}) {
		const payload = await this.#runPythonScript({
			ticker: symbol,
			history_days: Number(options.historyDays || 30),
		});

		if (!payload || payload.ok !== true || !payload.payload) {
			const errorMessage =
				payload?.error || 'yfinance provider returned invalid payload';
			throw new ProviderUnavailableError(errorMessage, {
				symbol,
				provider: 'yfinance',
				payload,
			});
		}

		const result = payload.payload;
		const info = result.info || {};
		const fastInfo = result.fast_info || {};
		const currentPrice =
			toNumberOrNull(info.regularMarketPrice) ??
			toNumberOrNull(fastInfo.lastPrice) ??
			toNumberOrNull(fastInfo.last_price);

		if (!currentPrice) {
			throw new DataIncompleteError('yfinance did not return a current price', {
				symbol,
				payload: result,
			});
		}

		return {
			data_source: 'yfinance',
			is_scraped: false,
			quote: {
				currentPrice,
				currency: info.currency || fastInfo.currency || null,
				previousClose:
					toNumberOrNull(info.previousClose) ??
					toNumberOrNull(fastInfo.previousClose) ??
					toNumberOrNull(fastInfo.previous_close),
				change:
					toNumberOrNull(info.regularMarketChange) ??
					toNumberOrNull(fastInfo.regularMarketChange),
				changePercent:
					toNumberOrNull(info.regularMarketChangePercent) ??
					toNumberOrNull(fastInfo.regularMarketChangePercent),
				volume: toNumberOrNull(info.volume),
				marketCap: toNumberOrNull(info.marketCap),
				regularMarketTime: info.regularMarketTime || null,
			},
			fundamentals: {
				info: result.info || {},
				financials: result.financials || null,
				quarterly_financials: result.quarterly_financials || null,
				balance_sheet: result.balance_sheet || null,
				quarterly_balance_sheet: result.quarterly_balance_sheet || null,
				cashflow: result.cashflow || null,
				quarterly_cashflow: result.quarterly_cashflow || null,
				recommendations: result.recommendations || null,
				institutional_holders: result.institutional_holders || null,
				major_holders: result.major_holders || null,
				calendar: result.calendar || null,
			},
			historical: {
				history_30d: result.history || [],
				dividends: result.dividends || [],
			},
			raw: result,
		};
	}

	#runPythonScript(inputPayload) {
		return new Promise((resolve, reject) => {
			const child = spawn(this.pythonBin, [this.scriptPath], {
				stdio: ['pipe', 'pipe', 'pipe'],
				env: { ...process.env },
			});

			let stdout = '';
			let stderr = '';
			let resolved = false;

			const timeout = setTimeout(() => {
				if (resolved) return;
				resolved = true;
				child.kill('SIGKILL');
				reject(
					new ProviderUnavailableError('yfinance python helper timed out', {
						timeoutMs: this.timeoutMs,
						stderr,
					})
				);
			}, this.timeoutMs);

			child.stdout.on('data', (chunk) => {
				stdout += chunk.toString('utf8');
			});

			child.stderr.on('data', (chunk) => {
				stderr += chunk.toString('utf8');
			});

			child.on('error', (error) => {
				if (resolved) return;
				resolved = true;
				clearTimeout(timeout);
				reject(
					new ProviderUnavailableError('failed to execute yfinance helper', {
						error: error.message,
						stderr,
					})
				);
			});

			child.on('close', (code) => {
				if (resolved) return;
				resolved = true;
				clearTimeout(timeout);

				if (code !== 0) {
					reject(
						new ProviderUnavailableError('yfinance helper exited with error', {
							code,
							stderr,
							stdout,
						})
					);
					return;
				}

				try {
					resolve(JSON.parse(stdout));
				} catch (error) {
					reject(
						new ProviderUnavailableError('invalid JSON from yfinance helper', {
							error: error.message,
							stdout,
							stderr,
						})
					);
				}
			});

			child.stdin.write(JSON.stringify(inputPayload));
			child.stdin.end();
		});
	}
}

module.exports = {
	YahooPythonProvider,
};
