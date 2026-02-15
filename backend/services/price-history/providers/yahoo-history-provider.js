const path = require('path');
const { spawn } = require('child_process');
const {
	ProviderUnavailableError,
	DataIncompleteError,
} = require('../../market-data/errors');
const { toNumberOrNull } = require('../../market-data/utils');

class YahooHistoryProvider {
	constructor(options = {}) {
		this.pythonBin =
			options.pythonBin || process.env.MARKET_DATA_PYTHON_BIN || 'python3';
		this.scriptPath =
			options.scriptPath ||
			path.join(
				__dirname,
				'..',
				'python',
				'yfinance_history_fetcher.py'
			);
		this.timeoutMs = Number(
			options.timeoutMs || process.env.MARKET_DATA_YFINANCE_TIMEOUT_MS || 30000
		);
	}

	async fetchHistory(symbol, options = {}) {
		const payload = await this.#runPythonScript({
			ticker: symbol,
			start_date: options.startDate || null,
			period: options.period || (options.startDate ? null : 'max'),
			interval: '1d',
		});

		if (!payload || payload.ok !== true || !payload.payload) {
			throw new ProviderUnavailableError(
				payload?.error || 'yfinance history provider returned invalid payload',
				{
					symbol,
					payload,
				}
			);
		}

		const result = payload.payload;
		const rows = Array.isArray(result.rows) ? result.rows : [];

		if (!rows.length && !options.allowEmpty) {
			throw new DataIncompleteError('yfinance history returned empty rows', {
				symbol,
				result,
			});
		}

		return {
			data_source: 'yfinance',
			is_scraped: false,
			currency: result.currency || null,
			rows: rows.map((row) => ({
				date: row.date,
				open: toNumberOrNull(row.open),
				high: toNumberOrNull(row.high),
				low: toNumberOrNull(row.low),
				close: toNumberOrNull(row.close),
				adjusted_close: toNumberOrNull(row.adjusted_close),
				volume: toNumberOrNull(row.volume),
				dividends: toNumberOrNull(row.dividends) || 0,
				stock_splits: toNumberOrNull(row.stock_splits) || 0,
			})),
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
					new ProviderUnavailableError(
						'yfinance history helper timed out',
						{
							timeoutMs: this.timeoutMs,
							stderr,
						}
					)
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
					new ProviderUnavailableError(
						'failed to execute yfinance history helper',
						{
							error: error.message,
							stderr,
						}
					)
				);
			});

			child.on('close', (code) => {
				if (resolved) return;
				resolved = true;
				clearTimeout(timeout);

				if (code !== 0) {
					reject(
						new ProviderUnavailableError(
							'yfinance history helper exited with error',
							{
								code,
								stderr,
								stdout,
							}
						)
					);
					return;
				}

				try {
					resolve(JSON.parse(stdout));
				} catch (error) {
					reject(
						new ProviderUnavailableError(
							'invalid JSON from yfinance history helper',
							{
								error: error.message,
								stdout,
								stderr,
							}
						)
					);
				}
			});

			child.stdin.write(JSON.stringify(inputPayload));
			child.stdin.end();
		});
	}
}

module.exports = {
	YahooHistoryProvider,
};
