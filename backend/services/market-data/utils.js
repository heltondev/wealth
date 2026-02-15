const DEFAULT_TIMEOUT_MS = 20000;

const sleep = (ms) =>
	new Promise((resolve) => {
		setTimeout(resolve, ms);
	});

const nowIso = () => new Date().toISOString();

const isFiniteNumber = (value) => Number.isFinite(Number(value));

const toNumberOrNull = (value) => {
	if (!isFiniteNumber(value)) return null;
	return Number(value);
};

const truncateForLog = (value, maxLength = 1200) => {
	try {
		const serialized = typeof value === 'string' ? value : JSON.stringify(value);
		if (serialized.length <= maxLength) return serialized;
		return `${serialized.slice(0, maxLength)}...<truncated>`;
	} catch {
		return '<unserializable>';
	}
};

async function fetchWithTimeout(url, options = {}) {
	const timeoutMs = options.timeoutMs || DEFAULT_TIMEOUT_MS;
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), timeoutMs);

	try {
		const response = await fetch(url, {
			...options,
			signal: controller.signal,
			headers: {
				'User-Agent': 'wealthhub-market-data/1.0 (+https://wealthhub.local)',
				Accept: 'application/json,text/csv,text/plain,text/html,*/*',
				...(options.headers || {}),
			},
		});
		return response;
	} finally {
		clearTimeout(timeout);
	}
}

async function withRetry(task, options = {}) {
	const {
		retries = 2,
		baseDelayMs = 400,
		factor = 2,
		maxDelayMs = 5000,
		onRetry = null,
		shouldRetry = null,
	} = options;

	let attempt = 0;
	let lastError;

	while (attempt <= retries) {
		try {
			return await task(attempt);
		} catch (error) {
			lastError = error;
			const canRetry = attempt < retries && (!shouldRetry || shouldRetry(error));
			if (!canRetry) break;

			const delayMs = Math.min(baseDelayMs * factor ** attempt, maxDelayMs);
			if (typeof onRetry === 'function') {
				onRetry(error, attempt + 1, delayMs);
			}
			await sleep(delayMs);
		}
		attempt += 1;
	}

	throw lastError;
}

const normalizeWhitespace = (value) =>
	String(value || '')
		.replace(/\s+/g, ' ')
		.trim();

const parseCsv = (rawCsv) => {
	const lines = String(rawCsv || '')
		.split(/\r?\n/)
		.filter((line) => line.trim().length > 0);
	if (lines.length === 0) return [];

	const parseLine = (line) => {
		const values = [];
		let current = '';
		let inQuotes = false;

		for (let index = 0; index < line.length; index += 1) {
			const char = line[index];
			const next = line[index + 1];

			if (char === '"' && inQuotes && next === '"') {
				current += '"';
				index += 1;
				continue;
			}

			if (char === '"') {
				inQuotes = !inQuotes;
				continue;
			}

			if (char === ',' && !inQuotes) {
				values.push(current);
				current = '';
				continue;
			}

			current += char;
		}

		values.push(current);
		return values.map((value) => value.trim());
	};

	const headers = parseLine(lines[0]);
	return lines.slice(1).map((line) => {
		const rowValues = parseLine(line);
		const row = {};
		headers.forEach((header, index) => {
			row[header] = rowValues[index] ?? '';
		});
		return row;
	});
};

module.exports = {
	DEFAULT_TIMEOUT_MS,
	sleep,
	nowIso,
	isFiniteNumber,
	toNumberOrNull,
	truncateForLog,
	fetchWithTimeout,
	withRetry,
	normalizeWhitespace,
	parseCsv,
};
