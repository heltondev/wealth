class ApiResponseCache {
	constructor(options = {}) {
		this.defaultTtlMs = Number(options.defaultTtlMs || 20 * 1000);
		this.maxEntries = Number(options.maxEntries || 400);
		this.maxBodyBytes = Number(options.maxBodyBytes || 512 * 1024);
		this.items = new Map();
		this.hitCount = 0;
		this.missCount = 0;
		this.storeCount = 0;
		this.storeSkipCount = 0;
		this.invalidateCount = 0;
		this.invalidatedEntriesCount = 0;
		this.clearCount = 0;
	}

	normalizeTtl(ttlMs) {
		const parsed = Number(ttlMs);
		if (!Number.isFinite(parsed)) return this.defaultTtlMs;
		return Math.max(0, parsed);
	}

	pruneExpired(now = Date.now()) {
		for (const [key, entry] of this.items.entries()) {
			if (entry.expiresAt <= now) {
				this.items.delete(key);
			}
		}
	}

	pruneOverflow() {
		const overflow = this.items.size - this.maxEntries;
		if (overflow <= 0) return;

		const ordered = Array.from(this.items.entries())
			.sort((left, right) => left[1].expiresAt - right[1].expiresAt)
			.slice(0, overflow);

		for (const [key] of ordered) {
			this.items.delete(key);
		}
	}

	get(key) {
		if (!key) return null;
		const entry = this.items.get(key);
		if (!entry) {
			this.missCount += 1;
			return null;
		}
		if (entry.expiresAt <= Date.now()) {
			this.items.delete(key);
			this.missCount += 1;
			return null;
		}
		this.hitCount += 1;

		return {
			statusCode: entry.statusCode,
			body: entry.body,
		};
	}

	set(key, value, ttlMs) {
		if (!key || !value) {
			this.storeSkipCount += 1;
			return false;
		}
		if (!Number.isInteger(value.statusCode)) {
			this.storeSkipCount += 1;
			return false;
		}
		if (typeof value.body !== 'string') {
			this.storeSkipCount += 1;
			return false;
		}
		const bodyBytes = Buffer.byteLength(value.body, 'utf8');
		if (bodyBytes > this.maxBodyBytes) {
			this.storeSkipCount += 1;
			return false;
		}

		const ttl = this.normalizeTtl(ttlMs);
		if (ttl <= 0) {
			this.storeSkipCount += 1;
			return false;
		}

		const now = Date.now();
		this.items.set(key, {
			statusCode: value.statusCode,
			body: value.body,
			expiresAt: now + ttl,
		});
		this.pruneExpired(now);
		this.pruneOverflow();
		this.storeCount += 1;
		return true;
	}

	invalidateByPrefix(prefix) {
		if (!prefix) return 0;
		this.invalidateCount += 1;
		let removed = 0;
		for (const key of this.items.keys()) {
			if (key.startsWith(prefix)) {
				this.items.delete(key);
				removed += 1;
			}
		}
		this.invalidatedEntriesCount += removed;
		return removed;
	}

	clear() {
		this.items.clear();
		this.clearCount += 1;
	}

	stats() {
		return {
			entries: this.items.size,
			maxEntries: this.maxEntries,
			defaultTtlMs: this.defaultTtlMs,
			maxBodyBytes: this.maxBodyBytes,
			hitCount: this.hitCount,
			missCount: this.missCount,
			storeCount: this.storeCount,
			storeSkipCount: this.storeSkipCount,
			invalidateCount: this.invalidateCount,
			invalidatedEntriesCount: this.invalidatedEntriesCount,
			clearCount: this.clearCount,
		};
	}
}

module.exports = {
	ApiResponseCache,
};
