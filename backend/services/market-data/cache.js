const DEFAULT_MAX_ENTRIES = 500;

class MemoryCache {
	constructor(defaultTtlMs = 15 * 60 * 1000, maxEntries = DEFAULT_MAX_ENTRIES) {
		this.defaultTtlMs = defaultTtlMs;
		this.maxEntries = maxEntries;
		this.items = new Map();
		this.hitCount = 0;
		this.missCount = 0;
		this.setCount = 0;
		this.evictCount = 0;
		this.deleteCount = 0;
		this.clearCount = 0;
	}

	set(key, value, ttlMs = this.defaultTtlMs) {
		this.items.set(key, {
			value,
			expiresAt: Date.now() + Math.max(0, ttlMs),
		});
		this.setCount += 1;
		this.clearExpired();
		if (this.items.size > this.maxEntries) {
			const overflow = this.items.size - this.maxEntries;
			const sorted = Array.from(this.items.entries())
				.sort((a, b) => a[1].expiresAt - b[1].expiresAt)
				.slice(0, overflow);
			for (const [evictKey] of sorted) {
				this.items.delete(evictKey);
				this.evictCount += 1;
			}
		}
	}

	get(key) {
		const cached = this.items.get(key);
		if (!cached) {
			this.missCount += 1;
			return null;
		}
		if (Date.now() > cached.expiresAt) {
			this.items.delete(key);
			this.missCount += 1;
			return null;
		}
		this.hitCount += 1;
		return cached.value;
	}

	delete(key) {
		this.items.delete(key);
		this.deleteCount += 1;
	}

	clearExpired() {
		const now = Date.now();
		for (const [key, item] of this.items.entries()) {
			if (now > item.expiresAt) this.items.delete(key);
		}
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
			hitCount: this.hitCount,
			missCount: this.missCount,
			setCount: this.setCount,
			evictCount: this.evictCount,
			deleteCount: this.deleteCount,
			clearCount: this.clearCount,
		};
	}
}

module.exports = {
	MemoryCache,
};
