class MemoryCache {
	constructor(defaultTtlMs = 15 * 60 * 1000) {
		this.defaultTtlMs = defaultTtlMs;
		this.items = new Map();
	}

	set(key, value, ttlMs = this.defaultTtlMs) {
		this.items.set(key, {
			value,
			expiresAt: Date.now() + Math.max(0, ttlMs),
		});
	}

	get(key) {
		const cached = this.items.get(key);
		if (!cached) return null;
		if (Date.now() > cached.expiresAt) {
			this.items.delete(key);
			return null;
		}
		return cached.value;
	}

	delete(key) {
		this.items.delete(key);
	}

	clearExpired() {
		const now = Date.now();
		for (const [key, item] of this.items.entries()) {
			if (now > item.expiresAt) this.items.delete(key);
		}
	}
}

module.exports = {
	MemoryCache,
};
