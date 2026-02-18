class MemoryCache {
	constructor(defaultTtlMs = 15 * 60 * 1000) {
		this.defaultTtlMs = defaultTtlMs;
		this.items = new Map();
		this.hitCount = 0;
		this.missCount = 0;
		this.setCount = 0;
		this.deleteCount = 0;
		this.clearCount = 0;
	}

	set(key, value, ttlMs = this.defaultTtlMs) {
		this.items.set(key, {
			value,
			expiresAt: Date.now() + Math.max(0, ttlMs),
		});
		this.setCount += 1;
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
			defaultTtlMs: this.defaultTtlMs,
			hitCount: this.hitCount,
			missCount: this.missCount,
			setCount: this.setCount,
			deleteCount: this.deleteCount,
			clearCount: this.clearCount,
		};
	}
}

module.exports = {
	MemoryCache,
};
