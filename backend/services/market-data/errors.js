class ProviderError extends Error {
	constructor(message, details = {}) {
		super(message);
		this.name = 'ProviderError';
		this.details = details;
	}
}

class ProviderUnavailableError extends ProviderError {
	constructor(message, details = {}) {
		super(message, details);
		this.name = 'ProviderUnavailableError';
	}
}

class DataIncompleteError extends ProviderError {
	constructor(message, details = {}) {
		super(message, details);
		this.name = 'DataIncompleteError';
	}
}

module.exports = {
	ProviderError,
	ProviderUnavailableError,
	DataIncompleteError,
};
