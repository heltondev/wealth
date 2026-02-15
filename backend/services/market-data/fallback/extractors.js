const { normalizeWhitespace, toNumberOrNull } = require('../utils');

const extractJsonScriptContent = (html, scriptId) => {
	if (!html) return null;
	const regex = new RegExp(
		`<script[^>]*id=["']${scriptId}["'][^>]*>([\\s\\S]*?)<\\/script>`,
		'i'
	);
	const match = String(html).match(regex);
	if (!match || !match[1]) return null;
	return match[1].trim();
};

const extractMetaContent = (html, propertyName) => {
	if (!html) return null;
	const regex = new RegExp(
		`<meta[^>]*(?:property|name)=["']${propertyName}["'][^>]*content=["']([^"']+)["'][^>]*>`,
		'i'
	);
	const match = String(html).match(regex);
	return match?.[1] ? normalizeWhitespace(match[1]) : null;
};

const extractFirstNumber = (value) => {
	if (value === undefined || value === null) return null;
	const normalized = String(value).replace(/\./g, '').replace(',', '.');
	const match = normalized.match(/-?\d+(?:\.\d+)?/);
	if (!match) return null;
	return toNumberOrNull(match[0]);
};

const extractByRegex = (html, patterns) => {
	const text = String(html || '');
	for (const pattern of patterns) {
		const match = text.match(pattern);
		if (match && match[1]) return match[1];
	}
	return null;
};

module.exports = {
	extractJsonScriptContent,
	extractMetaContent,
	extractFirstNumber,
	extractByRegex,
};
