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

const hasGroupedThousands = (value, separator) => {
	if (!value || !separator) return false;
	const escaped = separator === '.' ? '\\.' : separator;
	const groupedPattern = new RegExp(`^\\d{1,3}(?:${escaped}\\d{3})+$`);
	return groupedPattern.test(value);
};

const normalizeNumberToken = (token) => {
	const trimmed = String(token || '').trim();
	if (!trimmed) return null;

	const sign = trimmed.startsWith('-') ? '-' : '';
	const unsigned = trimmed.replace(/^[-+]/, '');
	const commaIndex = unsigned.lastIndexOf(',');
	const dotIndex = unsigned.lastIndexOf('.');

	let decimalSeparator = null;
	if (commaIndex >= 0 && dotIndex >= 0) {
		decimalSeparator = commaIndex > dotIndex ? ',' : '.';
	} else if (commaIndex >= 0) {
		decimalSeparator = hasGroupedThousands(unsigned, ',') ? null : ',';
	} else if (dotIndex >= 0) {
		decimalSeparator = hasGroupedThousands(unsigned, '.') ? null : '.';
	}

	let normalized = unsigned;
	if (decimalSeparator) {
		const thousandsSeparator = decimalSeparator === ',' ? '.' : ',';
		const thousandsPattern = new RegExp(`\\${thousandsSeparator}`, 'g');
		normalized = normalized.replace(thousandsPattern, '');
		normalized = normalized.replace(decimalSeparator, '.');
	} else {
		normalized = normalized.replace(/[.,]/g, '');
	}

	return `${sign}${normalized}`;
};

const extractFirstNumber = (value) => {
	if (value === undefined || value === null) return null;
	const source = String(value);
	const match = source.match(/[-+]?\d[\d.,]*/);
	if (!match) return null;
	const normalized = normalizeNumberToken(match[0]);
	return toNumberOrNull(normalized);
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
