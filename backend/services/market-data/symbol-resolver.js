const TESOURO_MARKET = 'TESOURO';

const resolveAssetMarket = (asset) => {
	if (!asset) return 'US';
	const ticker = String(asset.ticker || '').toUpperCase();
	const assetClass = String(asset.assetClass || '').toLowerCase();
	const country = String(asset.country || '').toUpperCase();

	// Bond-like Brazilian holdings are treated as Tesouro path first.
	if (
		ticker.startsWith('TESOURO') ||
		(assetClass === 'bond' && (country === 'BR' || !country))
	) {
		return TESOURO_MARKET;
	}

	if (country === 'BR') return 'BR';
	if (country === 'CA') return 'CA';
	return 'US';
};

const resolveYahooSymbol = (ticker, market) => {
	const normalizedTicker = String(ticker || '').trim().toUpperCase();
	if (!normalizedTicker) return '';

	if (market === 'BR') return normalizedTicker.endsWith('.SA') ? normalizedTicker : `${normalizedTicker}.SA`;
	if (market === 'CA') return normalizedTicker.endsWith('.TO') ? normalizedTicker : `${normalizedTicker}.TO`;
	return normalizedTicker;
};

const normalizeTesouroType = (ticker) => {
	const value = String(ticker || '').toUpperCase();
	if (value.includes('IPCA')) return 'NTN-B';
	if (value.includes('SELIC') || value.includes('LFT')) return 'LFT';
	if (value.includes('NTN-F') || value.includes('PREFIXADO COM JUROS')) return 'NTN-F';
	if (value.includes('LTN') || value.includes('PREFIXADO')) return 'LTN';
	return 'UNKNOWN';
};

const normalizeTesouroMaturity = (ticker) => {
	const value = String(ticker || '').toUpperCase();
	const dateMatch = value.match(/(\d{2})\/(\d{2})\/(\d{4})/);
	if (dateMatch) {
		return `${dateMatch[3]}-${dateMatch[2]}-${dateMatch[1]}`;
	}

	const yearMatch = value.match(/(20\d{2})/);
	if (!yearMatch) return null;
	return `${yearMatch[1]}-01-01`;
};

module.exports = {
	TESOURO_MARKET,
	resolveAssetMarket,
	resolveYahooSymbol,
	normalizeTesouroType,
	normalizeTesouroMaturity,
};
