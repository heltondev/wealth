const test = require('node:test');
const assert = require('node:assert/strict');

const { FundsExplorerProvider } = require('./fundsexplorer-provider');

const withMockedFetch = async (fetchImpl, task) => {
	const originalFetch = global.fetch;
	global.fetch = fetchImpl;
	try {
		return await task();
	} finally {
		global.fetch = originalFetch;
	}
};

const buildHtml = (slides) => `
<html><body>
<div class="swiper mySwiper mySwiper--locationGrid">
<div class="swiper-wrapper" data-element="properties-swiper-container">
${slides}
</div>
<div class="swiper-button-next"></div>
</div>
</body></html>
`;

const HGLG_SLIDES = `
<div class="swiper-slide">
	<div class="locationGrid__title">HGLG Itupeva</div>
	<ul>
		<li><b>Endereço: </b>Estrada Joaquim Bueno Neto</li>
		<li><b>Bairro: </b></li>
		<li><b>Cidade: </b>Itupeva - SP</li>
		<li><b>Área Bruta Locável: </b>90.000,00 m2</li>
	</ul>
</div>
<div class="swiper-slide">
	<div class="locationGrid__title">HGLG Vinhedo</div>
	<ul>
		<li><b>Endereço: </b>Avenida das Indústrias</li>
		<li><b>Bairro: </b>Vinhedo</li>
		<li><b>Cidade: </b>Vinhedo - SP</li>
		<li><b>Área Bruta Locável: </b>10.000,00 m2</li>
	</ul>
</div>
`;

test('FundsExplorerProvider parses property slides into portfolio rows', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('fundsexplorer.com.br/funds/hglg11')) {
			return new Response(buildHtml(HGLG_SLIDES), {
				status: 200,
				headers: { 'content-type': 'text/html' },
			});
		}
		throw new Error(`Unexpected URL: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetch({ ticker: 'HGLG11', market: 'BR', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.data_source, 'fundsexplorer');
	assert.equal(payload.fund_portfolio.length, 2);

	const itupeva = payload.fund_portfolio[0];
	assert.equal(itupeva.label, 'HGLG Itupeva');
	assert.equal(itupeva.category, 'Itupeva - SP');
	assert.equal(itupeva.source, 'fundsexplorer');
	assert.ok(Math.abs(itupeva.allocation_pct - 90) < 0.01);

	const vinhedo = payload.fund_portfolio[1];
	assert.equal(vinhedo.label, 'HGLG Vinhedo');
	assert.equal(vinhedo.category, 'Vinhedo - SP');
	assert.ok(Math.abs(vinhedo.allocation_pct - 10) < 0.01);
});

test('FundsExplorerProvider returns null for non-BR market', async () => {
	const provider = new FundsExplorerProvider();
	const payload = await provider.fetch({ ticker: 'AAPL', market: 'US', assetClass: 'stock' });
	assert.equal(payload, null);
});

test('FundsExplorerProvider returns null for non-FII asset class', async () => {
	const provider = new FundsExplorerProvider();
	const payload = await provider.fetch({ ticker: 'PETR4', market: 'BR', assetClass: 'stock' });
	assert.equal(payload, null);
});

test('FundsExplorerProvider returns null on HTTP error', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async () =>
		new Response('Not Found', { status: 404, headers: { 'content-type': 'text/html' } });

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetch({ ticker: 'XYZZ11', market: 'BR', assetClass: 'fii' })
	);
	assert.equal(payload, null);
});

test('FundsExplorerProvider returns empty portfolio when no properties section', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async () =>
		new Response('<html><body><h1>Some page</h1></body></html>', {
			status: 200,
			headers: { 'content-type': 'text/html' },
		});

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetch({ ticker: 'ALZR11', market: 'BR', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.fund_portfolio.length, 0);
});
