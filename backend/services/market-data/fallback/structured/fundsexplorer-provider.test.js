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

const buildHtml = (slides, extra = '') => `
<html><body>
<div class="swiper mySwiper mySwiper--locationGrid">
<div class="swiper-wrapper" data-element="properties-swiper-container">
${slides}
</div>
<div class="swiper-button-next"></div>
</div>
${extra}
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

const DESCRIPTION_SECTION = `
<section id="carbon_fields_fiis_description-3" class="widget carbon_fields_fiis_description">
	<h2 class="wrapper extraTitle">Descrição do HGLG11</h2>
	<div class="wrapper newsContent">
		<article class="newsContent__article">
			<h3>HGLG11: Pátria Logística</h3>
			<b>PÁTRIA LOG - FUNDO DE INVESTIMENTO IMOBILIÁRIO</b>
			HGLG11 é um fundo imobiliário do tipo tijolo.
			<br />
			Atua em galpões logísticos.
		</article>
	</div>
</section>
`;

test('FundsExplorerProvider parses property slides into portfolio rows', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('fundsexplorer.com.br/funds/hglg11')) {
			return new Response(buildHtml(HGLG_SLIDES, DESCRIPTION_SECTION), {
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
	assert.equal(itupeva.name, 'HGLG Itupeva');
	assert.equal(itupeva.category, 'Itupeva - SP');
	assert.equal(itupeva.source, 'fundsexplorer');
	assert.ok(Math.abs(itupeva.allocation_pct - 90) < 0.01);

	const vinhedo = payload.fund_portfolio[1];
	assert.equal(vinhedo.name, 'HGLG Vinhedo');
	assert.equal(vinhedo.category, 'Vinhedo - SP');
	assert.ok(Math.abs(vinhedo.allocation_pct - 10) < 0.01);

	assert.equal(payload.fund_info.source, 'fundsexplorer');
	assert.ok(payload.fund_info.description_html.includes('<article>'));
	assert.ok(payload.fund_info.description_html.includes('<h3>'));
	assert.ok(
		payload.fund_info.description.includes('HGLG11 é um fundo imobiliário do tipo tijolo.')
	);
	assert.ok(payload.fund_info.description.includes('Atua em galpões logísticos.'));
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

test('FundsExplorerProvider falls back to dataLayer description when section is missing', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });
	const dataLayerContent = JSON.stringify({
		pagePostTerms: {
			meta: {
				tudo_sobre: 'Descrição vinda do dataLayer.',
			},
		},
	});

	const fetchImpl = async (url) => {
		if (url.includes('fundsexplorer.com.br/funds/btlg11')) {
			return new Response(
				buildHtml(
					'',
					`<script>var dataLayer_content = ${dataLayerContent}; dataLayer.push( dataLayer_content );</script>`
				),
				{
					status: 200,
					headers: { 'content-type': 'text/html' },
				}
			);
		}
		throw new Error(`Unexpected URL: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetch({ ticker: 'BTLG11', market: 'BR', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.fund_info.description, 'Descrição vinda do dataLayer.');
});

test('FundsExplorerProvider falls back to meta description when section and dataLayer are missing', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('fundsexplorer.com.br/funds/bcff11')) {
			return new Response(
				buildHtml(
					'',
					'<meta name="description" content="Resumo do BCFF11 vindo do meta description." />'
				),
				{
					status: 200,
					headers: { 'content-type': 'text/html' },
				}
			);
		}
		throw new Error(`Unexpected URL: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetch({ ticker: 'BCFF11', market: 'BR', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.fund_info.description, 'Resumo do BCFF11 vindo do meta description.');
});

const buildEmissionsHtml = (cards) => `
<html><body>
<div class="emissoesSearch__results">
${cards}
</div>
</body></html>
`;

const EMISSION_CARD_ACTIVE = `
<div class="emissaoCard" data-ticker="hglg11" data-stage="ativo" data-offer="476">
	<div class="wrapper emissoesGrid">
		<div aria-status="Em andamento" class="emissoesGrid__box">
			<div aria-label="Ticker" class="emissoesGrid__box__row">
				<a class="rowTicker" href="https://www.fundsexplorer.com.br/funds/hglg11">HGLG11</a>
				<span class="rowPill">11ª emissão</span>
			</div>
			<div aria-label="Preço R$ Desconto" class="emissoesGrid__box__row">
				<p>166,58</p>
				<span class="rowPill--baixa">-5,82</span>
			</div>
			<div aria-label="Data-base" class="emissoesGrid__box__row">
				<p>10.02.26</p>
			</div>
			<div aria-label="Fator de proporcao" class="emissoesGrid__box__row">
				<p>9,91%</p>
			</div>
			<div aria-label="Período de preferência" class="emissoesGrid__box__row">
				<div class="emissoesRange">
					<div class="emissoesRange__progress" style="--prog:21%;" data-status="Em andamento">
						<div><i></i></div><div><i></i></div><div><i></i></div>
					</div>
					<div class="emissoesRange__date">
						<div>13.02.26</div>
						<div>27.02.26</div>
					</div>
				</div>
			</div>
			<div aria-label="Período de sobras" class="emissoesGrid__box__row">
				<div class="emissoesRange">
					<div class="emissoesRange__progress" style="--prog=0%" data-status="A definir">
						<div><i></i></div><div><i></i></div><div><i></i></div>
					</div>
					<div class="emissoesRange__date">
						<div>A definir</div>
						<div>A definir</div>
					</div>
				</div>
			</div>
			<div aria-label="Período público" class="emissoesGrid__box__row">
				<div class="emissoesRange">
					<div class="emissoesRange__progress" style="--prog=0%" data-status="Não há data">
						<div><i></i></div><div><i></i></div><div><i></i></div>
					</div>
					<div class="emissoesRange__date">
						<div>Não há</div>
						<div>Não há</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
`;

const EMISSION_CARD_ALTA = `
<div class="emissaoCard" data-ticker="newu11" data-stage="ativo" data-offer="476">
	<div class="wrapper emissoesGrid">
		<div aria-status="Em andamento" class="emissoesGrid__box">
			<div aria-label="Ticker" class="emissoesGrid__box__row">
				<a class="rowTicker" href="https://www.fundsexplorer.com.br/funds/newu11">NEWU11</a>
				<span class="rowPill">2ª emissão</span>
			</div>
			<div aria-label="Preço R$ Desconto" class="emissoesGrid__box__row">
				<p>85,97</p>
				<span class="rowPill--alta">24,46</span>
			</div>
			<div aria-label="Data-base" class="emissoesGrid__box__row">
				<p>29.01.26</p>
			</div>
			<div aria-label="Fator de proporcao" class="emissoesGrid__box__row">
				<p>440,12%</p>
			</div>
			<div aria-label="Período de preferência" class="emissoesGrid__box__row">
				<div class="emissoesRange">
					<div class="emissoesRange__progress" data-status="Data atingida">
						<div><i></i></div><div><i></i></div><div><i></i></div>
					</div>
					<div class="emissoesRange__date">
						<div>02.02.26</div>
						<div>12.02.26</div>
					</div>
				</div>
			</div>
			<div aria-label="Período de sobras" class="emissoesGrid__box__row">
				<div class="emissoesRange">
					<div class="emissoesRange__progress" data-status="A definir">
						<div><i></i></div><div><i></i></div><div><i></i></div>
					</div>
					<div class="emissoesRange__date">
						<div>A definir</div>
						<div>A definir</div>
					</div>
				</div>
			</div>
			<div aria-label="Período público" class="emissoesGrid__box__row">
				<div class="emissoesRange">
					<div class="emissoesRange__progress" data-status="Não há data">
						<div><i></i></div><div><i></i></div><div><i></i></div>
					</div>
					<div class="emissoesRange__date">
						<div>Não há</div>
						<div>Não há</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
`;

test('fetchEmissions parses emission cards and filters by ticker', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('emissoes-ipos')) {
			// Global listing page: contains both HGLG11 and NEWU11 cards
			return new Response(buildEmissionsHtml(EMISSION_CARD_ACTIVE + EMISSION_CARD_ALTA), {
				status: 200,
				headers: { 'content-type': 'text/html' },
			});
		}
		if (url.includes('fundsexplorer.com.br/funds/hglg11')) {
			// Fund detail page: also has the same HGLG11 card (tests dedup)
			return new Response(buildEmissionsHtml(EMISSION_CARD_ACTIVE), {
				status: 200,
				headers: { 'content-type': 'text/html' },
			});
		}
		throw new Error(`Unexpected URL: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetchEmissions({ ticker: 'HGLG11', market: 'BR', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.data_source, 'fundsexplorer');
	assert.equal(payload.ticker, 'HGLG11');
	// Only HGLG11 cards are returned (NEWU11 filtered out), duplicates removed
	assert.equal(payload.emissions.length, 1);

	const first = payload.emissions[0];
	assert.equal(first.ticker, 'HGLG11');
	assert.equal(first.emissionNumber, 11);
	assert.equal(first.stage, 'ativo');
	assert.ok(Math.abs(first.price - 166.58) < 0.01);
	assert.ok(first.discount < 0, 'discount should be negative for baixa');
	assert.ok(Math.abs(first.discount - (-5.82)) < 0.01);
	assert.equal(first.baseDate, '10.02.26');
	assert.equal(first.proportionFactor, '9,91%');
	assert.equal(first.preferenceStart, '13.02.26');
	assert.equal(first.preferenceEnd, '27.02.26');
	assert.equal(first.preferenceStatus, 'Em andamento');
	assert.equal(first.sobrasStart, 'A definir');
	assert.equal(first.sobrasStatus, 'A definir');
	assert.equal(first.publicStatus, 'Não há data');
});

test('fetchEmissions parses alta discount correctly', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('emissoes-ipos')) {
			return new Response(buildEmissionsHtml(EMISSION_CARD_ALTA), {
				status: 200,
				headers: { 'content-type': 'text/html' },
			});
		}
		if (url.includes('fundsexplorer.com.br/funds/')) {
			return new Response('<html></html>', { status: 200, headers: { 'content-type': 'text/html' } });
		}
		throw new Error(`Unexpected URL: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetchEmissions({ ticker: 'NEWU11', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.emissions.length, 1);
	const emission = payload.emissions[0];
	assert.equal(emission.ticker, 'NEWU11');
	assert.equal(emission.emissionNumber, 2);
	assert.ok(emission.discount > 0, 'discount should be positive for alta');
	assert.ok(Math.abs(emission.discount - 24.46) < 0.01);
	assert.equal(emission.preferenceStatus, 'Data atingida');
});

test('fetchEmissions works without market field on asset', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async (url) => {
		if (url.includes('emissoes-ipos')) {
			return new Response(buildEmissionsHtml(EMISSION_CARD_ACTIVE), {
				status: 200,
				headers: { 'content-type': 'text/html' },
			});
		}
		if (url.includes('fundsexplorer.com.br/funds/')) {
			return new Response('<html></html>', { status: 200, headers: { 'content-type': 'text/html' } });
		}
		throw new Error(`Unexpected URL: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetchEmissions({ ticker: 'HGLG11', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.emissions.length, 1);
	assert.equal(payload.emissions[0].ticker, 'HGLG11');
});

test('fetchEmissions returns null for non-FII asset', async () => {
	const provider = new FundsExplorerProvider();
	const payload = await provider.fetchEmissions({ ticker: 'PETR4', market: 'BR', assetClass: 'stock' });
	assert.equal(payload, null);
});

test('fetchEmissions returns null on HTTP error', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async () =>
		new Response('Not Found', { status: 404, headers: { 'content-type': 'text/html' } });

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetchEmissions({ ticker: 'HGLG11', market: 'BR', assetClass: 'fii' })
	);
	assert.equal(payload, null);
});

test('fetchEmissions returns empty emissions when no cards found', async () => {
	const provider = new FundsExplorerProvider({ timeoutMs: 1000 });

	const fetchImpl = async () =>
		new Response('<html><body><h1>No results</h1></body></html>', {
			status: 200,
			headers: { 'content-type': 'text/html' },
		});

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetchEmissions({ ticker: 'XYZZ11', assetClass: 'fii' })
	);

	assert.ok(payload);
	assert.equal(payload.emissions.length, 0);
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
