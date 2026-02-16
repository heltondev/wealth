const test = require('node:test');
const assert = require('node:assert/strict');

const { GoogleFinanceScraper } = require('./google-finance-scraper');

const withMockedFetch = async (fetchImpl, task) => {
	const originalFetch = global.fetch;
	global.fetch = fetchImpl;
	try {
		return await task();
	} finally {
		global.fetch = originalFetch;
	}
};

test('GoogleFinanceScraper parses embedded ds data for BVMF quotes', async () => {
	const html = `
		<html><body>
			<div data-entity-label="ALZR11:BVMF"></div>
			<script class="ds:2">
				AF_initDataCallback({key: 'ds:2', data:[[[["/g/11gf9f2wrb",["ALZR11","BVMF"],"Alianza Trust Renda Imobiliaria FII",0,"BRL",[10.82,0.07000000000000028,0.0065116279069767705,2,3,4],null,10.75,"#1a1818","BR","/g/11h_tchr_n",[1771029957],"America/Sao_Paulo",-10800,"/g/11gf9f2wrb",null,null,[1771016700],null,[[1,[2026,2,13,10,null,null,null,[-10800]],[2026,2,13,16,56,null,null,[-10800]]]],null,"ALZR11:BVMF",0,null,null,null,0]]]], sideChannel: {}});
			</script>
			<script class="ds:11">
				AF_initDataCallback({key: 'ds:11', data:[[[["ALZR11","BVMF"],"/g/11gf9f2wrb","BRL",[[[1],[[[2026,2,13,16,56,null,null,[-10800]],[10.82,0.07000000000000028,0.0065116279069767705,2,3,4],306173],[[2026,2,13,18,5,null,null,[-10800]],[10.82,0.07000000000000028,0.0065116279069767705,2,3,4]]]]],null,-10800,10.82,"Alianza Trust Renda Imobiliaria FII",86400,0]]], sideChannel: {}});
			</script>
		</body></html>
	`;

	const scraper = new GoogleFinanceScraper({ timeoutMs: 1000 });
	const payload = await withMockedFetch(
		async () => new Response(html, { status: 200, headers: { 'content-type': 'text/html' } }),
		() => scraper.scrape({ ticker: 'ALZR11', market: 'BR' })
	);

	assert.ok(payload);
	assert.equal(payload.data_source, 'scrape_google');
	assert.equal(payload.quote.currentPrice, 10.82);
	assert.equal(payload.quote.currency, 'BRL');
	assert.equal(payload.quote.change, 0.07000000000000028);
	assert.equal(payload.quote.changePercent, 0.0065116279069767705);
	assert.equal(payload.quote.previousClose, 10.75);
	assert.equal(payload.quote.volume, 306173);
	assert.equal(payload.raw.parsed_from_embedded_data, true);
	assert.equal(payload.raw.parsed_from_daily_bar, true);
});

test('GoogleFinanceScraper scopes YMlKec parsing to current entity block', async () => {
	const html = `
		<html><body>
			<div class="YMlKec">49,500.93</div>
			<div data-entity-label="ALZR11:BVMF">
				<div class="YMlKec">R$10.82</div>
				<div class="JwB6zf">+0.65%</div>
			</div>
		</body></html>
	`;

	const scraper = new GoogleFinanceScraper({ timeoutMs: 1000 });
	const payload = await withMockedFetch(
		async () => new Response(html, { status: 200, headers: { 'content-type': 'text/html' } }),
		() => scraper.scrape({ ticker: 'ALZR11', market: 'BR' })
	);

	assert.ok(payload);
	assert.equal(payload.quote.currentPrice, 10.82);
	assert.equal(payload.quote.changePercent, 0.65);
	assert.equal(payload.quote.currency, 'BRL');
});
