const test = require('node:test');
const assert = require('node:assert/strict');

const { B3FinancialStatementsProvider } = require('./b3-financial-statements-provider');

const withMockedFetch = async (fetchImpl, task) => {
	const originalFetch = global.fetch;
	global.fetch = fetchImpl;
	try {
		return await task();
	} finally {
		global.fetch = originalFetch;
	}
};

const decodeLastUrlSegment = (url) => {
	const serializedUrl = String(url || '');
	const encoded = serializedUrl.split('/').pop() || '';
	return JSON.parse(Buffer.from(encoded, 'base64').toString('utf8'));
};

test('B3FinancialStatementsProvider builds normalized statements from B3 structured reports', async () => {
	const provider = new B3FinancialStatementsProvider({
		timeoutMs: 1000,
		startYear: 2020,
		maxReportTypes: 4,
		relevantCategories: [1, 2],
		maxPages: 1,
	});

	const fetchImpl = async (url) => {
		if (url.includes('GetListFunds/')) {
			return new Response(JSON.stringify({
				page: {
					pageNumber: 1,
					pageSize: 20,
					totalRecords: 1,
					totalPages: 1,
				},
				results: [
					{
						id: 870,
						acronym: 'ALZR',
						fundName: 'ALZR Fund',
						tradingName: 'FII ALZR',
					},
				],
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('GetDetailFund/')) {
			return new Response(JSON.stringify({
				acronym: 'ALZR',
				quote: 'R$ 100,25',
				equity: '1.234.567,89',
				cnpj: '28737771000185',
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('GetTypesReport/')) {
			return new Response(JSON.stringify([
				{ inputId: 47, label: 'Demonstracao de Resultado Anual' },
				{ inputId: 45, label: 'Demonstracao Trimestral de Fluxo de Caixa e Balanco' },
			]), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('GetStructuredReports/')) {
			const decodedPayload = decodeLastUrlSegment(url);
			if (decodedPayload.type === 47) {
				return new Response(JSON.stringify({
					page: { totalRecords: 1, totalPages: 1 },
					results: [
						{
							referenceDate: '31/12/2025',
							receitaLiquida: '1.234,56',
							lucroLiquido: '234,56',
						},
					],
				}), { status: 200, headers: { 'content-type': 'application/json' } });
			}
			if (decodedPayload.type === 45) {
				return new Response(JSON.stringify({
					page: { totalRecords: 1, totalPages: 1 },
					results: [
						{
							referenceDate: '30/09/2025',
							patrimonioLiquido: '12.345,67',
							fluxoCaixaOperacional: '345,67',
						},
					],
				}), { status: 200, headers: { 'content-type': 'application/json' } });
			}
			return new Response(JSON.stringify({
				page: { totalRecords: 0 },
				results: [],
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		if (url.includes('GetReportsRelevants/')) {
			const decodedPayload = decodeLastUrlSegment(url);
			if (decodedPayload.category === 1) {
				return new Response(JSON.stringify({
					page: { totalRecords: 1, totalPages: 1 },
					results: [
						{
							urlFundosNet: 'https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id=1106412',
							urlViewerFundosNet: 'https://fnet.bmfbovespa.com.br/fnet/publico/visualizarDocumento?id=1106412',
							referenceDateFormat: '10/02/2026',
							deliveryDateFormat: '10/02/2026 09:28',
							referenceDate: '2026-02-10T00:00:00-03:00',
							describleCategory: 'Fato Relevante',
							subjects: 'Data Base do Direito de Preferência',
							status: '1 (Ativo)',
						},
					],
				}), { status: 200, headers: { 'content-type': 'application/json' } });
			}
			return new Response(JSON.stringify({
				page: { totalRecords: 0, totalPages: 1 },
				results: [],
			}), { status: 200, headers: { 'content-type': 'application/json' } });
		}

		throw new Error(`Unexpected URL in mock: ${url}`);
	};

	const payload = await withMockedFetch(fetchImpl, () =>
		provider.fetch({
			ticker: 'ALZR11',
			market: 'BR',
			assetClass: 'fii',
		})
	);

	assert.ok(payload);
	assert.equal(payload.quote.currentPrice, 100.25);
	assert.equal(payload.quote.marketCap, 1234567.89);

	assert.equal(payload.fundamentals.financials?.[0]?.period, '2025-12-31');
	assert.equal(payload.fundamentals.financials?.[0]?.receitaliquida, 1234.56);
	assert.equal(payload.fundamentals.financials?.[0]?.lucroliquido, 234.56);

	assert.equal(payload.fundamentals.quarterly_balance_sheet?.[0]?.period, '2025-09-30');
	assert.equal(payload.fundamentals.quarterly_balance_sheet?.[0]?.patrimonioliquido, 12345.67);
	assert.equal(payload.fundamentals.quarterly_cashflow?.[0]?.fluxocaixaoperacional, 345.67);
	assert.equal(payload.documents?.length, 1);
	assert.equal(payload.documents?.[0]?.source, 'b3_reports_relevants');
	assert.equal(payload.documents?.[0]?.category, 'Fato Relevante');
	assert.equal(payload.documents?.[0]?.url, 'https://fnet.bmfbovespa.com.br/fnet/publico/visualizarDocumento?id=1106412');
	assert.equal(payload.documents?.[0]?.reference_date, '2026-02-10');
	assert.equal(payload.documents?.[0]?.delivery_date, '2026-02-10');
});

test('B3FinancialStatementsProvider ignores non-BR and non-FII assets', async () => {
	const provider = new B3FinancialStatementsProvider();

	const payload = await provider.fetch({
		ticker: 'AAPL',
		market: 'US',
		assetClass: 'stock',
	});
	assert.equal(payload, null);
});
