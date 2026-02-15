import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import { api, type Portfolio } from '../services/api';
import { useToast } from '../context/ToastContext';
import './PlatformPage.scss';

type ActionState = Record<string, boolean>;
type ResultState = Record<string, unknown>;

const DEFAULT_BENCHMARK = 'IBOV';
const DEFAULT_PERIOD = '1A';
const DEFAULT_REPORT_TYPE = 'portfolio';

const parseNumber = (value: string, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const parseCommaList = (raw: string): string[] =>
  raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);

const parseTickerList = (raw: string): string[] =>
  parseCommaList(raw).map((item) => item.toUpperCase());

const JsonResult = ({ data }: { data: unknown }) => {
  if (data === undefined) return null;
  return (
    <details className="platform-page__result" open>
      <summary>Result</summary>
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </details>
  );
};

const PlatformPage = () => {
  const { t } = useTranslation();
  const { showToast } = useToast();

  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolio] = useState('');
  const [loadingInit, setLoadingInit] = useState(true);

  const [actionLoading, setActionLoading] = useState<ActionState>({});
  const [results, setResults] = useState<ResultState>({});

  const [ticker, setTicker] = useState('AAPL');
  const [priceDate, setPriceDate] = useState(new Date().toISOString().slice(0, 10));
  const [taxYear, setTaxYear] = useState(String(new Date().getUTCFullYear()));
  const [rebalanceAmount, setRebalanceAmount] = useState('1000');
  const [benchmark, setBenchmark] = useState(DEFAULT_BENCHMARK);
  const [benchmarkPeriod, setBenchmarkPeriod] = useState(DEFAULT_PERIOD);
  const [contributionAmount, setContributionAmount] = useState('1000');
  const [goalAmount, setGoalAmount] = useState('1000000');
  const [goalLabel, setGoalLabel] = useState('Net worth target');
  const [goalType, setGoalType] = useState('net_worth');
  const [goalDate, setGoalDate] = useState('2030-12-31');
  const [goalProgressId, setGoalProgressId] = useState('');
  const [alertType, setAlertType] = useState('price_target');
  const [alertTicker, setAlertTicker] = useState('AAPL');
  const [alertTarget, setAlertTarget] = useState('200');
  const [screenPeMax, setScreenPeMax] = useState('15');
  const [screenDyMin, setScreenDyMin] = useState('4');
  const [screenRoeMin, setScreenRoeMin] = useState('12');
  const [screenAssetClass, setScreenAssetClass] = useState('');
  const [screenSector, setScreenSector] = useState('');
  const [compareTickers, setCompareTickers] = useState('AAPL,MSFT');
  const [fixedPrincipal, setFixedPrincipal] = useState('10000');
  const [fixedCdiPct, setFixedCdiPct] = useState('100');
  const [fixedStartDate, setFixedStartDate] = useState('2025-01-01');
  const [fixedEndDate, setFixedEndDate] = useState(new Date().toISOString().slice(0, 10));
  const [simulationAmount, setSimulationAmount] = useState('1000');
  const [simulationRate, setSimulationRate] = useState('12');
  const [simulationYears, setSimulationYears] = useState('10');
  const [simulationTicker, setSimulationTicker] = useState('AAPL');
  const [simulationInitial, setSimulationInitial] = useState('10000');
  const [reportType, setReportType] = useState(DEFAULT_REPORT_TYPE);
  const [reportPeriod, setReportPeriod] = useState('1A');
  const [ideaTitle, setIdeaTitle] = useState('My market view');
  const [ideaContent, setIdeaContent] = useState('Long-term allocation still favors quality cash-flow assets.');
  const [ideaTags, setIdeaTags] = useState('macro,allocation,long-term');

  const hasPortfolio = Boolean(selectedPortfolio);

  useEffect(() => {
    api.getPortfolios()
      .then((items) => {
        setPortfolios(items);
        if (items.length > 0) setSelectedPortfolio(items[0].portfolioId);
      })
      .catch(() => {
        setPortfolios([]);
        showToast('Failed to load portfolios', 'error');
      })
      .finally(() => setLoadingInit(false));
  }, [showToast]);

  const runAction = async (key: string, fn: () => Promise<unknown>) => {
    setActionLoading((previous) => ({ ...previous, [key]: true }));
    try {
      const payload = await fn();
      setResults((previous) => ({ ...previous, [key]: payload }));
      showToast(`${key} loaded`, 'success');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unexpected error';
      setResults((previous) => ({ ...previous, [key]: { error: message } }));
      showToast(message, 'error');
    } finally {
      setActionLoading((previous) => ({ ...previous, [key]: false }));
    }
  };

  const runPortfolioAction = async (key: string, fn: (portfolioId: string) => Promise<unknown>) => {
    if (!hasPortfolio) {
      showToast('Select a portfolio first', 'warning');
      return;
    }
    await runAction(key, () => fn(selectedPortfolio));
  };

  const benchmarkOptions = useMemo(() => ['IBOV', 'CDI', 'IPCA', 'SNP500', 'IFIX', 'POUPANCA', 'TSX'], []);
  const periodOptions = useMemo(() => ['1M', '3M', '6M', '1A', '2A', '5A', 'MAX'], []);

  return (
    <Layout>
      <div className="platform-page">
        <header className="platform-page__header">
          <h1>{t('platform.title', { defaultValue: 'Platform Control Center' })}</h1>
          <p>{t('platform.subtitle', { defaultValue: 'Unified frontend for all investment platform features.' })}</p>
        </header>

        {loadingInit ? (
          <div className="platform-page__empty">{t('common.loading')}</div>
        ) : (
          <>
            <section className="platform-page__card">
              <h2>Context</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field">
                  <span>Portfolio</span>
                  <select
                    value={selectedPortfolio}
                    onChange={(event) => setSelectedPortfolio(event.target.value)}
                  >
                    {portfolios.length === 0 && <option value="">No portfolio</option>}
                    {portfolios.map((portfolio) => (
                      <option key={portfolio.portfolioId} value={portfolio.portfolioId}>
                        {portfolio.name}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="platform-page__field">
                  <span>Ticker</span>
                  <input value={ticker} onChange={(event) => setTicker(event.target.value.toUpperCase())} />
                </label>

                <label className="platform-page__field">
                  <span>Date</span>
                  <input type="date" value={priceDate} onChange={(event) => setPriceDate(event.target.value)} />
                </label>
              </div>
            </section>

            <section className="platform-page__card">
              <h2>Module 1 + 2: Market Data and Price History</h2>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('refreshMarketData', (portfolioId) => api.refreshMarketData(portfolioId))}
                  disabled={Boolean(actionLoading.refreshMarketData)}
                >
                  Refresh Market Data
                </button>
                <button
                  onClick={() => runPortfolioAction('refreshPriceHistory', (portfolioId) => api.refreshPriceHistory(portfolioId))}
                  disabled={Boolean(actionLoading.refreshPriceHistory)}
                >
                  Refresh Price History
                </button>
                <button
                  onClick={() => runPortfolioAction('priceAtDate', (portfolioId) => api.getPriceAtDate(portfolioId, ticker, priceDate))}
                  disabled={Boolean(actionLoading.priceAtDate)}
                >
                  Get Price At Date
                </button>
                <button
                  onClick={() => runPortfolioAction('averageCost', (portfolioId) => api.getAverageCost(portfolioId, ticker))}
                  disabled={Boolean(actionLoading.averageCost)}
                >
                  Get Average Cost
                </button>
                <button
                  onClick={() => runPortfolioAction('portfolioMetrics', (portfolioId) => api.getPortfolioMetrics(portfolioId))}
                  disabled={Boolean(actionLoading.portfolioMetrics)}
                >
                  Get Portfolio Metrics
                </button>
                <button
                  onClick={() => runPortfolioAction('priceChart', (portfolioId) => api.getPriceChart(portfolioId, ticker, 'price_history', '1A'))}
                  disabled={Boolean(actionLoading.priceChart)}
                >
                  Get Price Chart Data
                </button>
              </div>
              <JsonResult data={results.portfolioMetrics || results.averageCost || results.priceAtDate || results.priceChart || results.refreshMarketData || results.refreshPriceHistory} />
            </section>

            <section className="platform-page__card">
              <h2>Module 3: Dashboard</h2>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('dashboard', (portfolioId) => api.getDashboard(portfolioId))}
                  disabled={Boolean(actionLoading.dashboard)}
                >
                  Load Dashboard
                </button>
              </div>
              <JsonResult data={results.dashboard} />
            </section>

            <section className="platform-page__card">
              <h2>Module 4: Dividends</h2>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('dividends', (portfolioId) => api.getDividends(portfolioId))}
                  disabled={Boolean(actionLoading.dividends)}
                >
                  Load Dividends Analytics
                </button>
              </div>
              <JsonResult data={results.dividends} />
            </section>

            <section className="platform-page__card">
              <h2>Module 5: Tax (IR)</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field">
                  <span>Year</span>
                  <input value={taxYear} onChange={(event) => setTaxYear(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('tax', (portfolioId) => api.getTaxReport(portfolioId, parseNumber(taxYear, new Date().getUTCFullYear())))}
                  disabled={Boolean(actionLoading.tax)}
                >
                  Calculate Tax Report
                </button>
              </div>
              <JsonResult data={results.tax} />
            </section>

            <section className="platform-page__card">
              <h2>Module 6: Rebalancing</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field">
                  <span>Contribution Amount</span>
                  <input value={rebalanceAmount} onChange={(event) => setRebalanceAmount(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('rebalanceTargets', (portfolioId) =>
                    api.setRebalanceTargets(portfolioId, [
                      { scope: 'assetClass', value: 'stock', percent: 30 },
                      { scope: 'assetClass', value: 'fii', percent: 20 },
                      { scope: 'assetClass', value: 'bond', percent: 20 },
                      { scope: 'assetClass', value: 'international', percent: 30 },
                    ])
                  )}
                  disabled={Boolean(actionLoading.rebalanceTargets)}
                >
                  Save Sample Targets
                </button>
                <button
                  onClick={() => runPortfolioAction('rebalanceSuggestion', (portfolioId) => api.getRebalanceSuggestion(portfolioId, parseNumber(rebalanceAmount, 0)))}
                  disabled={Boolean(actionLoading.rebalanceSuggestion)}
                >
                  Get Rebalance Suggestion
                </button>
              </div>
              <JsonResult data={results.rebalanceSuggestion || results.rebalanceTargets} />
            </section>

            <section className="platform-page__card">
              <h2>Module 7 + 8: Risk and Benchmarks</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field">
                  <span>Benchmark</span>
                  <select value={benchmark} onChange={(event) => setBenchmark(event.target.value)}>
                    {benchmarkOptions.map((item) => (
                      <option key={item} value={item}>{item}</option>
                    ))}
                  </select>
                </label>
                <label className="platform-page__field">
                  <span>Period</span>
                  <select value={benchmarkPeriod} onChange={(event) => setBenchmarkPeriod(event.target.value)}>
                    {periodOptions.map((item) => (
                      <option key={item} value={item}>{item}</option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('risk', (portfolioId) => api.getRisk(portfolioId))}
                  disabled={Boolean(actionLoading.risk)}
                >
                  Load Risk Analysis
                </button>
                <button
                  onClick={() => runPortfolioAction('benchmarks', (portfolioId) => api.getBenchmarks(portfolioId, benchmark, benchmarkPeriod))}
                  disabled={Boolean(actionLoading.benchmarks)}
                >
                  Compare Benchmarks
                </button>
              </div>
              <JsonResult data={results.risk || results.benchmarks} />
            </section>

            <section className="platform-page__card">
              <h2>Module 9: Contributions</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field">
                  <span>Contribution Amount</span>
                  <input value={contributionAmount} onChange={(event) => setContributionAmount(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('createContribution', (portfolioId) =>
                    api.createContribution(portfolioId, {
                      amount: parseNumber(contributionAmount, 0),
                      date: new Date().toISOString().slice(0, 10),
                      destination: 'automatic',
                    })
                  )}
                  disabled={Boolean(actionLoading.createContribution)}
                >
                  Add Contribution
                </button>
                <button
                  onClick={() => runPortfolioAction('contributions', (portfolioId) => api.getContributions(portfolioId))}
                  disabled={Boolean(actionLoading.contributions)}
                >
                  Load Contribution Progress
                </button>
              </div>
              <JsonResult data={results.contributions || results.createContribution} />
            </section>

            <section className="platform-page__card">
              <h2>Module 10: Alerts</h2>
              <div className="platform-page__grid platform-page__grid--4">
                <label className="platform-page__field">
                  <span>Alert Type</span>
                  <select value={alertType} onChange={(event) => setAlertType(event.target.value)}>
                    <option value="price_target">price_target</option>
                    <option value="concentration">concentration</option>
                    <option value="rebalance_drift">rebalance_drift</option>
                  </select>
                </label>
                <label className="platform-page__field">
                  <span>Ticker</span>
                  <input value={alertTicker} onChange={(event) => setAlertTicker(event.target.value.toUpperCase())} />
                </label>
                <label className="platform-page__field">
                  <span>Target</span>
                  <input value={alertTarget} onChange={(event) => setAlertTarget(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('createAlert', (portfolioId) =>
                    api.createAlertRule({
                      type: alertType,
                      portfolioId,
                      enabled: true,
                      params: {
                        ticker: alertTicker,
                        target: parseNumber(alertTarget, 0),
                        direction: 'above',
                        thresholdPct: 15,
                        targetByClass: { stock: 30, fii: 20, bond: 20, international: 30 },
                      },
                    })
                  )}
                  disabled={Boolean(actionLoading.createAlert)}
                >
                  Create Alert Rule
                </button>
                <button
                  onClick={() => runAction('alerts', () => api.getAlerts())}
                  disabled={Boolean(actionLoading.alerts)}
                >
                  Load Alerts
                </button>
                <button
                  onClick={() => runPortfolioAction('evaluateAlerts', (portfolioId) => api.evaluateAlerts(portfolioId))}
                  disabled={Boolean(actionLoading.evaluateAlerts)}
                >
                  Evaluate Alerts
                </button>
              </div>
              <JsonResult data={results.alerts || results.createAlert || results.evaluateAlerts} />
            </section>

            <section className="platform-page__card">
              <h2>Module 11 + 12: Multi-Currency and Costs</h2>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('costs', (portfolioId) => api.getCostAnalysis(portfolioId))}
                  disabled={Boolean(actionLoading.costs)}
                >
                  Load Cost Analysis
                </button>
              </div>
              <JsonResult data={results.costs} />
            </section>

            <section className="platform-page__card">
              <h2>Module 13 + 14 + 15 + 16: Asset Intelligence</h2>
              <div className="platform-page__grid platform-page__grid--4">
                <label className="platform-page__field">
                  <span>PE max</span>
                  <input value={screenPeMax} onChange={(event) => setScreenPeMax(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>DY min (%)</span>
                  <input value={screenDyMin} onChange={(event) => setScreenDyMin(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>ROE min (%)</span>
                  <input value={screenRoeMin} onChange={(event) => setScreenRoeMin(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Compare tickers</span>
                  <input value={compareTickers} onChange={(event) => setCompareTickers(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Asset class filter</span>
                  <input value={screenAssetClass} onChange={(event) => setScreenAssetClass(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Sector filter</span>
                  <input value={screenSector} onChange={(event) => setScreenSector(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button onClick={() => runPortfolioAction('assetFairPrice', (portfolioId) => api.getAssetFairPrice(ticker, portfolioId))} disabled={Boolean(actionLoading.assetFairPrice)}>
                  Fair Price
                </button>
                <button onClick={() => runPortfolioAction('assetDetails', (portfolioId) => api.getAssetDetails(ticker, portfolioId))} disabled={Boolean(actionLoading.assetDetails)}>
                  Asset Details
                </button>
                <button onClick={() => runPortfolioAction('assetFinancials', (portfolioId) => api.getAssetFinancials(ticker, portfolioId))} disabled={Boolean(actionLoading.assetFinancials)}>
                  Financial Statements
                </button>
                <button onClick={() => runPortfolioAction('assetEvents', (portfolioId) => api.getAssetEvents(ticker, portfolioId))} disabled={Boolean(actionLoading.assetEvents)}>
                  Corporate Events
                </button>
                <button onClick={() => runPortfolioAction('assetNews', (portfolioId) => api.getAssetNews(ticker, portfolioId))} disabled={Boolean(actionLoading.assetNews)}>
                  Asset News
                </button>
                <button
                  onClick={() => runPortfolioAction('screenAssets', (portfolioId) =>
                    api.screenAssets({
                      portfolioId,
                      peMax: parseNumber(screenPeMax, 0),
                      dyMin: parseNumber(screenDyMin, 0),
                      roeMin: parseNumber(screenRoeMin, 0),
                      assetClass: screenAssetClass || undefined,
                      sector: screenSector || undefined,
                    })
                  )}
                  disabled={Boolean(actionLoading.screenAssets)}
                >
                  Screen Assets
                </button>
                <button
                  onClick={() => runPortfolioAction('compareAssets', (portfolioId) => api.compareAssets(parseTickerList(compareTickers), portfolioId))}
                  disabled={Boolean(actionLoading.compareAssets)}
                >
                  Compare Assets
                </button>
              </div>
              <JsonResult data={results.assetFairPrice || results.assetDetails || results.assetFinancials || results.assetEvents || results.assetNews || results.screenAssets || results.compareAssets} />
            </section>

            <section className="platform-page__card">
              <h2>Module 17: Corporate Events</h2>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('refreshCorporateEvents', (portfolioId) => api.refreshCorporateEvents({ portfolioId, ticker }))}
                  disabled={Boolean(actionLoading.refreshCorporateEvents)}
                >
                  Refresh Corporate Events Job
                </button>
              </div>
              <JsonResult data={results.refreshCorporateEvents} />
            </section>

            <section className="platform-page__card">
              <h2>Module 18: Fixed Income</h2>
              <div className="platform-page__grid platform-page__grid--4">
                <label className="platform-page__field">
                  <span>Principal</span>
                  <input value={fixedPrincipal} onChange={(event) => setFixedPrincipal(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>% CDI</span>
                  <input value={fixedCdiPct} onChange={(event) => setFixedCdiPct(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Start date</span>
                  <input type="date" value={fixedStartDate} onChange={(event) => setFixedStartDate(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>End date</span>
                  <input type="date" value={fixedEndDate} onChange={(event) => setFixedEndDate(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('fixedIncomeComparison', (portfolioId) => api.getFixedIncomeComparison(portfolioId))}
                  disabled={Boolean(actionLoading.fixedIncomeComparison)}
                >
                  Fixed Income Comparison
                </button>
                <button
                  onClick={() => runAction('privateFixedIncome', () => api.calculatePrivateFixedIncome({
                    principal: parseNumber(fixedPrincipal, 0),
                    cdiPct: parseNumber(fixedCdiPct, 100),
                    startDate: fixedStartDate,
                    endDate: fixedEndDate,
                  }))}
                  disabled={Boolean(actionLoading.privateFixedIncome)}
                >
                  Calculate Private Fixed Income
                </button>
              </div>
              <JsonResult data={results.fixedIncomeComparison || results.privateFixedIncome} />
            </section>

            <section className="platform-page__card">
              <h2>Module 19: Simulation</h2>
              <div className="platform-page__grid platform-page__grid--4">
                <label className="platform-page__field">
                  <span>Monthly amount</span>
                  <input value={simulationAmount} onChange={(event) => setSimulationAmount(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Annual rate (%)</span>
                  <input value={simulationRate} onChange={(event) => setSimulationRate(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Years</span>
                  <input value={simulationYears} onChange={(event) => setSimulationYears(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Backtest ticker</span>
                  <input value={simulationTicker} onChange={(event) => setSimulationTicker(event.target.value.toUpperCase())} />
                </label>
                <label className="platform-page__field">
                  <span>Backtest initial amount</span>
                  <input value={simulationInitial} onChange={(event) => setSimulationInitial(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('simulate', (portfolioId) => api.simulate({
                    monthlyAmount: parseNumber(simulationAmount, 0),
                    rate: parseNumber(simulationRate, 0),
                    years: parseNumber(simulationYears, 0),
                    ticker: simulationTicker,
                    initialAmount: parseNumber(simulationInitial, 0),
                    portfolioId,
                  }))}
                  disabled={Boolean(actionLoading.simulate)}
                >
                  Run Simulation
                </button>
              </div>
              <JsonResult data={results.simulate} />
            </section>

            <section className="platform-page__card">
              <h2>Module 20: Goals</h2>
              <div className="platform-page__grid platform-page__grid--4">
                <label className="platform-page__field">
                  <span>Goal label</span>
                  <input value={goalLabel} onChange={(event) => setGoalLabel(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Goal type</span>
                  <select value={goalType} onChange={(event) => setGoalType(event.target.value)}>
                    <option value="net_worth">net_worth</option>
                    <option value="passive_income">passive_income</option>
                  </select>
                </label>
                <label className="platform-page__field">
                  <span>Target amount</span>
                  <input value={goalAmount} onChange={(event) => setGoalAmount(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Target date</span>
                  <input type="date" value={goalDate} onChange={(event) => setGoalDate(event.target.value)} />
                </label>
                <label className="platform-page__field">
                  <span>Goal id for progress</span>
                  <input value={goalProgressId} onChange={(event) => setGoalProgressId(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runAction('createGoal', () => api.createGoal({
                    label: goalLabel,
                    type: goalType,
                    targetAmount: parseNumber(goalAmount, 0),
                    targetDate: goalDate,
                    currency: 'BRL',
                  }))}
                  disabled={Boolean(actionLoading.createGoal)}
                >
                  Create Goal
                </button>
                <button
                  onClick={() => runAction('goals', () => api.getGoals())}
                  disabled={Boolean(actionLoading.goals)}
                >
                  Load Goals
                </button>
                <button
                  onClick={() => {
                    if (!goalProgressId) {
                      showToast('Provide goal id first', 'warning');
                      return;
                    }
                    runAction('goalProgress', () => api.getGoalProgress(goalProgressId));
                  }}
                  disabled={Boolean(actionLoading.goalProgress)}
                >
                  Load Goal Progress
                </button>
              </div>
              <JsonResult data={results.goals || results.createGoal || results.goalProgress} />
            </section>

            <section className="platform-page__card">
              <h2>Module 21: News and Facts</h2>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('refreshNews', (portfolioId) => api.refreshNews({ portfolioId, ticker }))}
                  disabled={Boolean(actionLoading.refreshNews)}
                >
                  Refresh News Job
                </button>
              </div>
              <JsonResult data={results.refreshNews} />
            </section>

            <section className="platform-page__card">
              <h2>Module 22: Reports (PDF)</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field">
                  <span>Report type</span>
                  <select value={reportType} onChange={(event) => setReportType(event.target.value)}>
                    <option value="portfolio">portfolio</option>
                    <option value="tax">tax</option>
                    <option value="dividends">dividends</option>
                    <option value="performance">performance</option>
                  </select>
                </label>
                <label className="platform-page__field">
                  <span>Period</span>
                  <input value={reportPeriod} onChange={(event) => setReportPeriod(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runPortfolioAction('generateReport', (portfolioId) => api.generateReport(reportType, reportPeriod, portfolioId))}
                  disabled={Boolean(actionLoading.generateReport)}
                >
                  Generate Report
                </button>
                <button
                  onClick={() => runAction('reports', () => api.listReports())}
                  disabled={Boolean(actionLoading.reports)}
                >
                  List Reports
                </button>
              </div>
              <JsonResult data={results.generateReport || results.reports} />
            </section>

            <section className="platform-page__card">
              <h2>Module 23: Community</h2>
              <div className="platform-page__grid platform-page__grid--3">
                <label className="platform-page__field platform-page__field--full">
                  <span>Idea title</span>
                  <input value={ideaTitle} onChange={(event) => setIdeaTitle(event.target.value)} />
                </label>
                <label className="platform-page__field platform-page__field--full">
                  <span>Idea content</span>
                  <textarea value={ideaContent} onChange={(event) => setIdeaContent(event.target.value)} rows={3} />
                </label>
                <label className="platform-page__field platform-page__field--full">
                  <span>Tags (comma separated)</span>
                  <input value={ideaTags} onChange={(event) => setIdeaTags(event.target.value)} />
                </label>
              </div>
              <div className="platform-page__actions">
                <button
                  onClick={() => runAction('publishIdea', () => api.publishIdea({
                    title: ideaTitle,
                    content: ideaContent,
                    tags: parseCommaList(ideaTags),
                  }))}
                  disabled={Boolean(actionLoading.publishIdea)}
                >
                  Publish Idea
                </button>
                <button
                  onClick={() => runAction('ideas', () => api.listIdeas(50))}
                  disabled={Boolean(actionLoading.ideas)}
                >
                  List Ideas
                </button>
                <button
                  onClick={() => runAction('ranking', () => api.getLeagueRanking())}
                  disabled={Boolean(actionLoading.ranking)}
                >
                  League Ranking
                </button>
              </div>
              <JsonResult data={results.publishIdea || results.ideas || results.ranking} />
            </section>

            <section className="platform-page__card">
              <h2>Platform Jobs</h2>
              <div className="platform-page__actions">
                <button onClick={() => runAction('jobEconomic', () => api.refreshEconomicIndicators())} disabled={Boolean(actionLoading.jobEconomic)}>
                  Run Economic Job
                </button>
                <button
                  onClick={() => runPortfolioAction('jobEvents', (portfolioId) => api.refreshCorporateEvents({ portfolioId, ticker }))}
                  disabled={Boolean(actionLoading.jobEvents)}
                >
                  Run Events Job
                </button>
                <button
                  onClick={() => runPortfolioAction('jobNews', (portfolioId) => api.refreshNews({ portfolioId, ticker }))}
                  disabled={Boolean(actionLoading.jobNews)}
                >
                  Run News Job
                </button>
                <button
                  onClick={() => runPortfolioAction('jobAlerts', (portfolioId) => api.runAlertEvaluation(portfolioId))}
                  disabled={Boolean(actionLoading.jobAlerts)}
                >
                  Run Alert Job
                </button>
              </div>
              <JsonResult data={results.jobEconomic || results.jobEvents || results.jobNews || results.jobAlerts} />
            </section>
          </>
        )}
      </div>
    </Layout>
  );
};

export default PlatformPage;
