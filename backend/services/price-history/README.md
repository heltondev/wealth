# Price History Service

Complements the market-data module with:

- Daily OHLCV history persistence (incremental/idempotent)
- Transaction enrichment with close-at-date and slippage
- Portfolio cost/rentability metrics
- Chart-ready data builders (price history, average cost, cumulative return, dividends)

Provider chain for BR/US/CA history:
1) Yahoo chart public endpoint primary (`query1.finance.yahoo.com`)
2) Yahoo chart fallback provider (`query1.finance.yahoo.com`)
3) Market-data fallback manager (last-price synthetic row when needed)

## Main interface

- `fetchPriceHistory(ticker, market, context)`
- `fetchPortfolioPriceHistory(portfolioId, options)`
- `getPriceAtDate(ticker, date, options)`
- `getAverageCost(ticker, userId, options)`
- `getPortfolioMetrics(userId, options)`
- `getChartData(ticker, userId, chartType, period, options)`

## Storage keys used

- Daily price row:
  - `PK=PORTFOLIO#{portfolioId}`
  - `SK=ASSET_PRICE#{assetId}#{YYYY-MM-DD}`
- Secondary ticker/date index row:
  - `PK=PRICE#{ticker}`
  - `SK={YYYY-MM-DD}#PORTFOLIO#{portfolioId}#ASSET#{assetId}`

Each persisted row includes `data_source`, `fetched_at`, `currency`, and all day fields.
