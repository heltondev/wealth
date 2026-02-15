# Market Data Service

This module fetches full asset data for portfolio holdings using free sources:

- Yahoo Finance public APIs (`v7/finance/quote` + `v8/finance/chart`) for BR/US/CA listed assets
- Tesouro Transparente CSV endpoints for Tesouro Direto bonds
- Structured + scraping fallbacks when primary providers fail

## Runtime dependencies

Node.js only (no Python runtime required).

## Optional environment variables

- `MARKET_DATA_YAHOO_TIMEOUT_MS` (preferred timeout for Yahoo APIs)
- `MARKET_DATA_YFINANCE_TIMEOUT_MS` (legacy alias, still honored)
- `MARKET_DATA_TESOURO_TIMEOUT_MS`
- `MARKET_DATA_GOOGLE_TIMEOUT_MS`
- `MARKET_DATA_STATUSINVEST_TIMEOUT_MS`
- `MARKET_DATA_FUNDAMENTUS_TIMEOUT_MS`
- `MARKET_DATA_MIN_DELAY_MS` (throttle delay between requests)
- `MARKET_DATA_MAX_CONCURRENT` (throttle parallel fetches)

Tesouro CSV URL map (free/public endpoints):

- `TESOURO_CSV_URL_ALL`
- `TESOURO_CSV_URL_NTNB`
- `TESOURO_CSV_URL_NTNF`
- `TESOURO_CSV_URL_LTN`
- `TESOURO_CSV_URL_LFT`
- or `TESOURO_CSV_URLS_JSON` with a JSON map keyed by type.

## API routes wired in handler

- `POST /portfolios/{portfolioId}/market-data/refresh`
  - Optional body: `{ "assetId": "..." }` to refresh one asset.
- `GET /health/scrapers`
