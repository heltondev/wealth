# Market Data Service

This module fetches full asset data for portfolio holdings using free sources:

- `yfinance` (Python helper) for BR/US/CA listed assets
- Tesouro Transparente CSV endpoints for Tesouro Direto bonds
- Structured + scraping fallbacks when primary providers fail

## Runtime dependencies

Node service calls a Python helper at:

`backend/services/market-data/python/yfinance_fetcher.py`

Install Python dependencies:

```bash
python3 -m pip install -r backend/services/market-data/python/requirements.txt
```

## Optional environment variables

- `MARKET_DATA_PYTHON_BIN` (default: `python3`)
- `MARKET_DATA_YFINANCE_TIMEOUT_MS`
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
