# Platform Service

Cross-module service that implements the full feature surface from `features.md` over the existing DynamoDB single-table model.

## Covered modules

- Dashboard consolidation (multi-currency)
- Dividends analytics and calendar aggregation
- Tax report (monthly + annual)
- Rebalance targets and suggestions
- Portfolio risk and benchmark comparison
- Contributions tracking
- Alerts CRUD + evaluation
- Goals CRUD + progress
- Asset details / fair price / screening / comparison
- Economic indicators + FX refresh jobs
- Corporate events + news ingestion jobs
- Fixed income analytics
- Cost analysis
- Simulation and backtest
- PDF report generation (local filesystem or S3)
- Community ideas and ranking

## Storage traceability

Every persisted entity includes:

- `data_source`
- `fetched_at`
- `is_scraped`

## Runtime portability

Uses shared AWS runtime config (`backend/config/aws.js`) so the same code runs locally and in AWS with env-only changes.
