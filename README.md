# market-signal-pipeline

End-to-end ML inference pipeline on Azure. Stock direction prediction. Work in progress.

## Status

Done:
- Scheduled ingestion from Alpha Vantage (daily) and yfinance (historical bulk).
- Raw OHLCV lands in Azure Blob bronze layer.
- Azure infrastructure provisioned via Terraform: resource group, storage account, blob container, PostgreSQL Flexible Server.
- GitHub Actions CI on every PR. Daily cron runs the ingestion. Manual workflow for backfill.

Next:
- dbt transformations from bronze to silver (cleaned, typed, deduped) and gold (engineered features).
- Model training in Azure ML with walk-forward validation. LightGBM.
- FastAPI serving on Azure Container Apps. Streamlit dashboard with drift monitoring.

## Architecture

```
Alpha Vantage / yfinance
        │
        ▼
GitHub Actions cron
        │
        ▼
Azure Blob (bronze-raw)
        │
        ▼
Azure PostgreSQL Flexible Server (silver, gold)
        │
        ▼
Azure ML (training, model registry)
        │
        ▼
Azure Container Apps (FastAPI + Streamlit)
```

Bronze keeps raw API responses. Silver normalizes into typed Postgres tables. Gold computes features. Model trains on gold, registers to Azure ML. API loads the model, reads the latest features, returns predictions.

## Stack

- **Ingestion:** Python, httpx, tenacity, Pydantic
- **Storage:** Azure Blob Storage
- **Warehouse:** Azure PostgreSQL Flexible Server
- **Transformations:** dbt (planned)
- **Modeling:** LightGBM, Azure ML SDK (planned)
- **Serving:** FastAPI on Azure Container Apps (planned)
- **Monitoring:** Streamlit, Evidently (planned)
- **Infrastructure:** Terraform
- **CI/CD:** GitHub Actions
- **Secrets:** Azure Key Vault, GitHub Secrets

## Repo layout

```
infra/                       Terraform modules
src/market_signal_pipeline/  Python source
  ingest/                    Alpha Vantage client, Yahoo Finance client, bronze writer
  config.py                  pydantic-settings
scripts/                     Entrypoints
  run_ingest.py              Daily cron entrypoint
  run_backfill.py            Historical backfill entrypoint
tests/                       pytest suites
.github/workflows/           CI, daily ingestion cron, manual backfill
```

## Design choices

**Two ingestion sources.** Alpha Vantage's for daily incremental data. yfinance has unlimited backfill via Yahoo's unofficial endpoints.

**Bronze stores raw bytes.** Source-of-truth for replay. If silver-layer logic has a bug, rebuild from bronze. The transformation layer is the only thing to fix.

**Best-effort per-ticker ingestion.** Independent items don't take each other down. AAPL failing doesn't block MSFT.

**Idempotent writes.** Re-running ingestion produces the same outcome. Latest data wins. Backfill safe to re-run.

**Infrastructure-as-code only.** Every Azure resource is declared in Terraform. Every change reviewable.

**Lowest access level per module.**

## Running locally

Requires uv, Terraform, Azure CLI, Python 3.12.

```bash
# Install dependencies
uv sync --all-extras

# Configure environment (see .env.example)
cp .env.example .env
# Edit .env with your values

# Provision infrastructure
cd infra
terraform init
terraform apply
cd ..

# Run ingestion locally
uv run python scripts/run_ingest.py

# Run backfill locally
uv run python scripts/run_backfill.py
```

## Tests

```bash
uv run pytest
```

Tests use respx for HTTP mocking and unittest.mock for the Azure SDK. No real API calls in the test suite.

## Notes

This project demonstrates production-style data pipeline patterns: typed data models, retry with backoff, idempotent writes, IaC, CI/CD, structured logging, and least-privilege access. The ML layer is in progress.

For a licensed, production-grade data feed, replace yfinance with a provider like Polygon.io or Tiingo.

## License

MIT
