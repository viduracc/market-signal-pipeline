# market-signal-pipeline

Stock direction prediction system on Azure. Work in progress.

## Brief

Pulls daily OHLCV data from Alpha Vantage and yfinance into an Azure Blob bronze layer. Loads parsed rows into a PostgreSQL warehouse for transformation via dbt. Trains a LightGBM model on engineered features in Azure ML. Serves predictions through a FastAPI app on Azure Container Apps, with a Streamlit dashboard for monitoring.

The dbt, training, serving, and dashboard layers are in progress. Ingestion, storage, and database provisioning are running.

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

Bronze keeps raw API responses. Silver normalizes into typed Postgres tables. Gold computes features. Model trains on gold, registers to Azure ML. API loads the latest model, reads features from Postgres, returns predictions.

## Stack

- **Ingestion:** Python, httpx, tenacity, Pydantic
- **Storage:** Azure Blob Storage
- **Warehouse:** Azure PostgreSQL Flexible Server
- **Transformations:** dbt
- **Modeling:** LightGBM, Azure ML SDK
- **Serving:** FastAPI on Azure Container Apps
- **Monitoring:** Streamlit, Evidently
- **Infrastructure:** Terraform
- **CI/CD:** GitHub Actions
- **Secrets:** Azure Key Vault, GitHub Secrets

## Repo layout

```
infra/                       Terraform modules
src/market_signal_pipeline/  Python source
  ingest/                    Alpha Vantage client, Yahoo Finance client, bronze writer
  load/                      bronze blob → Postgres loader
  config.py                  pydantic-settings
scripts/                     Entrypoints
  run_ingest.py              Daily cron entrypoint
  run_backfill.py            Historical backfill entrypoint
  run_load_bronze.py         Loads bronze JSON into Postgres
dbt/                         dbt project (silver, gold)
tests/                       pytest suites
.github/workflows/           CI, daily ingestion cron, manual backfill
```

## Design choices

**Two ingestion sources.** Alpha Vantage handles daily incremental data on its free tier. yfinance handles historical bulk via Yahoo's unofficial endpoints, where the free tier has no quota.

**Bronze stores raw bytes.** Source-of-truth for replay. If a transformation has a bug, rebuild silver from bronze without re-fetching.

**Best-effort per-ticker ingestion.** Independent items don't take each other down. AAPL failing doesn't block MSFT.

**Idempotent writes.** Re-running ingestion produces the same outcome. Latest data wins. Backfill safe to re-run.

**Infrastructure-as-code.** Every Azure resource declared in Terraform. No portal click-ops.

**Lowest access level per module.** Storage clients are scoped to a single container, not the whole account.

## Running locally

Requires uv, Terraform, Azure CLI, Python 3.12.

```bash
# Install dependencies
uv sync --all-extras

# Configure environment
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

For a licensed, production-grade data feed, replace yfinance with a provider like Polygon.io or Tiingo.

## License

MIT
