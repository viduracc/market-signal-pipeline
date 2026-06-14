# Automated Financial Data Ingestion Pipeline

An infrastructure-first data pipeline built on Microsoft Azure, designed to automate data extraction, manage cloud storage layers, and enforce Infrastructure-as-Code (IaC) provisioning.

## Brief

Pulls daily OHLCV data from Alpha Vantage and yfinance into an Azure Blob bronze layer. Loads parsed rows into a PostgreSQL warehouse for transformation via dbt. Trains a LightGBM model on engineered features in Azure ML.
The GitHub Actions daily cron triggers have been disabled to prevent pipeline execution against offline infrastructure. The Terraform modules included in this repository can fully recreate the cloud environment on demand.

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
```

## Stack

- **Ingestion:** Python, httpx, Pydantic
- **Storage:** Azure Blob Storage
- **Warehouse:** Azure PostgreSQL Flexible Server
- **Infrastructure:** Terraform
- **CI/CD:** GitHub Actions
- **Secrets:** Azure Key Vault, GitHub Secrets
- **Transformations:** dbt
- **Modeling:** LightGBM, Azure ML SDK

## Repo layout

```
infra/                       Terraform modules
src/market_signal_pipeline/  Python source
  ingest/                    Alpha Vantage client, Yahoo Finance client, bronze writer
  load/                      blob → Postgres loader
  config.py                  pydantic-settings
scripts/                     Entrypoints
  run_ingest.py              Daily cron entrypoint
  run_backfill.py            Historical backfill entrypoint
  run_load_bronze.py         Loads bronze JSON into Postgres
dbt/                         dbt project
tests/                       pytest suites
.github/workflows/           CI, daily ingestion cron, manual backfill
```

## Design choices

**Two ingestion sources.** Alpha Vantage handles daily incremental data on its free tier. yfinance handles historical bulk via Yahoo's unofficial endpoints

**Bronze stores raw bytes.** Source-of-truth for replay. If a transformation has a bug, rebuild silver from bronze without re-fetching.

**Best-effort per-ticker ingestion.** Independent items don't take each other down.

**Idempotent writes.** Re-running ingestion produces the same outcome. Latest data wins. Backfill safe to re-run.

**Infrastructure-as-code.** Every Azure resource declared in Terraform.

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

## License

MIT
