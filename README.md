# Market Signal Pipeline

End-to-end MLOps pipeline that ingests financial market data, transforms it through a medallion architecture, trains ML signal models, serves them via API, and monitors its own health. Built on Azure with Terraform-managed infrastructure.

## Stack

- **Orchestration:** GitHub Actions scheduled workflows
- **Ingestion:** Python with Alpha Vantage API
- **Storage:** Azure Blob Storage
- **Transformation:** dbt on Postgres
- **Warehouse:** Neon Postgres
- **ML:** LightGBM with Azure ML SDK for tracking and registry
- **Serving:** FastAPI on Azure Container Apps
- **Monitoring:** Evidently AI with Streamlit dashboard
- **IaC:** Terraform
- **CI/CD:** GitHub Actions

## Quick start

```bash
git clone git@github.com:viduracc/market-signal-pipeline.git
cd market-signal-pipeline

uv sync --all-extras
uv run pre-commit install

cp .env.example .env

uv run pytest
```

## License

MIT — see [LICENSE](LICENSE).
