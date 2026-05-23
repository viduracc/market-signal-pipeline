"""Walk-forward LightGBM trainer — reads Postgres gold_features, registers model to Azure ML.

Reads: silver_gold.gold_features. Writes: model.pkl + Azure ML model registry.
Auth: DefaultAzureCredential (az login locally; service principal required in CI).
"""

import sys
from dataclasses import dataclass

import psycopg
import structlog
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Model
from azure.identity import DefaultAzureCredential

from market_signal_pipeline.config import get_settings
from market_signal_pipeline.model.trainer import TrainResult, WalkForwardTrainer

log = structlog.get_logger()

MODEL_NAME = "market-direction-lgbm"
MODEL_PATH = "model.pkl"


@dataclass
class RunResult:
    train_result: TrainResult | None = None
    registered_version: str = ""
    error: str = ""

    @property
    def success(self) -> bool:
        return not self.error and self.train_result is not None


def run_train() -> RunResult:
    """Load data, train, register. Returns RunResult."""
    settings = get_settings()
    result = RunResult()

    # Connect to Postgres
    try:
        conn = psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password.get_secret_value(),
            sslmode="require",
        )
    except psycopg.Error as exc:
        result.error = f"Postgres connection failed: {exc}"
        log.error("train.postgres.connection_failed", error_type=type(exc).__name__)
        return result

    # Load features and train
    try:
        trainer = WalkForwardTrainer()
        df = trainer.load_data(conn)
        train_result = trainer.train(df, output_path=MODEL_PATH)
        result.train_result = train_result
    except Exception as exc:
        result.error = f"Training failed: {exc}"
        log.error("train.failed", error_type=type(exc).__name__)
        return result
    finally:
        conn.close()

    if not train_result.folds:
        result.error = "No folds completed — insufficient data."
        return result

    log.info(
        "train.complete",
        folds=len(train_result.folds),
        mean_accuracy=round(train_result.mean_accuracy, 4),
        mean_roc_auc=round(train_result.mean_roc_auc, 4),
    )

    # Register to Azure ML
    try:
        ml_client = MLClient(
            credential=DefaultAzureCredential(),
            subscription_id=settings.azure_subscription_id,
            resource_group_name=settings.azure_resource_group,
            workspace_name=settings.azure_ml_workspace,
        )
        registered = ml_client.models.create_or_update(
            Model(
                path=MODEL_PATH,
                name=MODEL_NAME,
                description=(
                    f"LightGBM direction classifier. "
                    f"Folds: {len(train_result.folds)}. "
                    f"Mean ROC-AUC: {round(train_result.mean_roc_auc, 4)}."
                ),
                type="custom_model",
            )
        )
        result.registered_version = str(registered.version)
        log.info(
            "train.registered",
            model=MODEL_NAME,
            version=registered.version,
        )
    except Exception as exc:
        result.error = f"Azure ML registration failed: {exc}"
        log.error("train.registration_failed", error_type=type(exc).__name__)

    return result


def main() -> None:
    result = run_train()
    if not result.success:
        log.error("train.run.failed", error=result.error)
        sys.exit(1)
    log.info("train.run.success", version=result.registered_version)


if __name__ == "__main__":
    main()
