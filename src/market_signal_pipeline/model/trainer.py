"""Walk-forward LightGBM trainer for market direction prediction."""

import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
import structlog
from sklearn.dummy import DummyClassifier
from sklearn.metrics import accuracy_score, precision_score, roc_auc_score

log = structlog.get_logger()

FEATURE_COLS: list[str] = [
    "return_1d",
    "return_5d",
    "return_20d",
    "ma_5d",
    "ma_20d",
    "ma_50d",
    "volatility_20d",
    "volume_ratio_20d",
    "rsi_14",
]
TARGET_COL = "target"
GOLD_TABLE = "silver_gold.gold_features"
_VAL_FRACTION = 0.2
_EARLY_STOPPING_ROUNDS = 20


@dataclass
class FoldResult:
    """Metrics for one walk-forward fold."""

    train_end_year: int
    test_year: int
    accuracy: float
    precision: float
    roc_auc: float
    baseline_accuracy: float
    class_balance: float  # fraction of positive (up) days in test set
    n_train: int
    n_test: int


@dataclass
class TrainResult:
    """Aggregated result across all folds."""

    folds: list[FoldResult] = field(default_factory=list)
    model_path: str = ""

    @property
    def mean_accuracy(self) -> float:
        if not self.folds:
            return 0.0
        return sum(f.accuracy for f in self.folds) / len(self.folds)

    @property
    def mean_roc_auc(self) -> float:
        if not self.folds:
            return 0.0
        return sum(f.roc_auc for f in self.folds) / len(self.folds)

    @property
    def mean_baseline_accuracy(self) -> float:
        if not self.folds:
            return 0.0
        return sum(f.baseline_accuracy for f in self.folds) / len(self.folds)


def _make_lgbm() -> lgb.LGBMClassifier:
    return lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42,
        verbose=-1,
    )


def _fit_with_early_stopping(
    model: lgb.LGBMClassifier,
    x_train: pd.DataFrame,
    y_train: pd.Series,
) -> lgb.LGBMClassifier:
    """Chronological 80/20 split within training data for early stopping."""
    n_val = max(1, int(len(x_train) * _VAL_FRACTION))
    x_tr, x_val = x_train.iloc[:-n_val], x_train.iloc[-n_val:]
    y_tr, y_val = y_train.iloc[:-n_val], y_train.iloc[-n_val:]
    model.fit(
        x_tr,
        y_tr,
        eval_set=[(x_val, y_val)],
        callbacks=[lgb.early_stopping(_EARLY_STOPPING_ROUNDS, verbose=False)],
    )
    return model


class WalkForwardTrainer:
    """Trains LightGBM on gold_features with walk-forward validation."""

    def __init__(
        self,
        train_start_year: int = 2010,
        first_test_year: int = 2024,
    ) -> None:
        self._train_start_year = train_start_year
        self._first_test_year = first_test_year

    def __repr__(self) -> str:
        return (
            f"WalkForwardTrainer("
            f"train_start={self._train_start_year}, "
            f"first_test={self._first_test_year})"
        )

    def load_data(self, conn: Any) -> pd.DataFrame:
        """Fetch gold_features from Postgres and build target column."""
        cols = ", ".join(["ticker", "bar_date", "close", *FEATURE_COLS])
        query = f"SELECT {cols} FROM {GOLD_TABLE} ORDER BY ticker, bar_date"

        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=columns)
        df[["close", *FEATURE_COLS]] = df[["close", *FEATURE_COLS]].astype(float)
        df["bar_date"] = pd.to_datetime(df["bar_date"])
        df["year"] = df["bar_date"].dt.year

        # Target: 1 if next-day close is higher, else 0
        df["next_close"] = df.groupby("ticker")["close"].shift(-1)
        df[TARGET_COL] = (df["next_close"] > df["close"]).astype(int)
        df = df.dropna(subset=[*FEATURE_COLS, TARGET_COL])

        log.info("trainer.data.loaded", rows=len(df), tickers=df["ticker"].nunique())
        return df

    def train(self, df: pd.DataFrame, output_path: str = "/tmp/model.pkl") -> TrainResult:
        """Walk-forward evaluation, then train final model on all data."""
        result = TrainResult()
        max_year = int(df["year"].max())

        # --- Walk-forward folds (evaluation only) ---
        for test_year in range(self._first_test_year, max_year + 1):
            train_mask = (df["year"] >= self._train_start_year) & (df["year"] < test_year)
            test_mask = df["year"] == test_year

            if train_mask.sum() == 0 or test_mask.sum() == 0:
                continue

            x_train = df.loc[train_mask, FEATURE_COLS]
            y_train = df.loc[train_mask, TARGET_COL]
            x_test = df.loc[test_mask, FEATURE_COLS]
            y_test = df.loc[test_mask, TARGET_COL]

            model = _fit_with_early_stopping(_make_lgbm(), x_train, y_train)

            y_pred = np.asarray(model.predict(x_test))
            y_prob = np.asarray(model.predict_proba(x_test))[:, 1]

            baseline = DummyClassifier(strategy="most_frequent")
            baseline.fit(x_train, y_train)

            fold = FoldResult(
                train_end_year=test_year - 1,
                test_year=test_year,
                accuracy=float(accuracy_score(y_test, y_pred)),
                precision=float(precision_score(y_test, y_pred, zero_division=0)),
                roc_auc=float(roc_auc_score(y_test, y_prob)),
                baseline_accuracy=float(baseline.score(x_test, y_test)),
                class_balance=float(y_test.mean()),
                n_train=int(train_mask.sum()),
                n_test=int(test_mask.sum()),
            )
            result.folds.append(fold)
            log.info(
                "trainer.fold.done",
                test_year=test_year,
                accuracy=round(fold.accuracy, 4),
                baseline_accuracy=round(fold.baseline_accuracy, 4),
                lift=round(fold.accuracy - fold.baseline_accuracy, 4),
                roc_auc=round(fold.roc_auc, 4),
                class_balance=round(fold.class_balance, 4),
                n_train=fold.n_train,
                n_test=fold.n_test,
            )

        # --- Final model: trained on ALL data from train_start_year ---
        final_mask = df["year"] >= self._train_start_year
        final_x = df.loc[final_mask, FEATURE_COLS]
        final_y = df.loc[final_mask, TARGET_COL]

        final_model = _fit_with_early_stopping(_make_lgbm(), final_x, final_y)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "wb") as f:
            pickle.dump(final_model, f)
        result.model_path = output_path

        log.info(
            "trainer.final_model.saved",
            path=output_path,
            mean_accuracy=round(result.mean_accuracy, 4),
            mean_baseline_accuracy=round(result.mean_baseline_accuracy, 4),
            mean_roc_auc=round(result.mean_roc_auc, 4),
            folds=len(result.folds),
        )

        return result
