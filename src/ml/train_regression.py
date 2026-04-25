import argparse
import logging
import os
import sys

import numpy as np
from sklearn.linear_model import Lasso, LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from common.author import resolve_author
from ml.config import PATHS, RANDOM_STATE, TEST_SIZE
from ml.features import ALL_SPECS, DatasetSpec, feature_matrix, load_dataset
from ml.metrics_store import save_metrics_section


logger = logging.getLogger("ml.train_regression")


def score_predictions(y_true, y_pred) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": rmse,
        "r2": float(r2_score(y_true, y_pred)),
    }


def run_regression_for_spec(spec: DatasetSpec) -> dict:
    df = load_dataset(spec)
    logger.info("[%s] rows=%d target=%s", spec.name, len(df), spec.regression_target)
    X = feature_matrix(df, spec).values
    y = df[spec.regression_target].astype(float).values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    baseline_pred = np.full_like(y_test, fill_value=y_train.mean(), dtype=float)
    models = {
        "linear_regression": LinearRegression(),
        "ridge": Ridge(alpha=1.0, random_state=RANDOM_STATE),
        "lasso": Lasso(alpha=0.1, random_state=RANDOM_STATE, max_iter=20000),
    }
    results = {
        "rows_total": len(df),
        "rows_train": len(y_train),
        "rows_test": len(y_test),
        "feature_count": X.shape[1],
        "baseline_mean": score_predictions(y_test, baseline_pred),
        "models": {},
    }
    for name, model in models.items():
        model.fit(X_train_s, y_train)
        predictions = model.predict(X_test_s)
        scores = score_predictions(y_test, predictions)
        results["models"][name] = scores
        logger.info("[%s] %s -> MAE=%.3f RMSE=%.3f R2=%.3f", spec.name, name, scores["mae"], scores["rmse"], scores["r2"])
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train regression models for speed prediction")
    parser.add_argument(
        "--dataset",
        choices=list(ALL_SPECS.keys()) + ["all"],
        default="all",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    args = parse_args()
    targets = list(ALL_SPECS.values()) if args.dataset == "all" else [ALL_SPECS[args.dataset]]
    author = resolve_author()
    try:
        section_payload = {"author": author, "datasets": {}}
        for spec in targets:
            section_payload["datasets"][spec.name] = run_regression_for_spec(spec)
        save_metrics_section(PATHS.metrics, "regression", section_payload)
        logger.info("Regression metrics saved to %s", PATHS.metrics)
    except Exception as exc:
        logger.exception("Regression training failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
