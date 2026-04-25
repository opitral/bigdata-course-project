import argparse
import logging
import os
import sys

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from common.author import resolve_author
from ml.config import PATHS, RANDOM_STATE, TEST_SIZE
from ml.features import ALL_SPECS, DatasetSpec, feature_matrix, load_dataset
from ml.metrics_store import save_metrics_section


logger = logging.getLogger("ml.train_classification")


def classification_scores(y_true, y_pred, y_proba) -> dict:
    scores = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
    }
    if len(np.unique(y_true)) == 2:
        scores["roc_auc"] = float(roc_auc_score(y_true, y_proba))
    else:
        scores["roc_auc"] = None
    return scores


def run_classification_for_spec(spec: DatasetSpec, predictions_path: str, author: str) -> dict:
    df = load_dataset(spec)
    target = spec.classification_target
    logger.info("[%s] rows=%d target=%s positives=%d", spec.name, len(df), target, int(df[target].sum()))
    X = feature_matrix(df, spec).values
    y = df[target].astype(int).values

    indices = np.arange(len(df))
    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y, indices, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
    )
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    model = LogisticRegression(
        penalty="l2",
        C=1.0,
        solver="liblinear",
        max_iter=1000,
        random_state=RANDOM_STATE,
        class_weight="balanced",
    )
    model.fit(X_train_s, y_train)
    test_proba = model.predict_proba(X_test_s)[:, 1]
    test_pred = (test_proba >= 0.5).astype(int)
    scores = classification_scores(y_test, test_pred, test_proba)
    logger.info(
        "[%s] logreg -> acc=%.3f f1=%.3f auc=%s",
        spec.name,
        scores["accuracy"],
        scores["f1"],
        f"{scores['roc_auc']:.3f}" if scores["roc_auc"] is not None else "n/a",
    )

    X_all_s = scaler.transform(X)
    proba_all = model.predict_proba(X_all_s)[:, 1]
    pred_all = (proba_all >= 0.5).astype(int)
    predictions_df = pd.DataFrame(
        {
            "event_hour": df["event_hour"].values,
            "lat": df["lat"].values,
            "lon": df["lon"].values,
            "is_congested_actual": y,
            "is_congested_pred": pred_all,
            "congestion_proba": proba_all,
            "author": author,
        }
    )
    os.makedirs(os.path.dirname(predictions_path) or ".", exist_ok=True)
    predictions_df.to_parquet(predictions_path, index=False)
    logger.info("[%s] wrote predictions to %s (rows=%d)", spec.name, predictions_path, len(predictions_df))

    return {
        "rows_total": len(df),
        "rows_train": len(y_train),
        "rows_test": len(y_test),
        "feature_count": X.shape[1],
        "positive_rate_train": float(np.mean(y_train)),
        "logistic_regression": scores,
        "predictions_path": predictions_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train classification models for congestion")
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
    preds_path = {"kpt": PATHS.predictions_kpt, "tomtom": PATHS.predictions_tomtom}
    try:
        section_payload = {"author": author, "datasets": {}}
        for spec in targets:
            section_payload["datasets"][spec.name] = run_classification_for_spec(
                spec, preds_path[spec.name], author
            )
        save_metrics_section(PATHS.metrics, "classification", section_payload)
        logger.info("Classification metrics saved to %s", PATHS.metrics)
    except Exception as exc:
        logger.exception("Classification training failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
