import argparse
import json
import logging
import os
import sys

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from common.author import resolve_author
from ml.config import PATHS, RANDOM_STATE
from ml.features import ALL_SPECS, DatasetSpec, load_dataset
from ml.metrics_store import save_metrics_section


logger = logging.getLogger("ml.train_kmeans")


K_CANDIDATES = list(range(2, 11))


def congestion_feature_matrix(df, spec: DatasetSpec):
    if spec.name == "kpt":
        intensity_col = "events_count"
        filter_mask = df[spec.classification_target] == 1
    else:
        intensity_col = "current_travel_time_s"
        filter_mask = df[spec.classification_target] == 1
    congested = df.loc[filter_mask, ["lat", "lon", intensity_col]].dropna()
    return congested.rename(columns={intensity_col: "intensity"})


def choose_k(features: np.ndarray) -> dict:
    evaluations = []
    best = {"k": None, "silhouette": -1.0}
    for k in K_CANDIDATES:
        if k >= len(features):
            continue
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(features)
        if len(np.unique(labels)) < 2:
            continue
        sil = float(silhouette_score(features, labels))
        inertia = float(km.inertia_)
        evaluations.append({"k": k, "inertia": inertia, "silhouette": sil})
        logger.info("  k=%d silhouette=%.4f inertia=%.2f", k, sil, inertia)
        if sil > best["silhouette"]:
            best = {"k": k, "silhouette": sil}
    return {"evaluations": evaluations, "best": best}


def run_kmeans_for_spec(spec: DatasetSpec) -> dict:
    df = load_dataset(spec)
    congested = congestion_feature_matrix(df, spec)
    logger.info("[%s] congested rows for clustering: %d", spec.name, len(congested))
    if len(congested) < max(K_CANDIDATES) + 1:
        logger.warning("[%s] not enough congested samples for k-means; skipping", spec.name)
        return {"skipped": True, "reason": "insufficient_samples", "rows": len(congested)}

    scaler = StandardScaler()
    X = scaler.fit_transform(congested[["lat", "lon", "intensity"]].values)

    selection = choose_k(X)
    best_k = selection["best"]["k"]
    if best_k is None:
        return {"skipped": True, "reason": "no_valid_k"}

    model = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=20)
    labels = model.fit_predict(X)
    centers_scaled = model.cluster_centers_
    centers = scaler.inverse_transform(centers_scaled)
    cluster_sizes = [int((labels == i).sum()) for i in range(best_k)]

    centers_payload = [
        {
            "cluster_id": i,
            "lat": float(centers[i][0]),
            "lon": float(centers[i][1]),
            "intensity_mean": float(centers[i][2]),
            "size": cluster_sizes[i],
        }
        for i in range(best_k)
    ]
    return {
        "selection": selection,
        "best_k": best_k,
        "silhouette": float(selection["best"]["silhouette"]),
        "inertia": float(model.inertia_),
        "centers": centers_payload,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cluster congestion points with K-Means")
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
            section_payload["datasets"][spec.name] = run_kmeans_for_spec(spec)
        save_metrics_section(PATHS.metrics, "kmeans", section_payload)
        os.makedirs(os.path.dirname(PATHS.kmeans_centers) or ".", exist_ok=True)
        with open(PATHS.kmeans_centers, "w", encoding="utf-8") as fh:
            json.dump(section_payload, fh, indent=2, ensure_ascii=False, default=str)
        logger.info("KMeans metrics saved to %s", PATHS.metrics)
    except Exception as exc:
        logger.exception("KMeans training failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
