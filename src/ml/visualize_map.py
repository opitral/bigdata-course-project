import argparse
import json
import logging
import os
import sys

import folium
import pandas as pd
from folium.plugins import HeatMap

from ml.config import KYIV_CENTER, PATHS


logger = logging.getLogger("ml.visualize_map")


def load_predictions() -> dict:
    frames = {}
    for name, path in (("kpt", PATHS.predictions_kpt), ("tomtom", PATHS.predictions_tomtom)):
        if os.path.exists(path):
            frames[name] = pd.read_parquet(path)
            logger.info("Loaded %s predictions: %d rows", name, len(frames[name]))
        else:
            logger.warning("Missing predictions file: %s", path)
    return frames


def load_kmeans_centers() -> list:
    if not os.path.exists(PATHS.kmeans_centers):
        logger.warning("Missing KMeans centers file: %s", PATHS.kmeans_centers)
        return []
    with open(PATHS.kmeans_centers, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    centers = []
    for dataset_name, dataset_payload in payload.get("datasets", {}).items():
        if dataset_payload.get("skipped"):
            continue
        for center in dataset_payload.get("centers", []):
            enriched = dict(center)
            enriched["dataset"] = dataset_name
            centers.append(enriched)
    return centers


def congestion_points(df: pd.DataFrame, column: str) -> list:
    subset = df[df[column] == 1][["lat", "lon"]].dropna()
    return subset.values.tolist()


def proba_points(df: pd.DataFrame) -> list:
    subset = df[["lat", "lon", "congestion_proba"]].dropna()
    return subset.values.tolist()


def build_map(predictions: dict, centers: list) -> folium.Map:
    m = folium.Map(location=list(KYIV_CENTER), zoom_start=11, tiles="cartodbpositron")

    for name, df in predictions.items():
        real_group = folium.FeatureGroup(name=f"Real congestion ({name})", show=name == "kpt")
        real_points = congestion_points(df, "is_congested_actual")
        if real_points:
            HeatMap(real_points, radius=12, blur=20, min_opacity=0.3).add_to(real_group)
        real_group.add_to(m)

        predicted_group = folium.FeatureGroup(
            name=f"Predicted congestion ({name})", show=name == "kpt"
        )
        predicted_points = proba_points(df)
        if predicted_points:
            HeatMap(predicted_points, radius=12, blur=20, min_opacity=0.3).add_to(predicted_group)
        predicted_group.add_to(m)

    if centers:
        cluster_group = folium.FeatureGroup(name="K-Means cluster centers", show=True)
        for center in centers:
            folium.Marker(
                location=[center["lat"], center["lon"]],
                popup=folium.Popup(
                    html=(
                        f"<b>Cluster {center['cluster_id']}</b><br>"
                        f"Dataset: {center['dataset']}<br>"
                        f"Intensity: {center['intensity_mean']:.2f}<br>"
                        f"Points: {center['size']}"
                    ),
                    max_width=300,
                ),
                icon=folium.Icon(color="red", icon="exclamation-sign"),
            ).add_to(cluster_group)
        cluster_group.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    return m


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render heatmap for real vs predicted congestion")
    parser.add_argument("--output", default=PATHS.heatmap)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    args = parse_args()
    try:
        predictions = load_predictions()
        if not predictions:
            raise RuntimeError("No predictions found — run train_classification.py first")
        centers = load_kmeans_centers()
        folium_map = build_map(predictions, centers)
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        folium_map.save(args.output)
        logger.info("Heatmap saved to %s", args.output)
    except Exception as exc:
        logger.exception("Visualization failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
