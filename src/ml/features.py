from dataclasses import dataclass

import pandas as pd


KPT_NUMERIC_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "lat",
    "lon",
    "events_count",
    "heading_sin",
    "heading_cos",
    "temp_c",
    "precip_mm",
    "wind_mps",
    "humidity_pct",
]
KPT_CATEGORICAL_FEATURES = ["direction_bucket"]
KPT_TARGET_REGRESSION = "avg_speed_kmh"
KPT_TARGET_CLASSIFICATION = "is_congested"

TOMTOM_NUMERIC_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "lat",
    "lon",
    "free_flow_speed_kmh",
    "current_travel_time_s",
    "confidence",
    "temp_c",
    "precip_mm",
    "wind_mps",
    "humidity_pct",
]
TOMTOM_CATEGORICAL_FEATURES = ["road_id"]
TOMTOM_TARGET_REGRESSION = "current_speed_kmh"
TOMTOM_TARGET_CLASSIFICATION = "is_congested"


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    path: str
    numeric_features: list
    categorical_features: list
    regression_target: str
    classification_target: str


KPT_SPEC = DatasetSpec(
    name="kpt",
    path="reports/dataset_kpt.parquet",
    numeric_features=KPT_NUMERIC_FEATURES,
    categorical_features=KPT_CATEGORICAL_FEATURES,
    regression_target=KPT_TARGET_REGRESSION,
    classification_target=KPT_TARGET_CLASSIFICATION,
)

TOMTOM_SPEC = DatasetSpec(
    name="tomtom",
    path="reports/dataset_tomtom.parquet",
    numeric_features=TOMTOM_NUMERIC_FEATURES,
    categorical_features=TOMTOM_CATEGORICAL_FEATURES,
    regression_target=TOMTOM_TARGET_REGRESSION,
    classification_target=TOMTOM_TARGET_CLASSIFICATION,
)

ALL_SPECS = {KPT_SPEC.name: KPT_SPEC, TOMTOM_SPEC.name: TOMTOM_SPEC}


def load_dataset(spec: DatasetSpec) -> pd.DataFrame:
    df = pd.read_parquet(spec.path)
    required = (
        spec.numeric_features
        + spec.categorical_features
        + [spec.regression_target, spec.classification_target, "lat", "lon", "event_hour"]
    )
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset {spec.name} is missing columns: {missing}")
    df = df.dropna(subset=spec.numeric_features + [spec.regression_target])
    return df


def feature_matrix(df: pd.DataFrame, spec: DatasetSpec) -> pd.DataFrame:
    numeric = df[spec.numeric_features].astype(float)
    if spec.categorical_features:
        dummies = pd.get_dummies(
            df[spec.categorical_features].astype(str),
            prefix=spec.categorical_features,
            drop_first=False,
            dtype=float,
        )
        return pd.concat([numeric.reset_index(drop=True), dummies.reset_index(drop=True)], axis=1)
    return numeric
