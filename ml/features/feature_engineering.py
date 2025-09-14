"""Utilities for transforming raw data into model-ready features."""

from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def preprocess_features(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and encode feature columns.

    This function performs simple but sensible preprocessing steps so that
    the returned dataframe is suitable for model training:

    * Numeric columns have missing values filled with the median.
    * Categorical columns have missing values filled with ``"missing"`` and
      are one-hot encoded via :func:`pandas.get_dummies`.

    Parameters
    ----------
    df:
        Raw feature dataframe.

    Returns
    -------
    pd.DataFrame
        Transformed feature dataframe ready for modelling.
    """

    df = df.copy()

    # Fill numeric NaNs with column median
    num_cols = df.select_dtypes(include=["number"]).columns
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())

    # Fill categorical NaNs with placeholder string
    cat_cols = df.select_dtypes(exclude=["number"]).columns
    df[cat_cols] = df[cat_cols].fillna("missing")

    # One-hot encode categorical variables
    df = pd.get_dummies(df, columns=cat_cols, drop_first=True)

    return df


# ---------------------------------------------------------------------------
# Helper utilities used by the data loader
# ---------------------------------------------------------------------------

def prepare_training_frame(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Return a cleaned dataframe and feature lists used for modelling.

    This is a lightweight stand-in for a more sophisticated preparation
    routine used in production.  It makes a copy of ``df`` and derives a list
    of feature columns by excluding a handful of known non-feature columns.

    Parameters
    ----------
    df:
        Raw dataframe from the database.

    Returns
    -------
    tuple
        ``(df_clean, features, hist_cols)`` where ``features`` are the columns
        to feed into the model and ``hist_cols`` is reserved for any historical
        feature engineering (currently empty).
    """

    df_clean = df.copy()

    # Example of handling tags which may appear as strings in the database
    if "tags" in df_clean.columns:
        df_clean["raw_tags"] = df_clean["tags"].apply(
            lambda x: x if isinstance(x, list) else []
        )

    non_feature_cols = {
        "total_subscriptions",
        "tags",
        "raw_tags",
    }
    features = [c for c in df_clean.columns if c not in non_feature_cols]
    hist_cols: List[str] = []

    return df_clean, features, hist_cols


def drop_outliers(
    df: pd.DataFrame,
    cols: Sequence[str],
    *,
    method: str = "iqr",
    factor: float = 1.5,
) -> pd.DataFrame:
    """Remove rows considered outliers for the given columns.

    Currently supports an interquartile range (IQR) based method which drops
    rows lying outside ``[Q1 - factor*IQR, Q3 + factor*IQR]``.
    """

    if method != "iqr":
        raise ValueError("Only IQR-based outlier removal is implemented")

    mask = pd.Series(True, index=df.index)
    for col in cols:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lo = q1 - factor * iqr
        hi = q3 + factor * iqr
        mask &= df[col].between(lo, hi)

    return df.loc[mask].copy()


def build_preprocessor(df: pd.DataFrame) -> Pipeline:
    """Create a preprocessing pipeline suitable for the feature matrix.

    Numeric columns are imputed with their median and scaled, while
    categorical columns are imputed with the most frequent value and one-hot
    encoded.  The resulting pipeline exposes a ``"pre"`` step mirroring the
    structure expected by the training code.
    """

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "encoder",
                OneHotEncoder(handle_unknown="ignore", sparse=True),
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    pipe = Pipeline([("pre", preprocessor)])
    return pipe


__all__ = [
    "preprocess_features",
    "prepare_training_frame",
    "drop_outliers",
    "build_preprocessor",
]


