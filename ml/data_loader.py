"""Utilities for loading training data from the database.

This module connects to the configured PostgreSQL database, fetches the
``daily_stats`` table and prepares train/test splits suitable for time-series
models.  No caching is performed – each invocation issues a fresh SQL query.
"""

from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from ml.features.feature_engineering import (
    build_preprocessor,
    drop_outliers,
    prepare_training_frame,
)


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

def _get_engine() -> "Engine":
    """Create a SQLAlchemy engine using ``DATABASE_URL`` env variable."""

    load_dotenv()  # ensure .env values are loaded
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    # Heroku style URLs use ``postgres://`` which SQLAlchemy doesn't recognise
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    return create_engine(db_url)


def load_daily_stats() -> pd.DataFrame:
    """Fetch the ``daily_stats`` table from the database."""

    engine = _get_engine()
    # Always query the database when called
    df = pd.read_sql("SELECT * FROM daily_stats;", con=engine)
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_train_test_data(
    *,
    timesteps: int = 1,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Return ``(X_train, X_test, y_train, y_test)`` arrays.

    Data is pulled from the database every time this function is called.  The
    data is prepared using helper routines from :mod:`ml.features` and split
    per-stream in chronological order.  The preprocessor is fit on the training
    rows only, and the same transformation is applied to all rows.

    Parameters
    ----------
    timesteps:
        Length of the sliding time-window used to create sequences.
    """

    df_daily = load_daily_stats()

    df_clean, features, _ = prepare_training_frame(df_daily)
    df_clean = drop_outliers(df_clean, cols=["total_subscriptions"], factor=2.0)
    df_clean = df_clean.sort_values(["stream_name", "stream_date"]).reset_index(drop=True)

    # Build preprocessing pipeline and fit only on training rows later
    full_pipe = build_preprocessor(df_clean[features])
    pre = full_pipe.named_steps["pre"]

    # Determine per-stream split points
    df_sorted = df_clean

    def _make_split_points_with_test(
        df_sorted: pd.DataFrame,
        timesteps: int,
        start_ratio: float = 0.80,
        min_ratio: float = 0.60,
        step: float = 0.05,
    ) -> dict[str, int]:
        """Compute train split counts ensuring at least one test window."""

        ratio = start_ratio
        while ratio >= min_ratio:
            split_points: dict[str, int] = {}
            total_test_windows = 0

            for name, g in df_sorted.groupby("stream_name", sort=False):
                n = len(g)

                if n < timesteps:
                    split_points[name] = n
                    continue

                ntr = int(ratio * n)
                ntr = min(ntr, n - timesteps)

                if n >= 2 * timesteps:
                    ntr = max(ntr, timesteps)

                ntr = max(0, min(n, ntr))
                split_points[name] = ntr

                last_start_test = n - timesteps
                if last_start_test >= ntr:
                    total_test_windows += (last_start_test - ntr + 1)

            if total_test_windows > 0:
                return split_points

            ratio -= step

        split_points = {}
        for name, g in df_sorted.groupby("stream_name", sort=False):
            n = len(g)
            split_points[name] = max(0, n - timesteps)
        return split_points

    split_points = _make_split_points_with_test(df_sorted, timesteps=timesteps)

    row_is_train = np.zeros(len(df_sorted), dtype=bool)
    for name, g in df_sorted.groupby("stream_name", sort=False):
        ntr = split_points[name]
        row_is_train[g.index[:ntr]] = True

    pre.fit(df_sorted.loc[row_is_train, features])

    def _to_dense(X):
        return X.toarray() if hasattr(X, "toarray") else np.asarray(X)

    X_all = _to_dense(pre.transform(df_sorted[features])).astype(np.float32)
    y_all = df_sorted["total_subscriptions"].values.astype(np.float32)

    X_train_seq, y_train_seq = [], []
    X_test_seq, y_test_seq = [], []

    for name, g in df_sorted.groupby("stream_name", sort=False):
        idx = g.index.to_numpy()
        n = len(idx)
        ntr = split_points[name]

        last_start_train = ntr - timesteps
        if last_start_train >= 0:
            for start in range(0, last_start_train + 1):
                sl = idx[start : start + timesteps]
                X_train_seq.append(X_all[sl, :])
                y_train_seq.append(y_all[sl[-1]])

        last_start_test = n - timesteps
        if last_start_test >= ntr:
            for start in range(ntr, last_start_test + 1):
                sl = idx[start : start + timesteps]
                X_test_seq.append(X_all[sl, :])
                y_test_seq.append(y_all[sl[-1]])

    X_train = (
        np.stack(X_train_seq)
        if X_train_seq
        else np.empty((0, timesteps, X_all.shape[1]), dtype=np.float32)
    )
    y_train = np.asarray(y_train_seq, dtype=np.float32)

    X_test = (
        np.stack(X_test_seq)
        if X_test_seq
        else np.empty((0, timesteps, X_all.shape[1]), dtype=np.float32)
    )
    y_test = np.asarray(y_test_seq, dtype=np.float32)

    return X_train, X_test, y_train, y_test


__all__ = ["load_daily_stats", "get_train_test_data"]
