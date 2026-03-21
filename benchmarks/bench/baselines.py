"""Baseline selection and comparison helpers.

Baselines should be curated intentionally from runs that are worth keeping.
"""

from __future__ import annotations

import pandas as pd


def latest_baseline_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return one latest baseline row per scenario/backend/binding/profile/point."""

    if frame.empty:
        return frame

    baselines = frame.loc[frame["source"] == "baselines"].copy()
    if baselines.empty:
        return baselines

    baselines = baselines.sort_values(
        by=["run_timestamp", "file_name", "line_number"],
        ascending=[False, False, False],
    )
    keys = [
        "scenario_id",
        "backend",
        "binding",
        "profile",
        "consumers",
        "dequeue_batch_size",
    ]
    return baselines.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)


def attach_latest_baseline_delta(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach latest baseline throughput and latency deltas to each row."""

    if frame.empty:
        return frame

    latest = latest_baseline_rows(frame)
    if latest.empty:
        return frame

    keys = [
        "scenario_id",
        "backend",
        "binding",
        "profile",
        "consumers",
        "dequeue_batch_size",
    ]
    baseline_metrics = latest[
        keys
        + [
            "drain_messages_per_second",
            "total_drain_time_ms",
            "p95_dequeue_latency_ms",
            "p95_archive_latency_ms",
            "file_name",
        ]
    ].rename(
        columns={
            "drain_messages_per_second": "baseline_drain_messages_per_second",
            "total_drain_time_ms": "baseline_total_drain_time_ms",
            "p95_dequeue_latency_ms": "baseline_p95_dequeue_latency_ms",
            "p95_archive_latency_ms": "baseline_p95_archive_latency_ms",
            "file_name": "baseline_file_name",
        }
    )

    merged = frame.merge(baseline_metrics, how="left", on=keys)
    merged["throughput_delta_pct"] = (
        (
            merged["drain_messages_per_second"]
            - merged["baseline_drain_messages_per_second"]
        )
        / merged["baseline_drain_messages_per_second"]
        * 100.0
    )
    merged["drain_time_delta_pct"] = (
        (merged["total_drain_time_ms"] - merged["baseline_total_drain_time_ms"])
        / merged["baseline_total_drain_time_ms"]
        * 100.0
    )
    return merged
