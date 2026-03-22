"""Runs view."""

from __future__ import annotations

import pandas as pd
import streamlit as st


RUN_COLUMNS = [
    "scenario_id",
    "backend",
    "binding",
    "profile",
    "prefill_jobs",
    "consumers",
    "dequeue_batch_size",
    "drain_messages_per_second",
    "total_drain_time_ms",
    "p95_dequeue_latency_ms",
    "p95_archive_latency_ms",
    "throughput_delta_pct",
]


def _file_label(frame: pd.DataFrame, file_name: str) -> str:
    row = frame.loc[frame["file_name"] == file_name].iloc[0]
    timestamp = row["run_timestamp"]
    if pd.notna(timestamp):
        return (
            f"{file_name} | {row['scenario_id']} | {row['backend']} | "
            f"{timestamp:%Y-%m-%d %H:%M UTC}"
        )
    return f"{file_name} | {row['scenario_id']} | {row['backend']}"


def render(
    frame: pd.DataFrame,
    *,
    title: str,
    source: str,
    key_prefix: str,
) -> None:
    """Render a file-at-a-time browsing view."""

    st.subheader(title)
    source_frame = frame.loc[frame["source"] == source].copy()
    if source_frame.empty:
        st.info(f"No {source} result files found.")
        return

    files = source_frame["file_name"].drop_duplicates().tolist()
    selected_file = st.selectbox(
        "File",
        files,
        key=f"{key_prefix}_file",
        format_func=lambda file_name: _file_label(source_frame, file_name),
    )
    selected = source_frame.loc[source_frame["file_name"] == selected_file].copy()
    selected = selected.sort_values(by=["consumers", "dequeue_batch_size"])

    row = selected.iloc[0]
    meta = st.columns(4)
    meta[0].metric("Scenario", row["scenario_id"])
    meta[1].metric("Backend", row["backend"])
    meta[2].metric("Binding", row["binding"])
    meta[3].metric("Points", len(selected))

    st.caption(
        f"source={row['source']} | profile={row['profile']} | prefill={row['prefill_jobs']}"
    )

    display = (
        selected[RUN_COLUMNS]
        .copy()
        .rename(
            columns={
                "scenario_id": "scenario",
                "prefill_jobs": "prefill",
                "dequeue_batch_size": "batch",
                "drain_messages_per_second": "throughput",
                "total_drain_time_ms": "drain_ms",
                "p95_dequeue_latency_ms": "p95_dequeue_ms",
                "p95_archive_latency_ms": "p95_archive_ms",
                "throughput_delta_pct": "vs_baseline_%",
            }
        )
    )
    st.dataframe(display, width="stretch", hide_index=True)
