"""Compare view."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def _run_label(frame: pd.DataFrame) -> str:
    row = frame.iloc[0]
    return (
        f"{row['file_name']} | {row['backend']} | {row['binding']} | "
        f"{row['profile']} | prefill={row['prefill_jobs']}"
    )


def render(frame: pd.DataFrame) -> None:
    """Render run-to-run comparison charts and tables."""

    st.subheader("Compare")
    st.caption("Overlay two result files and inspect throughput or latency deltas.")

    if frame.empty:
        st.info("No benchmark result files found yet.")
        return

    run_files = frame["file_name"].drop_duplicates().tolist()
    default_left = 0
    default_right = 1 if len(run_files) > 1 else 0

    selectors = st.columns(2)
    left_file = selectors[0].selectbox(
        "Run A",
        run_files,
        index=default_left,
        key="compare_run_a",
    )
    right_file = selectors[1].selectbox(
        "Run B",
        run_files,
        index=default_right,
        key="compare_run_b",
    )

    left = frame.loc[frame["file_name"] == left_file].copy()
    right = frame.loc[frame["file_name"] == right_file].copy()
    if left.empty or right.empty:
        st.warning("Select two non-empty runs.")
        return

    st.write(f"Run A: `{_run_label(left)}`")
    st.write(f"Run B: `{_run_label(right)}`")

    combined = pd.concat(
        [
            left.assign(run=left_file),
            right.assign(run=right_file),
        ],
        ignore_index=True,
    )
    combined["point"] = (
        "c="
        + combined["consumers"].astype(str)
        + ", b="
        + combined["dequeue_batch_size"].astype(str)
    )

    metric = st.radio(
        "Metric",
        (
            "drain_messages_per_second",
            "total_drain_time_ms",
            "p95_dequeue_latency_ms",
            "p95_archive_latency_ms",
        ),
        horizontal=True,
        key="compare_metric",
        format_func=lambda value: {
            "drain_messages_per_second": "Throughput",
            "total_drain_time_ms": "Drain Time",
            "p95_dequeue_latency_ms": "P95 Dequeue",
            "p95_archive_latency_ms": "P95 Archive",
        }[value],
    )

    chart = (
        alt.Chart(combined)
        .mark_line(point=True)
        .encode(
            x=alt.X("dequeue_batch_size:O", title="Dequeue Batch Size"),
            y=alt.Y(f"{metric}:Q", title=metric),
            color=alt.Color("run:N", title="Run"),
            strokeDash=alt.StrokeDash("consumers:N", title="Consumers"),
            tooltip=[
                "run",
                "consumers",
                "dequeue_batch_size",
                "drain_messages_per_second",
                "total_drain_time_ms",
                "p95_dequeue_latency_ms",
                "p95_archive_latency_ms",
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(chart, use_container_width=True)

    pivot = combined.pivot_table(
        index=["consumers", "dequeue_batch_size"],
        columns="run",
        values=[
            "drain_messages_per_second",
            "total_drain_time_ms",
            "p95_dequeue_latency_ms",
            "p95_archive_latency_ms",
        ],
    )
    st.dataframe(pivot, use_container_width=True)
