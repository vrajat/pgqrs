"""Trends view."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def render(frame: pd.DataFrame) -> None:
    """Render scenario trends across runs and baselines."""

    st.subheader("Trends")
    st.caption(
        "Track how a scenario point moves over time across raw runs and curated baselines."
    )

    if frame.empty:
        st.info("No benchmark result files found yet.")
        return

    trendable = frame.loc[frame["run_timestamp"].notna()].copy()
    if trendable.empty:
        st.info("No timestamped result files found.")
        return

    selectors = st.columns(4)
    scenario_id = selectors[0].selectbox(
        "Scenario",
        sorted(trendable["scenario_id"].dropna().unique()),
        key="trends_scenario",
    )
    scenario_frame = trendable.loc[trendable["scenario_id"] == scenario_id].copy()

    backend = selectors[1].selectbox(
        "Backend",
        sorted(scenario_frame["backend"].dropna().unique()),
        key="trends_backend",
    )
    binding = selectors[2].selectbox(
        "Binding",
        sorted(
            scenario_frame.loc[scenario_frame["backend"] == backend, "binding"]
            .dropna()
            .unique()
        ),
        key="trends_binding",
    )
    filtered = scenario_frame.loc[
        (scenario_frame["backend"] == backend) & (scenario_frame["binding"] == binding)
    ].copy()

    point_labels = (
        filtered["consumers"].astype(str)
        + " consumers / batch "
        + filtered["dequeue_batch_size"].astype(str)
    )
    point_options = sorted(point_labels.unique())
    point_label = selectors[3].selectbox("Point", point_options, key="trends_point")
    filtered["point_label"] = point_labels
    filtered = filtered.loc[filtered["point_label"] == point_label].copy()

    metric = st.radio(
        "Metric",
        (
            "drain_messages_per_second",
            "total_drain_time_ms",
            "p95_dequeue_latency_ms",
            "p95_archive_latency_ms",
        ),
        horizontal=True,
        key="trends_metric",
        format_func=lambda value: {
            "drain_messages_per_second": "Throughput",
            "total_drain_time_ms": "Drain Time",
            "p95_dequeue_latency_ms": "P95 Dequeue",
            "p95_archive_latency_ms": "P95 Archive",
        }[value],
    )

    chart = (
        alt.Chart(filtered.sort_values("run_timestamp"))
        .mark_line(point=True)
        .encode(
            x=alt.X("run_timestamp:T", title="Run Time (UTC)"),
            y=alt.Y(f"{metric}:Q", title=metric),
            color=alt.Color("source:N", title="Source"),
            tooltip=[
                "file_name",
                "source",
                "run_timestamp",
                "drain_messages_per_second",
                "total_drain_time_ms",
                "p95_dequeue_latency_ms",
                "p95_archive_latency_ms",
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(chart, use_container_width=True)

    table = filtered[
        [
            "run_timestamp",
            "source",
            "file_name",
            "drain_messages_per_second",
            "total_drain_time_ms",
            "p95_dequeue_latency_ms",
            "p95_archive_latency_ms",
            "throughput_delta_pct",
        ]
    ].sort_values("run_timestamp", ascending=False)
    st.dataframe(table, use_container_width=True, hide_index=True)
