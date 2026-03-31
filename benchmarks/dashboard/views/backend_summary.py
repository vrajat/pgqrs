"""Backend-specific summary view for benchmark landing pages."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def _default_run_file(frame: pd.DataFrame) -> str:
    baselines = frame.loc[frame["source"] == "baselines"].copy()
    if not baselines.empty:
        baselines = baselines.sort_values(
            by=["run_timestamp", "file_name"],
            ascending=[False, False],
        )
        return str(baselines.iloc[0]["file_name"])

    latest = frame.sort_values(
        by=["run_timestamp", "file_name"], ascending=[False, False]
    )
    return str(latest.iloc[0]["file_name"])


def _lookup_metric(
    frame: pd.DataFrame,
    *,
    metric: str,
    consumers: int,
    batch_size: int,
) -> float | None:
    row = frame.loc[
        (frame["consumers"] == consumers) & (frame["dequeue_batch_size"] == batch_size),
        metric,
    ]
    if row.empty:
        return None
    return float(row.iloc[0])


def _scale_summary(
    frame: pd.DataFrame,
    *,
    metric: str,
    start_consumers: int,
    start_batch: int,
    end_consumers: int,
    end_batch: int,
) -> tuple[str, str]:
    start = _lookup_metric(
        frame,
        metric=metric,
        consumers=start_consumers,
        batch_size=start_batch,
    )
    end = _lookup_metric(
        frame,
        metric=metric,
        consumers=end_consumers,
        batch_size=end_batch,
    )
    if start is None or end is None or start == 0:
        return ("n/a", "missing point")

    ratio = end / start
    delta_pct = ((end - start) / start) * 100.0
    return (f"{ratio:.2f}x", f"{delta_pct:+.1f}%")


def _file_label(frame: pd.DataFrame, file_name: str) -> str:
    row = frame.loc[frame["file_name"] == file_name].iloc[0]
    source = row["source"]
    timestamp = row["run_timestamp"]
    if pd.notna(timestamp):
        return f"{file_name} [{source}] {timestamp:%Y-%m-%d %H:%M UTC}"
    return f"{file_name} [{source}]"


def _render_throughput_section(frame: pd.DataFrame, *, key_prefix: str) -> None:
    st.markdown("#### Throughput Scaling")

    cards = st.columns(4)
    value, delta = _scale_summary(
        frame,
        metric="drain_messages_per_second",
        start_consumers=1,
        start_batch=1,
        end_consumers=4,
        end_batch=1,
    )
    cards[0].metric("Consumers 1→4 @ batch 1", value, delta)

    value, delta = _scale_summary(
        frame,
        metric="drain_messages_per_second",
        start_consumers=1,
        start_batch=10,
        end_consumers=4,
        end_batch=10,
    )
    cards[1].metric("Consumers 1→4 @ batch 10", value, delta)

    value, delta = _scale_summary(
        frame,
        metric="drain_messages_per_second",
        start_consumers=1,
        start_batch=1,
        end_consumers=1,
        end_batch=50,
    )
    cards[2].metric("Batch 1→50 @ 1 consumer", value, delta)

    value, delta = _scale_summary(
        frame,
        metric="drain_messages_per_second",
        start_consumers=4,
        start_batch=1,
        end_consumers=4,
        end_batch=50,
    )
    cards[3].metric("Batch 1→50 @ 4 consumers", value, delta)

    chart_cols = st.columns(2)

    consumers_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("consumers:O", title="Consumers"),
            y=alt.Y(
                "drain_messages_per_second:Q",
                title="Throughput (msg/s)",
            ),
            color=alt.Color("dequeue_batch_size:N", title="Batch Size"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "drain_messages_per_second",
                "total_drain_time_ms",
            ],
        )
        .properties(height=340)
    )
    chart_cols[0].altair_chart(
        consumers_chart,
        width="stretch",
        key=f"{key_prefix}_throughput_by_consumers",
    )

    batch_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("dequeue_batch_size:O", title="Batch Size"),
            y=alt.Y(
                "drain_messages_per_second:Q",
                title="Throughput (msg/s)",
            ),
            color=alt.Color("consumers:N", title="Consumers"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "drain_messages_per_second",
                "total_drain_time_ms",
            ],
        )
        .properties(height=340)
    )
    chart_cols[1].altair_chart(
        batch_chart,
        width="stretch",
        key=f"{key_prefix}_throughput_by_batch",
    )


def _render_latency_section(frame: pd.DataFrame, *, key_prefix: str) -> None:
    st.markdown("#### Latency Scaling")

    cards = st.columns(4)
    value, delta = _scale_summary(
        frame,
        metric="p95_dequeue_latency_ms",
        start_consumers=1,
        start_batch=1,
        end_consumers=4,
        end_batch=1,
    )
    cards[0].metric("P95 dequeue 1→4 @ batch 1", value, delta)

    value, delta = _scale_summary(
        frame,
        metric="p95_dequeue_latency_ms",
        start_consumers=1,
        start_batch=50,
        end_consumers=4,
        end_batch=50,
    )
    cards[1].metric("P95 dequeue 1→4 @ batch 50", value, delta)

    value, delta = _scale_summary(
        frame,
        metric="p95_archive_latency_ms",
        start_consumers=1,
        start_batch=1,
        end_consumers=4,
        end_batch=1,
    )
    cards[2].metric("P95 archive 1→4 @ batch 1", value, delta)

    value, delta = _scale_summary(
        frame,
        metric="p95_archive_latency_ms",
        start_consumers=1,
        start_batch=50,
        end_consumers=4,
        end_batch=50,
    )
    cards[3].metric("P95 archive 1→4 @ batch 50", value, delta)

    latency_cols = st.columns(2)

    dequeue_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("consumers:O", title="Consumers"),
            y=alt.Y("p95_dequeue_latency_ms:Q", title="P95 Dequeue Latency (ms)"),
            color=alt.Color("dequeue_batch_size:N", title="Batch Size"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "p95_dequeue_latency_ms",
            ],
        )
        .properties(height=320)
    )
    latency_cols[0].altair_chart(
        dequeue_chart,
        width="stretch",
        key=f"{key_prefix}_dequeue_latency",
    )

    archive_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("consumers:O", title="Consumers"),
            y=alt.Y("p95_archive_latency_ms:Q", title="P95 Archive Latency (ms)"),
            color=alt.Color("dequeue_batch_size:N", title="Batch Size"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "p95_archive_latency_ms",
            ],
        )
        .properties(height=320)
    )
    latency_cols[1].altair_chart(
        archive_chart,
        width="stretch",
        key=f"{key_prefix}_archive_latency",
    )


def render(frame: pd.DataFrame, *, backend: str, title: str, key_prefix: str) -> None:
    """Render a backend-specific landing view."""

    backend_frame = frame.loc[
        (frame["scenario_id"] == "queue.drain_fixed_backlog")
        & (frame["binding"] == "rust")
        & (frame["backend"] == backend)
    ].copy()

    st.subheader(title)
    if backend_frame.empty:
        st.info(f"No `queue.drain_fixed_backlog` Rust results found for {backend}.")
        return

    if backend == "s3":
        st.warning(
            "S3 results are directional for now and are not directly comparable to the "
            "other backend baselines. Current S3 runs use the LocalStack+Toxiproxy "
            "emulation path and smaller practical backlogs than the curated non-S3 "
            "baseline runs."
        )

    run_files = backend_frame["file_name"].drop_duplicates().tolist()
    default_file = _default_run_file(backend_frame)
    selected_file = st.selectbox(
        "Run",
        run_files,
        index=run_files.index(default_file),
        key=f"{key_prefix}_run_file",
        format_func=lambda file_name: _file_label(backend_frame, file_name),
    )
    selected = backend_frame.loc[backend_frame["file_name"] == selected_file].copy()
    selected = selected.sort_values(by=["consumers", "dequeue_batch_size"])

    row = selected.iloc[0]
    st.caption(
        f"Showing `{selected_file}` | source={row['source']} | "
        f"prefill={row['prefill_jobs']} | profile={row['profile']}"
    )

    _render_throughput_section(selected, key_prefix=key_prefix)
    _render_latency_section(selected, key_prefix=key_prefix)

    with st.expander("Raw Points", expanded=False):
        st.dataframe(
            selected[
                [
                    "consumers",
                    "dequeue_batch_size",
                    "drain_messages_per_second",
                    "total_drain_time_ms",
                    "p95_dequeue_latency_ms",
                    "p95_archive_latency_ms",
                ]
            ],
            width="stretch",
            hide_index=True,
        )
