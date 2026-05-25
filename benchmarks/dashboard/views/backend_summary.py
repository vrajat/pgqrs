"""Backend-specific summary view for benchmark landing pages."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def _ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = pd.NA
    return out


def _row_value(row: pd.Series, key: str):
    return row.get(key)


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


def _latency_summary(
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
    if start is None or end is None:
        return ("n/a", "missing point")

    delta_ms = end - start
    if start == 0:
        return (f"{end:.3f} ms", f"{delta_ms:+.3f} ms")

    ratio = end / start
    return (f"{end:.3f} ms", f"{delta_ms:+.3f} ms ({ratio:.2f}x)")


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
    value, delta = _latency_summary(
        frame,
        metric="p95_dequeue_latency_ms",
        start_consumers=1,
        start_batch=1,
        end_consumers=4,
        end_batch=1,
    )
    cards[0].metric("P95 dequeue @ 4 consumers, batch 1", value, delta)

    value, delta = _latency_summary(
        frame,
        metric="p95_dequeue_latency_ms",
        start_consumers=1,
        start_batch=50,
        end_consumers=4,
        end_batch=50,
    )
    cards[1].metric("P95 dequeue @ 4 consumers, batch 50", value, delta)

    value, delta = _latency_summary(
        frame,
        metric="p95_archive_latency_ms",
        start_consumers=1,
        start_batch=1,
        end_consumers=4,
        end_batch=1,
    )
    cards[2].metric("P95 archive @ 4 consumers, batch 1", value, delta)

    value, delta = _latency_summary(
        frame,
        metric="p95_archive_latency_ms",
        start_consumers=1,
        start_batch=50,
        end_consumers=4,
        end_batch=50,
    )
    cards[3].metric("P95 archive @ 4 consumers, batch 50", value, delta)

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


def _render_s3_cost_section(frame: pd.DataFrame, *, key_prefix: str) -> None:
    frame = _ensure_columns(
        frame,
        [
            "s3_ops_put",
            "s3_ops_get",
            "s3_ops_head",
            "s3_ops_total",
            "s3_ops_per_message",
            "s3_est_payload_transfer_mb",
            "s3_est_request_cost_usd",
            "s3_est_request_cost_per_message_usd",
            "s3_cost_per_msg_vs_batch1_x",
            "s3_payload_vs_batch1_x",
            "s3_cost_per_msg_reduction_vs_batch1_pct",
            "s3_payload_reduction_vs_batch1_pct",
        ],
    )
    st.markdown("#### S3 API, Transfer, And Cost")

    cards = st.columns(4)
    ops_series = pd.to_numeric(frame["s3_ops_total"], errors="coerce")
    transfer_series = pd.to_numeric(
        frame["s3_est_payload_transfer_mb"], errors="coerce"
    )
    req_cost_series = pd.to_numeric(frame["s3_est_request_cost_usd"], errors="coerce")
    req_cost_per_msg_series = pd.to_numeric(
        frame["s3_est_request_cost_per_message_usd"], errors="coerce"
    )
    ops_total = ops_series.sum()
    transfer_total_mb = transfer_series.sum()
    req_cost_total = req_cost_series.sum()
    cards[0].metric(
        "S3 API calls (run)",
        f"{int(ops_total):,}" if ops_series.notna().any() else "n/a",
    )
    cards[1].metric(
        "Payload transfer (run)",
        f"{transfer_total_mb:.2f} MB" if transfer_series.notna().any() else "n/a",
    )
    cards[2].metric(
        "Estimated request cost (run)",
        f"${req_cost_total:.4f}" if req_cost_series.notna().any() else "n/a",
    )
    cards[3].metric(
        "Avg request cost/message",
        (
            f"${req_cost_per_msg_series.dropna().mean():.8f}"
            if req_cost_per_msg_series.notna().any()
            else "n/a"
        ),
    )

    rel_cards = st.columns(4)
    for idx, consumers in enumerate((1, 2, 4, None)):
        if consumers is None:
            cost_reduction = pd.to_numeric(
                frame["s3_cost_per_msg_reduction_vs_batch1_pct"], errors="coerce"
            ).dropna()
            payload_reduction = pd.to_numeric(
                frame["s3_payload_reduction_vs_batch1_pct"], errors="coerce"
            ).dropna()
            label = "All consumers"
        else:
            subset = frame.loc[frame["consumers"] == consumers]
            cost_reduction = pd.to_numeric(
                subset["s3_cost_per_msg_reduction_vs_batch1_pct"], errors="coerce"
            ).dropna()
            payload_reduction = pd.to_numeric(
                subset["s3_payload_reduction_vs_batch1_pct"], errors="coerce"
            ).dropna()
            label = f"Consumers={consumers}"
        rel_cards[idx].metric(
            f"{label} cost↓ vs b1",
            f"{cost_reduction.max():.1f}%" if not cost_reduction.empty else "n/a",
            (
                f"payload↓ {payload_reduction.max():.1f}%"
                if not payload_reduction.empty
                else None
            ),
        )

    chart_cols = st.columns(3)

    ops_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("dequeue_batch_size:O", title="Batch Size"),
            y=alt.Y("s3_ops_total:Q", title="S3 API Calls"),
            color=alt.Color("consumers:N", title="Consumers"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "s3_ops_put",
                "s3_ops_get",
                "s3_ops_head",
                "s3_ops_total",
            ],
        )
        .properties(height=320)
    )
    chart_cols[0].altair_chart(
        ops_chart,
        width="stretch",
        key=f"{key_prefix}_s3_ops",
    )

    cost_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("dequeue_batch_size:O", title="Batch Size"),
            y=alt.Y("s3_est_request_cost_usd:Q", title="Estimated Request Cost (USD)"),
            color=alt.Color("consumers:N", title="Consumers"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "s3_est_payload_transfer_mb",
                "s3_est_request_cost_usd",
                "s3_est_request_cost_per_message_usd",
            ],
        )
        .properties(height=320)
    )
    chart_cols[1].altair_chart(
        cost_chart,
        width="stretch",
        key=f"{key_prefix}_s3_cost",
    )

    relative_chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("dequeue_batch_size:O", title="Batch Size"),
            y=alt.Y(
                "s3_cost_per_msg_vs_batch1_x:Q", title="Cost/Message vs Batch1 (x)"
            ),
            color=alt.Color("consumers:N", title="Consumers"),
            tooltip=[
                "consumers",
                "dequeue_batch_size",
                "s3_cost_per_msg_vs_batch1_x",
                "s3_payload_vs_batch1_x",
                "s3_cost_per_msg_reduction_vs_batch1_pct",
            ],
        )
        .properties(height=320)
    )
    chart_cols[2].altair_chart(
        relative_chart,
        width="stretch",
        key=f"{key_prefix}_s3_relative",
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
    if backend == "s3":
        s3_parts = []
        durability_mode = _row_value(row, "durability_mode")
        latency_profile = _row_value(row, "s3_latency_profile")
        transport = _row_value(row, "s3_transport")
        latency_ms = _row_value(row, "s3_latency_ms")
        jitter_ms = _row_value(row, "s3_jitter_ms")
        endpoint_url = _row_value(row, "s3_endpoint_url")

        if pd.notna(durability_mode):
            s3_parts.append(f"durability={durability_mode}")
        if pd.notna(latency_profile):
            s3_parts.append(f"latency_profile={latency_profile}")
        if pd.notna(transport):
            s3_parts.append(f"transport={transport}")
        if pd.notna(latency_ms):
            s3_parts.append(f"toxiproxy_latency_ms={int(latency_ms)}")
        if pd.notna(jitter_ms):
            s3_parts.append(f"toxiproxy_jitter_ms={int(jitter_ms)}")
        if pd.notna(endpoint_url):
            s3_parts.append(f"endpoint={endpoint_url}")
        if s3_parts:
            st.caption(" | ".join(s3_parts))

    _render_throughput_section(selected, key_prefix=key_prefix)
    _render_latency_section(selected, key_prefix=key_prefix)
    if backend == "s3":
        _render_s3_cost_section(selected, key_prefix=key_prefix)

    with st.expander("Raw Points", expanded=False):
        selected = _ensure_columns(
            selected,
            [
                "s3_ops_put",
                "s3_ops_get",
                "s3_ops_head",
                "s3_ops_total",
                "s3_est_payload_transfer_mb",
                "s3_est_request_cost_usd",
                "s3_est_request_cost_per_message_usd",
                "s3_cost_per_msg_vs_batch1_x",
                "s3_payload_vs_batch1_x",
            ],
        )
        columns = [
            "consumers",
            "dequeue_batch_size",
            "drain_messages_per_second",
            "total_drain_time_ms",
            "p95_dequeue_latency_ms",
            "p95_archive_latency_ms",
        ]
        if backend == "s3":
            columns.extend(
                [
                    "s3_ops_put",
                    "s3_ops_get",
                    "s3_ops_head",
                    "s3_ops_total",
                    "s3_est_payload_transfer_mb",
                    "s3_est_request_cost_usd",
                    "s3_est_request_cost_per_message_usd",
                ]
            )
        st.dataframe(selected[columns], width="stretch", hide_index=True)
