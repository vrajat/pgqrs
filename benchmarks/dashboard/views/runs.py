"""Runs view."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def _ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = pd.NA
    return out


RUN_COLUMNS = [
    "scenario_id",
    "backend",
    "binding",
    "profile",
    "prefill_jobs",
    "durability_mode",
    "s3_latency_profile",
    "s3_transport",
    "s3_latency_ms",
    "s3_jitter_ms",
    "consumers",
    "dequeue_batch_size",
    "drain_messages_per_second",
    "total_drain_time_ms",
    "p95_dequeue_latency_ms",
    "p95_archive_latency_ms",
    "throughput_delta_pct",
    "s3_ops_put",
    "s3_ops_get",
    "s3_ops_head",
    "s3_ops_total",
    "s3_est_payload_transfer_mb",
    "s3_est_request_cost_usd",
    "s3_est_request_cost_per_message_usd",
    "s3_cost_per_msg_vs_batch1_x",
    "s3_payload_vs_batch1_x",
]

OPTIONAL_RUN_COLUMNS = [
    "durability_mode",
    "s3_latency_profile",
    "s3_transport",
    "s3_latency_ms",
    "s3_jitter_ms",
    "s3_ops_put",
    "s3_ops_get",
    "s3_ops_head",
    "s3_ops_total",
    "s3_est_payload_transfer_mb",
    "s3_est_request_cost_usd",
    "s3_est_request_cost_per_message_usd",
    "s3_cost_per_msg_vs_batch1_x",
    "s3_payload_vs_batch1_x",
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

    row = selected.iloc[0]
    meta = st.columns(4)
    meta[0].metric("Scenario", row["scenario_id"])
    meta[1].metric("Backend", row["backend"])
    meta[2].metric("Binding", row["binding"])
    meta[3].metric("Points", len(selected))

    st.caption(
        f"source={row['source']} | profile={row['profile']} | prefill={row['prefill_jobs']}"
    )
    if row["backend"] == "s3":
        s3_parts = []
        if pd.notna(row.get("durability_mode")):
            s3_parts.append(f"durability={row.get('durability_mode')}")
        if pd.notna(row.get("s3_latency_profile")):
            s3_parts.append(f"latency_profile={row.get('s3_latency_profile')}")
        if pd.notna(row.get("s3_transport")):
            s3_parts.append(f"transport={row.get('s3_transport')}")
        if pd.notna(row.get("s3_latency_ms")):
            s3_parts.append(f"toxiproxy_latency_ms={int(row['s3_latency_ms'])}")
        if pd.notna(row.get("s3_jitter_ms")):
            s3_parts.append(f"toxiproxy_jitter_ms={int(row['s3_jitter_ms'])}")
        if pd.notna(row.get("s3_endpoint_url")):
            s3_parts.append(f"endpoint={row['s3_endpoint_url']}")
        if s3_parts:
            st.caption(" | ".join(s3_parts))

        put_total = pd.to_numeric(selected["s3_ops_put"], errors="coerce").sum()
        get_total = pd.to_numeric(selected["s3_ops_get"], errors="coerce").sum()
        head_total = pd.to_numeric(selected["s3_ops_head"], errors="coerce").sum()
        ops_total = pd.to_numeric(selected["s3_ops_total"], errors="coerce").sum()
        transfer_total_mb = pd.to_numeric(
            selected["s3_est_payload_transfer_mb"], errors="coerce"
        ).sum()
        request_cost_total = pd.to_numeric(
            selected["s3_est_request_cost_usd"], errors="coerce"
        ).sum()
        request_cost_per_msg = pd.to_numeric(
            selected["s3_est_request_cost_per_message_usd"], errors="coerce"
        ).dropna()
        cost_vs_batch1 = pd.to_numeric(
            selected["s3_cost_per_msg_vs_batch1_x"], errors="coerce"
        ).dropna()
        payload_vs_batch1 = pd.to_numeric(
            selected["s3_payload_vs_batch1_x"], errors="coerce"
        ).dropna()
        st.caption(
            " | ".join(
                [
                    f"S3 ops total={int(ops_total)}",
                    f"PUT={int(put_total)}",
                    f"GET={int(get_total)}",
                    f"HEAD={int(head_total)}",
                    f"payload transfer≈{transfer_total_mb:.2f} MB",
                    f"est request cost≈${request_cost_total:.4f}",
                    (
                        f"avg cost/message≈${request_cost_per_msg.mean():.8f}"
                        if not request_cost_per_msg.empty
                        else "avg cost/message=n/a"
                    ),
                    (
                        f"cost vs batch1 avg={cost_vs_batch1.mean():.3f}x"
                        if not cost_vs_batch1.empty
                        else "cost vs batch1=n/a"
                    ),
                    (
                        f"payload vs batch1 avg={payload_vs_batch1.mean():.3f}x"
                        if not payload_vs_batch1.empty
                        else "payload vs batch1=n/a"
                    ),
                ]
            )
        )

    display_columns = list(RUN_COLUMNS)
    for column in OPTIONAL_RUN_COLUMNS:
        if column not in selected.columns:
            selected[column] = None

    display = (
        selected[display_columns]
        .copy()
        .rename(
            columns={
                "scenario_id": "scenario",
                "prefill_jobs": "prefill",
                "durability_mode": "durability",
                "s3_latency_profile": "s3_profile",
                "s3_transport": "s3_transport",
                "s3_latency_ms": "s3_latency_ms",
                "s3_jitter_ms": "s3_jitter_ms",
                "dequeue_batch_size": "batch",
                "drain_messages_per_second": "throughput",
                "total_drain_time_ms": "drain_ms",
                "p95_dequeue_latency_ms": "p95_dequeue_ms",
                "p95_archive_latency_ms": "p95_archive_ms",
                "throughput_delta_pct": "vs_baseline_%",
                "s3_ops_put": "s3_put",
                "s3_ops_get": "s3_get",
                "s3_ops_head": "s3_head",
                "s3_ops_total": "s3_ops_total",
                "s3_est_payload_transfer_mb": "s3_payload_mb",
                "s3_est_request_cost_usd": "s3_req_cost_usd",
                "s3_est_request_cost_per_message_usd": "s3_req_cost_per_msg_usd",
                "s3_cost_per_msg_vs_batch1_x": "s3_cost_vs_batch1_x",
                "s3_payload_vs_batch1_x": "s3_payload_vs_batch1_x",
            }
        )
    )
    st.dataframe(display, width="stretch", hide_index=True)
