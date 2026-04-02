"""Flatten JSONL records in memory for dashboard tables and charts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

S3_PUT_COST_PER_1000 = 0.005
S3_READ_COST_PER_1000 = 0.0004
S3_DB_BASE_BYTES = 108_000
S3_DB_BYTES_PER_MESSAGE = 194


def _parse_run_timestamp(path: Path) -> datetime | None:
    prefix = path.name.split("-", 1)[0]
    try:
        return datetime.strptime(prefix, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def iter_result_files(data_root: Path) -> list[tuple[str, Path]]:
    """Return JSONL result files from raw and baseline stores."""

    files: list[tuple[str, Path]] = []
    for source in ("raw", "baselines"):
        root = data_root / source
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.jsonl")):
            files.append((source, path))
    return files


def flatten_result_files(result_files: list[tuple[str, Path]]) -> pd.DataFrame:
    """Flatten JSONL result files into a single analysis table."""

    rows: list[dict[str, Any]] = []
    for source, path in result_files:
        run_timestamp = _parse_run_timestamp(path)
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            metadata = payload.get("metadata", {})
            summary = payload.get("summary", {})
            fixed = metadata.get("fixed_parameters", {})
            point = metadata.get("point_parameters", {})
            s3_toxics = fixed.get("s3_toxics", {}) or {}
            rows.append(
                {
                    "source": source,
                    "file_path": str(path),
                    "file_name": path.name,
                    "scenario_dir": path.parent.name,
                    "run_timestamp": run_timestamp,
                    "line_number": line_number,
                    "run_id": metadata.get("run_id"),
                    "scenario_id": metadata.get("scenario_id"),
                    "question": metadata.get("question"),
                    "backend": metadata.get("backend"),
                    "binding": metadata.get("binding"),
                    "profile": metadata.get("profile"),
                    "payload_profile": fixed.get("payload_profile"),
                    "prefill_jobs": fixed.get("prefill_jobs"),
                    "queue_mode": fixed.get("queue_mode"),
                    "process_mode": fixed.get("process_mode"),
                    "durability_mode": fixed.get("durability_mode"),
                    "s3_latency_profile": fixed.get("s3_latency_profile"),
                    "s3_transport": fixed.get("s3_transport"),
                    "s3_endpoint_url": fixed.get("s3_endpoint_url"),
                    "s3_latency_ms": s3_toxics.get("latency_ms"),
                    "s3_jitter_ms": s3_toxics.get("jitter_ms"),
                    "s3_proxy_name": s3_toxics.get("proxy_name"),
                    "consumers": point.get("consumers"),
                    "dequeue_batch_size": point.get("dequeue_batch_size"),
                    "drain_messages_per_second": summary.get(
                        "drain_messages_per_second"
                    ),
                    "total_drain_time_ms": summary.get("total_drain_time_ms"),
                    "p95_dequeue_latency_ms": summary.get("p95_dequeue_latency_ms"),
                    "p95_archive_latency_ms": summary.get("p95_archive_latency_ms"),
                    "processed_messages": summary.get("processed_messages"),
                    "dequeue_calls": summary.get("dequeue_calls"),
                    "archive_calls": summary.get("archive_calls"),
                    "dequeue_conflicts": summary.get("dequeue_conflicts"),
                    "empty_polls": summary.get("empty_polls"),
                    "error_rate": summary.get("error_rate"),
                    "s3_ops_put": summary.get("s3_ops_put"),
                    "s3_ops_get": summary.get("s3_ops_get"),
                    "s3_ops_head": summary.get("s3_ops_head"),
                    "s3_ops_delete": summary.get("s3_ops_delete"),
                    "s3_ops_total": summary.get("s3_ops_total"),
                    "s3_ops_per_message": summary.get("s3_ops_per_message"),
                    "s3_io_localstack_rx_bytes": summary.get(
                        "s3_io_localstack_rx_bytes"
                    ),
                    "s3_io_localstack_tx_bytes": summary.get(
                        "s3_io_localstack_tx_bytes"
                    ),
                    "s3_io_localstack_total_bytes": summary.get(
                        "s3_io_localstack_total_bytes"
                    ),
                    "s3_io_toxiproxy_rx_bytes": summary.get("s3_io_toxiproxy_rx_bytes"),
                    "s3_io_toxiproxy_tx_bytes": summary.get("s3_io_toxiproxy_tx_bytes"),
                    "s3_io_toxiproxy_total_bytes": summary.get(
                        "s3_io_toxiproxy_total_bytes"
                    ),
                    "s3_io_localstack_bytes_per_message": summary.get(
                        "s3_io_localstack_bytes_per_message"
                    ),
                    "s3_io_toxiproxy_bytes_per_message": summary.get(
                        "s3_io_toxiproxy_bytes_per_message"
                    ),
                    "s3_io_localstack_bytes_per_dequeue_call": summary.get(
                        "s3_io_localstack_bytes_per_dequeue_call"
                    ),
                    "s3_io_toxiproxy_bytes_per_dequeue_call": summary.get(
                        "s3_io_toxiproxy_bytes_per_dequeue_call"
                    ),
                    "s3_io_localstack_bytes_per_archive_call": summary.get(
                        "s3_io_localstack_bytes_per_archive_call"
                    ),
                    "s3_io_toxiproxy_bytes_per_archive_call": summary.get(
                        "s3_io_toxiproxy_bytes_per_archive_call"
                    ),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "source",
                "file_path",
                "file_name",
                "scenario_dir",
                "run_timestamp",
                "line_number",
                "run_id",
                "scenario_id",
                "question",
                "backend",
                "binding",
                "profile",
                "payload_profile",
                "prefill_jobs",
                "queue_mode",
                "process_mode",
                "durability_mode",
                "s3_latency_profile",
                "s3_transport",
                "s3_endpoint_url",
                "s3_latency_ms",
                "s3_jitter_ms",
                "s3_proxy_name",
                "consumers",
                "dequeue_batch_size",
                "drain_messages_per_second",
                "total_drain_time_ms",
                "p95_dequeue_latency_ms",
                "p95_archive_latency_ms",
                "processed_messages",
                "dequeue_calls",
                "archive_calls",
                "dequeue_conflicts",
                "empty_polls",
                "error_rate",
                "s3_ops_put",
                "s3_ops_get",
                "s3_ops_head",
                "s3_ops_delete",
                "s3_ops_total",
                "s3_ops_per_message",
                "s3_io_localstack_rx_bytes",
                "s3_io_localstack_tx_bytes",
                "s3_io_localstack_total_bytes",
                "s3_io_toxiproxy_rx_bytes",
                "s3_io_toxiproxy_tx_bytes",
                "s3_io_toxiproxy_total_bytes",
                "s3_io_localstack_bytes_per_message",
                "s3_io_toxiproxy_bytes_per_message",
                "s3_io_localstack_bytes_per_dequeue_call",
                "s3_io_toxiproxy_bytes_per_dequeue_call",
                "s3_io_localstack_bytes_per_archive_call",
                "s3_io_toxiproxy_bytes_per_archive_call",
            ]
        )

    frame = pd.DataFrame(rows)
    for column in ("s3_ops_put", "s3_ops_get", "s3_ops_head"):
        if column not in frame.columns:
            frame[column] = 0
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    s3_cost = (
        frame["s3_ops_put"].fillna(0) * S3_PUT_COST_PER_1000
        + (frame["s3_ops_get"].fillna(0) + frame["s3_ops_head"].fillna(0))
        * S3_READ_COST_PER_1000
    ) / 1000.0
    frame["s3_est_request_cost_usd"] = s3_cost.where(frame["backend"] == "s3", pd.NA)
    processed_messages = pd.to_numeric(
        frame.get("processed_messages"), errors="coerce"
    ).replace(0, pd.NA)
    frame["s3_est_request_cost_per_message_usd"] = (
        frame["s3_est_request_cost_usd"] / processed_messages
    ).where(frame["backend"] == "s3", pd.NA)

    s3_rows = frame["backend"] == "s3"
    put_get_ops = frame["s3_ops_put"].fillna(0) + frame["s3_ops_get"].fillna(0)
    prefill_jobs = pd.to_numeric(frame.get("prefill_jobs"), errors="coerce").fillna(0)
    object_size_estimate = S3_DB_BASE_BYTES + prefill_jobs * S3_DB_BYTES_PER_MESSAGE
    frame["s3_est_object_bytes"] = object_size_estimate.where(s3_rows, pd.NA)
    frame["s3_est_payload_transfer_mb"] = (
        (put_get_ops * object_size_estimate) / 1_000_000.0
    ).where(s3_rows, pd.NA)

    group_keys = [
        "source",
        "file_name",
        "scenario_id",
        "backend",
        "binding",
        "profile",
        "consumers",
    ]
    batch1 = frame.loc[
        frame["dequeue_batch_size"] == 1,
        group_keys
        + [
            "s3_est_request_cost_per_message_usd",
            "s3_est_payload_transfer_mb",
        ],
    ].rename(
        columns={
            "s3_est_request_cost_per_message_usd": "s3_cost_per_msg_batch1",
            "s3_est_payload_transfer_mb": "s3_payload_batch1_mb",
        }
    )
    frame = frame.merge(batch1, how="left", on=group_keys)
    frame["s3_cost_per_msg_vs_batch1_x"] = (
        frame["s3_est_request_cost_per_message_usd"] / frame["s3_cost_per_msg_batch1"]
    )
    frame["s3_payload_vs_batch1_x"] = (
        frame["s3_est_payload_transfer_mb"] / frame["s3_payload_batch1_mb"]
    )
    frame["s3_cost_per_msg_reduction_vs_batch1_pct"] = (
        1.0 - frame["s3_cost_per_msg_vs_batch1_x"]
    ) * 100.0
    frame["s3_payload_reduction_vs_batch1_pct"] = (
        1.0 - frame["s3_payload_vs_batch1_x"]
    ) * 100.0

    if "s3_io_toxiproxy_total_bytes" in frame.columns:
        frame["s3_transfer_mb"] = (
            pd.to_numeric(frame["s3_io_toxiproxy_total_bytes"], errors="coerce")
            / 1_000_000.0
        )
    else:
        frame["s3_transfer_mb"] = pd.NA
    if frame["run_timestamp"].notna().any():
        frame = frame.sort_values(
            by=["run_timestamp", "file_name", "line_number"],
            ascending=[False, True, True],
        )
    return frame.reset_index(drop=True)
