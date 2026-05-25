"""Dashboard data loading helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from benchmarks.bench.baselines import attach_latest_baseline_delta
from benchmarks.bench.flatten import flatten_result_files, iter_result_files

OPTIONAL_COLUMNS = (
    "process_mode",
    "durability_mode",
    "s3_latency_profile",
    "s3_transport",
    "s3_endpoint_url",
    "s3_latency_ms",
    "s3_jitter_ms",
    "s3_proxy_name",
    "s3_ops_put",
    "s3_ops_get",
    "s3_ops_head",
    "s3_ops_delete",
    "s3_ops_total",
    "s3_ops_per_message",
    "s3_est_request_cost_usd",
    "s3_est_request_cost_per_message_usd",
    "s3_cost_per_msg_vs_batch1_x",
    "s3_payload_vs_batch1_x",
    "s3_cost_per_msg_reduction_vs_batch1_pct",
    "s3_payload_reduction_vs_batch1_pct",
    "s3_est_object_bytes",
    "s3_est_payload_transfer_mb",
    "s3_transfer_mb",
    "s3_io_localstack_total_bytes",
    "s3_io_toxiproxy_total_bytes",
)


def load_dashboard_frame(repo_root: Path) -> pd.DataFrame:
    """Load and enrich benchmark result rows for dashboard views."""

    data_root = repo_root / "benchmarks" / "data"
    frame = flatten_result_files(iter_result_files(data_root))
    if frame.empty:
        return frame
    frame = attach_latest_baseline_delta(frame)
    for column in OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame
