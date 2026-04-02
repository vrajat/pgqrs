"""Flatten JSONL records in memory for dashboard tables and charts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


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
                    "empty_polls": summary.get("empty_polls"),
                    "error_rate": summary.get("error_rate"),
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
                "empty_polls",
                "error_rate",
            ]
        )

    frame = pd.DataFrame(rows)
    if frame["run_timestamp"].notna().any():
        frame = frame.sort_values(
            by=["run_timestamp", "file_name", "line_number"],
            ascending=[False, True, True],
        )
    return frame.reset_index(drop=True)
