"""Helpers for writing canonical JSONL results."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from benchmarks.bench.schema import RunPointResult, RunSpec


def default_output_path(spec: RunSpec) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = Path("benchmarks/data/raw") / spec.scenario_id
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"{ts}-{spec.backend}-{spec.binding}-{spec.profile}.jsonl"
    return directory / filename


def init_jsonl(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def append_jsonl(path: Path, result: RunPointResult) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "metadata": result.metadata,
                    "summary": result.summary,
                    "samples": result.samples,
                },
                sort_keys=True,
            )
        )
        handle.write("\n")
