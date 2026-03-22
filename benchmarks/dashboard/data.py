"""Dashboard data loading helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from benchmarks.bench.baselines import attach_latest_baseline_delta
from benchmarks.bench.flatten import flatten_result_files, iter_result_files


def load_dashboard_frame(repo_root: Path) -> pd.DataFrame:
    """Load and enrich benchmark result rows for dashboard views."""

    data_root = repo_root / "benchmarks" / "data"
    frame = flatten_result_files(iter_result_files(data_root))
    if frame.empty:
        return frame
    return attach_latest_baseline_delta(frame)
