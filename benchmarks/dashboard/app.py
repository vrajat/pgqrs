"""Streamlit dashboard for pgqrs benchmark review."""

from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.dashboard.data import load_dashboard_frame  # noqa: E402
from benchmarks.dashboard.views import backend_summary, runs  # noqa: E402

st.set_page_config(page_title="pgqrs Benchmarks", layout="wide")


@st.cache_data(show_spinner=False)
def _load_frame() -> object:
    return load_dashboard_frame(REPO_ROOT)


def _apply_filters(frame):
    if frame.empty:
        return frame

    st.sidebar.header("Filters")
    sources = st.sidebar.multiselect(
        "Source",
        options=sorted(frame["source"].dropna().unique()),
        default=sorted(frame["source"].dropna().unique()),
    )
    scenarios = st.sidebar.multiselect(
        "Scenario",
        options=sorted(frame["scenario_id"].dropna().unique()),
        default=sorted(frame["scenario_id"].dropna().unique()),
    )
    backends = st.sidebar.multiselect(
        "Backend",
        options=sorted(frame["backend"].dropna().unique()),
        default=sorted(frame["backend"].dropna().unique()),
    )
    bindings = st.sidebar.multiselect(
        "Binding",
        options=sorted(frame["binding"].dropna().unique()),
        default=sorted(frame["binding"].dropna().unique()),
    )
    profiles = st.sidebar.multiselect(
        "Profile",
        options=sorted(frame["profile"].dropna().unique()),
        default=sorted(frame["profile"].dropna().unique()),
    )

    filtered = frame.loc[
        frame["source"].isin(sources)
        & frame["scenario_id"].isin(scenarios)
        & frame["backend"].isin(backends)
        & frame["binding"].isin(bindings)
        & frame["profile"].isin(profiles)
    ].copy()

    if filtered.empty:
        return filtered

    prefill_values = sorted(filtered["prefill_jobs"].dropna().unique())
    selected_prefill = st.sidebar.multiselect(
        "Prefill Jobs",
        options=prefill_values,
        default=prefill_values,
    )
    if selected_prefill:
        filtered = filtered.loc[filtered["prefill_jobs"].isin(selected_prefill)]
    return filtered


st.title("pgqrs Benchmarks")
st.caption(
    "Review backend-specific scaling behavior first, then browse raw runs and curated baselines."
)

frame = _load_frame()
filtered = _apply_filters(frame)

tabs = st.tabs(["Postgres", "SQLite", "S3", "Raw Data", "Baselines"])
with tabs[0]:
    backend_summary.render(
        filtered,
        backend="postgres",
        title="Postgres",
        key_prefix="postgres",
    )
with tabs[1]:
    backend_summary.render(
        filtered,
        backend="sqlite",
        title="SQLite",
        key_prefix="sqlite",
    )
with tabs[2]:
    backend_summary.render(
        filtered,
        backend="s3",
        title="S3",
        key_prefix="s3",
    )
with tabs[3]:
    runs.render(
        filtered,
        title="Raw Data",
        source="raw",
        key_prefix="raw_browse",
    )
with tabs[4]:
    runs.render(
        filtered,
        title="Baselines",
        source="baselines",
        key_prefix="baseline_browse",
    )
