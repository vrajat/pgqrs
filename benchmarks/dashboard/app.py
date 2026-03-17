"""Placeholder Streamlit dashboard for pgqrs benchmarks."""

from __future__ import annotations

import streamlit as st


st.set_page_config(page_title="pgqrs Benchmarks", layout="wide")

st.title("pgqrs Benchmarks")
st.caption("Scaffold dashboard for benchmark review")

st.markdown(
    """
This dashboard is a placeholder.

Planned responsibilities:

- list available runs from `benchmarks/data/raw/`
- compare runs against curated baselines
- filter by scenario, backend, binding, profile, and S3 target
- flatten JSONL artifacts in memory for tables and charts
"""
)

st.info(
    "No benchmark loader is implemented yet. Start by adding a real loader in "
    "`benchmarks/bench/loader.py` and then wire the first view here."
)
