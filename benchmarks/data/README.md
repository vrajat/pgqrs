# Benchmark Data

This directory holds benchmark artifacts.

## Layout

- `raw/` contains local and ad hoc JSONL run artifacts
- `baselines/` contains curated reference artifacts

## Canonical Storage Format

Persist benchmark results as:

- `JSONL`

Do not store a second CSV representation by default.

If a table export is useful for debugging, generate it from JSONL on demand and treat it as disposable.

## Retention Guidance

- local experiments should usually stay in `raw/` and remain uncommitted
- only intentionally selected baseline runs should move into `baselines/`
