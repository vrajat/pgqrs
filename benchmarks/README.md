# Benchmarks

This directory is the home for the pgqrs benchmark program.

## What Exists Here Today

This directory is currently a scaffold.

It contains:

- a benchmark workspace layout
- high-level instructions
- placeholder Python orchestration modules
- placeholder executor modules
- scenario templates
- three concrete starter queue scenarios
- a placeholder Streamlit dashboard
- storage conventions for benchmark artifacts

It does not yet contain real benchmark implementations.

## Design Direction

The detailed design lives in:

- [`engg/design/queue-workflow-benchmarking.md`](/Users/rajatvenkatesh/code/pgqrs/benchmark_worktree/engg/design/queue-workflow-benchmarking.md)

`engg/` should be treated as the directional template for how this benchmark program evolves.

This directory should implement that design incrementally, not reinvent it locally.

## Directory Layout

```text
benchmarks/
  bench/         # orchestration, schema, loading, storage helpers
  scenarios/     # declarative workload specs
  executors/     # Rust and Python benchmark implementations
  dashboard/     # Streamlit review UI
  data/          # local raw runs and curated baselines
```

## Getting Started

### 0. Bootstrap The Benchmark Environment

Use the repository Make target:

```bash
make benchmark-bootstrap
```

This does two things:

- builds and installs the local `pgqrs` wheel into `.venv`
- installs the benchmark package, CLI, dashboard dependencies, and Python tooling into `.venv`

After bootstrap, the recommended entrypoints are:

```bash
make benchmark-list
make benchmark-run SCENARIO=queue.drain_fixed_backlog BACKEND=sqlite BINDING=python
make benchmark-dashboard
uv run python -m benchmarks.bench.cli run --help
```

The benchmark CLI uses `Typer`.
By default it shows phase progress bars.
Use `--verbose` for benchmark logs and `--no-progress` if you need plain output.

### 1. Pick One Scenario

Start with a single benchmark scenario, for example:

- `queue.e2e.steady_state`
- `queue.drain_fixed_backlog`
- `queue.e2e.capacity_knee`
- `workflow.simple.1_step`

Do not implement the entire matrix first.

The current starter scenarios live in:

- `benchmarks/scenarios/queue/e2e_steady_state.toml`
- `benchmarks/scenarios/queue/drain_fixed_backlog.toml`
- `benchmarks/scenarios/queue/e2e_capacity_knee.toml`

### 2. Write The Scenario Spec

Add or copy a template in:

- `benchmarks/scenarios/queue/`
- `benchmarks/scenarios/workflow/`

Scenario specs should be declarative.

They should describe:

- benchmark ID
- question
- backend support
- binding support
- action
- answer

The question should be the thing we want to know.
The action is the method used to answer it.
The answer describes the curves, series, or summaries the dashboard should present.

They should not contain execution logic.

### 3. Register The Scenario

Update:

- `benchmarks/bench/registry.py`

The registry should decide:

- which executors are allowed
- which backends are allowed
- which profiles are allowed

### 4. Implement The Executor

Add the first real implementation in one of:

- `benchmarks/executors/python/`
- `benchmarks/executors/rust/`

Keep executor logic benchmark-specific.
Keep orchestration logic in `benchmarks/bench/`.

### 5. Write Results As JSONL

The canonical stored format is JSONL.

Store local run artifacts under:

- `benchmarks/data/raw/`

The dashboard should read JSONL directly and flatten in memory.

### 6. Curate Baselines Deliberately

Do not treat every local run as something to check in.

Use:

- `benchmarks/data/raw/` for local or temporary runs
- `benchmarks/data/baselines/` for selected runs worth keeping as references

Suggested rule:

- raw runs are usually local and ignored
- curated baselines can be promoted intentionally and committed when useful

## Storage Rules

### Canonical Format

Persist only:

- `JSONL`

Do not persist a second CSV representation by default.

### Raw Runs

Raw runs belong in:

- `benchmarks/data/raw/`

This directory is for:

- local runs
- ad hoc experiments
- pre-baseline comparisons

### Baselines

Curated reference runs belong in:

- `benchmarks/data/baselines/`

These are the runs that should be stable enough to compare against later.

### What To Check In

Good candidates to check in:

- schema examples
- tiny sample data for dashboard development
- a small number of curated baselines

Bad candidates to check in:

- every local run
- large result dumps
- experimental or throwaway measurements

## Dashboard

The dashboard entrypoint is:

```bash
make benchmark-dashboard
```

The dashboard should help answer:

- what scenarios have been run?
- what baselines exist?
- how do two runs compare?
- how do results differ by backend, binding, or S3 target?

## Orchestrator

The long-term platform choice is:

- a thin custom Python orchestrator

It should own:

- scenario lookup
- backend setup and reset
- executor invocation
- result writing
- loading and comparison support

It should not own benchmark-specific hot-path logic.
