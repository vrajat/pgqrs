# Queue and Workflow Benchmarking Strategy

**Status**: Draft Design  
**Created**: 2026-03-17  
**Author**: Codex

---

## 1. Overview

pgqrs needs a benchmark program for queue and workflow features that can be run regularly, compared across commits, and used to make design decisions with clear intent.

This document defines that benchmark program.

The central idea is simple:

- every benchmark must answer a product question
- that question should determine the workload shape
- the workload shape should determine the implementation
- the implementation should determine what metrics we collect

The goal is not to produce a random set of performance numbers. The goal is to build a benchmark suite that tells us when queue behavior, workflow durability, or backend-specific performance has materially changed.

---

## 2. Goals & Non-Goals

### Goals

- Detect regressions in queue and workflow behavior across commits and releases.
- Benchmark pgqrs through its queue and workflow APIs rather than only through backend-specific SQL.
- Keep benchmark scenarios logically aligned across PostgreSQL, SQLite, and Turso.
- Treat S3Store as a separate but related benchmark family.
- Treat the Python bindings as a first-class benchmark surface, not as an afterthought.
- Measure the cost of workflow durability features such as step persistence, retry, pause/resume, and crash recovery.
- Support on-demand execution and local review rather than assuming scheduled CI-only benchmarking.
- Produce artifacts that can be tracked over time instead of one-off local observations.

### Non-Goals

- Declaring one backend the universal winner.
- Replacing correctness tests or concurrency tests.
- Comparing backends under invalid operating assumptions.
- Optimizing isolated SQL paths and treating that as the main product benchmark result.
- Building a benchmark system that only works in CI and is awkward to run manually.

---

## 3. Problem Statement

Today there is already a PostgreSQL-specific benchmark harness under [`crates/pgqrs/benchmark`](/Users/rajatvenkatesh/code/pgqrs/benchmark_worktree/crates/pgqrs/benchmark). That harness was used for the skip-locked/unlogged blog post, but it should be treated as legacy code and ignored by this design. It can be deleted once the new benchmark program exists.

Its limitations are structural:

- it focuses on PostgreSQL queue-table behavior
- it measures SQL patterns directly rather than pgqrs queue and workflow APIs
- it does not cover the three database backends with a shared benchmark taxonomy
- it does not cover workflow durability scenarios
- it does not model S3-specific sync and snapshot costs
- it does not treat the Python binding layer as a first-class measurement surface

That makes it good for storage experiments, but not good enough for regular product-level tracking.

We need a design that answers:

- what exactly are we trying to learn from queue benchmarks?
- what exactly are we trying to learn from workflow benchmarks?
- which backends should be compared directly?
- which backend behaviors need their own benchmark families?
- how do we keep results stable enough to track over time?

---

## 4. Design Principles

### 4.1 Every Benchmark Must Have a Decision Question

Each benchmark should exist because it informs a concrete decision.

Every benchmark spec should include:

- `Question`: what decision does this benchmark support?
- `Primary metric`: the one number that best answers the question
- `Secondary metrics`: supporting evidence such as p95 latency, retries, or WAL bytes
- `Valid comparisons`: which backends and profiles can be compared fairly

If the benchmark does not have a decision question, it should not be part of the regular suite.

### 4.2 Benchmark the Product Surface

The primary benchmark suite should measure pgqrs behavior through public APIs:

- queue creation
- producer enqueue and batch enqueue
- consumer dequeue, archive, delete, and visibility operations
- workflow trigger
- run creation
- step acquisition and persistence
- retry and pause/resume behavior

Backend-level SQL benchmarks can remain as supporting diagnostics, but they should not be the main release-tracking signal.

### 4.3 Shared Logical Scenarios Across the Three Database Backends

The three database backends are:

- PostgreSQL
- SQLite
- Turso

These backends should expose the same logical benchmark names and workload definitions whenever the semantics align.

That means:

- the same benchmark IDs should exist across PostgreSQL, SQLite, and Turso
- payload sizes, batch sizes, and workflow step counts should match where possible
- interpretation should stay stable across backends

### 4.4 Treat Language Binding as a First-Class Factor

The benchmark program should measure both:

- Rust API behavior
- Python API behavior

The Python layer matters because users will experience:

- Python binding overhead
- Python worker orchestration cost
- Python serialization and callback behavior

So benchmark identity should include a `binding` dimension where it matters:

- `binding=rust`
- `binding=python`

Not every scenario needs to be implemented in both languages on day 1, but the design should reserve space for that from the beginning.

### 4.5 Respect Concurrency Reality

The codebase already models backend concurrency differences:

- PostgreSQL: multi-process
- SQLite: single-process
- Turso: single-process
- S3Store: single-process with a local SQLite cache plus sync/snapshot behavior

That means "similar benchmarks" should not be interpreted as "force every backend into the same concurrency test".

Instead:

- PostgreSQL, SQLite, and Turso should share the same benchmark definitions
- concurrency profiles should differ when backend semantics require it
- PostgreSQL gets additional scale-out benchmarks
- S3 gets an overlay suite for durability behavior

### 4.6 Separate Steady-State Benchmarks From Recovery Benchmarks

Queue throughput and workflow recovery answer different questions.

- steady-state benchmarks tell us how fast the system runs normally
- recovery benchmarks tell us what happens after failure, retry, timeout, or pause

These should be reported separately so regressions are easier to interpret.

### 4.7 Optimize for Longitudinal Tracking

The benchmark program should keep enough metadata to compare results over time:

- git SHA
- benchmark ID
- backend
- feature flags
- benchmark parameters
- machine and runtime metadata
- raw metric samples
- summarized outputs

Without that metadata, trend tracking is weak or misleading.

---

## 5. Benchmark Taxonomy

## 5.1 Queue Benchmarks

Queue benchmarks measure queue behavior without workflow orchestration.

| Benchmark ID | Goal | Typical Workload | Primary Metric |
|---|---|---|---|
| `queue.enqueue.single` | Measure single-message enqueue cost | 1 producer, fixed payload sizes | messages/s |
| `queue.enqueue.batch` | Measure batching benefit | batch sizes 10 and 100 | messages/s |
| `queue.dequeue.single` | Measure claim + archive cost | 1 consumer, near-zero user work | messages/s |
| `queue.dequeue.batch` | Measure batch claim/archive efficiency | batch sizes 10 and 50 | messages/s |
| `queue.e2e.steady_state` | Measure producer-to-consumer pipeline throughput | sustained producer and consumer load | completed messages/s |
| `queue.redelivery.timeout` | Measure retry latency after lease expiry | short visibility timeout, forced non-archive | redelivery delay |
| `queue.visibility.extend` | Measure lease extension cost | long-running handler with extension | extend latency |
| `queue.delayed.activation` | Measure delayed-message scheduling accuracy | multiple delay offsets | activation skew |
| `queue.idle_poll` | Measure empty-queue polling overhead | repeated dequeue on empty queue | empty poll latency |

### What Queue Benchmarks Should Tell Us

These benchmarks should answer:

- How expensive is enqueue versus dequeue?
- How much value do we get from batching?
- What is the steady-state capacity of the queue path?
- Do visibility timeout semantics remain accurate?
- Do delayed messages become visible when expected?
- Does a backend or implementation change increase empty polling cost?

### Queue Payload Profiles

Start with fixed payload classes so regressions are easy to reason about:

- `small`: about 256 B JSON
- `medium`: about 4 KB JSON
- `large`: about 64 KB JSON

Fixed classes are better than mixed distributions for the first version of a regularly tracked suite.

## 5.2 Workflow Benchmarks

Workflow benchmarks measure orchestration and durability behavior on top of queue mechanics.

| Benchmark ID | Goal | Typical Workload | Primary Metric |
|---|---|---|---|
| `workflow.trigger` | Measure workflow submission cost | trigger only, no worker execution | triggers/s |
| `workflow.simple.1_step` | Measure minimal workflow overhead | 1 near-no-op step | runs/s |
| `workflow.simple.5_step` | Measure cost across several persisted steps | 5 short steps | runs/s |
| `workflow.simple.20_step` | Measure orchestration scaling with step count | 20 short steps | step overhead per run |
| `workflow.recovery.crash_after_step_n` | Measure resume cost and skipped work after crash | crash after step 1 or step 3 | recovered completion latency |
| `workflow.retry.transient` | Measure transient retry path | step fails once, then succeeds | retry delay |
| `workflow.pause.resume` | Measure pause/resume overhead | pause step, then resume by signal or timeout | resume-to-complete latency |
| `workflow.e2e.steady_state` | Measure sustained workflow throughput | trigger + worker load | completed runs/s |

### What Workflow Benchmarks Should Tell Us

These benchmarks should answer:

- What is the base cost of durable orchestration?
- How does that cost grow as step count increases?
- How much work is avoided after a crash because successful steps are reused?
- What latency does retry introduce?
- What latency does pause/resume introduce?

### Workflow Step Profiles

Workflow benchmarks should include two step-cost profiles:

- `control`: nearly no-op step body to isolate framework overhead
- `lightwork`: fixed small CPU or async delay to keep pgqrs overhead visible but not dominant

That split matters because:

- `control` tells us the framework tax
- `lightwork` tells us whether the tax still matters in realistic use

## 5.3 Benchmark Dimensions

In practice, each recorded benchmark result should be keyed by several dimensions:

- `scenario`: queue or workflow benchmark ID
- `backend`: postgres, sqlite, turso, or s3
- `binding`: rust or python
- `profile`: compat, single_process, scale_out, or another explicit profile
- `payload_profile`: small, medium, or large

For S3, one more factor matters:

- `s3_target`: localstack or aws

This prevents us from flattening unlike results into one chart and then misreading the outcome.

---

## 6. Backend Strategy

## 6.1 Core Database Backends

The main cross-backend matrix should cover:

- PostgreSQL
- SQLite
- Turso

The same logical benchmark IDs should exist across these three backends.

What should vary is the concurrency profile.

| Profile | PostgreSQL | SQLite | Turso | Purpose |
|---|---|---|---|---|
| `compat` | yes | yes | yes | strict apples-to-apples comparison |
| `single_process` | yes | yes | yes | compare low-concurrency behavior |
| `scale_out` | yes | no | no | measure multi-worker scaling where valid |

Recommended starting profiles:

- `compat`: 1 producer and 1 consumer, or 1 trigger source and 1 workflow worker
- `single_process`: modest async concurrency within one process
- `scale_out`: PostgreSQL only, increasing producer and consumer counts across processes

This keeps the benchmark suite honest. SQLite and Turso should not be judged by PostgreSQL-style scale-out numbers.

## 6.2 S3Store

S3Store should be benchmarked differently.

S3Store is not just "another database backend" because it adds:

- a local SQLite cache
- explicit or implicit sync boundaries
- explicit or implicit snapshot boundaries
- durable mode behavior that includes remote object-store cost

So S3 should have two layers of benchmarks:

- shared logical queue and workflow scenarios
- S3-only durability scenarios

### Shared S3 Scenarios

These should mirror the logical scenarios where possible:

- `queue.enqueue.single`
- `queue.dequeue.single`
- `queue.e2e.steady_state`
- `workflow.simple.1_step`
- `workflow.simple.5_step`
- `workflow.recovery.crash_after_step_n`

But results should be labeled separately as:

- `s3-local`
- `s3-durable`

They should not be mixed into the main PostgreSQL/SQLite/Turso comparison charts without calling out that different durability semantics are being measured.

S3 results should also record which object-store target was used:

- `localstack`
- `aws`

That factor matters because LocalStack is useful for fast, repeatable development runs, but AWS S3 is the source of truth for real remote latency and behavior.

### S3-Specific Benchmarks

| Benchmark ID | Goal | Primary Metric |
|---|---|---|
| `s3.sync.clean` | Measure sync cost when there are no local changes | sync latency |
| `s3.sync.dirty` | Measure sync cost after queue or workflow mutations | sync latency |
| `s3.snapshot.refresh` | Measure remote refresh cost | snapshot latency |
| `s3.e2e.durable_queue` | Measure queue throughput with durable sync in the hot path | completed messages/s |
| `s3.e2e.durable_workflow` | Measure workflow throughput with durable sync in the hot path | completed runs/s |

These benchmarks answer questions the database backends do not have:

- How much does durable sync reduce steady-state throughput?
- How much does snapshot refresh delay visibility?
- How much work can local mode amortize between sync boundaries?
- How different are LocalStack results from AWS S3 results for the same logical workload?

---

## 7. Metrics and Result Artifacts

## 7.1 Common Metrics

Every benchmark should emit:

- throughput: messages/s, runs/s, or steps/s
- latency: p50, p95, p99, max
- success rate and error rate
- backlog drain time where relevant
- retry count where relevant
- empty poll rate where relevant

## 7.2 Queue Metrics

Queue benchmarks should also collect:

- claim latency
- archive latency
- redelivery delay after visibility timeout
- delayed activation skew

## 7.3 Workflow Metrics

Workflow benchmarks should also collect:

- trigger latency
- run creation latency
- step acquisition latency
- run completion latency
- resumed completion latency after recovery
- skipped-step count versus re-executed-step count

The recovery benchmarks should explicitly record whether successful steps were reused. That is one of the main promises of durable workflows.

## 7.4 Backend-Specific Metrics

### PostgreSQL

- WAL bytes per message or per run
- table growth during long runs
- lock-wait indicators if available cheaply

### SQLite and Turso

- busy or lock-contention count
- time spent waiting on busy timeout if observable

### S3

- sync latency
- snapshot latency
- object PUT and GET counts
- transferred bytes

## 7.5 Result Artifact Format

The benchmark program should store results in one canonical persisted format.

### Canonical Persisted Format

Each run should write a stable artifact that includes:

- benchmark ID
- git SHA
- backend
- binding
- feature flags
- benchmark profile
- S3 target when applicable
- machine metadata
- runtime metadata
- raw samples
- summarized metrics

Artifacts should be stored as JSON Lines or JSON files because the schema is nested and will evolve over time.

JSON is better for raw results because it naturally holds:

- benchmark metadata
- parameter dictionaries
- nested metric groups
- optional backend-specific fields
- arrays of samples

Recommendation:

- persisted source of truth: JSONL
- dashboard and analysis code flatten JSONL in memory as needed

CSV can still be supported as an ad hoc export from the dashboard or CLI, but it should not be part of the persisted storage model.

Suggested layout:

```text
benchmarks/
  data/
    raw/
      queue.e2e.steady_state/
        2026-03-17T120000Z-postgres-rust-abc123.jsonl
        2026-03-17T120000Z-postgres-python-abc123.jsonl
```

This makes it possible to compare runs later without reconstructing context from memory.

---

## 8. Harness Design

The benchmark program should live in a dedicated top-level subdirectory:

```text
benchmarks/
```

This directory should contain both benchmark code and benchmark data.

### 8.1 Top-Level Structure

Recommended structure:

```text
benchmarks/
  README.md
  runner/
  rust/
  python/
  dashboard/
  scenarios/
  data/
    raw/
    derived/
    baselines/
```

### 8.2 Execution Model

Benchmarks should be on-demand.

That means the primary entrypoint should be a CLI that a developer can run locally when needed, for example:

```bash
python -m benchmarks.runner run --scenario queue.e2e.steady_state --backend postgres --binding python
python -m benchmarks.runner run --scenario workflow.simple.5_step --backend sqlite --binding rust
```

This design does not depend on scheduled CI to be useful. Scheduled execution can be added later, but it should not be required for the system to work.

### 8.3 Python as the Control Plane

Python should be the control plane for the benchmark program.

Reasons:

- the Python layer is itself important to benchmark
- Streamlit is a natural fit for review and exploration
- Python is well-suited to orchestrating runs, collecting artifacts, and building flat analysis tables

That suggests the benchmark program should use:

- Python runner and orchestration code
- Python dashboard code
- Rust scenario implementations for Rust API benchmarks
- Python scenario implementations for Python API benchmarks

### 8.4 Orchestrator Decision

The benchmark program should use a thin custom Python orchestrator as the platform-level runner.

This is a deliberate design choice, not something left open per benchmark.

The orchestrator should own:

- CLI entrypoints
- scenario lookup and validation
- backend and environment setup
- invoking the selected executor
- collecting and writing result artifacts
- baseline lookup
- dashboard-facing data loading

This should be fixed across the benchmark system so that:

- scenario identity is stable
- result schema is stable
- backend setup is consistent
- every benchmark is discoverable through one entrypoint

### 8.5 Executor Model

Executor technology should be benchmark-dependent, but only behind a common interface.

That means the system should standardize the platform and allow executor choice within it.

Recommended executor interface:

- `prepare(run_spec) -> env_handle`
- `execute(run_spec, env_handle) -> result`
- `cleanup(env_handle) -> None`

The first executors should be:

- `python_executor`
- `rust_executor`

An optional `locust_executor` can be added later for benchmarks where swarm-style load generation is useful, but Locust should not be the orchestrator.

Locust is a load generator. It is not the right place to define the benchmark platform, artifact model, or backend lifecycle.

### Recommended Structure

- `runner`: selects scenario, backend, binding, repetitions, and output path
- `scenario definitions`: shared workload specs that Rust and Python implementations both consume
- `backend adapters`: setup, bootstrap, seed, reset, cleanup, and connection logic
- `result writer`: canonical JSONL artifacts
- `dashboard`: Streamlit app for browsing and comparing results

### 8.6 Streamlit Dashboard

The codebase should include a Streamlit dashboard that can be run on demand to review the current benchmark state.

The dashboard should live under:

```text
benchmarks/dashboard/
```

It should support:

- filtering by scenario
- filtering by backend
- filtering by binding
- filtering by payload profile
- filtering by S3 target
- comparing current run against selected baselines
- showing trend tables by flattening the stored JSONL artifacts in memory

The dashboard is for review, not for generating the source-of-truth data.

### 8.7 Clean-Slate Directory Layout

With a clean slate, the benchmark directory should be organized like this:

```text
benchmarks/
  README.md
  pyproject.toml
  bench/
    cli.py
    registry.py
    schema.py
    results.py
    loader.py
    flatten.py
    baselines.py
    backends/
      postgres.py
      sqlite.py
      turso.py
      s3.py
    env/
      localstack.py
      aws.py
      reset.py
  scenarios/
    queue/
      enqueue_single.toml
      dequeue_batch.toml
      e2e_steady_state.toml
    workflow/
      trigger.toml
      simple_5_step.toml
      recovery_crash_after_step_1.toml
  executors/
    python/
      queue.py
      workflow.py
    rust/
      Cargo.toml
      src/
        main.rs
        queue.rs
        workflow.rs
  dashboard/
    app.py
    views/
      runs.py
      compare.py
      trends.py
  data/
    raw/
    baselines/
```

Key rules for this layout:

- `bench/` contains platform code shared by all benchmarks
- `scenarios/` contains declarative workload definitions, not execution logic
- `executors/` contains benchmark execution implementations
- `dashboard/` contains review and visualization code
- `data/` contains persisted benchmark artifacts

### 8.8 Role of the Existing PostgreSQL Harness

The current PostgreSQL harness under [`crates/pgqrs/benchmark`](/Users/rajatvenkatesh/code/pgqrs/benchmark_worktree/crates/pgqrs/benchmark) should remain available, but it should be treated as:

- a storage experiment harness
- a WAL-analysis harness
- a backend-specific diagnostic tool

It should not define the architecture of the regular queue and workflow benchmark program, and it is reasonable to delete it after the new benchmark subdirectory is in place.

---

## 9. Run Discipline

Benchmarks are only useful if runs are disciplined and comparable.

Recommended rules:

- run each benchmark more than once and report median plus dispersion
- warm up before collecting samples
- reset state between repetitions
- keep backend versions fixed where possible
- record exact profile parameters in the artifact
- treat benchmark execution as explicitly user-invoked and on-demand

Avoid comparing:

- runs from different machines without explicit labels
- local laptop runs against dedicated CI baselines as if they were equivalent
- PostgreSQL scale-out runs against single-process SQLite or Turso runs
- S3 durable-mode runs against non-S3 runs without calling out the included sync cost
- LocalStack S3 runs against AWS S3 runs without making the target explicit

---

## 10. Rollout Plan

## 10.1 Phase 1: Core Queue Benchmarks

Implement first:

- `queue.enqueue.single`
- `queue.dequeue.single`
- `queue.dequeue.batch`
- `queue.e2e.steady_state`
- `queue.redelivery.timeout`

Across:

- PostgreSQL
- SQLite
- Turso
- Rust binding
- Python binding

This establishes the shared cross-backend baseline.

## 10.2 Phase 2: Workflow Benchmarks

Implement next:

- `workflow.trigger`
- `workflow.simple.1_step`
- `workflow.simple.5_step`
- `workflow.recovery.crash_after_step_n`
- `workflow.retry.transient`
- `workflow.pause.resume`

For both:

- Rust binding
- Python binding

This phase measures the cost and value of durable execution itself.

## 10.3 Phase 3: S3 Overlay

Implement last:

- shared queue and workflow scenarios on S3
- `s3.sync.clean`
- `s3.sync.dirty`
- `s3.snapshot.refresh`
- durable-mode end-to-end queue and workflow scenarios
- LocalStack and AWS S3 targets
- Streamlit dashboard views over stored results

This keeps S3 comparable where appropriate while still measuring what makes it different.

---

## 11. Alternatives Considered

### 11.1 Keep the Existing PostgreSQL Harness as the Main Suite

Rejected because it does not cover:

- the three database backends with a shared benchmark vocabulary
- workflow durability scenarios
- S3-specific semantics
- pgqrs API-level behavior
- Python binding behavior

### 11.2 Use Only Microbenchmarks

Rejected because microbenchmarks are not enough for:

- producer/consumer coordination
- visibility timeout behavior
- workflow crash recovery
- pause/resume and retry semantics
- S3 sync and snapshot behavior

Microbenchmarks may still be useful later for isolated hot paths, but they should not be the main benchmark program.

### 11.3 Force All Backends Into Identical Concurrency Profiles

Rejected because it would create misleading comparisons.

PostgreSQL should be allowed to show multi-process scale-out behavior. SQLite, Turso, and S3 should be benchmarked within their actual concurrency model.

### 11.4 Store Only CSV or Only JSON

Rejected because each format serves a different purpose.

Persisting only CSV is a poor fit because benchmark records contain nested metadata and optional backend-specific fields.

Persisting only JSONL is a good fit because analysis code can flatten it at read time without requiring a second stored representation.

### 11.5 Use Locust as the Primary Orchestrator

Rejected because Locust is a load-generation tool, not a benchmark platform.

Using Locust as the primary orchestrator would make the system too dependent on one execution model and would mix together:

- workload generation
- backend lifecycle management
- artifact writing
- benchmark discovery
- dashboard data plumbing

Locust can still be added later as an optional executor for a subset of high-concurrency scenarios.

---

## 12. External References

These references are useful for shaping the benchmark program and for evaluating future tooling.

### 12.1 Primary Reference

- [Apache DataFusion Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html)

This is the strongest external reference for the direction of this design. It is useful because it combines:

- a contributor-facing benchmarking guide
- published current benchmark results
- explicit environment disclosure
- separate guides for local and cloud benchmarking

That combination is close to what pgqrs should aim for.

### 12.2 Tools to Watch

- [Airspeed Velocity (ASV)](https://asv.readthedocs.io/)
- [CodSpeed Overview](https://codspeed.io/docs/what-is-codspeed)
- [CodSpeed Quickstart](https://codspeed.io/docs/)

These tools are not part of the current design choice, but they are worth monitoring.

ASV is relevant because it is designed for tracking benchmark results over time with a strong history-oriented workflow.

CodSpeed is relevant because it focuses on continuous performance checks and PR-facing regression visibility, and it already supports Python and Rust-oriented workflows.

The current design should stay custom and lightweight, but we should periodically re-evaluate whether ASV or CodSpeed could replace part of the benchmark platform later.

---

## 13. Open Questions

- Should the first version of the harness focus only on Rust, or should Python-driven macrobenchmarks also be added later?
- Should the result artifacts live inside the repo tree permanently, or should old raw data be pruned after derived summaries are materialized?
- Should AWS S3 runs be manual-only at first, given cost and credential management?
- Do we want Streamlit to compare against checked-in baselines, local artifacts only, or both?
- Which initial scenarios, if any, actually justify a future `locust_executor` instead of the default Rust or Python executors?

---

## 14. Summary

The benchmark suite should be organized around product questions:

- queue cost
- workflow orchestration cost
- recovery cost
- backend-specific durability cost

PostgreSQL, SQLite, and Turso should share the same logical benchmark definitions, with concurrency profiles adjusted only where backend semantics require it.

The benchmark program should also treat Rust and Python as first-class binding surfaces, because Python overhead is part of the product users experience.

S3 should share some logical scenarios, but it should also have its own durability-focused suite because sync and snapshot behavior are first-order costs, not noise. For S3, `LocalStack` versus `AWS S3` should be an explicit factor in the recorded result.

The implementation should live under a dedicated `benchmarks/` subdirectory, use a custom Python orchestrator, run on demand, store canonical JSONL artifacts, and include a Streamlit dashboard for reviewing the current local benchmark state.

That structure gives pgqrs a benchmark program that can be run regularly, tracked over time, and used to make implementation decisions with clear intent.
