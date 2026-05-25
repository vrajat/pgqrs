# Benchmarks

pgqrs benchmarks exist to answer workload questions, not to publish a single global backend leaderboard.

The current benchmark set is meant to explain behavior under specific queue and workflow scenarios:

- how throughput changes as consumer count increases
- how throughput changes as batch size increases
- how latency changes as concurrency increases
- where a backend stops scaling cleanly
- when durable queue overhead becomes negligible relative to job execution time

## How To Read These Results

These benchmark pages compare a backend against itself across a controlled sweep.

They are most useful for questions like:

- Does PostgreSQL scale with more consumers?
- Does SQLite benefit from larger dequeue batches?
- How much latency do we pay when concurrency rises?

They are less useful for simplistic questions like:

- Which backend is "best" overall?

That framing is intentionally avoided. PostgreSQL, SQLite, Turso, and S3 serve different operational roles.

## Current Status

| Backend | Status | What the current data says |
|---------|--------|----------------------------|
| PostgreSQL | Usable baseline | Best scaling backend in the current drain benchmark; throughput rises close to linearly with more consumers |
| SQLite | Usable baseline | Similar single-consumer shape, but does not scale with more consumers |
| Turso | Directional guidance | Current local-path run behaves much like SQLite |
| S3 | Directional baseline | Much lower throughput because end-to-end per-message latency is much higher |

## Current Scenario Coverage

The current documented benchmark pages are:

- [Queue Drain Fixed Backlog](queue-drain-fixed-backlog.md)
- [Coarse Job Viability](coarse-job-viability.md)

These two pages answer different questions:

- `Queue Drain Fixed Backlog` is the stress case:
  for a fixed pre-populated backlog, how do throughput, completion time, and latency change as consumer count and dequeue batch size vary?
- `Coarse Job Viability` is the fit case:
  for coarse-grained durable jobs, when does queue overhead become small enough that a slower but more portable backend is still operationally acceptable?

Together they avoid a common mistake: using one tight-loop drain benchmark as a universal backend ranking.

## Workload Classes

The benchmark program now distinguishes between at least three workload classes:

- `Streaming queue` workloads optimize for low per-message latency and scale-out throughput.
- `General work queue` workloads optimize for reliable competing-consumer task dispatch.
- `Coarse-grained durable job` workloads optimize for portability and durability while individual jobs take much longer than queue operations.

This matters most for S3-backed queue state.

The current drain benchmark is a valid anti-fit test for S3 because it shows what happens when queue latency dominates useful work.
The coarse-job viability benchmark is the more decision-useful benchmark for S3 because it asks when queue overhead becomes small relative to job runtime.

## Methodology Notes

- Results shown here are from curated release-mode Rust baselines.
- The canonical stored benchmark format is JSONL.
- Static charts in the docs are generated from those checked-in baselines.
- The Streamlit dashboard remains the exploratory tool; these docs present the curated interpretation.
- Turso and S3 notes may cite directional runs when a like-for-like `prefill_jobs = 50000` baseline is not yet practical; those sections call out the exact workload and caveats explicitly.

## Related References

- [Backend Selection Guide](../user-guide/concepts/backends.md)
- [Benchmark design doc](https://github.com/vrajat/pgqrs/blob/main/engg/design/queue-workflow-benchmarking.md)
