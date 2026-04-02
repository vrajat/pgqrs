# Benchmarks

pgqrs benchmarks exist to answer workload questions, not to publish a single global backend leaderboard.

The current benchmark set is meant to explain behavior under specific queue and workflow scenarios:

- how throughput changes as consumer count increases
- how throughput changes as batch size increases
- how latency changes as concurrency increases
- where a backend stops scaling cleanly

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

The first documented benchmark scenario is:

- [Queue Drain Fixed Backlog](queue-drain-fixed-backlog.md)

This scenario asks a narrow, useful question:

- for a fixed pre-populated backlog, how do throughput, completion time, and latency change as consumer count and dequeue batch size vary?

That makes it a good first benchmark for understanding queue drain behavior before adding more complex steady-state or workflow benchmarks.

## Methodology Notes

- Results shown here are from curated release-mode Rust baselines.
- The canonical stored benchmark format is JSONL.
- Static charts in the docs are generated from those checked-in baselines.
- The Streamlit dashboard remains the exploratory tool; these docs present the curated interpretation.
- Turso and S3 notes may cite directional runs when a like-for-like `prefill_jobs = 50000` baseline is not yet practical; those sections call out the exact workload and caveats explicitly.

## Related References

- [Backend Selection Guide](../user-guide/concepts/backends.md)
- [Benchmark design doc](https://github.com/vrajat/pgqrs/blob/main/engg/design/queue-workflow-benchmarking.md)
