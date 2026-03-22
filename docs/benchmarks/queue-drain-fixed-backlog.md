# Queue Drain Fixed Backlog

`queue.drain_fixed_backlog` is the first curated queue benchmark scenario for pgqrs.

It measures how fast a pre-populated queue can be drained under a sweep of:

- consumer count
- dequeue batch size

## Question

For a fixed pre-populated backlog, how do throughput, completion time, and latency vary with consumer count and dequeue batch size?

## Why This Matters

This scenario isolates the consumer side of the queue.

That makes it useful for:

- understanding how well a backend scales under drain pressure
- separating batch-size effects from concurrency effects
- identifying whether higher concurrency improves useful work or only adds contention

## Setup

The current published baselines use:

- Rust executor
- release mode
- `prefill_jobs = 50000`
- compatibility profile
- variables:
  - `consumers = [1, 2, 4]`
  - `dequeue_batch_size = [1, 10, 50]`

## Primary Findings

### PostgreSQL

PostgreSQL scales with both consumers and batch size.

- At `batch_size = 1`, throughput improves from `149.5 msg/s` to `575.0 msg/s` as consumers rise from `1` to `4` (`3.85x`).
- At `batch_size = 10`, throughput improves from `1603.1 msg/s` to `5249.6 msg/s` (`3.27x`).
- At `batch_size = 50`, throughput improves from `6817.1 msg/s` to `20175.8 msg/s` (`2.96x`).
- At `1 consumer`, increasing batch size from `1` to `50` improves throughput from `149.5 msg/s` to `6817.1 msg/s` (`45.59x`).
- At `4 consumers`, increasing batch size from `1` to `50` improves throughput from `575.0 msg/s` to `20175.8 msg/s` (`35.09x`).

![PostgreSQL throughput vs consumers](../assets/benchmarks/queue-drain-fixed-backlog/postgres-throughput-vs-consumers.svg)

![PostgreSQL throughput vs batch size](../assets/benchmarks/queue-drain-fixed-backlog/postgres-throughput-vs-batch-size.svg)

### SQLite

SQLite benefits strongly from larger batch sizes, but does not scale with more consumers in this scenario.

- At `batch_size = 1`, throughput changes from `261.0 msg/s` to `247.0 msg/s` as consumers rise from `1` to `4` (`0.95x`).
- At `batch_size = 10`, throughput changes from `2709.0 msg/s` to `2422.7 msg/s` (`0.89x`).
- At `batch_size = 50`, throughput changes from `12630.3 msg/s` to `11232.9 msg/s` (`0.89x`).
- At `1 consumer`, increasing batch size from `1` to `50` improves throughput from `261.0 msg/s` to `12630.3 msg/s` (`48.40x`).
- At `4 consumers`, increasing batch size from `1` to `50` improves throughput from `247.0 msg/s` to `11232.9 msg/s` (`45.47x`).

![SQLite throughput vs consumers](../assets/benchmarks/queue-drain-fixed-backlog/sqlite-throughput-vs-consumers.svg)

![SQLite throughput vs batch size](../assets/benchmarks/queue-drain-fixed-backlog/sqlite-throughput-vs-batch-size.svg)

## Latency Behavior

### PostgreSQL

PostgreSQL latency stays comparatively flat as consumer count rises.

- `p95 dequeue latency` rises from `8.78 ms` to `11.16 ms` at `batch_size = 1` (`1.27x`).
- `p95 dequeue latency` rises from `10.09 ms` to `13.48 ms` at `batch_size = 50` (`1.34x`).
- `p95 archive latency` rises from `1.13 ms` to `2.64 ms` at `batch_size = 1` (`2.33x`).
- `p95 archive latency` rises from `1.55 ms` to `4.23 ms` at `batch_size = 50` (`2.73x`).

![PostgreSQL dequeue latency vs consumers](../assets/benchmarks/queue-drain-fixed-backlog/postgres-dequeue-latency-vs-consumers.svg)

![PostgreSQL archive latency vs consumers](../assets/benchmarks/queue-drain-fixed-backlog/postgres-archive-latency-vs-consumers.svg)

### SQLite

SQLite latency rises sharply as more consumers are added, even though throughput does not improve.

- `p95 dequeue latency` rises from `7.13 ms` to `21.06 ms` at `batch_size = 1` (`2.96x`).
- `p95 dequeue latency` rises from `6.88 ms` to `22.02 ms` at `batch_size = 50` (`3.20x`).
- `p95 archive latency` rises from `0.15 ms` to `14.30 ms` at `batch_size = 1` (`98.64x`).
- `p95 archive latency` rises from `0.24 ms` to `15.20 ms` at `batch_size = 50` (`64.15x`).

![SQLite dequeue latency vs consumers](../assets/benchmarks/queue-drain-fixed-backlog/sqlite-dequeue-latency-vs-consumers.svg)

![SQLite archive latency vs consumers](../assets/benchmarks/queue-drain-fixed-backlog/sqlite-archive-latency-vs-consumers.svg)

## How To Interpret This

The current benchmark says:

- PostgreSQL is the backend that scales with concurrency for this queue scenario.
- SQLite is functional and predictable, but extra consumers mostly add contention rather than throughput.
- Batch size is an important lever for both backends.

This should be read as scenario behavior, not as a universal backend ranking.

SQLite remains useful for embedded, test, and low-operational-overhead use cases even when it is not the scalable choice for multi-consumer drain workloads.

## Artifacts

Curated baselines used for this page:

- [`postgres-rust-compat-release-20260321.jsonl`](https://github.com/vrajat/pgqrs/blob/main/benchmarks/data/baselines/queue.drain_fixed_backlog/postgres-rust-compat-release-20260321.jsonl)
- [`sqlite-rust-compat-release-20260321.jsonl`](https://github.com/vrajat/pgqrs/blob/main/benchmarks/data/baselines/queue.drain_fixed_backlog/sqlite-rust-compat-release-20260321.jsonl)

To explore runs interactively:

```bash
make benchmark-dashboard
```

## Turso Status

Turso support for this scenario is still a work in progress.

The current docs intentionally do not publish Turso baseline guidance yet.
