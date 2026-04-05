# Coarse Job Viability

`queue.coarse_job_viability` defines the benchmark that should answer the practical S3 question:

- when is a slower but durable and portable queue backend still good enough for the workload?

This benchmark is intentionally different from `queue.drain_fixed_backlog`.
It is not trying to find the maximum message rate of a tight dequeue loop.
It is trying to find the workload envelope where queue overhead is small compared with the work done by each job.

## Question

For coarse-grained durable jobs, at what task duration does queue overhead become small enough for the backend to be operationally acceptable?

## Why This Matters

Some backends are naturally evaluated by throughput and tail latency under continuous pressure.
That is the right framing for a streaming-style queue.

S3 is different.
It is better described as a durable coarse-grained work queue substrate:

- portable
- operationally simple
- much higher per-operation latency
- much more sensitive to write amplification and contention

That means the key question is not "how many tiny messages per second can it push?"
The key question is "how much queue overhead do we pay relative to the job itself?"

## Workload Model

This scenario should process a prefilled backlog and simulate work after each dequeue.
Messages are archived only after the simulated work completes.

Fixed settings:

- `prefill_jobs = 500`
- `queue_mode = drain`
- `payload_profile = small`
- `work_model = simulated_service_time`

Sweep variables:

- `simulated_task_duration_ms = [0, 100, 1000, 10000, 60000]`
- `consumers = [1, 2, 4]`
- `dequeue_batch_size = [1, 10, 50]`

The most important axis is `simulated_task_duration_ms`.
That is the axis that reveals the break-even point for higher-latency durable backends.

## What This Benchmark Should Prove

This scenario should make the following claims testable:

- some backends are good fits for low-latency queue drain and some are not
- S3 is not a good fit for tight-loop streaming-style dequeue workloads
- S3 may still be a good fit for coarse-grained jobs where task runtime dominates queue overhead
- the fit boundary depends on task duration, batch size, and contention

## Primary Metrics

The primary reported metric should be:

- `queue_overhead_fraction`

That value should represent the portion of total end-to-end job time spent in queue operations rather than useful work.

The dashboard should also report:

- `completed_jobs_per_second`
- `p95_end_to_end_job_time_ms`
- `request_operations_per_completed_job`
- `estimated_payload_transfer_bytes_per_completed_job`
- `estimated_request_cost_per_completed_job_usd`
- `conflicts_per_1000_completed_jobs`

For S3, these are more decision-useful than a single throughput number.

## Interpretation

The resulting view should read more like a viability map than a leaderboard.

Example interpretation:

- if `queue_overhead_fraction < 5%`, the backend is a strong fit for that workload point
- if `queue_overhead_fraction` is between `5%` and `20%`, the backend may be acceptable when portability or durability is worth the cost
- if `queue_overhead_fraction > 20%`, queue overhead is dominating too much of total job time

Conflict and amplification metrics matter as guardrails.
Even if queue overhead fraction looks acceptable, a backend is a poor fit if conflicts or request amplification rise sharply with more consumers.

## Relationship To Other Benchmarks

`queue.drain_fixed_backlog` should remain in the benchmark set.
It plays a different role:

- it is the low-latency stress test
- it is the anti-fit benchmark for remote durable stores such as S3
- it shows what happens when useful work is close to zero and queue overhead dominates

Taken together, the two scenarios answer both sides of the backend-selection question:

- where does the backend fail as a queue?
- where does the backend become good enough as a durable job substrate?
