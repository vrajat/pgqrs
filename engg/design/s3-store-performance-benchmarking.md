# S3Store Performance Benchmarking Design

**Status**: Draft Design  
**Created**: 2026-03-29  
**Author**: Codex

---

## 1. Overview

`S3Store` should not be benchmarked as "SQLite with LocalStack attached".

That would mostly measure:

- local SQLite execution cost
- local filesystem cost
- LocalStack request handling cost on loopback

It would not answer the product question we actually care about:

- what does `pgqrs` look like when queue state is stored as a single remote S3 object with realistic object-store latency and hot-object contention?

This document defines a benchmark design for `S3Store` that keeps LocalStack as the storage target for repeatability, but introduces an emulation layer that injects the remote characteristics we care about.

The initial benchmark defaults should distinguish between:

- externally informed latency ranges
- application-level workload assumptions

For latency, published references suggest that standard S3 can easily land in the low-`100 ms` to mid-`100 ms` tail range for read-like paths, and object-storage-backed write paths can be materially higher depending on the full application path.

Useful reference points:

- same-Region random-read benchmarks have reported `100+ ms` `p99` latency for standard S3 reads
- WarpStream reports roughly `400-600 ms` `p99` write latency on S3 Standard for its full write path, not raw S3 `PUT` latency

For write cadence, there is no AWS S3 guarantee that maps to:

- `1 write/sec`

So any such cap should be treated as a conservative workload-model input for `S3Store`, not as an AWS guarantee.

The harness must be able to dial all of these values up or down.

This design complements [`engg/design/queue-workflow-benchmarking.md`](/Users/rajatvenkatesh/code/pgqrs/s3store-perf_worktree/engg/design/queue-workflow-benchmarking.md) and narrows in on the S3-specific part of that program.

---

## 2. Current Implementation Context

The benchmark must match the implementation that exists today.

The current `S3Store` is not the earlier background-sync design in [`engg/design/s3-backed-sqlite-queue-implementation.md`](/Users/rajatvenkatesh/code/pgqrs/s3store-perf_worktree/engg/design/s3-backed-sqlite-queue-implementation.md).

Today:

- `DurabilityMode::Durable` performs the local SQLite write and then immediately runs `sync()` before returning.
- `DurabilityMode::Local` writes only to the local SQLite cache; the application calls `sync()` and `snapshot()` explicitly.
- `sync()` publishes the whole SQLite file to object storage with compare-and-swap semantics using the last observed ETag.
- `snapshot()` refreshes local state from object storage and refuses to run if the local cache is dirty.
- the concurrency model reported by the backend is `SingleProcess`

That means the hot path we need to measure is:

- durable write latency = local SQLite mutation + full-object `PUT`
- follower refresh latency = `HEAD` and sometimes `GET`
- contention behavior = CAS conflicts, dirty snapshot conflicts, and local serialization around writes

It also means this benchmark should not assume:

- a background flusher
- hidden read/write database separation
- asynchronous publish after `enqueue()`

---

## 3. Goals and Non-Goals

### Goals

- Measure `S3Store` under emulated remote object-store conditions rather than plain LocalStack loopback.
- Keep the emulated S3 parameters dialable from benchmark config.
- Separate queue semantics from S3-specific durability and synchronization cost.
- Compare `Durable` and `Local` modes fairly.
- Measure not just throughput and latency, but also contention, stale-state behavior, and conflict rates.
- Produce artifacts that remain compatible with the broader benchmark program.

### Non-Goals

- Perfectly reproducing every internal behavior of AWS S3.
- Replacing real AWS validation runs.
- Using LocalStack numbers alone as a proxy for production S3 behavior.
- Benchmarking hypothetical future `S3Store` architectures that do not exist yet.

---

## 4. Questions This Benchmark Must Answer

### 4.1 Durable Path

- What is the max sustained throughput of `S3Store` in `Durable` mode when each durable write pays remote latency and contends on one hot object?
- How much of durable latency is local SQLite cost versus emulated S3 cost?
- At what point does increasing local client concurrency stop helping because remote writes are serialized?

### 4.2 Local Mode Amortization

- How much throughput improvement do we get by batching many local writes behind a single `sync()`?
- What batch size is needed to amortize hot-object contention and remote latency?
- How much stale-read exposure does `Local` mode create between sync points?

### 4.3 Contention and Freshness

- How often do concurrent writers hit CAS conflicts on the same object?
- How often do followers hit dirty `snapshot()` conflicts or observe stale state?
- What is the freshness lag between a leader commit and a follower seeing that commit after `snapshot()`?

### 4.4 Workflow Impact

- How much does S3 durability reduce workflow run throughput for 1-step and multi-step workflows?
- Does workflow throughput collapse primarily because of queue writes, workflow step persistence writes, or conflict retries?

---

## 5. Emulation Requirements

The S3 benchmark environment must support these knobs:

- `base_latency_ms`
- `jitter_ms`
- optional timeout / connection failure injection

For the first version, the critical requirement is **contention-aware latency injection**, not operation-aware proxying.

What needs to be modeled first is:

- multiple concurrent writers attempting CAS updates against one hot object
- realistic network / object-store latency on that path
- the resulting conflict, retry, and blocked-time behavior

For v1, that means plain TCP latency injection in front of LocalStack is enough.

Operation-aware `HEAD` / `GET` / `PUT` modeling is a later refinement if the coarse model proves insufficient.

---

## 6. Proxy / Emulation Options

### 6.1 Plain NGINX

NGINX can act as a reverse proxy in front of LocalStack and it can rate-limit requests with `limit_req`.

It is a partial fit:

- good at HTTP reverse proxying
- usable for coarse request-rate shaping
- not a clean fit for injecting a fixed delay on every proxied S3 operation
- not a clean fit for per-object write serialization unless we add Lua or njs logic

Conclusion:

- NGINX alone is not the best primary solution for this benchmark
- OpenResty or NGINX+Lua could work, but that is effectively a custom proxy implementation

### 6.2 Toxiproxy

Toxiproxy is a good fit for:

- fixed latency
- jitter
- bandwidth shaping
- connection failure injection

Conclusion:

- best off-the-shelf fit for the first version of this benchmark
- sufficient for contended-writer experiments because LocalStack still provides the real CAS winner/loser behavior

### 6.3 `tc` / `netem`

Linux traffic shaping is good for:

- deterministic packet delay
- jitter
- bandwidth ceilings

It is not a good fit for:

- application-level `PUT` pacing
- per-object semantics
- easy benchmark-specific configuration inside the existing harness

Conclusion:

- valuable for validating broad network sensitivity
- not sufficient for the primary S3 emulation goal

### 6.4 Envoy

Envoy is closer to what we need:

- reverse proxying is straightforward
- fault injection can add request delay
- local/global rate limiting can be configured

The main limitation is semantic:

- rate limiting usually rejects excess requests instead of serializing them into a controlled "one successful write per second" stream

Conclusion:

- workable if we want an off-the-shelf proxy quickly
- still awkward if we want object-aware queued write gating instead of 429 responses

### 6.5 Recommended Approach: Toxiproxy First

The recommended v1 design is:

- benchmark executor
- `-> Toxiproxy`
- `-> LocalStack`

Responsibilities:

- Toxiproxy injects TCP latency, jitter, and optional failure modes
- LocalStack provides S3-compatible object semantics
- the benchmark workload creates real concurrent CAS contention

Recommendation:

- use `Toxiproxy + LocalStack` as the main benchmark path for v1
- only add a custom HTTP proxy later if operation-specific latency modeling becomes necessary

### 6.6 Libraries and Harness Integration

There are two useful conclusions from looking at existing libraries:

- they are good accelerators for transport-level fault injection
- the off-the-shelf options are good enough for the first contended-writer benchmark shape

#### Toxiproxy

Toxiproxy is the strongest off-the-shelf fit for the first network-latency version of this problem.

Good fit:

- TCP proxy already packaged as a container
- runtime control through an HTTP API
- built-in toxics for latency, jitter, bandwidth, timeout, connection reset, and data limiting
- built-in Prometheus-style metrics
- custom toxics are explicitly supported

Limitations for this benchmark:

- it operates on connections and byte streams, not S3 operations or object keys
- it cannot natively say "delay only `PUT`"
- it does not give us direct `HEAD` / `GET` / `PUT` attribution without extra instrumentation

Practical recommendation:

- use Toxiproxy for v1
- let LocalStack and the benchmark workload generate the actual CAS conflicts
- treat a custom proxy as a future refinement, not a prerequisite

#### Testcontainers modules

Toxiproxy also has maintained Testcontainers modules across several ecosystems.

That is useful if we later want benchmark smoke tests that stand up:

- LocalStack
- Toxiproxy
- benchmark executors

inside one ephemeral test environment.

It is not the main recommendation for this repo today because the benchmark program is currently scaffolded around repository-local tooling and Docker-based setup rather than a Testcontainers-driven harness.

#### WireMock and mountebank

HTTP simulation tools such as WireMock and mountebank can proxy traffic and inject delays.

They are a weaker fit than Toxiproxy or a custom S3 proxy because:

- they are designed mainly for API stubbing and replay
- they are better at route-level behavior than at transparent S3-compatible object-store proxying
- they still do not solve per-object `PUT` pacing cleanly

They could be useful for:

- targeted HTTP delay experiments
- fault injection on a mocked S3 surface

They are not recommended as the primary benchmark environment for `S3Store`.

---

## 7. Benchmark Topology

Recommended local topology:

```text
benchmark executor
  -> AWS_ENDPOINT_URL=http://toxiproxy:port
  -> toxiproxy
  -> localstack:4566
```

Properties:

- LocalStack provides the object-store API and persistence
- Toxiproxy is the source of latency and fault emulation
- the benchmark executor records pgqrs-facing metrics
- the benchmark harness records the active Toxiproxy toxic configuration

This is important because we need to verify that the benchmark really ran under the requested profile.

Each recorded benchmark run should include:

- the Toxiproxy image version
- the emulation profile name
- the exact toxic settings

---

## 8. Emulation Semantics

### 8.1 Default Baseline Profile

The first emulation profile should be:

- `base_latency_ms = 60`
- `jitter_ms` tuned so the observed tail can reach roughly `p99 ~= 200 ms`
- `jitter_ms = 0`

This is intended as a first coarse standard-S3-inspired application profile:

- the baseline latency target is driven by the desired percentile shape rather than by a fixed requests/sec cap
- throughput should be measured as an output of latency plus contention
- the first target is roughly `p50 ~= 60 ms`, `p99 ~= 200 ms` on the contended writer path

### 8.2 Contended Writer Semantics

For the primary contention scenarios:

- multiple writers should be allowed to attempt `sync()` concurrently
- LocalStack and conditional writes should determine which request wins
- losing writers should fail naturally through the CAS path
- the benchmark should measure successful throughput, conflict rate, retry rate, and blocked time

This matters because the design question is not "what fixed rate should the proxy allow?"

It is:

- what throughput emerges when a hot object is updated under realistic latency and contention?

Separate error-path scenarios may intentionally switch to:

- extra timeouts
- connection resets

to measure retry behavior.

### 8.3 Read Semantics

For v1:

- all traffic shares the same coarse TCP latency model
- this is acceptable because the first benchmark priority is contended writer behavior, not per-method fidelity

If v1 results show that follower freshness or snapshot behavior needs more fidelity, operation-specific HTTP-aware latency can be added later.

### 8.4 Dialable Profiles

The harness should support named profiles such as:

| Profile | HEAD/GET/PUT latency | Write interval | Purpose |
|---|---:|---:|---|
| `direct_localstack` | `0 ms` | n/a | sanity check, lowest overhead |
| `toxiproxy_baseline` | `p50 ~ 60 ms`, `p99 ~ 200 ms` | n/a | first coarse contention profile |
| `toxiproxy_low_latency` | lower-latency variant | n/a | optimistic cloud path |
| `toxiproxy_degraded` | heavier tail | n/a | stress / tail scenario |

These should be benchmark config, not code changes.

---

## 9. Scenario Matrix

### 9.1 Scenario Reuse Rule

The benchmark taxonomy should reuse the existing shared scenario IDs whenever the product question is still the same.

That means S3 should usually not introduce a new top-level scenario ID just because it has:

- different latency profiles
- different durability modes
- extra backend-specific metrics

Instead:

- shared question -> shared scenario ID
- S3-specific environment -> extra run dimensions
- S3-specific question -> new S3-only scenario ID

Recommended S3-specific run dimensions:

- `backend = s3`
- `durability_mode = durable | local`
- `s3_latency_profile = direct_localstack | toxiproxy_baseline | toxiproxy_degraded | ...`

So the identity of an S3 run should usually be:

- common scenario ID
- plus S3-specific dimensions

not:

- a separate S3-prefixed scenario family

### 9.2 Shared Queue Scenarios Reused For S3

These should be reused from the common benchmark program and extended with S3-specific profiles:

| Benchmark ID | Mode | Purpose | Primary Metric |
|---|---|---|---|
| `queue.enqueue.single` | durable | single-message enqueue cost with inline sync | enqueue latency |
| `queue.enqueue.batch` | local or durable | batching benefit under S3-backed operation | messages/sec |
| `queue.dequeue.single` | local follower or durable | dequeue cost when state may require remote refresh | dequeue latency |
| `queue.e2e.steady_state` | durable or local | end-to-end steady-state throughput under coarse remote latency | completed messages/sec |
| `queue.e2e.capacity_knee` | durable or local | find the point where contention and latency flatten throughput | knee throughput |
| `queue.drain_fixed_backlog` | durable or local | consumer-side drain behavior for a prefetched backlog | drain messages/sec |

Interpretation:

- shared metrics remain primary because the user question is still the same
- S3-specific fields are additional result data, not a reason to fork the scenario

Example:

- `queue.drain_fixed_backlog`
- `backend=s3`
- `durability_mode=durable`
- `s3_latency_profile=toxiproxy_baseline`

is a distinct benchmark run without becoming a distinct scenario family.

### 9.3 S3-Only Scenarios

These are the new scenarios that matter specifically for `S3Store`:

| Benchmark ID | Purpose | Primary Metric |
|---|---|---|
| `s3.sync.cas_conflict` | two writers race on one object key | conflict rate |
| `s3.snapshot.dirty_conflict` | follower attempts snapshot with unsynced local writes | conflict rate |
| `s3.write.concurrency_knee` | sweep local concurrency against hot-object CAS contention | p95 latency and throughput |
| `s3.read.staleness_window` | measure how stale follower reads remain between leader syncs | freshness lag ms |

These scenarios deserve S3-specific IDs because the questions are not shared with the other stores.

### 9.4 Shared Workflow Scenarios Reused For S3

Start with a minimal workflow set:

| Benchmark ID | Mode | Purpose | Primary Metric |
|---|---|---|---|
| `workflow.simple.1_step` | durable | measure minimal workflow overhead with S3 in the hot path | runs/sec |
| `workflow.simple.5_step` | durable | show write amplification from persisted steps | runs/sec |
| `workflow.e2e.steady_state` | durable or local | sustained workflow throughput under S3-backed state updates | completed runs/sec |

When the question becomes specific to `sync()`, `snapshot()`, freshness, or CAS conflicts, use S3-only scenario IDs instead of overloading shared workflow scenarios.

The first implementation should stay small. It is better to get the queue scenarios correct first than to create a large workflow matrix early.

---

## 10. Metrics

### 10.1 Keep The Core Metrics

The main benchmark metrics from the generic queue/workflow design should still be recorded:

- throughput
- p50, p95, p99, and max latency
- success rate and error rate
- backlog drain time where relevant

So the answer to "should the same metrics be measured?" is: yes, but they are not enough by themselves for S3.

### 10.2 Add S3-Specific Metrics

Every S3 benchmark should also record:

- `sync_latency_ms`
- `snapshot_latency_ms`
- `head_latency_ms`
- `put_latency_ms`
- `get_latency_ms`
- `bytes_uploaded`
- `bytes_downloaded`
- `object_put_count`
- `object_get_count`
- `object_head_count`

### 10.3 Concurrency Metrics

Concurrency should be treated as a first-class metric family:

- `local_concurrency`
- `inflight_sync_count`
- `time_blocked_on_sync_ms`
- `retry_wait_ms`
- `conflict_rate`
- `conflict_count_by_type`

Conflict types should distinguish:

- remote CAS conflict on `sync()`
- dirty-local conflict on `snapshot()`
- local-state-changed-during-sync conflict

### 10.4 Staleness Metrics

Stale-state behavior should be measured explicitly:

- `freshness_lag_ms`
- `stale_read_count`
- `stale_read_fraction`
- `snapshot_interval_ms`
- `leader_commit_to_follower_visible_ms`

`freshness_lag_ms` should mean:

- time from leader successful `sync()` completion
- to follower successfully observing that state after `snapshot()` and read

### 10.5 Mode Comparison Metrics

For `Local` versus `Durable`, the key comparison metrics are:

- operations per remote `PUT`
- messages per `sync()`
- runs per `sync()`
- latency per message
- throughput per object write

Those metrics answer whether `Local` mode is actually amortizing the remote bottleneck or just moving it around.

---

## 11. Result Dimensions and Artifact Schema

Every persisted result should include the normal benchmark metadata plus:

- `backend = s3`
- `durability_mode = durable | local`
- `s3_target = localstack`
- `s3_emulation_profile`
- `s3_emulation` object with the exact knobs used
- `proxy_version`

Suggested additional nested fields:

```json
{
  "s3_emulation": {
    "base_latency_ms": 60,
    "jitter_ms": 40,
    "profile_notes": "coarse TCP latency profile via Toxiproxy"
  },
  "s3_metrics": {
    "object_put_count": 0,
    "object_get_count": 0,
    "object_head_count": 0,
    "bytes_uploaded": 0,
    "bytes_downloaded": 0,
    "conflict_count": 0,
    "freshness_lag_ms_p95": 0
  }
}
```

---

## 12. Cost Estimator

The benchmark program should include a simple cost estimator for `S3Store`.

This is not meant to replace the AWS Pricing Calculator.

It is meant to answer practical engineering questions such as:

- what does a benchmarked workload cost per hour or per month?
- how much of that cost comes from requests versus storage?
- does batching in `Local` mode reduce cost materially by reducing `PUT` count?

### 12.1 Scope

For the first version, estimate:

- S3 request cost
- S3 storage cost
- S3 data transfer cost

Keep EC2 compute separate.

Reason:

- the application instance cost is real, but it is not specific to `S3Store`
- for backend comparisons, folding EC2 instance price into the S3 backend number would make interpretation worse

So the benchmark should report:

- `s3_monthly_cost_estimate_usd`
- `s3_request_cost_estimate_usd`
- `s3_storage_cost_estimate_usd`
- `s3_transfer_cost_estimate_usd`
- optional `ec2_compute_cost_estimate_usd` as a separate field if the user supplies an instance hourly rate

### 12.2 Same-Region EC2 Assumption

Assume:

- the application runs on EC2
- the EC2 instance and the S3 bucket are in the same AWS Region

Under that assumption, the estimator should default to:

- `s3_transfer_cost_estimate_usd = 0`

Reasoning:

- AWS states that data transferred in to S3 is free
- AWS also states that data transferred from an S3 bucket to AWS services within the same Region is free

That means the dominant recurring S3 costs for this design are usually:

- `PUT` requests
- `GET` / `HEAD` requests
- stored object bytes

Not regional bandwidth charges.

If we later benchmark cross-Region followers or internet clients, transfer pricing should become a separate scenario dimension.

### 12.3 Input Parameters

The estimator should be parameterized, not hard-coded.

Suggested fields:

```json
{
  "s3_cost_model": {
    "region": "us-east-1",
    "storage_class": "standard",
    "put_price_per_1000": 0.005,
    "get_price_per_1000": 0.0004,
    "head_price_per_1000": 0.0004,
    "storage_price_per_gb_month": null,
    "same_region_transfer_price_per_gb": 0.0,
    "ec2_hourly_price": null
  }
}
```

Notes:

- `put_price_per_1000` and `get_price_per_1000` should default from the current AWS pricing page for S3 Standard
- `head_price_per_1000` should be configurable; for v1 it is reasonable to price it the same as `GET` class requests
- `storage_price_per_gb_month` should be supplied from the selected pricing profile rather than inferred from benchmark data
- `ec2_hourly_price` is optional and should stay separate from S3 cost totals

### 12.4 Formulas

For a benchmark window, compute:

```text
monthly_put_requests   = observed_put_requests   * monthly_scale_factor
monthly_get_requests   = observed_get_requests   * monthly_scale_factor
monthly_head_requests  = observed_head_requests  * monthly_scale_factor
monthly_stored_gb      = average_object_bytes / 1024^3

request_cost_usd =
  (monthly_put_requests  / 1000) * put_price_per_1000 +
  (monthly_get_requests  / 1000) * get_price_per_1000 +
  (monthly_head_requests / 1000) * head_price_per_1000

storage_cost_usd =
  monthly_stored_gb * storage_price_per_gb_month

transfer_cost_usd =
  transferred_gb * transfer_price_per_gb

s3_monthly_cost_estimate_usd =
  request_cost_usd + storage_cost_usd + transfer_cost_usd
```

For benchmark reporting, also derive:

- `cost_per_1m_messages_usd`
- `cost_per_1m_workflow_runs_usd`
- `cost_per_successful_sync_usd`
- `cost_per_gb_uploaded_usd`

Those normalized metrics are more useful than monthly totals when comparing scenario variants.

### 12.5 What Matters For `S3Store`

For the current single-object SQLite design:

- storage cost is usually negligible because only one or a few SQLite objects are stored
- request cost is usually the main S3 billable component
- same-Region transfer cost should usually be zero in the estimator

This is important because it means batching in `Local` mode may have a visible cost benefit even when storage cost does not change much:

- fewer `sync()` calls
- fewer `PUT` requests
- similar stored bytes

### 12.6 Benchmark Artifact Integration

Each result should persist:

- raw observed counts: `object_put_count`, `object_get_count`, `object_head_count`, `bytes_uploaded`, `bytes_downloaded`
- cost model inputs
- derived cost estimates

Suggested summary block:

```json
{
  "cost_estimate": {
    "region": "us-east-1",
    "same_region_ec2": true,
    "request_cost_estimate_usd": 0.0,
    "storage_cost_estimate_usd": 0.0,
    "transfer_cost_estimate_usd": 0.0,
    "s3_monthly_cost_estimate_usd": 0.0,
    "cost_per_1m_messages_usd": 0.0
  }
}
```

### 12.7 Validation and Source of Truth

The benchmark doc should be explicit that this is an estimate.

Source of truth:

- AWS pricing pages
- AWS Pricing Calculator for final budgeting

The benchmark harness should not fetch live prices during every run.

Instead:

- store a versioned pricing profile in the repo or benchmark config
- update it intentionally when AWS prices change
- record the pricing profile version in each run artifact

### 12.8 Latency Reference Inputs

The benchmark should also record where its latency defaults came from.

Suggested metadata:

```json
{
  "latency_reference": {
    "read_profile_source": "nixiesearch-standard-s3-read-benchmark",
    "write_profile_source": "warpstream-s3-standard-write-path-blog",
    "notes": "Workload-level references, not AWS latency guarantees"
  }
}
```

---

## 13. Validation Strategy

The benchmark environment itself must be validated before trusting results.

Validation checks:

- with `direct_localstack`, proxy-added delay should be effectively zero
- with a Toxiproxy latency profile, observed end-to-end timings should roughly match the configured coarse delay envelope
- under concurrent writers, CAS conflicts should still occur and be observable through benchmark results
- the benchmark artifact should fail validation if the proxy did not apply the configured profile

This should be automated as a lightweight preflight benchmark check.

---

## 14. Implementation Plan

### Phase 1: Benchmark Stack Smoke Test

Scope:

- add Make support for `localstack` plus `toxiproxy`
- add minimal benchmark runtime support for backend `s3`
- point `AWS_ENDPOINT_URL` at Toxiproxy
- add a preflight or smoke path that configures one latency toxic and runs a tiny `sync()` / `snapshot()` cycle

Runnable goal:

- a developer can start the stack locally and run one smoke check that proves:
- requests flow through Toxiproxy to LocalStack
- injected latency affects observed timings
- S3-backed `pgqrs` operations still succeed end to end

This phase should not add real benchmark scenarios yet.

### Phase 2: First Shared Queue Scenario On S3

Scope:

- reuse one existing shared queue scenario, preferably `queue.drain_fixed_backlog`
- add S3-specific run dimensions such as `durability_mode` and `s3_latency_profile`
- persist Toxiproxy settings in benchmark artifacts
- add the first S3-specific extra metrics needed for this scenario

Runnable goal:

- one shared queue scenario runs successfully with `backend=s3`
- results can be stored and viewed with common metrics plus a small set of S3-specific fields

This phase should stay intentionally narrow:

- one scenario
- one latency profile
- Rust only is acceptable

### Phase 3: First S3-Only Contention Scenario

Scope:

- implement `s3.sync.cas_conflict`
- measure conflict rate, retry or failure count, and blocked `sync()` time
- validate that concurrent writers produce observable CAS winner/loser behavior under Toxiproxy

Runnable goal:

- a dedicated contention benchmark can be run locally and produces stable enough outputs to compare across changes

This is the first phase that directly answers the unique S3Store question.

### Phase 4: Second Shared Scenario Or Local-Mode Variant

Scope:

- add exactly one of:
- a second shared queue scenario such as `queue.e2e.steady_state`
- or a `Local`-mode variant of the first shared scenario
- extend extra S3 metrics only where that scenario needs them

Runnable goal:

- the benchmark matrix expands beyond one shared scenario without changing the harness shape

This phase should still avoid workflow benchmarking.

### Phase 5: Workflow And Cost Expansion

Scope:

- add one minimal shared workflow scenario such as `workflow.simple.1_step`
- wire cost estimator fields into benchmark summaries
- promote an initial curated baseline if the results are stable enough

Runnable goal:

- one workflow benchmark and one queue benchmark both run on `backend=s3`
- artifacts include the first cost estimate fields

### Phase 6: Real-S3 Sanity Runs

Scope:

- run a very small subset against real AWS S3
- compare shape, not exact numbers, against the emulated profile
- tune latency profiles only if the qualitative behavior is clearly wrong

Runnable goal:

- the team has at least one documented comparison between emulated S3 runs and real-S3 sanity runs

Non-goal for this phase:

- matching AWS numbers exactly

---

## 15. Recommendation

Yes, the S3 behavior can be emulated and dialed to arbitrary values in a Docker-based benchmark setup.

But the recommended solution is **not** plain NGINX by itself.

The clean benchmark design is:

- LocalStack for storage semantics
- Toxiproxy for coarse TCP latency and failure injection
- the existing pgqrs benchmark harness for scenario execution and artifact storage

And yes, the generic throughput and latency metrics should still be measured, but for `S3Store` they must be extended with:

- concurrency metrics
- conflict / stale-state metrics
- freshness lag metrics
- remote operation counts and timings

Without those additions, the benchmark would miss the main S3-specific costs and failure modes.

---

## 16. Toxiproxy Implementation Sketch

This section describes the concrete first implementation using Toxiproxy.

### 16.1 Service Topology

Use:

- LocalStack as the S3-compatible object store
- Toxiproxy as a TCP proxy in front of LocalStack

Recommended path:

```text
benchmark executor
  -> AWS_ENDPOINT_URL=http://localhost:<toxiproxy-port>
  -> toxiproxy
  -> localstack:4566
```

### 16.2 Control Model

Toxiproxy should be configured at benchmark startup through its HTTP API.

For each run:

1. create or reset the proxy
2. apply the desired toxics
3. run the benchmark
4. record the toxic configuration in the artifact
5. tear down or reset the proxy

The main toxics for v1 are:

- latency
- jitter
- timeout
- connection reset

### 16.3 Repo Integration

The repo already starts LocalStack through Make rather than `docker compose`.

So the simplest integration path is:

- add `make start-toxiproxy`
- add `make stop-toxiproxy`
- optionally add `make start-s3-bench-stack` that starts LocalStack then Toxiproxy

Benchmark runtime changes:

- add backend `s3` support in [`benchmarks/bench/runtime.py`](/Users/rajatvenkatesh/code/pgqrs/s3store-perf_worktree/benchmarks/bench/runtime.py)
- point `AWS_ENDPOINT_URL` at the Toxiproxy listener
- store toxic settings in `RunSpec.fixed_parameters`
- store any benchmark-side proxy observations in `RunPointResult.summary`

Scenario-spec changes:

- use `notes.s3_emulation_profile`
- use `notes.s3_toxics` for explicit latency and failure settings

### 16.4 Minimum Viable Build Order

Build the benchmark environment in this order:

1. LocalStack pass-through via Toxiproxy
2. fixed TCP latency
3. latency plus jitter
4. timeout / reset injection
5. benchmark harness integration

### 16.5 Tests

Add focused smoke tests for the stack:

- pass-through requests succeed through Toxiproxy
- configured latency increases observed `sync()` time
- concurrent writers still produce CAS conflicts through LocalStack
- timeout toxic surfaces as failed `sync()` operations

### 16.6 Future Refinement: Custom HTTP Proxy

Only build a custom HTTP-aware proxy later if we need:

- distinct `PUT` / `GET` / `HEAD` latency models
- direct request attribution by method
- benchmark-owned histograms for injected delay by operation type
- finer control over percentile-shaped latency distributions

At that point a small benchmark-owned Rust proxy is the better next step than introducing Envoy or complex NGINX scripting.

---

## 17. Reference Notes

The benchmark defaults in this document should be interpreted carefully:

- AWS does not publish a general S3 Standard latency SLA like `150 ms` or `500 ms`
- AWS also does not publish a general "1 write/sec per object" limit for S3 Standard

The latency defaults here are informed by public workload measurements:

- Nixiesearch reports `100+ ms` `p99` read latency for standard S3 random reads from same-Region EC2
- WarpStream reports roughly `400-600 ms` `p99` write latency on S3 Standard for its end-to-end write path

Those are useful external anchors for benchmark configuration, but they remain workload-level references rather than S3 service guarantees.
