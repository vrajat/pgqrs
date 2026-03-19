"""Python queue benchmark executors."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from statistics import quantiles
from typing import Any

import pgqrs

from benchmarks.bench.reporting import NullObserver, RunObserver
from benchmarks.bench.runtime import BackendRuntime
from benchmarks.bench.schema import RunPointResult, RunSpec

LOGGER = logging.getLogger("benchmarks.queue")


def _percentile_ms(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return round(values[0] * 1000, 3)
    # quantiles(..., n=100) gives 99 cut points; p95 is index 94.
    percentiles = quantiles(values, n=100, method="inclusive")
    return round(percentiles[int(percentile) - 1] * 1000, 3)


@dataclass
class DrainState:
    remaining: int
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    done: asyncio.Event = field(default_factory=asyncio.Event)

    async def mark_archived(self, count: int) -> int:
        async with self.lock:
            self.remaining -= count
            if self.remaining <= 0:
                self.done.set()
            return self.remaining

    async def get_remaining(self) -> int:
        async with self.lock:
            return self.remaining


@dataclass
class ConsumerStats:
    dequeues: list[float] = field(default_factory=list)
    archives: list[float] = field(default_factory=list)
    processed: int = 0
    empty_polls: int = 0
    errors: int = 0


async def _setup_store(
    backend_runtime: BackendRuntime,
    *,
    disable_enqueue_rate_limit: bool = False,
) -> Any:
    if disable_enqueue_rate_limit:
        config = pgqrs.Config(backend_runtime.dsn)
        config.max_enqueue_per_second = None
        config.max_enqueue_burst = None
        store = await pgqrs.connect_with(config)
    else:
        store = await pgqrs.connect(backend_runtime.dsn)
    await store.bootstrap()
    return store


async def _prefill_queue(
    store: Any,
    queue_name: str,
    prefill_jobs: int,
    payload_profile: str,
    observer: RunObserver,
) -> None:
    await store.queue(queue_name)
    producer = await store.producer(queue_name)
    payload = {"kind": "benchmark", "payload_profile": payload_profile}
    max_batch_size = 100
    produced = 0

    observer.phase_started(name="prefill", total=prefill_jobs)
    while produced < prefill_jobs:
        batch_size = min(max_batch_size, prefill_jobs - produced)
        payloads = [payload for _ in range(batch_size)]
        await pgqrs.enqueue_batch(producer, payloads)
        produced += batch_size
        observer.phase_progress(
            name="prefill",
            current=produced,
            total=prefill_jobs,
        )
    observer.phase_finished(name="prefill")


async def _consumer_loop(
    consumer: Any,
    batch_size: int,
    state: DrainState,
    stats: ConsumerStats,
    observer: RunObserver,
    total_jobs: int,
) -> None:
    while True:
        if state.done.is_set():
            return

        dequeue_start = time.perf_counter()
        messages = await consumer.dequeue(batch_size=batch_size)
        stats.dequeues.append(time.perf_counter() - dequeue_start)

        if not messages:
            stats.empty_polls += 1
            if await state.get_remaining() <= 0:
                return
            await asyncio.sleep(0.001)
            continue

        archive_start = time.perf_counter()
        results = await consumer.archive_many([msg.id for msg in messages])
        stats.archives.append(time.perf_counter() - archive_start)

        archived = sum(1 for result in results if result)
        failures = len(results) - archived
        stats.errors += failures
        stats.processed += archived
        remaining = await state.mark_archived(archived)
        observer.phase_progress(
            name="drain",
            current=total_jobs - max(remaining, 0),
            total=total_jobs,
        )


async def run_drain_fixed_backlog(
    spec: RunSpec,
    backend_runtime: BackendRuntime,
    observer: RunObserver | None = None,
) -> RunPointResult:
    observer = observer or NullObserver()
    store = await _setup_store(
        backend_runtime,
        disable_enqueue_rate_limit=True,
    )
    fixed = spec.fixed_parameters
    point = spec.point_parameters

    prefill_jobs = int(fixed["prefill_jobs"])
    batch_size = int(point["dequeue_batch_size"])
    consumers_n = int(point["consumers"])
    payload_profile = str(fixed["payload_profile"])
    queue_name = f"bench_drain_{uuid.uuid4().hex[:12]}"

    LOGGER.info(
        "starting drain_fixed_backlog point backend=%s consumers=%s dequeue_batch_size=%s prefill_jobs=%s",
        spec.backend,
        consumers_n,
        batch_size,
        prefill_jobs,
    )
    await _prefill_queue(
        store,
        queue_name,
        prefill_jobs,
        payload_profile,
        observer,
    )

    state = DrainState(remaining=prefill_jobs)
    consumers = [await store.consumer(queue_name) for _ in range(consumers_n)]
    consumer_stats = [ConsumerStats() for _ in range(consumers_n)]

    observer.phase_started(name="drain", total=prefill_jobs)
    start = time.perf_counter()
    try:
        await asyncio.gather(
            *[
                _consumer_loop(
                    consumer,
                    batch_size,
                    state,
                    stats,
                    observer,
                    prefill_jobs,
                )
                for consumer, stats in zip(consumers, consumer_stats, strict=True)
            ]
        )
    finally:
        observer.phase_finished(name="drain")
    elapsed = time.perf_counter() - start

    all_dequeue_latencies = [lat for stats in consumer_stats for lat in stats.dequeues]
    all_archive_latencies = [lat for stats in consumer_stats for lat in stats.archives]
    total_processed = sum(stats.processed for stats in consumer_stats)
    total_errors = sum(stats.errors for stats in consumer_stats)
    empty_polls = sum(stats.empty_polls for stats in consumer_stats)

    result = RunPointResult(
        metadata={
            "run_id": spec.run_id,
            "scenario_id": spec.scenario_id,
            "question": spec.question,
            "backend": spec.backend,
            "binding": spec.binding,
            "profile": spec.profile,
            "fixed_parameters": fixed,
            "point_parameters": point,
        },
        summary={
            "drain_messages_per_second": round(total_processed / elapsed, 3)
            if elapsed
            else 0.0,
            "total_drain_time_ms": round(elapsed * 1000, 3),
            "p95_dequeue_latency_ms": _percentile_ms(all_dequeue_latencies, 95),
            "p95_archive_latency_ms": _percentile_ms(all_archive_latencies, 95),
            "error_rate": round(
                total_errors / max(total_processed + total_errors, 1), 6
            ),
            "processed_messages": total_processed,
            "empty_polls": empty_polls,
        },
    )
    LOGGER.info(
        "completed drain_fixed_backlog point backend=%s consumers=%s dequeue_batch_size=%s throughput=%s drain_ms=%s",
        spec.backend,
        consumers_n,
        batch_size,
        result.summary["drain_messages_per_second"],
        result.summary["total_drain_time_ms"],
    )
    return result
