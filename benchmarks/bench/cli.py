"""CLI entrypoint for the benchmark program."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import typer

from benchmarks.bench.loader import expand_points, load_scenario
from benchmarks.bench.reporting import TyperRunObserver, configure_logging
from benchmarks.bench.registry import SCENARIOS, get_registration
from benchmarks.bench.results import append_jsonl, default_output_path, init_jsonl
from benchmarks.bench.runtime import resolve_backend_runtime, resolve_process_mode
from benchmarks.bench.schema import RunPointResult, RunSpec
from benchmarks.bench.toxiproxy import ToxiproxyClient, ToxiproxyConfig

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run and inspect pgqrs benchmark scenarios.",
)


def _parse_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


_SIZE_SUFFIXES = {
    "B": 1,
    "KB": 1_000,
    "MB": 1_000_000,
    "GB": 1_000_000_000,
    "TB": 1_000_000_000_000,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}
_NET_IO_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*$")
_LOCALSTACK_CONTAINER = "pgqrs-test-localstack"
_TOXIPROXY_CONTAINER = "pgqrs-bench-toxiproxy"
_LOCALSTACK_OP_PATTERNS = {
    "put": re.compile(r"AWS s3\.PutObject =>", re.IGNORECASE),
    "get": re.compile(r"AWS s3\.GetObject =>", re.IGNORECASE),
    "head": re.compile(r"AWS s3\.HeadObject =>", re.IGNORECASE),
    "delete": re.compile(r"AWS s3\.DeleteObject =>", re.IGNORECASE),
}


def _parse_size_to_bytes(raw: str) -> int | None:
    match = _NET_IO_PATTERN.match(raw)
    if not match:
        return None
    value = float(match.group(1))
    suffix = match.group(2).upper()
    multiplier = _SIZE_SUFFIXES.get(suffix)
    if multiplier is None:
        return None
    return int(value * multiplier)


def _read_container_net_io(container_name: str) -> tuple[int, int] | None:
    completed = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.NetIO}}",
            container_name,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    net_io = completed.stdout.strip()
    if not net_io or " / " not in net_io:
        return None
    rx_raw, tx_raw = net_io.split(" / ", 1)
    rx_bytes = _parse_size_to_bytes(rx_raw)
    tx_bytes = _parse_size_to_bytes(tx_raw)
    if rx_bytes is None or tx_bytes is None:
        return None
    return (rx_bytes, tx_bytes)


def _capture_s3_io_snapshot() -> dict[str, tuple[int, int] | None]:
    return {
        "localstack": _read_container_net_io(_LOCALSTACK_CONTAINER),
        "toxiproxy": _read_container_net_io(_TOXIPROXY_CONTAINER),
    }


def _append_s3_io_metrics(
    payload: dict[str, object],
    *,
    before: dict[str, tuple[int, int] | None],
    after: dict[str, tuple[int, int] | None],
) -> None:
    summary_obj = payload.get("summary")
    if not isinstance(summary_obj, dict):
        return

    for name in ("localstack", "toxiproxy"):
        before_io = before.get(name)
        after_io = after.get(name)
        if before_io is None or after_io is None:
            continue
        delta_rx = max(after_io[0] - before_io[0], 0)
        delta_tx = max(after_io[1] - before_io[1], 0)
        delta_total = delta_rx + delta_tx
        summary_obj[f"s3_io_{name}_rx_bytes"] = delta_rx
        summary_obj[f"s3_io_{name}_tx_bytes"] = delta_tx
        summary_obj[f"s3_io_{name}_total_bytes"] = delta_total

    processed_messages = summary_obj.get("processed_messages")
    dequeue_calls = summary_obj.get("dequeue_calls")
    archive_calls = summary_obj.get("archive_calls")
    localstack_total = summary_obj.get("s3_io_localstack_total_bytes")
    toxiproxy_total = summary_obj.get("s3_io_toxiproxy_total_bytes")

    if isinstance(processed_messages, int) and processed_messages > 0:
        if isinstance(localstack_total, int):
            summary_obj["s3_io_localstack_bytes_per_message"] = round(
                localstack_total / processed_messages, 3
            )
        if isinstance(toxiproxy_total, int):
            summary_obj["s3_io_toxiproxy_bytes_per_message"] = round(
                toxiproxy_total / processed_messages, 3
            )

    if isinstance(dequeue_calls, int) and dequeue_calls > 0:
        if isinstance(localstack_total, int):
            summary_obj["s3_io_localstack_bytes_per_dequeue_call"] = round(
                localstack_total / dequeue_calls, 3
            )
        if isinstance(toxiproxy_total, int):
            summary_obj["s3_io_toxiproxy_bytes_per_dequeue_call"] = round(
                toxiproxy_total / dequeue_calls, 3
            )

    if isinstance(archive_calls, int) and archive_calls > 0:
        if isinstance(localstack_total, int):
            summary_obj["s3_io_localstack_bytes_per_archive_call"] = round(
                localstack_total / archive_calls, 3
            )
        if isinstance(toxiproxy_total, int):
            summary_obj["s3_io_toxiproxy_bytes_per_archive_call"] = round(
                toxiproxy_total / archive_calls, 3
            )


def _to_rfc3339_utc(ts: datetime) -> str:
    return (
        ts.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _capture_localstack_ops_between(
    *,
    start: datetime,
    end: datetime,
) -> dict[str, int] | None:
    completed = subprocess.run(
        [
            "docker",
            "logs",
            "--since",
            _to_rfc3339_utc(start),
            "--until",
            _to_rfc3339_utc(end),
            _LOCALSTACK_CONTAINER,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    lines = completed.stdout.splitlines()
    counts = {key: 0 for key in _LOCALSTACK_OP_PATTERNS}
    for line in lines:
        for key, pattern in _LOCALSTACK_OP_PATTERNS.items():
            if pattern.search(line):
                counts[key] += 1
    counts["total"] = sum(counts.values())
    return counts


def _append_s3_operation_counts(
    payload: dict[str, object],
    *,
    start: datetime,
    end: datetime,
) -> None:
    summary_obj = payload.get("summary")
    if not isinstance(summary_obj, dict):
        return
    counts = _capture_localstack_ops_between(start=start, end=end)
    if counts is None:
        return
    summary_obj["s3_ops_put"] = counts["put"]
    summary_obj["s3_ops_get"] = counts["get"]
    summary_obj["s3_ops_head"] = counts["head"]
    summary_obj["s3_ops_delete"] = counts["delete"]
    summary_obj["s3_ops_total"] = counts["total"]

    processed_messages = summary_obj.get("processed_messages")
    if isinstance(processed_messages, int) and processed_messages > 0:
        summary_obj["s3_ops_per_message"] = round(
            counts["total"] / processed_messages, 6
        )


def _prepare_s3_run(
    backend: str,
    backend_runtime,
    *,
    default_durability_mode: str | None = None,
) -> dict[str, object]:
    if backend != "s3":
        return {}

    profile = os.environ.get("PGQRS_BENCH_S3_LATENCY_PROFILE", "toxiproxy_baseline")
    result: dict[str, object] = {
        "s3_latency_profile": profile,
    }
    durability_mode = os.environ.get("PGQRS_BENCH_S3_DURABILITY_MODE")
    if durability_mode is None:
        durability_mode = default_durability_mode or "durable"
    result["durability_mode"] = durability_mode

    if profile == "direct_localstack":
        endpoint_url = os.environ.get(
            "PGQRS_BENCH_S3_DIRECT_ENDPOINT",
            "http://localhost:4566",
        )
        backend_runtime.env_overrides["AWS_ENDPOINT_URL"] = endpoint_url
        result["s3_endpoint_url"] = endpoint_url
        result["s3_transport"] = "direct_localstack"
        return result

    defaults = {
        "toxiproxy_baseline": {"latency_ms": 60, "jitter_ms": 0},
        "toxiproxy_degraded": {"latency_ms": 150, "jitter_ms": 25},
    }
    profile_defaults = defaults.get(profile, defaults["toxiproxy_baseline"])
    latency_ms = _parse_int_env(
        "PGQRS_BENCH_S3_LATENCY_MS",
        profile_defaults["latency_ms"],
    )
    jitter_ms = _parse_int_env(
        "PGQRS_BENCH_S3_JITTER_MS",
        profile_defaults["jitter_ms"],
    )
    proxy = ToxiproxyConfig(
        api_url=os.environ.get(
            "PGQRS_BENCH_S3_TOXIPROXY_API_URL", "http://localhost:8474"
        ),
        proxy_name=os.environ.get(
            "PGQRS_BENCH_S3_TOXIPROXY_PROXY_NAME", "pgqrs_s3_bench"
        ),
        listen=os.environ.get("PGQRS_BENCH_S3_TOXIPROXY_LISTEN", "0.0.0.0:4567"),
        upstream=os.environ.get(
            "PGQRS_BENCH_S3_TOXIPROXY_UPSTREAM",
            "host.docker.internal:4566",
        ),
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
    )
    endpoint_url = os.environ.get(
        "PGQRS_BENCH_S3_ENDPOINT",
        "http://localhost:4567",
    )
    client = ToxiproxyClient(proxy.api_url)
    client.ping()
    client.recreate_proxy(
        name=proxy.proxy_name,
        listen=proxy.listen,
        upstream=proxy.upstream,
    )
    if proxy.latency_ms > 0 or proxy.jitter_ms > 0:
        client.add_latency(
            proxy_name=proxy.proxy_name,
            name="s3_latency_downstream",
            latency_ms=proxy.latency_ms,
            jitter_ms=proxy.jitter_ms,
        )
    backend_runtime.env_overrides["AWS_ENDPOINT_URL"] = endpoint_url
    result["s3_endpoint_url"] = endpoint_url
    result["s3_transport"] = "toxiproxy"
    result["s3_toxics"] = {
        "latency_ms": proxy.latency_ms,
        "jitter_ms": proxy.jitter_ms,
        "proxy_name": proxy.proxy_name,
    }
    return result


def _validate_registration(
    *,
    scenario_id: str,
    backend: str,
    binding: str,
    profile: str,
):
    registration = get_registration(scenario_id)

    if backend not in registration.backends:
        raise SystemExit(
            f"Backend {backend!r} is not valid for scenario {registration.scenario_id!r}."
        )
    if binding not in registration.bindings:
        raise SystemExit(
            f"Binding {binding!r} is not valid for scenario {registration.scenario_id!r}."
        )
    if backend == "s3" and binding == "python":
        raise SystemExit("S3 benchmark execution is rust-only for now.")
    if profile not in registration.profiles:
        raise SystemExit(
            f"Profile {profile!r} is not valid for scenario {registration.scenario_id!r}."
        )
    try:
        resolve_process_mode(backend, profile)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    return registration, load_scenario(registration.scenario_path)


async def _run_python_queue(
    *,
    scenario_id: str,
    backend: str,
    binding: str,
    profile: str,
    output: Path | None,
    prefill_jobs: int | None,
    verbose: bool,
    progress: bool,
) -> int:
    try:
        from benchmarks.executors.python.queue import run_drain_fixed_backlog
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Python benchmark execution requires the pgqrs Python package to be available "
            "in the current interpreter."
        ) from exc

    configure_logging(verbose=verbose)
    registration, scenario = _validate_registration(
        scenario_id=scenario_id,
        backend=backend,
        binding=binding,
        profile=profile,
    )
    if registration.executor_hint != "queue":
        raise SystemExit(f"Unsupported executor hint: {registration.executor_hint}")
    if scenario.scenario_id != "queue.drain_fixed_backlog":
        raise SystemExit(
            f"Python runner not implemented yet for {scenario.scenario_id!r}"
        )

    backend_runtime = resolve_backend_runtime(backend)
    process_mode = resolve_process_mode(backend, profile)
    observer = TyperRunObserver(show_progress=progress)
    try:
        fixed_parameters = dict(scenario.action.fixed)
        fixed_parameters["process_mode"] = process_mode
        s3_parameters = _prepare_s3_run(
            backend,
            backend_runtime,
            default_durability_mode=fixed_parameters.get("durability_mode"),
        )
        with backend_runtime.activate_env():
            run_id = uuid.uuid4().hex
            points = expand_points(scenario)
            if prefill_jobs is not None:
                fixed_parameters["prefill_jobs"] = prefill_jobs
            fixed_parameters.update(s3_parameters)

            output_path = output
            if output_path is None:
                output_path = default_output_path(
                    RunSpec(
                        run_id=run_id,
                        scenario_id=scenario.scenario_id,
                        backend=backend,
                        binding=binding,
                        profile=profile,
                        question=scenario.question,
                    )
                )
            init_jsonl(Path(output_path))

            for index, point in enumerate(points, start=1):
                point_backend_runtime = (
                    resolve_backend_runtime(backend)
                    if backend == "s3"
                    else backend_runtime
                )
                observer.point_started(
                    index=index,
                    total=len(points),
                    point_parameters=point,
                )
                spec = RunSpec(
                    run_id=run_id,
                    scenario_id=scenario.scenario_id,
                    backend=backend,
                    binding=binding,
                    profile=profile,
                    question=scenario.question,
                    fixed_parameters=fixed_parameters,
                    point_parameters=point,
                    output_path=str(output_path),
                )
                result = await run_drain_fixed_backlog(
                    spec,
                    point_backend_runtime,
                    observer=observer,
                )
                observer.point_finished(result=result)
                append_jsonl(Path(output_path), result)
                if point_backend_runtime is not backend_runtime:
                    point_backend_runtime.cleanup()

            typer.echo(str(output_path))
            return 0
    finally:
        observer.close()
        backend_runtime.cleanup()


async def _run_rust_queue(
    *,
    scenario_id: str,
    backend: str,
    profile: str,
    output: Path | None,
    progress: bool,
    prefill_jobs: int | None,
) -> int:
    registration, scenario = _validate_registration(
        scenario_id=scenario_id,
        backend=backend,
        binding="rust",
        profile=profile,
    )
    if registration.executor_hint != "queue":
        raise SystemExit(f"Unsupported executor hint: {registration.executor_hint}")
    if scenario.scenario_id != "queue.drain_fixed_backlog":
        raise SystemExit(
            f"Rust runner not implemented yet for {scenario.scenario_id!r}"
        )

    backend_runtime = resolve_backend_runtime(backend)
    process_mode = resolve_process_mode(backend, profile)
    observer = TyperRunObserver(show_progress=progress)
    try:
        fixed_parameters = dict(scenario.action.fixed)
        fixed_parameters["process_mode"] = process_mode
        s3_parameters = _prepare_s3_run(
            backend,
            backend_runtime,
            default_durability_mode=fixed_parameters.get("durability_mode"),
        )
        with backend_runtime.activate_env():
            run_id = uuid.uuid4().hex
            points = expand_points(scenario)
            if prefill_jobs is not None:
                fixed_parameters["prefill_jobs"] = prefill_jobs
            fixed_parameters.update(s3_parameters)

            output_path = output
            if output_path is None:
                output_path = default_output_path(
                    RunSpec(
                        run_id=run_id,
                        scenario_id=scenario.scenario_id,
                        backend=backend,
                        binding="rust",
                        profile=profile,
                        question=scenario.question,
                    )
                )
            init_jsonl(Path(output_path))

            for index, point in enumerate(points, start=1):
                point_backend_runtime = (
                    resolve_backend_runtime(backend)
                    if backend == "s3"
                    else backend_runtime
                )
                observer.point_started(
                    index=index,
                    total=len(points),
                    point_parameters=point,
                )
                args = [
                    "cargo",
                    "run",
                    "--release",
                    "--quiet",
                    "--manifest-path",
                    "benchmarks/executors/rust/Cargo.toml",
                    "--no-default-features",
                    "--features",
                    backend,
                    "--bin",
                    "pgqrs-bench-rust",
                    "--",
                    "run",
                    "--run-id",
                    run_id,
                    "--scenario-id",
                    scenario.scenario_id,
                    "--question",
                    scenario.question,
                    "--backend",
                    backend,
                    "--profile",
                    profile,
                    "--dsn",
                    point_backend_runtime.dsn,
                    "--prefill-jobs",
                    str(fixed_parameters["prefill_jobs"]),
                    "--consumers",
                    str(point["consumers"]),
                    "--dequeue-batch-size",
                    str(point["dequeue_batch_size"]),
                    "--payload-profile",
                    str(fixed_parameters["payload_profile"]),
                    "--process-mode",
                    process_mode,
                ]
                if backend == "s3":
                    args.extend(
                        [
                            "--durability-mode",
                            str(fixed_parameters["durability_mode"]),
                        ]
                    )

                s3_io_before: dict[str, tuple[int, int] | None] | None = None
                s3_point_start: datetime | None = None
                if backend == "s3":
                    s3_io_before = _capture_s3_io_snapshot()
                    s3_point_start = datetime.now(timezone.utc)
                completed = await asyncio.to_thread(
                    subprocess.run,
                    args,
                    cwd=os.getcwd(),
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if completed.returncode != 0:
                    raise SystemExit(
                        completed.stderr.strip() or completed.stdout.strip()
                    )

                payload = json.loads(completed.stdout)
                payload_metadata = payload.setdefault("metadata", {})
                payload_fixed = dict(payload_metadata.get("fixed_parameters", {}))
                payload_fixed.update(fixed_parameters)
                payload_metadata["fixed_parameters"] = payload_fixed
                if backend == "s3" and s3_io_before is not None:
                    s3_io_after = _capture_s3_io_snapshot()
                    _append_s3_io_metrics(
                        payload,
                        before=s3_io_before,
                        after=s3_io_after,
                    )
                    if s3_point_start is not None:
                        _append_s3_operation_counts(
                            payload,
                            start=s3_point_start,
                            end=datetime.now(timezone.utc),
                        )
                result = RunPointResult(
                    metadata=payload["metadata"],
                    summary=payload["summary"],
                    samples=payload.get("samples", []),
                )
                observer.point_finished(result=result)
                append_jsonl(Path(output_path), result)
                if point_backend_runtime is not backend_runtime:
                    point_backend_runtime.cleanup()

            typer.echo(str(output_path))
            return 0
    finally:
        observer.close()
        backend_runtime.cleanup()


async def _run_s3_smoke(
    *,
    output: Path | None,
    toxiproxy: ToxiproxyConfig,
    toxiproxy_endpoint_url: str,
    progress: bool,
) -> int:
    configure_logging(verbose=False)
    backend_runtime = resolve_backend_runtime("s3")
    backend_runtime.env_overrides["AWS_ENDPOINT_URL"] = toxiproxy_endpoint_url
    observer = TyperRunObserver(show_progress=progress)
    try:
        observer.phase_started(name="toxiproxy", total=1)
        client = ToxiproxyClient(toxiproxy.api_url)
        client.ping()
        client.recreate_proxy(
            name=toxiproxy.proxy_name,
            listen=toxiproxy.listen,
            upstream=toxiproxy.upstream,
        )
        if toxiproxy.latency_ms > 0 or toxiproxy.jitter_ms > 0:
            client.add_latency(
                proxy_name=toxiproxy.proxy_name,
                name="s3_latency_downstream",
                latency_ms=toxiproxy.latency_ms,
                jitter_ms=toxiproxy.jitter_ms,
            )
        observer.phase_finished(name="toxiproxy")

        output_path = output
        run_id = uuid.uuid4().hex
        if output_path is None:
            output_path = default_output_path(
                RunSpec(
                    run_id=run_id,
                    scenario_id="s3.stack.smoke",
                    backend="s3",
                    binding="rust",
                    profile="smoke",
                    question="Does the LocalStack+Toxiproxy S3 benchmark stack work end to end?",
                )
            )
        init_jsonl(Path(output_path))

        args = [
            "cargo",
            "run",
            "--release",
            "--quiet",
            "--manifest-path",
            "benchmarks/executors/rust/Cargo.toml",
            "--no-default-features",
            "--features",
            "s3",
            "--bin",
            "s3_smoke",
            "--",
            "--run-id",
            run_id,
            "--dsn",
            backend_runtime.dsn,
            "--latency-ms",
            str(toxiproxy.latency_ms),
            "--jitter-ms",
            str(toxiproxy.jitter_ms),
        ]
        with backend_runtime.activate_env():
            completed = await asyncio.to_thread(
                subprocess.run,
                args,
                cwd=os.getcwd(),
                capture_output=True,
                text=True,
                check=False,
            )
        if completed.returncode != 0:
            raise SystemExit(completed.stderr.strip() or completed.stdout.strip())

        payload = json.loads(completed.stdout)
        result = RunPointResult(
            metadata=payload["metadata"],
            summary=payload["summary"],
            samples=payload.get("samples", []),
        )
        append_jsonl(Path(output_path), result)
        typer.echo(str(output_path))
        return 0
    finally:
        observer.close()
        backend_runtime.cleanup()


@app.command("list")
def list_scenarios() -> None:
    """List known benchmark scenarios."""

    for scenario in SCENARIOS:
        typer.echo(scenario.scenario_id)


@app.command()
def run(
    scenario: str = typer.Option(..., "--scenario"),
    backend: str = typer.Option(..., "--backend"),
    binding: str = typer.Option(..., "--binding"),
    profile: str = typer.Option("compat", "--profile"),
    output: Path | None = typer.Option(None, "--output"),
    prefill_jobs: int | None = typer.Option(None, "--prefill-jobs"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    progress: bool = typer.Option(True, "--progress/--no-progress"),
) -> None:
    """Run a benchmark scenario."""

    if binding == "python":
        registration = get_registration(scenario)
        if registration.executor_hint != "queue":
            raise SystemExit(
                f"Binding {binding!r} is not implemented for {registration.executor_hint!r}."
            )
        raise SystemExit(
            asyncio.run(
                _run_python_queue(
                    scenario_id=scenario,
                    backend=backend,
                    binding=binding,
                    profile=profile,
                    output=output,
                    prefill_jobs=prefill_jobs,
                    verbose=verbose,
                    progress=progress,
                )
            )
        )
    if binding == "rust":
        registration = get_registration(scenario)
        if registration.executor_hint == "queue":
            raise SystemExit(
                asyncio.run(
                    _run_rust_queue(
                        scenario_id=scenario,
                        backend=backend,
                        profile=profile,
                        output=output,
                        progress=progress,
                        prefill_jobs=prefill_jobs,
                    )
                )
            )
        raise SystemExit(f"Unsupported executor hint: {registration.executor_hint}")
    raise SystemExit(f"Binding {binding!r} is not implemented yet.")


@app.command("s3-smoke")
def s3_smoke(
    output: Path | None = typer.Option(None, "--output"),
    toxiproxy_api_url: str = typer.Option(
        "http://localhost:8474",
        "--toxiproxy-api-url",
    ),
    toxiproxy_proxy_name: str = typer.Option(
        "pgqrs_s3_bench",
        "--toxiproxy-proxy-name",
    ),
    toxiproxy_listen: str = typer.Option(
        "0.0.0.0:4567",
        "--toxiproxy-listen",
    ),
    toxiproxy_upstream: str = typer.Option(
        "host.docker.internal:4566",
        "--toxiproxy-upstream",
    ),
    toxiproxy_endpoint_url: str = typer.Option(
        "http://localhost:4567",
        "--toxiproxy-endpoint-url",
    ),
    latency_ms: int = typer.Option(60, "--latency-ms"),
    jitter_ms: int = typer.Option(0, "--jitter-ms"),
    progress: bool = typer.Option(True, "--progress/--no-progress"),
) -> None:
    """Run the LocalStack+Toxiproxy S3 smoke check."""

    raise SystemExit(
        asyncio.run(
            _run_s3_smoke(
                output=output,
                toxiproxy=ToxiproxyConfig(
                    api_url=toxiproxy_api_url,
                    proxy_name=toxiproxy_proxy_name,
                    listen=toxiproxy_listen,
                    upstream=toxiproxy_upstream,
                    latency_ms=latency_ms,
                    jitter_ms=jitter_ms,
                ),
                toxiproxy_endpoint_url=toxiproxy_endpoint_url,
                progress=progress,
            )
        )
    )


if __name__ == "__main__":
    app()
