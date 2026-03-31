"""CLI entrypoint for the benchmark program."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid
from pathlib import Path

import typer

from benchmarks.bench.loader import expand_points, load_scenario
from benchmarks.bench.reporting import TyperRunObserver, configure_logging
from benchmarks.bench.registry import SCENARIOS, get_registration
from benchmarks.bench.results import append_jsonl, default_output_path, init_jsonl
from benchmarks.bench.runtime import resolve_backend_runtime
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
    observer = TyperRunObserver(show_progress=progress)
    try:
        fixed_parameters = dict(scenario.action.fixed)
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
    observer = TyperRunObserver(show_progress=progress)
    try:
        fixed_parameters = dict(scenario.action.fixed)
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
                ]
                if backend == "s3":
                    args.extend(
                        [
                            "--durability-mode",
                            str(fixed_parameters["durability_mode"]),
                        ]
                    )
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
