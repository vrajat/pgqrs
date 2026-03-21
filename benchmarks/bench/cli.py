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

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run and inspect pgqrs benchmark scenarios.",
)


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
        run_id = uuid.uuid4().hex
        points = expand_points(scenario)
        fixed_parameters = dict(scenario.action.fixed)
        if prefill_jobs is not None:
            fixed_parameters["prefill_jobs"] = prefill_jobs

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
                backend_runtime,
                observer=observer,
            )
            observer.point_finished(result=result)
            append_jsonl(Path(output_path), result)

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
        run_id = uuid.uuid4().hex
        points = expand_points(scenario)
        fixed_parameters = dict(scenario.action.fixed)
        if prefill_jobs is not None:
            fixed_parameters["prefill_jobs"] = prefill_jobs

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
                backend_runtime.dsn,
                "--prefill-jobs",
                str(fixed_parameters["prefill_jobs"]),
                "--consumers",
                str(point["consumers"]),
                "--dequeue-batch-size",
                str(point["dequeue_batch_size"]),
                "--payload-profile",
                str(fixed_parameters["payload_profile"]),
            ]
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
            observer.point_finished(result=result)
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
    raise SystemExit(f"Binding {binding!r} is not implemented yet.")


if __name__ == "__main__":
    app()
