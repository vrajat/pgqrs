"""Runtime helpers for benchmark execution."""

from __future__ import annotations

import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PROCESS_MODE_BY_BACKEND = {
    "postgres": "multi_process",
    "s3": "multi_process",
    "sqlite": "single_process",
    "turso": "single_process",
}


@dataclass
class BackendRuntime:
    dsn: str
    cleanup_paths: list[Path]
    env_overrides: dict[str, str]

    @contextmanager
    def activate_env(self):
        previous: dict[str, str | None] = {
            key: os.environ.get(key) for key in self.env_overrides
        }
        try:
            for key, value in self.env_overrides.items():
                os.environ[key] = value
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def cleanup(self) -> None:
        for path in self.cleanup_paths:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


def resolve_process_mode(backend: str, profile: str) -> str:
    if profile == "single_process":
        if backend == "s3":
            raise RuntimeError("S3 benchmarks require multi_process store isolation.")
        return "single_process"

    if profile == "multi_process":
        return "multi_process"

    if profile == "compat":
        try:
            return DEFAULT_PROCESS_MODE_BY_BACKEND[backend]
        except KeyError as exc:
            raise RuntimeError(f"Unsupported backend runtime: {backend}") from exc

    raise RuntimeError(f"Unsupported benchmark profile for process mode: {profile}")


def resolve_backend_runtime(backend: str) -> BackendRuntime:
    if backend == "postgres":
        dsn = (
            os.environ.get("PGQRS_BENCH_POSTGRES_DSN")
            or os.environ.get("PGQRS_TEST_POSTGRES_DSN")
            or os.environ.get("PGQRS_TEST_DSN")
        )
        if not dsn:
            raise RuntimeError(
                "Postgres benchmark requires PGQRS_BENCH_POSTGRES_DSN, "
                "PGQRS_TEST_POSTGRES_DSN, or PGQRS_TEST_DSN."
            )
        return BackendRuntime(dsn=dsn, cleanup_paths=[], env_overrides={})

    if backend in {"sqlite", "turso"}:
        env_key = (
            "PGQRS_BENCH_SQLITE_DSN" if backend == "sqlite" else "PGQRS_BENCH_TURSO_DSN"
        )
        test_key = (
            "PGQRS_TEST_SQLITE_DSN" if backend == "sqlite" else "PGQRS_TEST_TURSO_DSN"
        )
        dsn = os.environ.get(env_key) or os.environ.get(test_key)
        if dsn:
            return BackendRuntime(dsn=dsn, cleanup_paths=[], env_overrides={})

        tmp = tempfile.NamedTemporaryFile(
            prefix=f"pgqrs-bench-{backend}-",
            suffix=".db",
            delete=False,
        )
        tmp.close()
        path = Path(tmp.name)
        return BackendRuntime(
            dsn=f"{backend}://{path}",
            cleanup_paths=[path],
            env_overrides={},
        )

    if backend == "s3":
        dsn = (
            os.environ.get("PGQRS_BENCH_S3_DSN")
            or os.environ.get("PGQRS_TEST_S3_DSN")
            or os.environ.get("PGQRS_TEST_DSN")
        )
        if not dsn:
            bucket = (
                os.environ.get("PGQRS_BENCH_S3_BUCKET")
                or os.environ.get("PGQRS_S3_BUCKET")
                or "pgqrs-test-bucket"
            )
            dsn = f"s3://{bucket}/benchmarks/{uuid.uuid4().hex}.sqlite"

        endpoint = os.environ.get("PGQRS_BENCH_S3_ENDPOINT") or os.environ.get(
            "AWS_ENDPOINT_URL"
        )
        env_overrides = {
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        }
        if endpoint:
            env_overrides["AWS_ENDPOINT_URL"] = endpoint

        return BackendRuntime(dsn=dsn, cleanup_paths=[], env_overrides=env_overrides)

    raise RuntimeError(f"Unsupported backend runtime: {backend}")
