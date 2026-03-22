"""Runtime helpers for benchmark execution."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass
class BackendRuntime:
    dsn: str
    cleanup_paths: list[Path]

    def cleanup(self) -> None:
        for path in self.cleanup_paths:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


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
        return BackendRuntime(dsn=dsn, cleanup_paths=[])

    if backend in {"sqlite", "turso"}:
        env_key = (
            "PGQRS_BENCH_SQLITE_DSN" if backend == "sqlite" else "PGQRS_BENCH_TURSO_DSN"
        )
        test_key = (
            "PGQRS_TEST_SQLITE_DSN" if backend == "sqlite" else "PGQRS_TEST_TURSO_DSN"
        )
        dsn = os.environ.get(env_key) or os.environ.get(test_key)
        if dsn:
            return BackendRuntime(dsn=dsn, cleanup_paths=[])

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
        )

    raise RuntimeError(f"Unsupported backend runtime: {backend}")
