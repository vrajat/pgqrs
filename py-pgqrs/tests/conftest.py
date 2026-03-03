import pytest
import os
import psycopg
import uuid
import itertools
import re
import shutil
from enum import Enum
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from testcontainers.postgres import PostgresContainer
import boto3

_S3_SEQ = itertools.count(1)
_LOCAL_DB_SEQ = itertools.count(1)


class TestBackend(Enum):
    __test__ = False  # prevent pytest from collecting this as a test class

    POSTGRES = "postgres"
    SQLITE = "sqlite"
    S3 = "s3"
    TURSO = "turso"

    @classmethod
    def from_env(cls) -> "TestBackend":
        backend = os.environ.get("PGQRS_TEST_BACKEND", "postgres").lower()
        try:
            return cls(backend)
        except ValueError:
            return cls.POSTGRES


def get_backend() -> TestBackend:
    return TestBackend.from_env()


@pytest.fixture(scope="session")
def test_backend() -> TestBackend:
    """Returns the current test backend."""
    return get_backend()


@pytest.fixture(scope="session")
def base_dsn(test_backend: TestBackend) -> Generator[str | None, None, None]:
    """
    Provides a base database DSN for the session.
    - Postgres: Returns env DSN or starts a container.
    - SQLite/Turso: Returns env DSN if set, else None (to trigger per-test isolation).
    """
    if test_backend == TestBackend.POSTGRES:
        dsn = os.environ.get("PGQRS_TEST_POSTGRES_DSN") or os.environ.get(
            "PGQRS_TEST_DSN"
        )
        if dsn:
            yield dsn
        else:
            with PostgresContainer("postgres:15") as postgres:
                yield postgres.get_connection_url().replace("+psycopg2", "")

    elif test_backend == TestBackend.SQLITE:
        dsn = os.environ.get("PGQRS_TEST_SQLITE_DSN")
        yield dsn

    elif test_backend == TestBackend.S3:
        dsn = os.environ.get("PGQRS_TEST_S3_DSN")
        yield dsn

    elif test_backend == TestBackend.TURSO:
        dsn = os.environ.get("PGQRS_TEST_TURSO_DSN")
        yield dsn


# Convenience decorators for backend-specific tests
def requires_backend(backend: TestBackend):
    """Skip test unless running on specified backend."""
    return pytest.mark.skipif(
        get_backend() != backend, reason=f"Test requires {backend.value} backend"
    )


def skip_on_backend(backend: TestBackend):
    """Skip test when running on specified backend."""
    return pytest.mark.skipif(
        get_backend() == backend,
        reason=f"Test not supported on {backend.value} backend",
    )


@pytest.fixture(scope="function")
def test_dsn(
    test_backend: TestBackend, base_dsn: str | None, request
) -> Generator[str, None, None]:
    """
    Provides a per-test DSN.
    - Postgres: Returns the shared base DSN.
    - SQLite/Turso: Creates a new unique database file if base_dsn is None (isolation).
    """
    if test_backend == TestBackend.POSTGRES:
        if not base_dsn:
            raise ValueError("Postgres backend requires a base_dsn")
        yield base_dsn

    elif test_backend == TestBackend.SQLITE:
        if base_dsn:
            yield base_dsn
        else:
            tmp_path = _build_local_db_path("sqlite", request)
            dsn = f"sqlite://{tmp_path}"
            yield dsn

            if not _keep_test_data():
                _cleanup_local_db_file(tmp_path)
    elif test_backend == TestBackend.S3:
        if base_dsn:
            yield base_dsn
        else:
            bucket = os.environ.get("PGQRS_S3_BUCKET", "pgqrs-test-bucket")
            module_name = request.module.__name__.split(".")[-1]
            test_name = request.node.name
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            pid = os.getpid()
            seq = next(_S3_SEQ)
            key = (
                f"{_sanitize_key_part(module_name)}-"
                f"{_sanitize_key_part(test_name)}-"
                f"{ts}-{pid}-{seq}.sqlite"
            )
            dsn = f"s3://{bucket}/{key}"
            yield dsn

            if not _keep_test_data():
                _cleanup_s3_object(dsn)
                _cleanup_local_s3_cache()
    elif test_backend == TestBackend.TURSO:
        if base_dsn:
            yield base_dsn
        else:
            tmp_path = _build_local_db_path("turso", request)
            dsn = f"turso://{tmp_path}"
            yield dsn

            if not _keep_test_data():
                _cleanup_local_db_file(tmp_path)


@pytest.fixture(scope="function")
def schema(
    test_backend: TestBackend, test_dsn: str, request
) -> Generator[str | None, None, None]:
    """
    Creates a unique schema for the test module (Postgres) or returns None (SQLite).
    """
    if test_backend == TestBackend.POSTGRES:
        # Santize module name
        module_name = request.module.__name__.replace(".", "_")
        unique_suffix = str(uuid.uuid4())[:8]
        schema_name = f"test_{module_name}_{unique_suffix}"

        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(f"CREATE SCHEMA {schema_name}")
            try:
                yield schema_name
            finally:
                try:
                    conn.execute(f"DROP SCHEMA {schema_name} CASCADE")
                except Exception:
                    pass
    else:
        # SQLite doesn't use schemas for isolation (we use separate DB files)
        yield None


def _sanitize_key_part(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def _build_local_db_path(backend: str, request) -> str:
    module_name = request.module.__name__.split(".")[-1]
    test_name = request.node.name
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    pid = os.getpid()
    seq = next(_LOCAL_DB_SEQ)
    filename = (
        f"{_sanitize_key_part(backend)}-"
        f"{_sanitize_key_part(module_name)}-"
        f"{_sanitize_key_part(test_name)}-"
        f"{ts}-{pid}-{seq}.db"
    )
    base = Path(os.getenv("TMPDIR", "/tmp"))
    base.mkdir(parents=True, exist_ok=True)
    path = base / filename
    path.touch(exist_ok=True)
    return str(path)


def _keep_test_data() -> bool:
    raw = os.environ.get("PGQRS_KEEP_TEST_DATA", "").strip().lower()
    return raw not in ("", "0", "false", "no")


def _parse_s3_dsn(dsn: str) -> tuple[str, str] | None:
    if dsn.startswith("s3://"):
        full = dsn[len("s3://") :]
    elif dsn.startswith("s3:"):
        full = dsn[len("s3:") :]
    else:
        return None
    parts = full.split("/", 1)
    if len(parts) != 2:
        return None
    bucket, key = parts[0].strip(), parts[1].strip()
    if not bucket or not key:
        return None
    return bucket, key


def _cleanup_s3_object(dsn: str) -> None:
    parsed = _parse_s3_dsn(dsn)
    if not parsed:
        return
    bucket, key = parsed
    endpoint = os.environ.get("PGQRS_S3_ENDPOINT")
    region = os.environ.get("PGQRS_S3_REGION", "us-east-1")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    kwargs = {"region_name": region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key

    try:
        client = boto3.client("s3", **kwargs)
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        # Best-effort cleanup; tests should fail on functional assertions, not teardown IO.
        pass


def _cleanup_local_s3_cache() -> None:
    base = Path(os.getenv("PGQRS_S3_LOCAL_CACHE_DIR", Path(os.getenv("TMPDIR", "/tmp")) / "pgqrs_s3_cache"))
    if not base.exists():
        return
    for db in base.glob("s3_cache_*.db"):
        try:
            db.unlink(missing_ok=True)
            wal = Path(f"{db}-wal")
            shm = Path(f"{db}-shm")
            wal.unlink(missing_ok=True)
            shm.unlink(missing_ok=True)
            state_dir = db.with_suffix(".s3state")
            if state_dir.exists():
                shutil.rmtree(state_dir, ignore_errors=True)
        except Exception:
            pass


def _cleanup_local_db_file(path: str) -> None:
    db = Path(path)
    try:
        db.unlink(missing_ok=True)
        Path(f"{db}-wal").unlink(missing_ok=True)
        Path(f"{db}-shm").unlink(missing_ok=True)
    except Exception:
        pass
