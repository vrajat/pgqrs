import pytest
import os
import psycopg
import uuid
import tempfile
from enum import Enum
from typing import Generator
from testcontainers.postgres import PostgresContainer


class TestBackend(Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
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
def database_dsn(test_backend: TestBackend) -> Generator[str, None, None]:
    """
    Provides a database DSN appropriate for the selected backend.
    """
    if test_backend == TestBackend.POSTGRES:
        dsn = os.environ.get("PGQRS_TEST_POSTGRES_DSN") or os.environ.get("PGQRS_TEST_DSN")
        if dsn:
            yield dsn
        else:
            with PostgresContainer("postgres:15") as postgres:
                yield postgres.get_connection_url().replace("+psycopg2", "")

    elif test_backend == TestBackend.SQLITE:
        dsn = os.environ.get("PGQRS_TEST_SQLITE_DSN")
        if not dsn:
            pytest.skip("PGQRS_TEST_SQLITE_DSN not set")
        yield dsn

    elif test_backend == TestBackend.TURSO:
        dsn = os.environ.get("PGQRS_TEST_TURSO_DSN")
        if not dsn:
            pytest.skip("PGQRS_TEST_TURSO_DSN not set")
        yield dsn


# Convenience decorators for backend-specific tests
def requires_backend(backend: TestBackend):
    """Skip test unless running on specified backend."""
    return pytest.mark.skipif(
        get_backend() != backend,
        reason=f"Test requires {backend.value} backend"
    )


def skip_on_backend(backend: TestBackend):
    """Skip test when running on specified backend."""
    return pytest.mark.skipif(
        get_backend() == backend,
        reason=f"Test not supported on {backend.value} backend"
    )


# Legacy alias for postgres_dsn compatibility, but now function-scoped to support SQLite isolation
@pytest.fixture(scope="function")
def postgres_dsn(test_backend: TestBackend, database_dsn: str) -> Generator[str, None, None]:
    """
    Provides a per-test DSN.
    - Postgres: Returns the session-shared DSN.
    - SQLite: Creates a new unique database file for isolation.
    """
    if test_backend == TestBackend.POSTGRES:
        yield database_dsn
    elif test_backend == TestBackend.SQLITE:
        # Create a unique temporary file
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name

        dsn = f"sqlite://{tmp_path}"
        yield dsn

        # Cleanup
        try:
            os.remove(tmp_path)
            # Also remove limit/wal files if they exist
            if os.path.exists(f"{tmp_path}-shm"): os.remove(f"{tmp_path}-shm")
            if os.path.exists(f"{tmp_path}-wal"): os.remove(f"{tmp_path}-wal")
        except OSError:
            pass
    elif test_backend == TestBackend.TURSO:
        # For Turso, we might need a similar isolation strategy or dedicated test database
        yield database_dsn

@pytest.fixture(scope="function")
def schema(test_backend: TestBackend, postgres_dsn: str, request) -> Generator[str, None, None]:
    """
    Creates a unique schema for the test module (Postgres) or returns None (SQLite).
    """
    if test_backend == TestBackend.POSTGRES:
        # Santize module name
        module_name = request.module.__name__.replace(".", "_")
        unique_suffix = str(uuid.uuid4())[:8]
        schema_name = f"test_{module_name}_{unique_suffix}"

        with psycopg.connect(postgres_dsn, autocommit=True) as conn:
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
