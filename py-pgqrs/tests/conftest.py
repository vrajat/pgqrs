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
        path = os.environ.get("PGQRS_TEST_SQLITE_PATH")
        if path:
            yield f"sqlite://{path}"
        else:
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
                yield f"sqlite://{f.name}"

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


# Legacy alias for postgres_dsn compatibility during migration
@pytest.fixture(scope="session")
def postgres_dsn(database_dsn: str) -> str:
    """Legacy alias for database_dsn."""
    return database_dsn

@pytest.fixture(scope="function")
def schema(postgres_dsn, request):
    """
    Creates a unique schema for the test module and tears it down after.
    """
    # Use test module name + hash or similar to ensure uniqueness but readability
    # Santize module name
    module_name = request.module.__name__.replace(".", "_")
    # Add a short unique suffix to avoid collisions if tests run in parallel or rapid sequence
    unique_suffix = str(uuid.uuid4())[:8]
    schema_name = f"test_{module_name}_{unique_suffix}"

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {schema_name}")
        yield schema_name
        conn.execute(f"DROP SCHEMA {schema_name} CASCADE")
