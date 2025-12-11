import pytest
import os
import psycopg
import uuid
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def postgres_dsn():
    """
    Provides a PostgreSQL DSN.
    Uses PGQRS_TEST_DSN if available, otherwise starts a testcontainer.
    """
    dsn = os.environ.get("PGQRS_TEST_DSN")
    if dsn:
        yield dsn
    else:
        with PostgresContainer("postgres:15") as postgres:
            yield postgres.get_connection_url().replace("+psycopg2", "")

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
