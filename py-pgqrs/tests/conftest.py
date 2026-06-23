import os
import uuid
from enum import Enum
from typing import Generator

import pgqrs
import psycopg
import pytest
from testcontainers.postgres import PostgresContainer


class TestBackend(Enum):
    POSTGRES = "postgres"


@pytest.fixture(scope="session")
def test_backend() -> str:
    return "postgres"


@pytest.fixture(scope="session")
def base_dsn() -> Generator[str, None, None]:
    dsn = os.environ.get("PGQRS_TEST_POSTGRES_DSN") or os.environ.get("PGQRS_TEST_DSN")
    if dsn:
        yield dsn
        return

    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("+psycopg2", "")


@pytest.fixture(scope="function")
def schema(base_dsn: str, request) -> Generator[str, None, None]:
    module_name = request.module.__name__.replace(".", "_")
    unique_suffix = str(uuid.uuid4())[:8]
    schema_name = f"test_{module_name}_{unique_suffix}"

    with psycopg.connect(base_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema_name}"')
        try:
            yield schema_name
        finally:
            try:
                conn.execute(f'DROP SCHEMA "{schema_name}" CASCADE')
            except Exception:
                pass


@pytest.fixture(scope="function")
def test_dsn(base_dsn: str) -> str:
    return base_dsn


@pytest.fixture(scope="function")
async def store(test_dsn: str, schema: str):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    await store.bootstrap()
    return store


def requires_backend(_backend: str):
    return pytest.mark.skipif(False, reason="Postgres-only test suite")


def skip_on_backend(_backend: str):
    return pytest.mark.skipif(False, reason="Postgres-only test suite")
