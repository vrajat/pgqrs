from pathlib import Path
import os

import boto3
import pytest

import pgqrs
from .conftest import TestBackend, requires_backend


def local_s3_config(dsn: str) -> pgqrs.Config:
    config = pgqrs.Config(dsn, schema="s3_python")
    config.s3_mode = pgqrs.DurabilityMode.Local
    return config


@requires_backend(TestBackend.S3)
def test_config_s3_mode_uses_durability_mode_enum():
    config = pgqrs.Config("s3://pgqrs-test-bucket/config-mode", schema="s3_python")

    assert config.s3_mode == pgqrs.DurabilityMode.Durable
    assert type(config.s3_mode).__name__ == "DurabilityMode"

    config.s3_mode = pgqrs.DurabilityMode.Local

    assert config.s3_mode == pgqrs.DurabilityMode.Local


@requires_backend(TestBackend.S3)
def test_config_s3_cache_prefix_rejects_empty_value():
    config = pgqrs.Config("s3://pgqrs-test-bucket/config-prefix", schema="s3_python")

    with pytest.raises(pgqrs.ConfigError, match="cache prefix cannot be empty"):
        config.s3_cache_prefix = "   "


def parse_s3_dsn(dsn: str) -> tuple[str, str]:
    assert dsn.startswith("s3://")
    bucket, key = dsn[len("s3://") :].split("/", 1)
    return bucket, key


def s3_client():
    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("s3", **kwargs)


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_as_s3_rejects_non_s3_store(tmp_path):
    sqlite_path = Path(tmp_path) / "generic-store.db"
    sqlite_path.touch()

    store = await pgqrs.connect(f"sqlite://{sqlite_path}")

    with pytest.raises(TypeError, match="expected 's3'"):
        pgqrs.as_s3(store)


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_snapshot_rejects_dirty_local_state(test_dsn):
    store = await pgqrs.connect_with(local_s3_config(test_dsn))
    admin = pgqrs.admin(store)
    await admin.install()
    await store.queue("dirty")

    with pytest.raises(pgqrs.PgqrsError, match="unsynced writes"):
        await pgqrs.as_s3(store).snapshot()


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_sync_and_snapshot_restore_remote_state(test_dsn):
    config = local_s3_config(test_dsn)

    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    queue_name = "seeded"
    await store.queue(queue_name)
    producer = await store.producer(queue_name)
    await pgqrs.enqueue(producer, {"job": "seeded"})
    await pgqrs.as_s3(store).sync()

    restored = await pgqrs.connect_with(config)
    await pgqrs.as_s3(restored).snapshot()

    messages = await restored.get_messages()
    assert await messages.count() == 1


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_sync_publishes_missing_remote_state(test_dsn):
    bucket, key = parse_s3_dsn(test_dsn)
    client = s3_client()
    store = await pgqrs.connect_with(local_s3_config(test_dsn))
    admin = pgqrs.admin(store)
    await admin.install()

    await pgqrs.as_s3(store).sync()

    head = client.head_object(Bucket=bucket, Key=key)
    assert "ETag" in head


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_as_s3_returns_handle_for_s3_store(test_dsn):
    store = await pgqrs.connect_with(local_s3_config(test_dsn))
    handle = pgqrs.as_s3(store)

    assert handle is not None
    assert type(handle).__name__ == "S3StoreHandle"


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_state_reports_in_sync_after_sync(test_dsn):
    store = await pgqrs.connect_with(local_s3_config(test_dsn))
    admin = pgqrs.admin(store)
    await admin.install()
    await pgqrs.as_s3(store).sync()

    state = await pgqrs.as_s3(store).state()
    assert type(state).__name__ == "SyncState"
    assert state == pgqrs.SyncState.InSync


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_sync_is_idempotent(test_dsn):
    bucket, key = parse_s3_dsn(test_dsn)
    client = s3_client()
    store = await pgqrs.connect_with(local_s3_config(test_dsn))
    admin = pgqrs.admin(store)
    await admin.install()

    await pgqrs.as_s3(store).sync()
    before = client.head_object(Bucket=bucket, Key=key)["ETag"]

    await pgqrs.as_s3(store).sync()
    await pgqrs.as_s3(store).sync()
    after = client.head_object(Bucket=bucket, Key=key)["ETag"]

    assert before == after


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_sync_recreates_missing_remote_state(test_dsn):
    bucket, key = parse_s3_dsn(test_dsn)
    client = s3_client()
    store = await pgqrs.connect_with(local_s3_config(test_dsn))
    admin = pgqrs.admin(store)
    await admin.install()

    await pgqrs.as_s3(store).sync()
    client.delete_object(Bucket=bucket, Key=key)

    await store.queue("recreated")
    await pgqrs.as_s3(store).sync()

    head = client.head_object(Bucket=bucket, Key=key)
    assert "ETag" in head


@requires_backend(TestBackend.S3)
@pytest.mark.asyncio
async def test_s3_handle_sync_rejects_fresh_replacement_writer(test_dsn):
    config = local_s3_config(test_dsn)

    seed = await pgqrs.connect_with(config)
    admin = pgqrs.admin(seed)
    await admin.install()
    await seed.queue("seeded")
    await pgqrs.as_s3(seed).sync()

    replacement = await pgqrs.connect_with(config)
    replacement_admin = pgqrs.admin(replacement)
    await replacement_admin.install()

    with pytest.raises(pgqrs.PgqrsError) as exc_info:
        await pgqrs.as_s3(replacement).sync()

    assert "conflict" in str(exc_info.value).lower()
