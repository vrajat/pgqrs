"""Integration tests for workflow retry functionality.

These tests mirror the Rust workflow_retry_integration_tests.rs to verify that:
1. Transient step failures schedule retries
2. Steps become ready after retry_at timestamp passes
3. Steps eventually succeed after retries
4. Custom retry delays work
5. Retries are exhausted after max attempts
"""

import pytest
import pgqrs
from datetime import datetime, timedelta, timezone


async def setup_test(test_dsn, schema):
    """Helper to setup store and admin."""
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()
    await admin.verify()
    return store, admin


@pytest.mark.asyncio
async def test_step_returns_not_ready_on_transient_error(test_dsn, schema):
    """Test that a step with transient error returns StepNotReady."""
    store, admin = await setup_test(test_dsn, schema)

    # Create workflow
    wf = await admin.create_workflow("not_ready_test", {"msg": "test"})
    await wf.start()

    # Step 1: Acquire and fail with transient error
    step_id = "transient_step"
    result = await wf.acquire_step(step_id)

    assert result.status == "EXECUTE", "Step should execute first time"
    guard = result.guard

    # Fail with transient error
    await guard.fail_transient("NETWORK_TIMEOUT", "Connection timeout")

    # Try to acquire again - should raise StepNotReadyError
    with pytest.raises(pgqrs.StepNotReadyError) as exc_info:
        await wf.acquire_step(step_id)

    # Just verify we got the error - the retry_count isn't exposed in Python
    assert exc_info.value is not None


@pytest.mark.asyncio
async def test_step_ready_after_retry_at(test_dsn, schema):
    """Test that step becomes ready after retry_at timestamp passes."""
    store, admin = await setup_test(test_dsn, schema)

    wf = await admin.create_workflow("ready_after_retry", {"msg": "test"})
    await wf.start()

    step_id = "delayed_step"

    # Fail with transient error
    result = await wf.acquire_step(step_id)
    assert result.status == "EXECUTE"
    await result.guard.fail_transient("TIMEOUT", "Initial failure")

    # Get retry_at
    with pytest.raises(pgqrs.StepNotReadyError):
        await wf.acquire_step(step_id)

    # Simulate time advancing by using current_time
    base_time = datetime.now(timezone.utc)
    future_time = base_time + timedelta(seconds=2)

    # Should be able to acquire now
    result = await wf.acquire_step(step_id, current_time=future_time.isoformat())
    assert result.status == "EXECUTE", "Step should execute after retry_at"

    # Succeed
    await result.guard.success({"msg": "success_after_retry"})

    # Verify cached
    result = await wf.acquire_step(step_id)
    assert result.status == "SKIPPED"
    assert result.value["msg"] == "success_after_retry"


@pytest.mark.asyncio
async def test_step_exhausts_retries(test_dsn, schema):
    """Test that retries are exhausted after max attempts."""
    store, admin = await setup_test(test_dsn, schema)

    wf = await admin.create_workflow("exhaust_retries", {"msg": "test"})
    await wf.start()

    step_id = "failing_step"
    base_time = datetime.now(timezone.utc)

    # Default policy: max_attempts=3 means 3 retries after initial (4 total executions)
    # Fail 4 times, then the 5th acquire should exhaust

    # Attempt 1 (initial): Fail
    result = await wf.acquire_step(step_id, current_time=base_time.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 1")

    # Attempt 2 (retry 1): Fail (advance time past first retry delay which is 1s with jitter)
    time2 = base_time + timedelta(seconds=2)
    result = await wf.acquire_step(step_id, current_time=time2.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 2")

    # Attempt 3 (retry 2): Fail (advance time past second retry delay which is ~2s with jitter)
    time3 = time2 + timedelta(seconds=4)
    result = await wf.acquire_step(step_id, current_time=time3.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 3")

    # Attempt 4 (retry 3): Fail (advance time past third retry delay which is ~4s with jitter)
    time4 = time3 + timedelta(seconds=8)
    result = await wf.acquire_step(step_id, current_time=time4.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 4")

    # Now we've failed 4 times (retry_count=4, exhausted max_attempts=3)
    # The 5th acquire should raise RetriesExhaustedError
    time5 = time4 + timedelta(seconds=16)
    with pytest.raises(pgqrs.RetriesExhaustedError):
        await wf.acquire_step(step_id, current_time=time5.isoformat())


@pytest.mark.asyncio
async def test_non_transient_error_no_retry(test_dsn, schema):
    """Test that non-transient errors don't schedule retries."""
    store, admin = await setup_test(test_dsn, schema)

    wf = await admin.create_workflow("non_transient_test", {"msg": "test"})
    await wf.start()

    step_id = "non_transient_step"

    # Fail with non-transient error
    result = await wf.acquire_step(step_id)
    await result.guard.fail("Permanent error")

    # Try again - should get RetriesExhaustedError immediately
    with pytest.raises(pgqrs.RetriesExhaustedError):
        await wf.acquire_step(step_id)


@pytest.mark.asyncio
async def test_custom_retry_after_delay(test_dsn, schema):
    """Test that custom retry_after delay is respected."""
    store, admin = await setup_test(test_dsn, schema)

    wf = await admin.create_workflow("custom_delay_test", {"msg": "test"})
    await wf.start()

    step_id = "custom_delay_step"

    # Fail with custom retry_after
    result = await wf.acquire_step(step_id)
    await result.guard.fail_transient(
        "RATE_LIMITED", "Too many requests", retry_after=10.0
    )

    # Try immediately - should not be ready
    with pytest.raises(pgqrs.StepNotReadyError):
        await wf.acquire_step(step_id)

    # Try with time advanced by only 5 seconds - still not ready
    base_time = datetime.now(timezone.utc)
    time1 = base_time + timedelta(seconds=5)
    with pytest.raises(pgqrs.StepNotReadyError):
        await wf.acquire_step(step_id, current_time=time1.isoformat())

    # Try with time advanced by 11 seconds - should be ready
    time2 = base_time + timedelta(seconds=11)
    result = await wf.acquire_step(step_id, current_time=time2.isoformat())
    assert result.status == "EXECUTE", "Step should be ready after custom delay"

    await result.guard.success({"msg": "success"})


@pytest.mark.asyncio
async def test_retry_count_persisted(test_dsn, schema):
    """Test that retry_count is correctly persisted and incremented."""
    store, admin = await setup_test(test_dsn, schema)

    wf = await admin.create_workflow("retry_count_test", {"msg": "test"})
    await wf.start()

    step_id = "counted_step"
    base_time = datetime.now(timezone.utc)

    # Attempt 1: Fail at base_time
    result = await wf.acquire_step(step_id, current_time=base_time.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 1")

    # Try immediately - should get StepNotReady (retry scheduled for ~base_time+1s)
    with pytest.raises(pgqrs.StepNotReadyError):
        await wf.acquire_step(step_id, current_time=base_time.isoformat())

    # Attempt 2: Advance time past retry delay
    time2 = base_time + timedelta(seconds=2)
    result = await wf.acquire_step(step_id, current_time=time2.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 2")

    # Try at same time - should get StepNotReady (retry scheduled for ~time2+2s with backoff)
    with pytest.raises(pgqrs.StepNotReadyError):
        await wf.acquire_step(step_id, current_time=time2.isoformat())

    # Attempt 3: Advance time past retry delay
    time3 = time2 + timedelta(seconds=5)
    result = await wf.acquire_step(step_id, current_time=time3.isoformat())
    await result.guard.fail_transient("ERROR", "Attempt 3")

    # Try at same time - should get StepNotReady (retry scheduled for ~time3+4s with backoff)
    with pytest.raises(pgqrs.StepNotReadyError):
        await wf.acquire_step(step_id, current_time=time3.isoformat())
