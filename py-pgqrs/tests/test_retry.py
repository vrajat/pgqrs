"""
Test suite for workflow step retry functionality.

This test demonstrates:
1. Transient error handling with automatic retries
2. Backoff strategies (fixed, exponential, exponential with jitter)
3. Custom retry delays (e.g., Retry-After headers)
4. Retry exhaustion behavior
5. Non-transient errors (immediate failure)
"""

import pytest
from datetime import datetime, timezone, timedelta

from pgqrs import PyWorkflow, BackoffStrategy, StepRetryPolicy
from pgqrs.decorators import step, workflow
import pgqrs


# ============================================================================
# Test Fixtures & Helpers
# ============================================================================


class RetryTracker:
    """Track retry attempts across workflow executions."""

    def __init__(self):
        self.attempts = 0

    def record_attempt(self):
        self.attempts += 1
        return self.attempts


# Global tracker (reset per test)
retry_tracker = RetryTracker()


@pytest.fixture(autouse=True)
def reset_retry_tracker():
    """Reset the retry tracker before each test."""
    global retry_tracker
    retry_tracker = RetryTracker()
    yield


# ============================================================================
# Basic Retry Functionality Tests
# ============================================================================


@step
async def transient_failure_step(
    ctx: PyWorkflow, max_failures: int, current_time: str
) -> str:
    """
    Step that fails transiently N times before succeeding.

    Args:
        max_failures: Number of times to fail before succeeding
        current_time: ISO 8601 timestamp for deterministic testing
    """
    attempt = retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("transient_step", current_time=current_time)
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    if attempt <= max_failures:
        # Fail transiently
        await guard.fail_transient(
            "SIMULATED_TIMEOUT", f"Attempt {attempt} failed (simulated)"
        )
    else:
        # Succeed
        await guard.success(f"Success after {attempt} attempts")

    return f"Success after {attempt} attempts"


@workflow
async def basic_retry_workflow(
    ctx: PyWorkflow, max_failures: int, current_time: str
) -> dict:
    """Workflow that demonstrates basic retry behavior."""
    result = await transient_failure_step(ctx, max_failures, current_time)
    return {"result": result, "attempts": retry_tracker.attempts}


@pytest.mark.asyncio
async def test_basic_transient_retry(test_dsn: str, schema: str | None):
    """Test that transient errors trigger automatic retries."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    # Create workflow that fails twice, then succeeds
    wf = await admin.create_workflow("basic_retry_test", {"max_failures": 2})
    await wf.start()

    # Use controlled time for deterministic testing
    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # First attempt: will fail transiently
    try:
        await basic_retry_workflow(
            wf, max_failures=2, current_time=base_time.isoformat()
        )
    except pgqrs.StepNotReadyError:
        # Expected: step scheduled for retry
        pass

    # Second attempt: advance time past retry_at, will fail again
    retry_time = base_time + timedelta(seconds=2)  # Default backoff starts at 1s
    try:
        await basic_retry_workflow(
            wf, max_failures=2, current_time=retry_time.isoformat()
        )
    except pgqrs.StepNotReadyError:
        # Expected: step scheduled for another retry
        pass

    # Third attempt: advance time past second retry_at, should succeed
    final_time = retry_time + timedelta(seconds=3)  # Exponential backoff: 2s
    result = await basic_retry_workflow(
        wf, max_failures=2, current_time=final_time.isoformat()
    )

    assert result["attempts"] == 3, "Should succeed on 3rd attempt"
    assert "Success after 3 attempts" in result["result"]


# ============================================================================
# Custom Retry Delay Tests
# ============================================================================


@step
async def rate_limited_step(
    ctx: PyWorkflow, retry_after_seconds: float, current_time: str
) -> str:
    """Step that simulates rate limiting with custom retry delay."""
    attempt = retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("rate_limited_step", current_time=current_time)
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    if attempt == 1:
        # First attempt: fail with custom retry delay (simulating Retry-After header)
        await guard.fail_transient(
            "RATE_LIMITED",
            "Too many requests (simulated)",
            retry_after=retry_after_seconds,
        )
    else:
        # Second attempt: succeed
        await guard.success("Success after rate limit")

    return "Success after rate limit"


@workflow
async def rate_limit_workflow(
    ctx: PyWorkflow, retry_after: float, current_time: str
) -> dict:
    """Workflow demonstrating custom retry delays."""
    result = await rate_limited_step(ctx, retry_after, current_time)
    return {"result": result, "attempts": retry_tracker.attempts}


@pytest.mark.asyncio
async def test_custom_retry_delay(test_dsn: str, schema: str | None):
    """Test that custom retry_after delay is respected."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("rate_limit_test", {"retry_after": 60.0})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # First execution: will fail with rate limit (retry after 60s)
    try:
        await rate_limit_workflow(
            wf, retry_after=60.0, current_time=base_time.isoformat()
        )
    except pgqrs.StepNotReadyError as e:
        # Expected: step scheduled for retry
        pass

    # Second execution: advance time past retry_after, should succeed
    retry_time = base_time + timedelta(seconds=61)
    result = await rate_limit_workflow(
        wf, retry_after=60.0, current_time=retry_time.isoformat()
    )

    assert result["attempts"] == 2
    assert "Success after rate limit" in result["result"]


# ============================================================================
# Retry Exhaustion Tests
# ============================================================================


@step
async def always_fails_transient(ctx: PyWorkflow, current_time: str) -> str:
    """Step that always fails transiently (to test retry exhaustion)."""
    attempt = retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("always_fails_step", current_time=current_time)
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    # Always fail transiently
    await guard.fail_transient("PERSISTENT_ERROR", f"Attempt {attempt} failed")

    return "Never succeeds"


@workflow
async def retry_exhaustion_workflow(ctx: PyWorkflow, current_time: str) -> dict:
    """Workflow that exhausts retries."""
    result = await always_fails_transient(ctx, current_time)
    return {"result": result}


@pytest.mark.asyncio
async def test_retry_exhaustion(test_dsn: str, schema: str | None):
    """Test that retries exhaust after max_attempts."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("retry_exhaustion_test", {})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # Default retry policy: max_attempts = 3
    # Attempt 1: initial failure
    try:
        await retry_exhaustion_workflow(wf, current_time=base_time.isoformat())
    except pgqrs.StepNotReadyError:
        pass

    # Attempt 2: retry after backoff
    retry_time1 = base_time + timedelta(seconds=2)
    try:
        await retry_exhaustion_workflow(wf, current_time=retry_time1.isoformat())
    except pgqrs.StepNotReadyError:
        pass

    # Attempt 3: second retry after backoff
    retry_time2 = retry_time1 + timedelta(seconds=3)
    try:
        await retry_exhaustion_workflow(wf, current_time=retry_time2.isoformat())
    except pgqrs.StepNotReadyError:
        pass

    # Attempt 4: retries exhausted
    final_time = retry_time2 + timedelta(seconds=5)
    retries_exhausted = False
    try:
        await retry_exhaustion_workflow(wf, current_time=final_time.isoformat())
    except pgqrs.RetriesExhaustedError:
        retries_exhausted = True

    assert retries_exhausted, "Should exhaust retries after max_attempts"
    assert retry_tracker.attempts >= 3, "Should have attempted at least 3 times"


# ============================================================================
# Non-Transient Error Tests
# ============================================================================


@step
async def non_transient_failure_step(ctx: PyWorkflow, current_time: str) -> str:
    """Step that fails with a non-transient error."""
    retry_tracker.record_attempt()

    step_res = await ctx.acquire_step("non_transient_step", current_time=current_time)
    if step_res.status == "SKIPPED":
        return step_res.value

    guard = step_res.guard

    # Fail with non-transient error (no retry)
    await guard.fail("Permanent validation error")

    return "Never succeeds"


@workflow
async def non_transient_workflow(ctx: PyWorkflow, current_time: str) -> dict:
    """Workflow with non-transient failure."""
    result = await non_transient_failure_step(ctx, current_time)
    return {"result": result}


@pytest.mark.asyncio
async def test_non_transient_error_no_retry(test_dsn: str, schema: str | None):
    """Test that non-transient errors fail immediately without retry."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("non_transient_test", {})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # Execute workflow - should fail immediately without retry
    try:
        await non_transient_workflow(wf, current_time=base_time.isoformat())
    except pgqrs.RetriesExhaustedError:
        # Non-transient errors result in RetriesExhausted with attempts=0
        pass

    # Second execution: should return cached error
    step_res = await wf.acquire_step(
        "non_transient_step", current_time=base_time.isoformat()
    )
    # Step should be in ERROR state, not retrying
    # The exact status depends on implementation - could be SKIPPED with error or throw

    # Should NOT have retried
    assert retry_tracker.attempts == 1, "Non-transient error should not retry"


# ============================================================================
# Backoff Strategy Type Construction Tests
# ============================================================================


def test_backoff_strategy_construction():
    """Test that BackoffStrategy types can be constructed."""

    # Fixed backoff
    fixed = BackoffStrategy.fixed(5)
    assert fixed is not None

    # Exponential backoff
    exponential = BackoffStrategy.exponential(2, 60)
    assert exponential is not None

    # Exponential with jitter
    jitter = BackoffStrategy.exponential_with_jitter(1, 120)
    assert jitter is not None


def test_retry_policy_construction():
    """Test that StepRetryPolicy can be constructed."""

    # Default policy
    default_policy = StepRetryPolicy()
    assert default_policy.max_attempts == 3

    # Custom policy
    custom_policy = StepRetryPolicy(
        max_attempts=5, backoff=BackoffStrategy.exponential_with_jitter(2, 60)
    )
    assert custom_policy.max_attempts == 5


# ============================================================================
# Validation Tests
# ============================================================================


@pytest.mark.asyncio
async def test_negative_retry_after_rejected(test_dsn: str, schema: str | None):
    """Test that negative retry_after values are rejected."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("validation_test", {})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    step_res = await wf.acquire_step(
        "validation_step", current_time=base_time.isoformat()
    )
    guard = step_res.guard

    # Attempt to fail with negative retry_after
    with pytest.raises(ValueError, match="retry_after must be non-negative"):
        await guard.fail_transient("TEST", "Test error", retry_after=-5.0)


@pytest.mark.asyncio
async def test_infinity_retry_after_rejected(test_dsn: str, schema: str | None):
    """Test that Infinity retry_after is rejected."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("infinity_test", {})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    step_res = await wf.acquire_step(
        "infinity_step", current_time=base_time.isoformat()
    )
    guard = step_res.guard

    # Attempt to fail with Infinity retry_after
    with pytest.raises(ValueError, match="retry_after must be finite"):
        await guard.fail_transient("TEST", "Test error", retry_after=float("inf"))


@pytest.mark.asyncio
async def test_nan_retry_after_rejected(test_dsn: str, schema: str | None):
    """Test that NaN retry_after is rejected."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("nan_test", {})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    step_res = await wf.acquire_step("nan_step", current_time=base_time.isoformat())
    guard = step_res.guard

    # Attempt to fail with NaN retry_after
    with pytest.raises(ValueError, match="retry_after must be finite"):
        await guard.fail_transient("TEST", "Test error", retry_after=float("nan"))


@pytest.mark.asyncio
async def test_fractional_retry_after_accepted(test_dsn: str, schema: str | None):
    """Test that fractional retry_after values (milliseconds) are accepted."""
    store = await pgqrs.connect(test_dsn)
    admin = pgqrs.admin(store)

    await admin.install()

    wf = await admin.create_workflow("fractional_test", {})
    await wf.start()

    base_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    step_res = await wf.acquire_step(
        "fractional_step", current_time=base_time.isoformat()
    )
    guard = step_res.guard

    # Fractional seconds (500ms) should be accepted
    try:
        await guard.fail_transient("TEST", "Test error", retry_after=0.5)
    except pgqrs.StepNotReadyError:
        # Expected: step scheduled for retry
        pass
    except ValueError:
        pytest.fail("Fractional retry_after should be accepted")
