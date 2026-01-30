# Bug Report: Workflow Cannot Restart After Error or Crash

## Summary

Workflows cannot be restarted after entering ERROR state, which defeats the purpose of durable workflows. The `start()` method incorrectly blocks workflows in ERROR or RUNNING state from continuing, preventing crash recovery and error retry scenarios.

## Affected Code

**Files:**
- `crates/pgqrs/src/store/turso/workflow/handle.rs`
- `crates/pgqrs/src/store/sqlite/workflow/handle.rs`
- `crates/pgqrs/src/store/postgres/workflow/handle.rs`

**Method:** `Workflow::start()`

## Root Cause

The `start()` method has two issues:

### Issue 1: UPDATE Only Allows PENDING → RUNNING

```sql
UPDATE pgqrs_workflows
SET status = 'RUNNING', updated_at = datetime('now')
WHERE workflow_id = ? AND status = 'PENDING'
RETURNING status, error
```

This UPDATE only succeeds if `status = 'PENDING'`. If the workflow is already RUNNING (e.g., after a crash) or in ERROR state (after a failure), the UPDATE returns no rows.

### Issue 2: Blocks ERROR State

When the UPDATE fails (returns no rows), the code checks the current status:

```rust
if result.is_none() {
    let status_str: Option<String> = crate::store::turso::query_scalar(
        "SELECT status FROM pgqrs_workflows WHERE workflow_id = ?",
    )
    .bind(self.id)
    .fetch_optional(&self.db)
    .await?;

    if let Some(s) = status_str {
        if let Ok(WorkflowStatus::Error) = WorkflowStatus::from_str(&s) {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!("Workflow {} is in terminal ERROR state", self.id),
            });
        }
    }
}
```

This explicitly blocks workflows in ERROR state from restarting.

## Impact

**Severity:** Critical - Breaks core workflow functionality

**Affected Use Cases:**

1. **Crash Recovery**: If a workflow process crashes while RUNNING, restarting it will fail because the UPDATE won't match (status is already RUNNING, not PENDING).

2. **Error Retry**: If a workflow hits an error (e.g., transient network failure, temporary resource unavailability), it cannot be retried because ERROR state is blocked.

3. **Manual Recovery**: Users cannot manually restart failed workflows for debugging or recovery.

## Current Broken Behavior

```python
# Create and start workflow
wf_ctx = await admin.create_workflow("my_workflow", config)

@workflow
async def my_workflow(ctx: PyWorkflow, cfg: dict) -> dict:
    await step_1(ctx, cfg)
    await step_2(ctx, cfg)
    # Process crashes here or hits an error
    await step_3(ctx, cfg)  # Never reached
    return result

# First run - fails/crashes
try:
    await my_workflow(wf_ctx, config)
except Exception:
    pass  # Process crashed or error occurred

# Try to restart - FAILS!
await my_workflow(wf_ctx, config)  # ERROR: "Workflow X is in terminal ERROR state"
```

## Expected Behavior

Workflows should be restartable in the following scenarios:

1. **PENDING → RUNNING**: First start (✅ currently works)
2. **RUNNING → RUNNING**: Idempotent restart after crash (❌ currently fails)
3. **ERROR → RUNNING**: Retry after error (❌ currently blocked)

The only state that should block restart is:
- **SUCCESS → anything**: Workflow completed successfully, shouldn't restart

## Reproduction

```python
import pytest
import pgqrs
from pgqrs import PyWorkflow
from pgqrs.decorators import step, workflow

@pytest.mark.asyncio
async def test_workflow_error_restart(test_dsn, schema):
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    wf_ctx = await admin.create_workflow("error_restart_test", {})
    
    run_count = [0]
    
    @step
    async def step_1(ctx: PyWorkflow, cfg: dict) -> dict:
        return {"step": 1}
    
    @workflow
    async def test_workflow(ctx: PyWorkflow, cfg: dict) -> dict:
        result = await step_1(ctx, cfg)
        
        # Fail on first run, succeed on second
        if run_count[0] == 0:
            run_count[0] += 1
            raise RuntimeError("Simulated transient error")
        
        return result
    
    # First run - fails
    try:
        await test_workflow(wf_ctx, {})
        assert False, "Expected error"
    except RuntimeError:
        pass
    
    # Second run - should succeed but currently fails with:
    # "Workflow X is in terminal ERROR state"
    result = await test_workflow(wf_ctx, {})  # ❌ FAILS
    assert result["step"] == 1
```

## Recommended Fix

### Short-term: Allow ERROR and RUNNING to Restart

**Change the UPDATE to allow RUNNING and ERROR states:**

```sql
UPDATE pgqrs_workflows
SET status = 'RUNNING', updated_at = datetime('now')
WHERE workflow_id = ? 
  AND status IN ('PENDING', 'RUNNING', 'ERROR')
RETURNING status, error
```

**Remove the ERROR state check:**

```rust
if result.is_none() {
    // Only check if workflow is in SUCCESS (completed) state
    let status_str: Option<String> = crate::store::turso::query_scalar(
        "SELECT status FROM pgqrs_workflows WHERE workflow_id = ?",
    )
    .bind(self.id)
    .fetch_optional(&self.db)
    .await?;

    if let Some(s) = status_str {
        if let Ok(WorkflowStatus::Success) = WorkflowStatus::from_str(&s) {
            return Err(crate::error::Error::ValidationFailed {
                reason: format!("Workflow {} has already completed successfully", self.id),
            });
        }
    }
}
```

### Long-term: Consider Workflow Reset API

Provide explicit API for workflow management:

```rust
trait Workflow {
    async fn start(&mut self) -> Result<()>;
    async fn reset(&mut self) -> Result<()>;  // Clear ERROR/SUCCESS state, reset to PENDING
    async fn get_status(&self) -> Result<WorkflowStatus>;
}
```

## Workarounds

Currently, the only workaround is to **not use the `@workflow` decorator** for the orchestrator function. This means:

1. Workflow never enters SUCCESS or ERROR state at the workflow level
2. Only individual steps are tracked
3. Loses workflow-level completion tracking

```python
# Workaround: Don't use @workflow decorator
async def my_workflow_orchestrator(ctx: PyWorkflow, cfg: dict) -> dict:
    # Just call steps directly - no workflow-level state
    await step_1(ctx, cfg)
    await step_2(ctx, cfg)
    await step_3(ctx, cfg)
    return result
```

This is how the current tests work, but it defeats the purpose of having workflow-level state management.

## Related Code

All three backends have the same issue:

1. **Turso**: `crates/pgqrs/src/store/turso/workflow/handle.rs`
2. **SQLite**: `crates/pgqrs/src/store/sqlite/workflow/handle.rs`
3. **PostgreSQL**: `crates/pgqrs/src/store/postgres/workflow/handle.rs`

## References

- Existing test that works around this: `py-pgqrs/tests/test_pgqrs.py::test_workflow_crash_recovery`
- Failing scenario documented in: `py-pgqrs/tests/test_agent_workflow_orchestration.py::test_workflow_crash_recovery`
