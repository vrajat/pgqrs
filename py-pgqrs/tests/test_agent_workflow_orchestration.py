"""
Multi-Step Workflow Orchestration Example using py-pgqrs

This test demonstrates a workflow coordination pattern using pgqrs durable workflows
to orchestrate sequential tasks with state persistence and crash recovery.

Key concepts demonstrated:
1. Sequential task execution with state persistence
2. Crash recovery and resumption
3. Error handling with required/optional tasks
4. Workflow progress tracking
"""

import pytest

from pgqrs import Run
from pgqrs.decorators import step, workflow
import pgqrs


# ============================================================================
# Individual Workflow Tasks
# ============================================================================


@step
async def bootstrap_environment(ctx: Run, config: dict) -> dict:
    """
    Task 1: Bootstrap the environment and setup configuration
    """
    # Simulate environment setup
    bootstrapped_config = {
        **config,
        "api_key": "test-api-key",
        "database": "snowflake",
        "service_type": "saas-integration",
    }

    return bootstrapped_config


@step
async def define_boundaries(ctx: Run, config: dict) -> dict:
    """
    Task 2: Define system boundaries and data models
    """
    # Simulate boundary definition
    boundary_spec = {
        "entities": ["users", "tickets", "organizations"],
        "sync_pattern": "incremental",
        "update_frequency": "hourly",
    }

    return {**config, "boundaries": boundary_spec}


@step
async def analyze_interface(ctx: Run, config: dict) -> dict:
    """
    Task 3: Analyze external interface structure
    """
    # Simulate interface analysis
    interface_spec = {
        "base_url": "https://api.example.com/v2",
        "auth_type": "bearer",
        "rate_limit": 100,
        "pagination": "cursor",
    }

    return {**config, "interface": interface_spec}


@step
async def verify_connectivity(ctx: Run, config: dict) -> dict:
    """
    Task 4: Verify system connectivity with generated configuration
    """
    # Simulate connectivity verification
    connectivity_report = {
        "connection_ok": True,
        "latency_ms": 45,
        "auth_validated": True,
    }

    return {**config, "connectivity": connectivity_report}


# ============================================================================
# Main Orchestration Workflow
# ============================================================================


@workflow
async def integration_development_flow(ctx: Run, initial_config: dict) -> dict:
    """
    Main orchestration workflow that coordinates all tasks.

    This workflow demonstrates:
    - Sequential execution of dependent tasks
    - State persistence between tasks
    - Automatic crash recovery (via @workflow and @step decorators)
    """
    # Task 1: Bootstrap
    config = await bootstrap_environment(ctx, initial_config)

    # Task 2: Define Boundaries
    config = await define_boundaries(ctx, config)

    # Task 3: Analyze Interface
    config = await analyze_interface(ctx, config)

    # Task 4: Verify Connectivity
    final_config = await verify_connectivity(ctx, config)

    return final_config


# ============================================================================
# Test Cases
# ============================================================================


@pytest.mark.asyncio
async def test_complete_workflow_execution(test_dsn, schema):
    """
    Test a complete successful execution of the workflow.
    """
    # Setup
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    # Create workflow
    initial_config = {"project_name": "zendesk-integration", "version": "1.0.0"}

    wf_ctx = await admin.create_workflow("integration_flow", initial_config)

    # Execute workflow
    result = await integration_development_flow(wf_ctx, initial_config)

    # Verify results
    assert result["api_key"] == "test-api-key"
    assert result["boundaries"]["entities"] == ["users", "tickets", "organizations"]
    assert result["interface"]["base_url"] == "https://api.example.com/v2"
    assert result["connectivity"]["connection_ok"] is True


@pytest.mark.asyncio
async def test_workflow_crash_recovery(test_dsn, schema):
    """
    Test workflow recovery after a simulated crash.

    This demonstrates:
    1. Partial execution (tasks 1-2 complete)
    2. Simulated crash (process dies before completing all steps)
    3. Resumption with the same workflow context (tasks 1-2 skipped, tasks 3-4 execute)

    Note: This test does NOT use @workflow decorator on the orchestrator function
    due to a product bug (see issue #152). When a workflow decorated with @workflow
    fails, it enters ERROR state and cannot be restarted. This defeats the purpose
    of durable workflows. As a workaround, we call steps directly without the
    @workflow decorator to demonstrate step-level idempotency.

    TODO: Once issue #152 is fixed, update this test to use @workflow decorator
    and test proper workflow-level restart after error/crash.
    """
    # Setup
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    initial_config = {"project_name": "salesforce-integration"}
    wf_ctx = await admin.create_workflow("recovery_test", initial_config)

    # Track which tasks actually execute
    execution_log = []

    # Modified tasks that log execution
    @step
    async def bootstrap_logged(ctx: Run, cfg: dict) -> dict:
        execution_log.append("bootstrap")
        return await bootstrap_environment(ctx, cfg)

    @step
    async def boundaries_logged(ctx: Run, cfg: dict) -> dict:
        execution_log.append("define_boundaries")
        return await define_boundaries(ctx, cfg)

    @step
    async def interface_logged(ctx: Run, cfg: dict) -> dict:
        execution_log.append("analyze_interface")
        return await analyze_interface(ctx, cfg)

    @step
    async def connectivity_logged(ctx: Run, cfg: dict) -> dict:
        execution_log.append("verify_connectivity")
        return await verify_connectivity(ctx, cfg)

    # First run: Execute first two tasks, then simulate process crash
    # (no @workflow decorator - simulates process dying)
    cfg = await bootstrap_logged(wf_ctx, initial_config)
    cfg = await boundaries_logged(wf_ctx, cfg)
    # Process "crashes" here before completing all steps

    assert execution_log == ["bootstrap", "define_boundaries"]

    # Second run: Resume from crash using the same workflow context
    # Previously completed steps should be skipped due to idempotency
    cfg = await bootstrap_logged(wf_ctx, initial_config)
    cfg = await boundaries_logged(wf_ctx, cfg)
    cfg = await interface_logged(wf_ctx, cfg)
    result = await connectivity_logged(wf_ctx, cfg)

    # The execution log contains all 4 steps, but steps 1-2 were only executed once
    # (skipped on the second run due to idempotency)
    assert execution_log == [
        "bootstrap",
        "define_boundaries",
        "analyze_interface",
        "verify_connectivity",
    ]
    assert result["connectivity"]["connection_ok"] is True


@pytest.mark.asyncio
async def test_workflow_with_conditional_steps(test_dsn, schema):
    """
    Test workflow with conditional execution (dynamic DAG).

    This demonstrates how pgqrs supports dynamic workflows where
    the structure is determined at runtime.
    """
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    @step
    async def determine_service_type(ctx: Run, cfg: dict) -> dict:
        """Determine service type and set requirements"""
        service_type = cfg.get("type", "simple")
        return {**cfg, "requires_oauth": service_type == "enterprise"}

    @step
    async def configure_oauth(ctx: Run, cfg: dict) -> dict:
        """Only runs for enterprise services"""
        return {**cfg, "oauth_configured": True}

    @step
    async def configure_basic_auth(ctx: Run, cfg: dict) -> dict:
        """Only runs for simple services"""
        return {**cfg, "basic_auth_configured": True}

    @workflow
    async def conditional_flow(ctx: Run, cfg: dict) -> dict:
        cfg = await determine_service_type(ctx, cfg)

        # Conditional execution based on service type
        if cfg["requires_oauth"]:
            cfg = await configure_oauth(ctx, cfg)
        else:
            cfg = await configure_basic_auth(ctx, cfg)

        return cfg

    # Test 1: Enterprise service (OAuth path)
    wf_ctx_enterprise = await admin.create_workflow(
        "conditional_enterprise", {"type": "enterprise"}
    )
    result_enterprise = await conditional_flow(
        wf_ctx_enterprise, {"type": "enterprise"}
    )
    assert result_enterprise["oauth_configured"] is True
    assert "basic_auth_configured" not in result_enterprise

    # Test 2: Simple service (Basic auth path)
    wf_ctx_simple = await admin.create_workflow(
        "conditional_simple", {"type": "simple"}
    )
    result_simple = await conditional_flow(wf_ctx_simple, {"type": "simple"})
    assert result_simple["basic_auth_configured"] is True
    assert "oauth_configured" not in result_simple


@pytest.mark.asyncio
async def test_workflow_error_handling(test_dsn, schema):
    """
    Test error handling in workflows.

    Demonstrates:
    1. Task failure propagates to the caller as an exception.
    """
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    @step
    async def failing_task(ctx: Run, msg: str) -> str:
        """A task that always fails"""
        raise ValueError(f"Intentional failure: {msg}")

    @workflow
    async def error_workflow(ctx: Run, input_data: dict) -> dict:
        # This task will fail
        await failing_task(ctx, input_data["error_msg"])
        return {"success": True}  # Never reached

    wf_ctx = await admin.create_workflow("error_test", {"error_msg": "test error"})

    # Workflow should raise the error
    with pytest.raises(ValueError, match="Intentional failure: test error"):
        await error_workflow(wf_ctx, {"error_msg": "test error"})
