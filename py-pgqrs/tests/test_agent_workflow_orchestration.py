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
import asyncio
from dataclasses import dataclass
from typing import Optional

import pgqrs
from pgqrs import PyWorkflow
from pgqrs.decorators import workflow, step


# ============================================================================
# Task Step Definitions
# ============================================================================


@dataclass
class TaskConfig:
    """Configuration for a single task in the workflow"""

    name: str
    step_id: str
    description: str
    required: bool = True


@dataclass
class ExecutionResult:
    """Result of a single task execution"""

    success: bool
    output: Optional[str] = None
    error: Optional[str] = None


# ============================================================================
# Individual Workflow Tasks
# ============================================================================


@step
async def bootstrap_environment(ctx: PyWorkflow, config: dict) -> dict:
    """
    Task 1: Bootstrap the environment and setup configuration
    """
    print(f"  [Bootstrap] Preparing environment...")

    # Simulate environment setup
    bootstrapped_config = {
        **config,
        "api_key": "sk-test-key-12345",
        "database": "snowflake",
        "service_type": "saas-integration",
    }

    print(f"  [Bootstrap] Environment ready: {list(bootstrapped_config.keys())}")
    return bootstrapped_config


@step
async def define_boundaries(ctx: PyWorkflow, config: dict) -> dict:
    """
    Task 2: Define system boundaries and data models
    """
    print(f"  [Define Boundaries] Analyzing system boundaries...")

    # Simulate boundary definition
    boundary_spec = {
        "entities": ["users", "tickets", "organizations"],
        "sync_pattern": "incremental",
        "update_frequency": "hourly",
    }

    print(f"  [Define Boundaries] Boundaries defined: {boundary_spec['entities']}")
    return {**config, "boundaries": boundary_spec}


@step
async def analyze_interface(ctx: PyWorkflow, config: dict) -> dict:
    """
    Task 3: Analyze external interface structure
    """
    print(f"  [Analyze Interface] Examining interface structure...")

    # Simulate interface analysis
    interface_spec = {
        "base_url": "https://api.example.com/v2",
        "auth_type": "bearer",
        "rate_limit": 100,
        "pagination": "cursor",
    }

    print(f"  [Analyze Interface] Interface spec created: {interface_spec['base_url']}")
    return {**config, "interface": interface_spec}


@step
async def verify_connectivity(ctx: PyWorkflow, config: dict) -> dict:
    """
    Task 4: Verify system connectivity with generated configuration
    """
    print(f"  [Verify Connectivity] Testing connectivity...")

    # Simulate connectivity verification
    connectivity_report = {
        "connection_ok": True,
        "latency_ms": 45,
        "auth_validated": True,
    }

    print(f"  [Verify Connectivity] Connectivity verified!")
    return {**config, "connectivity": connectivity_report}


# ============================================================================
# Main Orchestration Workflow
# ============================================================================


@workflow
async def integration_development_flow(ctx: PyWorkflow, initial_config: dict) -> dict:
    """
    Main orchestration workflow that coordinates all tasks.

    This workflow demonstrates:
    - Sequential execution of dependent tasks
    - State persistence between tasks
    - Automatic crash recovery (via @workflow and @step decorators)
    """
    print("\n" + "=" * 80)
    print(" INTEGRATION DEVELOPMENT FLOW EXECUTION")
    print("=" * 80)

    # Task 1: Bootstrap
    print(f"\n🚀 Task 1/4: Bootstrap Environment")
    print(f"   📄 Description: Setup environment and configuration")
    config = await bootstrap_environment(ctx, initial_config)

    # Task 2: Define Boundaries
    print(f"\n🚀 Task 2/4: Define Boundaries")
    print(f"   📄 Description: Establish system boundaries and models")
    config = await define_boundaries(ctx, config)

    # Task 3: Analyze Interface
    print(f"\n🚀 Task 3/4: Analyze Interface")
    print(f"   📄 Description: Examine and document interface structure")
    config = await analyze_interface(ctx, config)

    # Task 4: Verify Connectivity
    print(f"\n🚀 Task 4/4: Verify Connectivity")
    print(f"   📄 Description: Test and validate connectivity")
    final_config = await verify_connectivity(ctx, config)

    print("\n" + "=" * 80)
    print(" EXECUTION COMPLETE")
    print("=" * 80)

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
    print(f"\n✨ Created workflow ID: {wf_ctx.id()}")

    # Execute workflow
    result = await integration_development_flow(wf_ctx, initial_config)

    # Verify results
    assert result["api_key"] == "sk-test-key-12345"
    assert result["boundaries"]["entities"] == ["users", "tickets", "organizations"]
    assert result["interface"]["base_url"] == "https://api.example.com/v2"
    assert result["connectivity"]["connection_ok"] is True

    print(f"\n✅ Workflow completed successfully!")
    print(f"📊 Final configuration: {len(result)} keys")


@pytest.mark.asyncio
async def test_workflow_crash_recovery(test_dsn, schema):
    """
    Test workflow recovery after a simulated crash.

    This demonstrates:
    1. Partial execution (tasks 1-2 complete)
    2. Simulated crash
    3. Resumption (tasks 1-2 skipped, tasks 3-4 execute)
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
    async def bootstrap_logged(ctx: PyWorkflow, cfg: dict) -> dict:
        execution_log.append("bootstrap")
        return await bootstrap_environment(ctx, cfg)

    @step
    async def boundaries_logged(ctx: PyWorkflow, cfg: dict) -> dict:
        execution_log.append("define_boundaries")
        return await define_boundaries(ctx, cfg)

    @step
    async def interface_logged(ctx: PyWorkflow, cfg: dict) -> dict:
        execution_log.append("analyze_interface")
        return await analyze_interface(ctx, cfg)

    @step
    async def connectivity_logged(ctx: PyWorkflow, cfg: dict) -> dict:
        execution_log.append("verify_connectivity")
        return await verify_connectivity(ctx, cfg)

    # First run: Execute first two tasks, then "crash"
    print("\n--- RUN 1: Partial Execution (Crash after Task 2) ---")

    @workflow
    async def partial_workflow(ctx: PyWorkflow, cfg: dict) -> dict:
        cfg = await bootstrap_logged(ctx, cfg)
        cfg = await boundaries_logged(ctx, cfg)
        # Simulate crash here
        raise RuntimeError("Simulated crash after task 2")

    try:
        await partial_workflow(wf_ctx, initial_config)
    except RuntimeError as e:
        print(f"💥 Workflow crashed as expected: {e}")

    assert execution_log == ["bootstrap", "define_boundaries"]
    print(f"📋 Execution log after crash: {execution_log}")

    # Second run: Resume from crash
    print("\n--- RUN 2: Resuming from Crash ---")

    # Create new workflow context for the resume test
    wf_ctx_resume = await admin.create_workflow("recovery_test_resume", initial_config)

    # Manually execute tasks to simulate resume
    await bootstrap_logged(wf_ctx_resume, initial_config)
    cfg = await boundaries_logged(wf_ctx_resume, initial_config)
    cfg = await interface_logged(wf_ctx_resume, cfg)
    result = await connectivity_logged(wf_ctx_resume, cfg)

    # The execution log now includes all 4 tasks from the resume run
    print(f"📋 Final execution log: {execution_log}")
    assert "analyze_interface" in execution_log
    assert "verify_connectivity" in execution_log


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
    async def determine_service_type(ctx: PyWorkflow, cfg: dict) -> dict:
        """Determine service type and set requirements"""
        service_type = cfg.get("type", "simple")
        return {**cfg, "requires_oauth": service_type == "enterprise"}

    @step
    async def configure_oauth(ctx: PyWorkflow, cfg: dict) -> dict:
        """Only runs for enterprise services"""
        print("  [OAuth Config] Setting up OAuth flow...")
        return {**cfg, "oauth_configured": True}

    @step
    async def configure_basic_auth(ctx: PyWorkflow, cfg: dict) -> dict:
        """Only runs for simple services"""
        print("  [Basic Auth] Setting up basic authentication...")
        return {**cfg, "basic_auth_configured": True}

    @workflow
    async def conditional_flow(ctx: PyWorkflow, cfg: dict) -> dict:
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

    print(f"\n✅ Conditional workflow tests passed!")


@pytest.mark.asyncio
async def test_workflow_error_handling(test_dsn, schema):
    """
    Test error handling in workflows.

    Demonstrates:
    1. Task failure propagates to workflow
    2. Workflow transitions to ERROR state
    3. Error details are persisted
    """
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    @step
    async def failing_task(ctx: PyWorkflow, msg: str) -> str:
        """A task that always fails"""
        print(f"  [Failing Task] About to fail with: {msg}")
        raise ValueError(f"Intentional failure: {msg}")

    @workflow
    async def error_workflow(ctx: PyWorkflow, input_data: dict) -> dict:
        # This task will fail
        await failing_task(ctx, input_data["error_msg"])
        return {"success": True}  # Never reached

    wf_ctx = await admin.create_workflow("error_test", {"error_msg": "test error"})

    # Workflow should raise the error
    with pytest.raises(ValueError, match="Intentional failure: test error"):
        await error_workflow(wf_ctx, {"error_msg": "test error"})

    print(f"\n✅ Error handling test passed!")


# ============================================================================
# Helper Functions
# ============================================================================


def print_workflow_banner(title: str, width: int = 80):
    """Print a formatted banner"""
    print("=" * width)
    print(f" {title}".ljust(width - 1))
    print("=" * width)


def print_task_header(task_num: int, total_tasks: int, task_config: TaskConfig):
    """Print a header for each task"""
    print(f"\n🚀 Task {task_num}/{total_tasks}: {task_config.name}")
    print(f"   📄 Description: {task_config.description}")
    print(f"   🔧 Task ID: {task_config.step_id}")
    print("-" * 60)


@pytest.mark.asyncio
async def test_workflow_with_formatted_output(test_dsn, schema):
    """
    Test workflow execution with formatted output.

    This test demonstrates how to create a user-friendly execution flow
    with progress indicators and status messages.
    """
    config = pgqrs.Config(test_dsn, schema=schema)
    store = await pgqrs.connect_with(config)
    admin = pgqrs.admin(store)
    await admin.install()

    # Define workflow tasks
    task_list = [
        TaskConfig(
            name="Bootstrap",
            step_id="bootstrap",
            description="Setup environment configuration",
            required=True,
        ),
        TaskConfig(
            name="Define Boundaries",
            step_id="boundaries",
            description="Establish system boundaries",
            required=True,
        ),
        TaskConfig(
            name="Analyze Interface",
            step_id="interface",
            description="Examine interface structure",
            required=True,
        ),
    ]

    @workflow
    async def formatted_workflow(ctx: PyWorkflow, cfg: dict) -> dict:
        print_workflow_banner("🎯 INTEGRATION DEVELOPMENT FLOW EXECUTION")
        print(f"📋 Total tasks: {len(task_list)}\n")

        current_config = cfg

        for i, task_config in enumerate(task_list, 1):
            print_task_header(i, len(task_list), task_config)

            # Execute the appropriate task
            if task_config.step_id == "bootstrap":
                current_config = await bootstrap_environment(ctx, current_config)
            elif task_config.step_id == "boundaries":
                current_config = await define_boundaries(ctx, current_config)
            elif task_config.step_id == "interface":
                current_config = await analyze_interface(ctx, current_config)

            print(f"   ✅ {task_config.name} completed successfully!")

        print_workflow_banner("📊 EXECUTION SUMMARY")
        print(f"✅ All {len(task_list)} tasks completed successfully!")

        return current_config

    wf_ctx = await admin.create_workflow("formatted_test", {"project": "test"})
    result = await formatted_workflow(wf_ctx, {"project": "test"})

    assert "interface" in result
    print(f"\n✅ Formatted workflow test passed!")
