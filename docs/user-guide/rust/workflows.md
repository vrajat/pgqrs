# Rust Workflow API

This page documents the Rust API for durable workflows in pgqrs.

## Overview

The Rust workflow API provides two levels of abstraction:

1. **Procedural Macros** (recommended): `#[pgqrs_workflow]` and `#[pgqrs_step]` for clean, declarative code
2. **Low-level API**: `Workflow` handle and `StepGuard` for advanced control

## Quick Start with Macros

The easiest way to build durable workflows is with the `pgqrs_macros` crate:

```rust
use pgqrs::workflow::Workflow;
use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

#[pgqrs_step]
async fn fetch_data(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
    // This body automatically gets durability!
    // - Skipped if already completed
    // - Result persisted on success/failure
    let response = reqwest::get(url).await?.text().await?;
    Ok(response)
}

#[pgqrs_step]
async fn process_data(ctx: &Workflow, data: String) -> Result<i32, anyhow::Error> {
    let count = data.lines().count() as i32;
    Ok(count)
}

#[pgqrs_workflow]
async fn data_pipeline(ctx: &Workflow, url: &str) -> Result<String, anyhow::Error> {
    let data = fetch_data(ctx, url).await?;
    let count = process_data(ctx, data).await?;
    Ok(format!("Processed {} lines", count))
}
```

## Procedural Macros

### `#[pgqrs_step]`

Transforms a function into a durable workflow step with automatic retry/resume semantics.

#### What It Does

The macro wraps your function to:

1. **Derive step ID** from the function name
2. **Check completion** before executing (skip if already done)
3. **Persist results** automatically on success or failure
4. **Enable resume** by consulting stored step state

#### Requirements

| Requirement | Description |
|-------------|-------------|
| First argument | Must be `&Workflow` (provides `pool()` and `id()`) |
| Return type | Must be `Result<T, E>` |
| Async | Must be `async fn` |

#### Example

```rust
use pgqrs::workflow::Workflow;
use pgqrs_macros::pgqrs_step;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct UserData {
    id: i32,
    name: String,
}

#[pgqrs_step]
async fn fetch_user(ctx: &Workflow, user_id: i32) -> Result<UserData, anyhow::Error> {
    println!("Fetching user {}", user_id);

    // Simulate API call
    let user = UserData {
        id: user_id,
        name: "John Doe".to_string(),
    };

    Ok(user)
}

#[pgqrs_step]
async fn send_welcome_email(ctx: &Workflow, user: &UserData) -> Result<String, anyhow::Error> {
    println!("Sending email to {}", user.name);

    // Simulate email sending
    Ok(format!("Email sent to user {}", user.id))
}
```

#### Step ID

The step ID is automatically derived from the function name:

```rust
#[pgqrs_step]
async fn charge_customer(...) -> Result<...> { ... }
// Step ID: "charge_customer"
```

!!! warning "Renaming Functions"
    Renaming a step function changes its step ID. This affects how previously persisted state is matched. Choose stable, descriptive names.

### `#[pgqrs_workflow]`

Marks a function as a workflow entry point with automatic lifecycle management.

#### What It Does

The macro wraps your function to:

1. **Start** the workflow (transition `PENDING` → `RUNNING`)
2. **Execute** the workflow body
3. **Complete** with `SUCCESS` or `ERROR` status

#### Requirements

| Requirement | Description |
|-------------|-------------|
| First argument | Must be `&Workflow` |
| Return type | Must be `Result<T, E>` |
| Async | Must be `async fn` |

#### Example

```rust
use pgqrs::workflow::Workflow;
use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

#[pgqrs_step]
async fn step_one(ctx: &Workflow, data: &str) -> Result<String, anyhow::Error> {
    Ok(format!("step1_{}", data))
}

#[pgqrs_step]
async fn step_two(ctx: &Workflow, data: String) -> Result<String, anyhow::Error> {
    Ok(format!("step2_{}", data))
}

#[pgqrs_workflow]
async fn my_pipeline(ctx: &Workflow, input: &str) -> Result<String, anyhow::Error> {
    // Workflow automatically started
    let result1 = step_one(ctx, input).await?;
    let result2 = step_two(ctx, result1).await?;
    // Workflow automatically marked SUCCESS or ERROR
    Ok(result2)
}
```

## Complete Example with Macros

```rust
use pgqrs::{Admin, Config};
use pgqrs::workflow::Workflow;
use pgqrs_macros::{pgqrs_workflow, pgqrs_step};
use serde::{Deserialize, Serialize};

// Data types
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: i32,
    customer_id: i32,
    amount_cents: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PaymentResult {
    transaction_id: String,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShipmentInfo {
    tracking_number: String,
    carrier: String,
}

// Steps
#[pgqrs_step]
async fn validate_order(ctx: &Workflow, order: &Order) -> Result<Order, anyhow::Error> {
    println!("Validating order {}", order.id);

    if order.amount_cents <= 0 {
        anyhow::bail!("Invalid order amount");
    }

    Ok(order.clone())
}

#[pgqrs_step]
async fn charge_payment(ctx: &Workflow, order: &Order) -> Result<PaymentResult, anyhow::Error> {
    println!("Charging {} cents for order {}", order.amount_cents, order.id);

    // Simulate payment processing
    Ok(PaymentResult {
        transaction_id: format!("txn_{}", order.id),
        status: "completed".to_string(),
    })
}

#[pgqrs_step]
async fn create_shipment(ctx: &Workflow, order: &Order) -> Result<ShipmentInfo, anyhow::Error> {
    println!("Creating shipment for order {}", order.id);

    // Simulate shipment creation
    Ok(ShipmentInfo {
        tracking_number: format!("TRACK{}", order.id),
        carrier: "FastShip".to_string(),
    })
}

#[pgqrs_step]
async fn send_confirmation(
    ctx: &Workflow,
    order: &Order,
    payment: &PaymentResult,
    shipment: &ShipmentInfo,
) -> Result<String, anyhow::Error> {
    println!("Sending confirmation for order {}", order.id);

    Ok(format!(
        "Order {} confirmed. Transaction: {}, Tracking: {}",
        order.id, payment.transaction_id, shipment.tracking_number
    ))
}

// Workflow
#[pgqrs_workflow]
async fn process_order(ctx: &Workflow, order: Order) -> Result<String, anyhow::Error> {
    // Each step is durable - survives crashes!
    let validated = validate_order(ctx, &order).await?;
    let payment = charge_payment(ctx, &validated).await?;
    let shipment = create_shipment(ctx, &validated).await?;
    let confirmation = send_confirmation(ctx, &validated, &payment, &shipment).await?;

    Ok(confirmation)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let config = Config::from_dsn("postgresql://localhost/mydb".to_string());
    let admin = Admin::new(&config).await?;
    admin.install().await?;

    // Create order
    let order = Order {
        id: 12345,
        customer_id: 42,
        amount_cents: 9999,
    };

    // Create workflow (generates ID in database)
    let workflow = Workflow::create(
        admin.pool().clone(),
        "process_order",
        &order,
    ).await?;

    println!("Created workflow: {}", workflow.id());

    // Execute workflow
    let result = process_order(&workflow, order).await?;
    println!("Result: {}", result);

    Ok(())
}
```

### Output

```
Created workflow: 1
Validating order 12345
Charging 9999 cents for order 12345
Creating shipment for order 12345
Sending confirmation for order 12345
Result: Order 12345 confirmed. Transaction: txn_12345, Tracking: TRACK12345
```

### After a Crash and Resume

```
Created workflow: 1
# validate_order: SKIPPED (cached)
# charge_payment: SKIPPED (cached)
Creating shipment for order 12345    # Resumes here!
Sending confirmation for order 12345
Result: Order 12345 confirmed. Transaction: txn_12345, Tracking: TRACK12345
```

## Workflow Handle

The `Workflow` struct is the handle for workflow execution.

### Creating a Workflow

```rust
use pgqrs::workflow::Workflow;

// Create new workflow
let workflow = Workflow::create(pool, "my_workflow", &input_data).await?;
println!("ID: {}", workflow.id());

// Get a handle to an existing workflow by ID (for resuming)
let workflow = Workflow::new(pool, existing_id);
```

!!! note "Resuming Workflows"
    `Workflow::new(pool, id)` creates a handle to an existing workflow without checking the database. 
    The workflow must already exist. Use this when resuming a workflow after a crash or restart.

### Methods

| Method | Description |
|--------|-------------|
| `create(pool, name, input)` | Create a new workflow in the database |
| `new(pool, id)` | Create a handle for an existing workflow |
| `id()` | Get the workflow ID |
| `pool()` | Get a reference to the database pool |
| `start()` | Transition status from `PENDING` to `RUNNING` |
| `success(output)` | Mark workflow as `SUCCESS` with output |
| `fail(error)` | Mark workflow as `ERROR` with error details |

## Error Handling

### Step Errors

Errors in steps are automatically persisted:

```rust
#[pgqrs_step]
async fn risky_step(ctx: &Workflow, data: &str) -> Result<String, anyhow::Error> {
    let result = external_api_call(data).await?;  // Error auto-persisted
    Ok(result)
}
```

### Custom Error Handling

For more control, handle errors explicitly:

```rust
use std::io;

#[pgqrs_step]
async fn careful_step(ctx: &Workflow, data: &str) -> Result<String, anyhow::Error> {
    match external_api_call(data).await {
        Ok(result) => Ok(result),
        Err(e) => {
            // Check if it's a transient error worth retrying
            if let Some(io_err) = e.downcast_ref::<io::Error>() {
                if io_err.kind() == io::ErrorKind::TimedOut {
                    // Re-raise to retry on next workflow run
                    return Err(e);
                }
            }
            // Non-retryable: return error as data instead of failing
            Ok(format!("Error: {}", e))
        }
    }
}
```

### Terminal Error State

Once a step fails, it's terminal. Re-running the workflow will fail at that step:

```rust
// First run - step fails
let result = my_workflow(&workflow, input).await;
// Err(...)

// Second run - fails immediately at the failed step
let result = my_workflow(&workflow, input).await;
// Err(ValidationFailed: "Step X is in terminal ERROR state")
```

## Type Requirements

### Serialization

Step outputs must implement `Serialize` and `Deserialize`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct MyOutput {
    data: String,
    count: usize,
}
```

## Best Practices

### 1. Stable Function Names

Function names become step IDs. Don't rename after deployment:

```rust
// Good - descriptive and stable
#[pgqrs_step]
async fn charge_customer_payment(ctx: &Workflow, ...) -> Result<...>

// Bad - generic, might be renamed
#[pgqrs_step]
async fn step2(ctx: &Workflow, ...) -> Result<...>
```

### 2. Idempotent External Calls

Use idempotency keys for external side effects:

```rust
#[pgqrs_step]
async fn charge_payment(ctx: &Workflow, order_id: i32) -> Result<PaymentResult, anyhow::Error> {
    // Use workflow ID for idempotency
    let idempotency_key = format!("workflow-{}-charge-{}", ctx.id(), order_id);

    payment_api.charge(amount, idempotency_key).await
}
```

### 3. Appropriate Step Granularity

Balance durability vs. overhead:

```rust
// Good - logical unit of work
#[pgqrs_step]
async fn process_batch(ctx: &Workflow, items: Vec<Item>) -> Result<BatchResult, ...>

// Bad - too granular (1000 items = 1000 DB writes)
#[pgqrs_step]
async fn process_item(ctx: &Workflow, item: Item) -> Result<ItemResult, ...>
```

---

## Advanced: Low-Level StepGuard API

For cases requiring direct control over step execution, use `StepGuard` directly.

!!! note "When to Use"
    Use `StepGuard` when you need:

    - Custom step ID logic
    - Dynamic step creation
    - Fine-grained control over success/failure handling
    - Integration with existing non-macro code

### StepGuard

RAII guard for workflow step execution:

```rust
use pgqrs::workflow::{StepGuard, StepResult};

pub enum StepResult<T> {
    Execute(StepGuard),  // Step needs execution
    Skipped(T),          // Already completed, cached output
}
```

### Usage Pattern

```rust
use pgqrs::workflow::{StepGuard, StepResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct StepOutput {
    processed: bool,
    result: String,
}

async fn my_step(
    pool: &PgPool,
    workflow_id: i64,
    data: &str,
) -> Result<StepOutput, pgqrs::Error> {
    match StepGuard::acquire::<StepOutput>(pool, workflow_id, "my_step").await? {
        StepResult::Skipped(cached) => {
            // Step already completed, return cached result
            println!("Step skipped, using cached result");
            Ok(cached)
        }
        StepResult::Execute(guard) => {
            // Execute step logic
            println!("Executing step...");

            let output = StepOutput {
                processed: true,
                result: format!("processed_{}", data),
            };

            // Mark step as complete (REQUIRED)
            guard.success(&output).await?;

            Ok(output)
        }
    }
}
```

### StepGuard Methods

| Method | Description |
|--------|-------------|
| `acquire<T>(pool, workflow_id, step_id)` | Acquire a step, checking DB state |
| `success<T>(output)` | Mark step as `SUCCESS` and persist output |
| `fail<E>(error)` | Mark step as `ERROR` and persist error |

### Complete Low-Level Example

```rust
use pgqrs::workflow::{StepGuard, StepResult, Workflow};
use pgqrs::{Admin, Config};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Debug, Serialize, Deserialize)]
struct UserData {
    id: i32,
    name: String,
}

// Low-level step using StepGuard directly
async fn fetch_user(
    pool: &PgPool,
    wf_id: i64,
    user_id: i32,
) -> Result<UserData, Box<dyn std::error::Error>> {
    match StepGuard::acquire::<UserData>(pool, wf_id, "fetch_user").await? {
        StepResult::Skipped(cached) => Ok(cached),
        StepResult::Execute(guard) => {
            println!("Fetching user {}", user_id);

            let user = UserData {
                id: user_id,
                name: "John Doe".to_string(),
            };

            guard.success(&user).await?;
            Ok(user)
        }
    }
}

async fn process_user(
    pool: &PgPool,
    wf_id: i64,
    user: UserData,
) -> Result<String, Box<dyn std::error::Error>> {
    match StepGuard::acquire::<String>(pool, wf_id, "process_user").await? {
        StepResult::Skipped(cached) => Ok(cached),
        StepResult::Execute(guard) => {
            let result = format!("Processed: {}", user.name.to_uppercase());
            guard.success(&result).await?;
            Ok(result)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_dsn("postgresql://localhost/mydb".to_string());
    let admin = Admin::new(&config).await?;
    admin.install().await?;

    let pool = admin.pool().clone();

    // Create and start workflow manually
    let workflow = Workflow::create(pool.clone(), "user_onboarding", &42i32).await?;
    workflow.start().await?;

    // Execute steps
    let user = fetch_user(&pool, workflow.id(), 42).await?;
    let result = process_user(&pool, workflow.id(), user).await?;

    // Complete workflow manually
    workflow.success(&result).await?;

    println!("Result: {}", result);
    Ok(())
}
```

### Error Handling with StepGuard

```rust
async fn risky_step(
    pool: &PgPool,
    wf_id: i64,
) -> Result<String, pgqrs::Error> {
    match StepGuard::acquire::<String>(pool, wf_id, "risky_step").await? {
        StepResult::Skipped(cached) => Ok(cached),
        StepResult::Execute(guard) => {
            match external_api_call().await {
                Ok(result) => {
                    guard.success(&result).await?;
                    Ok(result)
                }
                Err(e) => {
                    // Persist error - step becomes terminal
                    guard.fail(&e.to_string()).await?;
                    Err(e.into())
                }
            }
        }
    }
}
```

!!! warning "Always Complete the Guard"
    When using `StepGuard` directly, you **must** call either `success()` or `fail()` before dropping the guard.

---

## See Also

- [Durable Workflows Concepts](../concepts/durable-workflows.md)
- [Durable Workflows Guide](../guides/durable-workflows.md)
- [Python Workflow API](../python/workflows.md)
