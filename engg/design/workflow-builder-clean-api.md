# Workflow Builder Clean API (Macro-Defined Workflows)

## Overview

This document proposes a cleaner workflow developer experience while keeping the existing public entrypoint `pgqrs::workflow()`.

It aligns workflow execution with the queue "poll" model, and standardizes workflow/step definitions on attribute macros.

## Goals

- Keep the existing builder entrypoint: `pgqrs::workflow()`.
- Consumer DX:
  - `pgqrs::workflow().name(workflow_def).consumer(...).poll(...)` runs a background poll loop.
  - No handler is required from the user. The runnable workflow function comes from attribute macros.
- Producer DX:
  - `pgqrs::workflow().name(workflow_def).producer(...).trigger(...).execute(...)` triggers a workflow run.
  - If producer is omitted, `.execute(...)` creates an ephemeral producer (new every call).
- Define workflows and steps using attribute macros exclusively.
- Explicitly allow breaking changes (no backward compatibility requirement for this redesign).

## Non-Goals

- No new top-level builder entrypoints (no `workflow_consumer()` / `workflow_trigger()` functions).
- No declarative workflow macro (`workflow!{}`), attribute macros only.
- Not redesigning workflow persistence or step state machinery.
- Not introducing a global workflow registry (no runtime lookup by string name).

## Current State

- Queue polling is handled via `pgqrs::dequeue().worker(...).handle(...).poll(&store)`.
- Workflows already have a handler adapter:
  - `pgqrs::workflow_handler(store.clone(), |run, input| async move { ... })`
  - This wraps message handling with run lifecycle semantics.
- Triggering currently routes through the store (`store.trigger(...)`) rather than a producer.
- Attribute macros exist in `pgqrs-macros` (e.g. `#[pgqrs_step]`).

## Key Idea

Attribute macros generate a workflow definition value that packages:

- The canonical workflow name (used for queue naming / routing)
- The typed runner function (used to build the internal `workflow_handler`)

The builder consumes this value via `.name(...)`.

In other words, `.name(...)` changes meaning from "set a string" to "select a workflow definition".

## Proposed Public API

### Workflow definition via attribute macro

Conceptual user code:

```rust
use pgqrs_macros::{pgqrs_step, pgqrs_workflow};

#[derive(serde::Serialize, serde::Deserialize)]
struct TaskParams {
    id: i32,
}

#[pgqrs_step]
async fn step_one(run: &pgqrs::Run, id: i32) -> pgqrs::Result<()> {
    let _ = (run, id);
    Ok(())
}

#[pgqrs_workflow(name = "process_task")]
async fn process_task(run: pgqrs::Run, input: TaskParams) -> pgqrs::Result<serde_json::Value> {
    step_one(&run, input.id).await?;
    Ok(serde_json::json!({"ok": true}))
}
```

The macro expands to a value that can be passed to the builder:

- `process_task` (a `WorkflowDef<...>`), not a string.

### Consumer poll

```rust
pgqrs::workflow()
    .name(process_task)
    .consumer(&consumer)
    .poll(&store)
    .await?;
```

Semantics:

- `.poll(&store)` starts an infinite poll loop.
- Messages are dequeued from the workflow queue (1:1 mapping with workflow definition name).
- Each message is executed using an internal `pgqrs::workflow_handler(store.clone(), macro_runner)`.
- Interrupt semantics match queue polling: `consumer.interrupt()` results in suspension and poll termination.

### Producer trigger

```rust
let run_msg = pgqrs::workflow()
    .name(process_task)
    .producer(&producer) // optional
    .trigger(&input)?
    .execute(&store)
    .await?;
```

If `producer(...)` is omitted:

- `.execute(&store)` creates an ephemeral producer (new worker row every call).
- The trigger payload is enqueued via the producer.

## Macro UX

Attribute macros only.

Rationale:

- Keeps workflow/step semantics close to the functions that execute.
- Allows signature validation with good compile-time errors.
- Avoids introducing additional DSLs.

### Macro output contract

The workflow macro must output a definition value that is usable by the builder.

Minimum requirements for the generated definition:

- `name: &'static str`
- A runner that can be used to build a queue handler equivalent to:
  - `pgqrs::workflow_handler(store.clone(), |run, input| async move { ... })`

The builder should not accept user-provided handlers.

## Detailed Design

### Workflow definition type

Introduce a public definition type in `pgqrs` (exact generics TBD):

- `WorkflowDef<TInput, TOutput>`

It should contain:

- `name: &'static str`
- A function pointer / closure factory that can create a typed runner compatible with `workflow_handler`.

No global registration is required since the definition is passed directly.

### WorkflowBuilder changes

Update `WorkflowBuilder` (in `crates/pgqrs/src/builders/workflow.rs`):

- Replace `name: Option<String>` with `workflow: Option<WorkflowDef<...>>`
- Add optional producer/consumer context:
  - `consumer: Option<&Consumer>`
  - `producer: Option<&Producer>`

Add fluent methods:

- `.name(workflow_def)`
- `.consumer(&Consumer)`
- `.producer(&Producer)`
- `.poll(&Store)`

### Poll implementation

Internally, `.poll(&store)`:

1. Validates `workflow` and `consumer` are present.
2. Builds a message handler from the macro runner:
   - `pgqrs::workflow_handler(store.clone(), macro_runner)`
3. Runs queue polling using the existing dequeue poller:

```rust
pgqrs::dequeue()
    .worker(consumer)
    .batch(1)
    .handle(workflow_message_handler)
    .poll(store)
    .await
```

### Trigger implementation

Internally, `.trigger(input)?.execute(&store)`:

1. Serializes input into the canonical trigger payload.
2. If `producer` is present, enqueue via `pgqrs::enqueue().worker(producer) ...`.
3. Otherwise create a producer on-demand (new each call) and enqueue.

### Canonical trigger payload

This redesign should standardize the payload emitted by the builder.

- Always include `input` as the workflow input.
- Optionally include `run_id` if needed by existing handler semantics.

## Error Handling

- Missing required fields return `Error::ValidationFailed` with actionable messages.
- Poll termination on interrupt returns `Error::Suspended` (consistent with queue polling).
- Serialization errors use `Error::Serialization`.

## Testing Strategy

- Integration tests covering:
  - `workflow().name(def).producer(&producer).trigger(...).execute(&store)`
  - `workflow().name(def).trigger(...).execute(&store)` creates an ephemeral producer
  - `workflow().name(def).consumer(&consumer).poll(&store)` processes a run and suspends on interrupt
- Guide tests with snippet regions (same approach as Basic Queue), feeding docs.

## Migration / Rollout

- Breaking change is acceptable.
- Remove/replace the string-based workflow builder APIs.
- Update docs to use snippet-included, tested examples.

## Alternatives Considered

- Runtime registry keyed by string workflow name: rejected (adds global state + duplication).
- Declarative macro (`workflow!{}`): rejected (explicit requirement).

## Open Questions

- Exact shape of `WorkflowDef` (how the runner factory is typed / stored).
- Should `.poll(&store)` accept `.batch(n)` and `.poll_interval(...)` similar to dequeue?
- How should macro-generated workflow identity be named to avoid collisions with the original function name?
