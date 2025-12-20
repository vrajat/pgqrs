# pgqrs Durable Workflows — Product Requirements & Engineering Guardrails

**Status:** Investigation / Pre-Implementation  
**Audience:** Core pgqrs maintainers, coding agents, contributors  
**Scope:** Durable workflow execution (Rust + Python)

---

## 1. Problem Statement

pgqrs provides durable, transactional queues on PostgreSQL.  
This project extends pgqrs to support **durable workflows**, where multi-step execution:

- Survives crashes, restarts, and long pauses
- Resumes deterministically from persisted state
- Provides exactly-once *logical* execution of workflow steps

The system must persist workflow and step state in PostgreSQL and allow restoration without relying on in-memory state.

---

## 2. Goals

### Functional Goals
- Durable execution of multi-step workflows
- Step-level progress tracking
- Resume execution after failures
- Support nested (child) workflows
- Rust and Python developer support

### Non-Goals (Initial Phases)
- Visual workflow editors
- Distributed consensus beyond PostgreSQL guarantees
- Exactly-once guarantees for *external* side effects
- Full metrics / tracing stacks (Prometheus, OTEL, etc.)

---

## 3. Product Invariants (Set in Stone)

These are **non-negotiable requirements**.

1. **Durability**
   - Workflow and step progress must be persisted in PostgreSQL.
2. **Restorability**
   - Execution must resume after crash or restart without re-running completed steps.
3. **Exactly-Once (Logical) Step Semantics**
   - A completed step must not execute again.
4. **Language-Native APIs**
   - Rust feels idiomatic Rust.
   - Python feels idiomatic Python.
5. **Deterministic Execution**
   - Workflow structure must be deterministic for a given execution.
6. **Composable with pgqrs**
   - Workflows may enqueue jobs or be triggered by queues.

---

## 4. Design Flexibility (NOT Set in Stone)

The following are **illustrative**, not contracts:

- Exact database schema layout
- Column names and SQL types
- Macro syntax and attribute names
- Python decorator signatures
- Internal execution engine structure

Engineering may evolve these freely **as long as Product Invariants are preserved**.

---

## 5. Conceptual Model

### Core Concepts

| Concept | Description |
|------|-------------|
| Workflow (DAG) | A deterministic sequence or DAG of steps |
| Step (Node) | A single function execution |
| Execution | One run of a workflow |
| Executor | Process/thread running the workflow |
| State | Persisted workflow + step progress |

---

## 6. Developer Experience (Illustrative)

### 6.1 Rust — RAII Guard (Foundational API)

```rust
fn step_one() {
    let _guard = pgqrs::StepGuard::new("step_one");
    println!("Step one completed");
}
```

---

### 6.2 Rust — Procedural Macros (Ergonomic Layer)

```rust
#[pgqrs_node]
fn step_one() {
    println!("Step one completed!");
}

#[pgqrs_dag]
fn workflow() {
    step_one();
    step_two();
}
```

---

### 6.3 Python (Illustrative)

```python
from pgqrs import step, workflow

@step
def step_one(ctx):
    print("Step one completed")

@workflow
def my_workflow():
    step_one()
    step_two()
```

---

## 7. Persistence Model (Conceptual)

### 7.1 Workflow (DAG) Table — Conceptual

| Column | Description |
|------|-------------|
| dag_uuid | Workflow execution ID |
| status | PENDING, RUNNING, SUCCESS, ERROR |
| name | Workflow function name |
| class_name | Optional |
| config_name | Optional |
| output | Serialized output |
| error | Serialized error |
| executor_id | Executor identifier |
| created_at | Epoch ms |
| updated_at | Epoch ms |

---

### 7.2 Step (Node) Table — Conceptual

| Column | Description |
|------|-------------|
| dag_uuid | Parent workflow |
| function_id | Sequence ID |
| function_name | Step name |
| output | Serialized output |
| error | Serialized error |
| child_workflow_id | Optional child DAG |
| started_at_epoch_ms | Start time |
| completed_at_epoch_ms | Completion time |

---

## 8. Observability (Product Requirement)

Observability is **in scope**, but intentionally minimal in early phases.

### Required (Phase 1)
- Persisted workflow status
- Persisted step status
- Start / end timestamps
- Error inspection
- Executor identification

---

## 9. Phased Delivery Plan

### Phase 1 — Rust Core (MVP)
- Workflow + step persistence
- RAII StepGuard
- Linear workflows
- Resume support

### Phase 2 — Rust Macros
- `#[pgqrs_node]`
- `#[pgqrs_dag]`

### Phase 3 — Python SDK
- Decorators
- Shared schema

---

## 10. Engineering Guardrails

1. Do not lock schema prematurely  
2. RAII is the execution primitive  
3. Macros must lower to guards  
4. Persist before advancing state  
5. Assume crashes at every line  

---

## 11. Summary

This document defines product-level requirements and guardrails for adding
durable workflow support to pgqrs. Schemas and APIs are illustrative unless
explicitly marked as invariants.
