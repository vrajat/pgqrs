# Workflow Roadmap — Tier 1 and Tier 2 Plan

**Status:** Planning  
**Scope:** Workflow features only  
**Out of scope in this plan:** child workflows, workflow versioning, advanced orchestration semantics

## Purpose

This document defines the next workflow-focused planning surface for `pgqrs`.

It is intentionally narrow:

- focus on **foundational quality** first
- focus on **simple, inspectable primitives**
- avoid pulling `pgqrs` toward Temporal-class complexity
- use **one issue per feature**
- defer detailed design docs until work starts on a feature branch

## Product Position

`pgqrs` should remain a narrow, operationally simple durable execution engine.

The target shape is:

- library-first
- Postgres-native
- easy to inspect with SQL
- easy to operate without a control plane
- minimal public concepts

If cross-process integration is required, the preferred path is a **small gRPC service**
around `pgqrs`, not a broad set of native SDKs.

## Simplicity Constraints

Every workflow feature in Tier 1 and Tier 2 should satisfy these constraints.

### 1. Small, inspectable state model

Each feature should map to a small number of rows and explicit state transitions.

Good:

- a run is paused until a wake-up time
- a run is cancelled and stops before the next step
- a message is waiting on a topic for a specific run

Bad:

- hidden in-memory coordination
- opaque replay-only semantics
- large event-history machinery required to understand behavior

### 2. Debuggable from SQL and logs

An operator should be able to answer the important questions with direct inspection:

- Why is this run paused?
- What step last completed?
- Why is this run not progressing?
- What message or timer is it waiting on?
- Was this run cancelled?

If a feature cannot be explained clearly from persisted state, it is probably too
complex for the current scope.

### 3. One obvious behavior per primitive

Avoid policy explosion early.

Examples:

- cancellation means "take effect at the next step boundary"
- sleep means "pause until timestamp T"
- schedule means "trigger at computed times according to one clear overlap rule"

Do not start with multiple cancellation modes, multiple schedule policies, or multiple
message-delivery contracts unless production experience demands them.

### 4. Prefer operator quality over orchestration breadth

Before adding more orchestration power, improve:

- run inspection
- step inspection
- DLQ workflows
- cancellation controls

This reduces operational risk and creates better feedback loops for later features.

### 5. Build primitives before hierarchy

`pgqrs` should gain experience with:

- cancellation
- durable waiting
- signals/messages
- schedules

before adding:

- child workflows
- cancellation propagation trees
- versioning branches

### 6. Service boundary is allowed, orchestration server is not the goal

If a feature needs external access, expose it through a thin service boundary.
Do not let this expand the core product into a large always-on orchestration control
plane.

## Notes on Existing Philosophy

Current [PHILOSOPHY.md](../PHILOSOPHY.md) says built-in sleep, automatic event delivery,
and automatic retry scheduling are out of scope.

If Tier 1 and Tier 2 features are accepted, that document will need a follow-up update.

The intended direction is still consistent with the broader philosophy:

- keep primitives small
- keep operations simple
- avoid hidden daemons and server-heavy architecture

But the exact "out of scope" language will need to be narrowed or revised.

## Issue Strategy

Use **one issue per feature**.

Each issue should contain:

- problem statement
- proposed user-facing API
- persistence / schema impact
- backend impact
- failure semantics
- operator / admin surface impact
- acceptance criteria
- docs/tests required

Do not create epics for this work.

If a feature needs deeper design, write that design doc in the feature branch with the
implementation.

## Priority Tiers

## Tier 1 — Foundational Quality

These features improve control, inspectability, and production confidence in the
existing workflow model.

### Issue 1: Add Workflow Cancellation

**Why now**

- cancellation is a basic control-plane capability
- it improves safety without requiring hierarchical workflow semantics
- it exercises clear step-boundary lifecycle rules

**Initial contract**

- cancellation is cooperative at the next step boundary
- the current step is allowed to finish
- no resume support in the first issue
- no child cancellation propagation in this issue

**Acceptance criteria**

- a running workflow can be marked cancelled
- no new step starts after cancellation is observed
- final state is visible and queryable
- cancellation reason and timestamp are inspectable
- Python and Rust surfaces are both supported, or the gap is explicitly documented

### Issue 2: Add Stable Workflow Query APIs

**Why now**

- operators should not need raw SQL for normal inspection
- better query surfaces will make every later feature easier to operate

**Initial contract**

- list runs with filters
- get a run by id
- list steps for a run
- expose stable status/error/timestamp fields

**Acceptance criteria**

- query APIs exist for run list and step list
- filtering supports a minimal useful set: workflow, status, since, until, limit
- output shape is documented and consistent across Rust/Python where applicable
- docs include operator examples

### Issue 3: Add Python DLQ/Admin Parity for Existing Rust Semantics

**Why now**

- production operations should not depend on raw SQL in Python deployments
- this is lower-risk than inventing new orchestration features

**Initial contract**

- parity with existing Rust admin behavior first
- do not redesign DLQ semantics in the first pass

**Acceptance criteria**

- Python can trigger current DLQ archival behavior
- Python can inspect archived DLQ messages
- Python can replay archived DLQ messages if Rust already supports it
- docs describe the exact semantics clearly

### Issue 4: Add Durable Workflow Sleep

**Why now**

- sleep is a small, high-value primitive
- it reuses pause/resume concepts already present in the engine
- it is simpler and safer than jumping straight to more advanced orchestration

**Initial contract**

- `sleep` pauses a run until a wake-up timestamp
- no cron logic
- no generalized retry DSL
- no multiple timer policies

**Acceptance criteria**

- a workflow can durably sleep and continue after restart
- wake-up time is stored explicitly and inspectable
- paused reason/state is queryable
- timeout behavior is deterministic and tested

## Tier 2 — Simple Coordination Primitives

These features expand the workflow model in useful ways without committing to
hierarchical workflow semantics.

### Issue 5: Add Durable Run-Scoped Messaging (`send` / `recv`)

**Why now**

- this is the simplest useful coordination primitive beyond polling
- it enables waiting for external approval or external completion events

**Initial contract**

- messages are scoped to a run
- messages have a topic
- `recv` can wait with timeout
- first version should choose one clear delivery rule and document it

**Acceptance criteria**

- producers can send a message to a run + topic
- workflows can wait for a topic and resume when a message arrives
- timeout and consumption semantics are explicit and tested
- waiting state is inspectable

### Issue 6: Add Built-In Schedules

**Why now**

- schedules are operationally common
- they remove the need for ad hoc re-enqueue logic in applications
- they are easier to reason about than child workflows

**Initial contract**

- basic cron expression support
- one clear overlap rule in v1
- explicit pause/resume/delete operations
- backfill is not required in the first pass unless it falls out naturally

**Acceptance criteria**

- schedules can be created, listed, paused, resumed, and deleted
- a schedule produces runs deterministically
- overlap behavior is documented and tested
- schedule state is inspectable from the database and APIs

### Issue 7: Add Workflow Event Publication (`set_event` / `get_event`)

**Why now**

- users need simple progress publication without inventing their own schema
- this is smaller than a general query system and more direct than raw run polling

**Initial contract**

- named key/value state per run
- last-write-wins semantics
- optional blocking read with timeout
- not a general search attribute system

**Acceptance criteria**

- workflows can publish named event state
- callers can read current state for a run
- timeout semantics are explicit if blocking reads are included
- event state is inspectable and documented

## Deferred Features

These are intentionally not in Tier 1 or Tier 2.

- child workflows
- parent/child cancellation propagation
- workflow versioning / patch semantics
- advanced schedule policies
- search-attribute systems
- large replay/history abstractions

These may become important later, but they should be revisited only after production
experience with Tier 1 and Tier 2.

## Recommended Filing Order

If filing issues now, use this order:

1. Add workflow cancellation
2. Add stable workflow query APIs
3. Add Python DLQ/admin parity for existing Rust semantics
4. Add durable workflow sleep
5. Add durable run-scoped messaging (`send` / `recv`)
6. Add built-in schedules
7. Add workflow event publication (`set_event` / `get_event`)

This order favors control and observability first, then coordination primitives.

## What Not to Do Yet

- do not introduce child workflows first
- do not introduce multiple policy knobs for every feature
- do not expand native language commitments beyond the current product shape
- do not require a large always-on control plane
- do not let the public API outgrow what the persisted state model can explain

## Summary

The next workflow roadmap should optimize for:

- simple semantics
- operator confidence
- inspectable state
- small durable coordination primitives

That means Tier 1 and Tier 2 should focus on cancellation, inspection, DLQ/admin
parity, sleep, run-scoped messaging, schedules, and event publication.

More advanced orchestration features should wait until these primitives have been used
and validated in production.
