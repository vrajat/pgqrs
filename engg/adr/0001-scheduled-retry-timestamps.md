# ADR-0001: Scheduled Retry with retry_at Timestamps

## Status

Accepted

## Context

The workflow step retry mechanism initially used `tokio::time::sleep()` to block worker threads while waiting for retry delays (exponential backoff). This approach had several problems:

1. **Blocking behavior**: Workers calling `sleep()` cannot process other workflow steps, reducing throughput
2. **Resource inefficiency**: Blocked threads waste resources while waiting
3. **Poor scalability**: With many workflows retrying simultaneously, worker pool gets exhausted
4. **Testing challenges**: Tests had to use real wall-clock time, making them slow (8.62s for 10 tests) and potentially flaky
5. **Limited observability**: No way to query which steps are waiting for retry or when they'll be ready

The original implementation in workflow step guards:
```rust
// Before: Blocking sleep
if should_retry {
    tokio::time::sleep(Duration::from_secs(delay)).await;
    // Worker blocked here, can't do other work
}
```

This became particularly problematic as:
- Workflow orchestration use cases increased
- Need for higher throughput became apparent
- Tests became slower with each retry test added

## Decision

Replace blocking sleep-based retries with **scheduled retries using database timestamps**:

1. Add `retry_at TIMESTAMPTZ` column to `pgqrs_workflow_steps` table
2. When a step fails with a transient error, calculate `retry_at = current_time + backoff_delay` and store it in the database
3. Return a new `Error::StepNotReady { retry_at, retry_count }` error immediately (non-blocking)
4. Workers poll the database and skip steps where `current_time < retry_at`
5. When `current_time >= retry_at`, clear the `retry_at` field and return `Execute` guard

Key implementation details:
- Add `current_time: DateTime<Utc>` parameter to `Store::acquire_step()` for deterministic testing
- Add `StepBuilder::with_time()` method to allow tests to inject custom time
- Implement in all three backends: PostgreSQL, SQLite, Turso
- Add validation: `retry_at` must be in the future
- Document known race condition where concurrent workers may receive duplicate Execute guards

## Consequences

### Positive

1. **Non-blocking**: Workers can process other workflow steps while waiting for retries
2. **Better throughput**: Worker pool stays available for other work
3. **Scalability**: System can handle many concurrent retrying workflows
4. **Observable**: `retry_at` visible in database for debugging and monitoring
5. **Deterministic testing**: Tests can simulate time advancement using `with_time()`
   - Test execution time reduced from 8.62s to 1.81s (79% faster)
   - Tests are deterministic and reliable (no wall-clock dependencies)
6. **Consistent with database-driven architecture**: Retry state lives in database alongside workflow state

### Negative

1. **Known race condition**: If two workers poll simultaneously when `retry_at` is ready, both may receive Execute guards
   - This can cause duplicate step execution
   - Documented with WARNING comments in code
   - Accepted trade-off for current architecture
   - Proper fix would require SELECT FOR UPDATE or optimistic locking (larger redesign)

2. **Polling overhead**: Workers query the database to check retry readiness
   - Mitigated by: queries are fast, partial index on `(workflow_id, step_key) WHERE retry_at IS NOT NULL`
   - Overhead is minimal compared to benefits

3. **Database dependency**: Retry scheduling relies on database timestamp precision
   - Not an issue in practice - database timestamps are sufficiently precise

### Neutral

1. **API change**: Added `current_time` parameter to internal `Store::acquire_step()`
   - Public API remains backwards compatible
   - `StepBuilder::acquire()` calls `Utc::now()` by default
   - Optional `with_time()` for testing

2. **Migration required**: Schema change adds `retry_at` column
   - Migration is straightforward (ALTER TABLE ADD COLUMN)
   - Backwards compatible (nullable column)

## Alternatives Considered

### Alternative 1: Keep tokio::time::sleep()
- **Description**: Continue using sleep-based blocking retries
- **Pros**: 
  - No schema changes needed
  - Simpler implementation
- **Cons**: 
  - Workers blocked during retry delays
  - Poor scalability and throughput
  - Slow, non-deterministic tests
- **Reason for rejection**: Blocking behavior is a fundamental scalability bottleneck

### Alternative 2: External Scheduler (Temporal/Cron)
- **Description**: Use external scheduling service to trigger retries
- **Pros**: 
  - Fully non-blocking
  - Proven at scale
  - No race conditions
- **Cons**: 
  - Massive architectural change
  - External dependency
  - Operational complexity
  - Overkill for current needs
- **Reason for rejection**: Too complex and invasive for the problem being solved

### Alternative 3: SELECT FOR UPDATE for Race-Free Acquisition
- **Description**: Use SELECT FOR UPDATE to prevent duplicate execution
- **Pros**: 
  - Eliminates race condition
  - Database-enforced serialization
- **Cons**: 
  - Requires transaction refactoring across all acquire paths
  - Performance impact of row-level locks
  - Complicates implementation significantly
- **Reason for rejection**: Benefits don't outweigh complexity; accepted race condition is low-impact

### Alternative 4: In-Memory Scheduler
- **Description**: Keep retry state in memory with a scheduler
- **Pros**: 
  - No database polling
  - No schema changes
- **Cons**: 
  - State lost on worker restart
  - Doesn't work in multi-worker scenarios
  - Inconsistent with database-driven architecture
- **Reason for rejection**: Violates principle of database as source of truth

## References

- [PR #160: Scheduled retry implementation](https://github.com/tembo-io/pgqrs/pull/160)
- [Design discussion: `.tmp/retry-design-discussion.md`](https://github.com/tembo-io/pgqrs/pull/160) (local notes)
- Implementation files:
  - `crates/pgqrs/src/error.rs` - `StepNotReady` error variant
  - `crates/pgqrs/src/builders/workflow.rs` - `with_time()` method
  - `crates/pgqrs/src/store/*/workflow/guard.rs` - Backend implementations
  - `crates/pgqrs/migrations/*_add_step_retry_columns.sql` - Schema migrations
- Tests: `crates/pgqrs/tests/workflow_retry_integration_tests.rs`

## Implementation Notes

### Schema Changes
```sql
-- PostgreSQL
ALTER TABLE pgqrs_workflow_steps 
ADD COLUMN retry_at TIMESTAMPTZ;

CREATE INDEX CONCURRENTLY idx_workflow_steps_retry 
ON pgqrs_workflow_steps (workflow_id, step_key) 
WHERE retry_at IS NOT NULL;
```

### Test Time Control
```rust
// Production: uses real time
let result = step(workflow_id, "my_step")
    .acquire(&store)
    .await?;

// Tests: inject custom time
let simulated_time = retry_at + Duration::seconds(1);
let result = step(workflow_id, "my_step")
    .with_time(simulated_time)
    .acquire(&store)
    .await?;
```

### Performance Impact
- Test execution: **79% faster** (8.62s → 1.81s)
- Database queries: Minimal overhead (indexed queries, only for retrying steps)
- Worker throughput: **Significantly improved** (workers no longer blocked)

---

**Date**: 2026-01-31  
**Author(s)**: Rajat Venkatesh, OpenCode AI  
**Reviewers**: CodeReviewer (automated review)  
**Implementation**: PR #160
