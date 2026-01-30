# Bug Report: Phantom Duplicate INSERTs in Turso Backend Due to Unsafe Retry Logic

## Summary

The Turso backend's retry logic in `fetch_all_on_connection()` naively retries `INSERT...RETURNING` statements after they have already succeeded, creating phantom duplicate rows. This is a **critical data integrity bug**.

## Affected Code

**File:** `crates/pgqrs/src/store/turso/mod.rs`
**Method:** `TursoQueryBuilder::fetch_all_on_connection()`
**Lines:** ~247-408

## Root Cause

The retry logic retries the entire `conn.query()` call when it receives a "database is locked" error, even when:

1. The error occurs during **result fetching** (`rows.next()`), not during query execution
2. The INSERT has **already succeeded** and written data to the database
3. Retrying will execute the INSERT **again**, creating duplicates

### The Problem Sequence

```rust
// Attempt 1
let res = conn.query(&self.sql, self.params.clone()).await;  // Returns Rows iterator
match res {
    Ok(mut rows) => {
        // Query prepared successfully
        match rows.next().await {
            // INSERT completes here, row is written
            Err(e) if e.to_string().contains("database is locked") => {
                // ERROR: Lock error during RETURNING result fetch
                // INSERT ALREADY SUCCEEDED!
                retries += 1;
                continue;  // ⚠️ LOOPS BACK AND RETRIES ENTIRE QUERY
            }
        }
    }
}

// Attempt 2
let res = conn.query(&self.sql, self.params.clone()).await;
// 💥 BUG: INSERT executes AGAIN, creating duplicate row
```

## Why This Happens with Turso

Turso/libsql uses **async non-blocking I/O** and returns `Busy` errors immediately instead of blocking. For `INSERT...RETURNING`, the execution happens in two distinct phases:

1. **INSERT opcode** - Writes the row (can succeed)
2. **ResultRow opcode** - Fetches the RETURNING result (can fail with "database is locked")

When lock contention occurs between these phases:
- The INSERT has already completed
- The failure is only in fetching the result
- Retrying the entire query creates a duplicate INSERT

### Lock Errors Can Occur After INSERT Succeeds

The "database is locked" error during `rows.next()` can come from:
- WAL checkpoint lock contention
- MVCC snapshot staleness checks
- Schema lock checks
- Read lock escalation failures

These can fail **even for the connection that holds the write lock**, because they're checking different lock types or transaction state.

## Evidence

### Reproducer

Run 1000 sequential `INSERT...RETURNING` operations with ephemeral connections:

```rust
for i in 0..1000 {
    let producer = queue.producer().await?;
    producer.send(&json!({"i": i})).await?;
}
```

**Expected:** 1000 rows
**Actual:** 1000+ rows (e.g., 1071, 1201)

### Database Proof

Query shows duplicate rows with identical payload and worker_id:

```sql
SELECT m1.id, m1.payload, m1.producer_worker_id, m2.id 
FROM pgqrs_messages m1 
JOIN pgqrs_messages m2 
  ON m1.payload = m2.payload 
  AND m1.producer_worker_id = m2.producer_worker_id 
  AND m1.id < m2.id;
```

Returns pairs like:
- Worker 13: id=13 AND id=14, both with payload={"i":12}
- Worker 19: id=20 AND id=21, both with payload={"i":18}

## Impact

**Severity:** Critical - Data corruption

**Affected operations:**
- `INSERT...RETURNING` (used by producers)
- Potentially `UPDATE...RETURNING` and `DELETE...RETURNING`

**Scope:**
- Turso backend only
- Worse with:
  - Ephemeral connections (more lock contention)
  - High concurrency
  - Sequential operations

## The Core Problem: Two Distinct Issues

### Issue 1: DML Operations Should Never Be Retried

**Problem:** `conn.query()` for DML statements should not be retried on lock errors because the statement may have already partially or fully executed.

**Solution:** Detect DML operations (INSERT, UPDATE, DELETE) and fail immediately on lock errors instead of retrying.

```rust
if is_dml && is_locked {
    // Do NOT retry - may have already executed
    return Err(crate::error::Error::QueryFailed {
        query: self.sql.clone(),
        source: Box::new(e),
        context: "DML statement failed with lock error - not safe to retry".into(),
    });
}
```

### Issue 2: rows.next() Failures Are Not Query Failures

**Problem:** When `conn.query()` succeeds but `rows.next()` fails with a lock error, the current code retries the entire query. But the query already executed successfully - only result fetching failed.

**This is trickier because:**
- For SELECT: Retrying might be acceptable (idempotent read)
- For DML with RETURNING: The write already happened, only RETURNING failed
- We can't distinguish "query failed to start" vs "query succeeded but result fetch failed"

**Potential Solutions:**

1. **Never retry rows.next() failures** - Fail immediately if fetching results fails
2. **Track execution state** - Mark query as "started" once conn.query() succeeds, never retry if started
3. **Use different error types** - Distinguish "query execution error" from "result fetch error"

## Recommended Fix

### Short-term: Disable Retry for DML

```rust
fn fetch_all_on_connection(self, conn: &turso::Connection) -> Result<Vec<Row>> {
    let is_dml = self.sql.trim_start().to_uppercase().starts_with("INSERT")
        || self.sql.trim_start().to_uppercase().starts_with("UPDATE")
        || self.sql.trim_start().to_uppercase().starts_with("DELETE");
    
    let res = conn.query(&self.sql, self.params.clone()).await;
    
    match res {
        Ok(mut rows) => {
            let mut result = Vec::new();
            loop {
                match rows.next().await {
                    Ok(Some(row)) => result.push(row),
                    Ok(None) => break,
                    Err(e) => {
                        let msg = e.to_string();
                        let is_locked = msg.contains("database is locked")
                            || msg.contains("SQLITE_BUSY")
                            || msg.contains("snapshot is stale");
                        
                        if is_dml && is_locked {
                            return Err(crate::error::Error::QueryFailed {
                                query: self.sql.clone(),
                                source: Box::new(e),
                                context: "DML statement failed during result fetch - not safe to retry".into(),
                            });
                        }
                        // ... existing retry logic for SELECT
                    }
                }
            }
            Ok(result)
        }
        Err(e) => {
            // Handle query startup failure
            // For DML, do NOT retry
        }
    }
}
```

### Long-term: Better Architecture

1. **Separate result fetching from query execution** - Make it clear when a query has "started"
2. **Connection pooling** - Reduce ephemeral connection overhead and lock contention
3. **Report to turso** - This behavior is confusing; consider internal busy_timeout for writes
4. **Idempotency tokens** - Allow application to detect and ignore duplicates

## Workarounds

Until fixed, users can:

1. **Use connection pooling** - Reduces lock contention
2. **Add application-level deduplication** - Check for duplicate messages
3. **Use PostgreSQL backend** - Not affected by this issue
4. **Retry at application level** - Don't rely on pgqrs internal retries

## Related

- This is a fundamental issue with async SQLite + naive retry logic
- Similar issues likely exist in other code using turso with retries
- The postgres backend does not have this issue (uses different connection/transaction model)

## References

- Turso crate: https://docs.rs/turso/0.4.2/turso/
- Investigation directory: `phantom_inserts_investigation/` (not in git)
- Working reproducer: `phantom_inserts_investigation/repro_code/rust_repro.rs`
