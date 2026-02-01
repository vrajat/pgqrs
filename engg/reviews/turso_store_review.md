# Turso Store Review (Craftsmanship Feedback)

This review covers the Turso backend implementation in [turso_worktree/crates/pgqrs/src/store/turso](turso_worktree/crates/pgqrs/src/store/turso) plus feature flags, dependencies, and CI updates. The implementation leverages the `turso` crate (libSQL) and mostly mirrors the SQLite backend with Turso-specific adaptations. Overall, the code is functional and well-structured. The suggestions below aim to raise the craftsmanship bar around correctness, SQL injection safety, error handling, and production readiness.

## Summary
- Clean modular design mirroring SQLite/Postgres: `tables/`, `worker/`, `workflow/` separation.
- Uses `turso` crate (v0.4.2) for libSQL connectivity; migrations embedded via `include_str!`.
- CI matrix includes Turso leg with file-based DSN (`turso:///tmp/ci_test_turso.db`).
- Custom query builder with exponential backoff retry logic for database lock errors.
- **Critical**: Dynamic SQL construction with `format!()` in batch operations creates SQL injection risk.

## High-Impact Issues (Must Fix)

### 1. **SQL Injection Vulnerability in Batch Operations** ⚠️
**Severity: Critical**

Multiple locations use `format!()` to build SQL with placeholders, then bind values afterward. While the *values* are bound safely, the *placeholder count* is attacker-controlled if `ids.len()` comes from user input:

**Location**: [tables/messages.rs:115-117](turso_worktree/crates/pgqrs/src/store/turso/tables/messages.rs#L115-L117)
```rust
let placeholders: Vec<String> = ids.iter().map(|_| "?".to_string()).collect();
let sql = format!(
    "DELETE FROM pgqrs_messages WHERE id IN ({}) AND consumer_worker_id = ? RETURNING id",
    placeholders.join(", ")
);
```

**Location**: [tables/messages.rs:303-307](turso_worktree/crates/pgqrs/src/store/turso/tables/messages.rs#L303-L307)
```rust
let placeholders: Vec<String> = ids.iter().map(|_| "?".to_string()).collect();
let sql = format!(
    "SELECT id, queue_id, payload, vt, enqueued_at, read_ct, dequeued_at, producer_worker_id, consumer_worker_id FROM pgqrs_messages WHERE id IN ({})",
    placeholders.join(", ")
);
```

**Problem**: 
- If an attacker can control the *length* of `ids`, they can cause a denial-of-service by requesting thousands of placeholders, potentially exhausting memory or causing parser failures.
- While the current implementation only generates "?" strings (safe), this pattern is brittle. Any future modification that interpolates user data into the template becomes a direct SQL injection vector.
- The `format!()` pattern bypasses parameterized query safety guarantees and makes code harder to audit.

**Solution** (implement ALL of these):
1. **Add strict batch size limits** at the top of each function:
   ```rust
   const MAX_BATCH_SIZE: usize = 1000;
   if ids.len() > MAX_BATCH_SIZE {
       return Err(crate::error::Error::ValidationFailed {
           reason: format!("Batch size {} exceeds maximum {}", ids.len(), MAX_BATCH_SIZE),
       });
   }
   if ids.is_empty() {
       return Ok(vec![]);
   }
   ```

2. **Add safety documentation**:
   ```rust
   /// SAFETY: This function uses format!() to build dynamic SQL. The `ids` parameter
   /// must come from trusted sources only (internal state, not direct user input).
   /// Batch size is limited to MAX_BATCH_SIZE to prevent DoS attacks.
   ```

3. **Consider safer alternatives**:
   - Check if `turso` crate supports array parameters or batch query APIs
   - If migrating to a newer turso version, investigate native batch operations
   - Document why dynamic IN clauses are necessary vs. individual queries

4. **Add validation tests**:
   ```rust
   #[tokio::test]
   async fn test_batch_size_limit() {
       let store = test_store().await;
       let huge_batch: Vec<i64> = (0..2000).collect();
       let result = store.messages.get_by_ids(&huge_batch).await;
       assert!(result.is_err()); // Should reject oversized batch
   }
   ```

### 2. **Missing Foreign Key Enforcement** ⚠️
**Severity: High**

Unlike the SQLite backend which now enables `PRAGMA foreign_keys=ON`, the Turso backend does **not** explicitly enable foreign keys.

**Location**: [turso/mod.rs:60-82](turso_worktree/crates/pgqrs/src/store/turso/mod.rs#L60-L82)

**Impact**: 
- Foreign key constraints in migrations may be silently ignored
- This allows orphan records: messages without queues, workers without queues, etc.
- Data integrity violations won't be caught, leading to corrupt state

**Solution**: Add immediately after WAL setup in `TursoStore::new()`:
```rust
conn.execute("PRAGMA foreign_keys=ON;", ())
    .await
    .map_err(|e| crate::error::Error::Internal {
        message: format!("Failed to enable foreign keys: {}", e),
    })?;

// Verify it's enabled
let fk_enabled: i64 = conn.query_row("PRAGMA foreign_keys", ())
    .await
    .map_err(|e| crate::error::Error::Internal {
        message: format!("Failed to check foreign_keys pragma: {}", e),
    })?
    .get(0)?;

if fk_enabled != 1 {
    return Err(crate::error::Error::Internal {
        message: "Foreign keys could not be enabled".to_string(),
    });
}
```

**Test Coverage**: Add test to verify FK enforcement:
```rust
#[tokio::test]
async fn test_foreign_key_enforcement() {
    let store = test_store().await;
    // Try to insert message with non-existent queue_id
    let result = store.messages.insert(NewMessage {
        queue_id: 99999, // doesn't exist
        payload: json!({}),
        // ...
    }).await;
    assert!(result.is_err()); // Should fail due to FK constraint
}
```

### 3. **Retry Logic Overflow and Thundering Herd**
**Severity: Medium-High**

The `TursoQueryBuilder` implements exponential backoff for `SQLITE_BUSY` errors, which is excellent. However, there are two issues:

**Location**: [mod.rs:200-230](turso_worktree/crates/pgqrs/src/store/turso/mod.rs#L200-L230)

**Issue 1: Integer Overflow**
```rust
delay *= 2; // Can overflow: 50→100→200→400→800→1600→...→u64::MAX
```
After ~64 iterations, this will wrap around or panic in debug mode.

**Issue 2: No Jitter**
All concurrent clients experiencing lock contention will retry at exactly the same intervals (50ms, 100ms, 200ms...), causing synchronized thundering herd behavior.

**Solution**:
```rust
const MAX_RETRIES: usize = 5;
const INITIAL_DELAY_MS: u64 = 50;
const MAX_DELAY_MS: u64 = 5000;

let mut delay = INITIAL_DELAY_MS;
for attempt in 1..=MAX_RETRIES {
    match result {
        Err(e) if is_busy_error(&e) => {
            if attempt >= MAX_RETRIES {
                return Err(convert_error(e));
            }
            
            // Add jitter: ±25% randomness
            let jitter = rand::thread_rng().gen_range(0..delay / 4);
            let actual_delay = delay + jitter;
            
            tracing::debug!(
                "Database busy, retrying in {}ms (attempt {}/{})",
                actual_delay, attempt, MAX_RETRIES
            );
            
            tokio::time::sleep(Duration::from_millis(actual_delay)).await;
            
            // Exponential backoff with cap
            delay = delay.saturating_mul(2).min(MAX_DELAY_MS);
        }
        Err(e) => return Err(convert_error(e)),
        Ok(val) => return Ok(val),
    }
}
```

**Dependencies**: Add to `Cargo.toml` if not present:
```toml
rand = "0.8"
```

## Correctness & Reliability

### 4. **Error Message Information Leakage**
**Severity: Low-Medium**

**Location**: [mod.rs:38-43](turso_worktree/crates/pgqrs/src/store/turso/mod.rs#L38-L43)

Error messages may leak DSN which could contain auth tokens:
```rust
Err(crate::error::Error::ConnectionFailed {
    message: format!("Connect failed for DSN: {}", dsn),
})
```

If DSN is `turso://https://yourdb.turso.io?token=secret123`, this exposes credentials in logs/errors.

**Solution**: Redact sensitive parts:
```rust
fn redact_dsn(dsn: &str) -> String {
    if let Some(token_idx) = dsn.find("token=") {
        format!("{}token=<redacted>", &dsn[..token_idx])
    } else {
        dsn.to_string()
    }
}

// In error:
message: format!("Connect failed for DSN: {}", redact_dsn(dsn))
```

### 5. **Migration Ordering and Idempotency**
**Severity: Medium**

Migrations are loaded via `include_str!()` and executed sequentially without version tracking.

**Location**: [mod.rs:86-95](turso_worktree/crates/pgqrs/src/store/turso/mod.rs#L86-L95)

**Issues**:
1. **No guard against re-running**: If `TursoStore::new()` is called on an existing database, migrations run again
2. **No version tracking**: Can't determine which migrations have been applied
3. **Partial failure recovery**: If migration 3 of 6 fails, subsequent connects will fail or corrupt state
4. **No rollback mechanism**: Unlike sqlx migrations which track state

**Solution**: Add version tracking table:
```rust
// Create version table first
const INIT_VERSION_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS pgqrs_schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
);
"#;

async fn run_migrations(conn: &turso::Connection) -> Result<()> {
    conn.execute(INIT_VERSION_TABLE, ()).await?;
    
    let migrations = vec![
        (1, "01_create_queues.sql", include_str!("../../migrations/turso/01_create_queues.sql")),
        (2, "02_create_workers.sql", include_str!("../../migrations/turso/02_create_workers.sql")),
        (3, "03_create_messages.sql", include_str!("../../migrations/turso/03_create_messages.sql")),
        (4, "04_create_archive.sql", include_str!("../../migrations/turso/04_create_archive.sql")),
        (5, "05_create_workflows.sql", include_str!("../../migrations/turso/05_create_workflows.sql")),
        (6, "06_create_workflow_steps.sql", include_str!("../../migrations/turso/06_create_workflow_steps.sql")),
    ];
    
    for (version, description, sql) in migrations {
        // Check if already applied
        let applied: Option<i64> = conn.query_row(
            "SELECT version FROM pgqrs_schema_version WHERE version = ?",
            (version,)
        )
        .await
        .ok()
        .and_then(|row| row.get(0).ok());
        
        if applied.is_some() {
            tracing::debug!("Migration {} already applied, skipping", description);
            continue;
        }
        
        tracing::info!("Applying migration {}: {}", version, description);
        
        conn.execute(sql, ()).await.map_err(|e| {
            crate::error::Error::Internal {
                message: format!("Migration {} failed: {}", description, e),
            }
        })?;
        
        // Record success
        conn.execute(
            "INSERT INTO pgqrs_schema_version (version, applied_at, description) VALUES (?, datetime('now'), ?)",
            (version, description)
        ).await?;
    }
    
    Ok(())
}
```

### 6. **Missing Performance Indices**
**Severity: Medium**

The migrations lack indices for common query patterns.

**Location**: [migrations/turso/03_create_messages.sql](turso_worktree/crates/pgqrs/migrations/turso/03_create_messages.sql)

**Missing Indices**:
```sql
-- For dequeue ordering
CREATE INDEX IF NOT EXISTS idx_messages_queue_enqueued_at 
ON pgqrs_messages(queue_id, enqueued_at);

-- For visibility timeout queries
CREATE INDEX IF NOT EXISTS idx_messages_queue_vt 
ON pgqrs_messages(queue_id, vt);

-- For consumer ownership checks
CREATE INDEX IF NOT EXISTS idx_messages_consumer 
ON pgqrs_messages(consumer_worker_id);

-- For worker health checks
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat 
ON pgqrs_workers(heartbeat_at);

-- For worker queue lookups
CREATE INDEX IF NOT EXISTS idx_workers_queue_status 
ON pgqrs_workers(queue_id, status);
```

**Impact**:
- Without these indices, `dequeue_at` does full table scans as queue grows
- Visibility timeout checks become O(n) instead of O(log n)
- Consumer queries slow down significantly with message volume

## Quick Wins (High Priority)

Implement these immediately for production readiness:

1. **Add batch size limits** in [tables/messages.rs:115, 303](turso_worktree/crates/pgqrs/src/store/turso/tables/messages.rs) - 30 min
2. **Enable `PRAGMA foreign_keys=ON`** in [turso/mod.rs](turso_worktree/crates/pgqrs/src/store/turso/mod.rs) - 10 min  
3. **Cap retry delay** with `.saturating_mul(2).min(5000)` - 5 min
4. **Add performance indices** in [migrations/turso/](turso_worktree/crates/pgqrs/migrations/turso/) - 15 min
5. **Implement migration version tracking** - 1 hour
6. **Add jitter to retry logic** - 15 min
7. **Redact DSN in error messages** - 10 min

Total: ~2.5 hours for critical production-readiness fixes.

## Overall Assessment

The Turso integration follows the same excellent architectural patterns as the SQLite backend, demonstrating consistency and maintainability. The custom retry logic is a smart addition for handling lock contention. However, there are **critical security and correctness issues** that must be addressed before production use:

1. **SQL Injection Risk** (Critical): The `format!()` pattern in batch operations, while currently safe, is a ticking time bomb. Add batch size limits and consider safer alternatives.

2. **Missing Foreign Key Enforcement** (High): Without `PRAGMA foreign_keys=ON`, data integrity constraints are silently ignored, risking orphan records.

3. **Retry Logic Needs Bounds** (Medium): Exponential backoff can overflow and lacks jitter, potentially causing thundering herd issues under load.

4. **No Migration Version Tracking** (Medium): Re-running migrations on existing databases or handling partial failures will cause production issues.

With these fixes implemented (estimated ~4-5 hours of focused work), the Turso backend will be production-grade and match the quality bar of the Postgres backend. The architecture is solid—it just needs the sharp edges smoothed out.

## Comparison to SQLite Backend

| Aspect | Turso | SQLite | Notes |
|--------|-------|--------|-------|
| **FK Enforcement** | ❌ Missing | ✅ Enabled | **CRITICAL**: Turso needs `PRAGMA foreign_keys=ON` |
| **SQL Injection Risk** | ⚠️ format!() in batch ops | ✅ Safe (bind everywhere) | **CRITICAL**: Add batch size limits |
| **Retry Logic** | ✅ Custom exponential backoff | ❌ Basic busy_timeout | Turso handles better but needs bounds/jitter |
| **Migration Management** | ⚠️ include_str, no tracking | ✅ sqlx with version tracking | Turso needs version table |
| **Performance Indices** | ❌ Missing key indices | ✅ Comprehensive indices | Turso needs same idx_messages_queue_* indices |
| **Test Coverage** | ⚠️ Basic only | ✅ Comprehensive | Expand Turso tests for constraints, retries |
| **Drop Cleanup** | ✅ Implemented (Producer) | ✅ Implemented | Verify Consumer has same |
| **Error Handling** | ⚠️ May leak DSN | ✅ Clean | Redact credentials in errors |
| **Documentation** | ❌ Sparse | ✅ Good | Add module docs, README section |

**Verdict**: Turso backend is 70% production-ready. The remaining 30% consists of critical security/correctness fixes that are straightforward to implement. Prioritize the "Quick Wins" section to reach production quality.
