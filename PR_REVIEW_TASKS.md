# Post-PR Review Tasks

## Security & Code Quality Issues from PR Review

### Task 1: Fix SQL Identifier Quoting (High Priority - Security) ✅ COMPLETED
**Issue**: Table names are interpolated into SQL without quoting in queue.rs:240
**Action**: ✅ Fixed table identifier quoting in queue.rs
**Action**: ✅ Fixed schema identifier quoting in uninstall statement

## Clippy Warnings (Must Fix for Clean Build)

### Task 2: Fix &String parameter types ✅ COMPLETED
**Issue**: `src/admin.rs:193` - Using `&String` instead of `&str`
**Action**: ✅ Changed create_queue parameter from &String to &str

### Task 3: Fix Needless Borrows ✅ COMPLETED
**Issue**: `src/admin.rs:624` and `src/admin.rs:666` - Unnecessary borrowing
**Action**: ✅ Removed & from sqlx::query calls

### Task 4: Fix Additional Clippy Errors (CURRENT)
**Multiple Issues Found**:
- Dead code warnings in test common modules
- Await holding lock in container.rs
- Unnecessary to_string() calls throughout tests
- Bool assertion comparisons in lib_tests.rs
- Needless borrows for generic args

## Identified Clippy Errors (25+ total)

### Dead Code (3 errors)
- `tests/common/mod.rs:31` - get_database_dsn_with_schema
- `tests/common/container.rs:133` - get_database_dsn_with_schema
- `tests/common/mod.rs:17` - get_postgres_dsn

### Await Holding Lock (2 errors)
- `tests/common/container.rs:84` - MutexGuard across await
- `tests/common/container.rs:162` - MutexGuard across await

### Unnecessary to_string (15+ errors)
- Multiple test files calling .to_string() unnecessarily
- Pattern: `.create_queue(&"queue_name".to_string(), false)`
- Fix: `.create_queue("queue_name", false)`

### Bool Assert Comparison (2 errors)
- `tests/lib_tests.rs:48` - assert_eq! with literal bool
- `tests/lib_tests.rs:86` - assert_eq! with literal bool

### Match Result OK (1 error)
- `tests/common/container.rs:92` - redundant ok() matching

### Needless Borrows (1 error)
- `tests/common/pgbouncer.rs:54` - unnecessary array borrow

## Current Status
- [x] Security fixes (SQL identifier quoting)
- [x] Core clippy errors (admin.rs)
- [ ] Test clippy errors (25+ remaining)
- [ ] Build verification

## Next Steps
1. Fix dead code warnings (allow or remove)
2. Fix unnecessary to_string calls in tests
3. Fix await holding lock issues
4. Fix remaining minor clippy issues
5. Verify all tests pass with strict clippy