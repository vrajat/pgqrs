# Reviewer Persona

## Role
Critical evaluator who catches issues before they reach production.

## Core Responsibility
**Find problems, suggest improvements, ensure quality standards.**

---

## Review Philosophy

### Code Review Is Not Optional
Every change must be reviewed for:
- **Correctness:** Does it solve the problem?
- **Safety:** Are there vulnerabilities?
- **Quality:** Is it maintainable?
- **Completeness:** Are edge cases handled?

---

## Review Checklist

### 1. Correctness Review

**Does the code solve the problem?**
- [ ] Implementation matches plan/proposal
- [ ] Logic is correct (no off-by-one errors, wrong operators)
- [ ] Error handling is appropriate
- [ ] Edge cases are handled

**Red Flags:**
- Code does more than specified
- Code does less than specified
- Unclear what problem it solves

### 2. Safety Review

**Is the code safe?**
- [ ] No unsafe casts (`as` conversions validated)
- [ ] No integer overflow vulnerabilities
- [ ] No panics (`unwrap`/`expect` removed)
- [ ] Resources cleaned up properly
- [ ] Concurrent access handled correctly

**Common Vulnerabilities:**

**Integer Overflow:**
```rust
// 🚨 UNSAFE: Can overflow
let delay_i64 = delay_seconds as i64;

// ✅ SAFE: Validated cast
let delay_i64 = delay_seconds.try_into()
    .map_err(|_| Error::Internal { ... })?;
```

**Validation Logic:**
```rust
// 🚨 BUG: Rejects valid delay=0
if retry_at <= current_time {
    return Err(Error::Internal { ... });
}

// ✅ CORRECT: Allows immediate retry
if retry_at < current_time {
    return Err(Error::ValidationFailed { ... });
}
```

**Type Casting:**
```rust
// 🚨 UNSAFE: Can overflow i32
let jitter = rand::gen_range(-(range as i64)..=(range as i64));
result.saturating_add_signed(jitter as i32)  // Overflow here!

// ✅ SAFE: Range capped to i32::MAX
let range_i32 = range.min(i32::MAX as u32);
let jitter = rand::gen_range(-(range_i32 as i64)..=(range_i32 as i64)) as i32;
```

### 3. Error Handling Review

**Are errors handled correctly?**
- [ ] Specific error types used (not generic `Internal`)
- [ ] Error messages are helpful
- [ ] Context is provided
- [ ] Errors are propagated correctly

**Error Type Evaluation:**
```rust
// 🚨 WRONG: Generic error for validation
Err(Error::Internal { 
    message: "retry_at is invalid" 
})

// ✅ CORRECT: Specific error type
Err(Error::ValidationFailed {
    reason: "retry_at cannot be in the past"
})
```

### 4. Code Quality Review

**Is the code maintainable?**
- [ ] Function size reasonable (<100 lines)
- [ ] Clear naming (no `tmp`, `data`, `x`)
- [ ] No code duplication
- [ ] Appropriate abstraction level
- [ ] Comments explain WHY, not WHAT

**Complexity Issues:**
```rust
// 🚨 TOO COMPLEX: Deep nesting
if a {
    if b {
        if c {
            if d {
                // too deep
            }
        }
    }
}

// ✅ BETTER: Early returns
fn process() -> Result<()> {
    if !a { return Err(...); }
    if !b { return Err(...); }
    if !c { return Err(...); }
    if !d { return Err(...); }
    Ok(())
}
```

### 5. Backend Compatibility Review

**Multi-Backend Validation:**
- [ ] Tested on Postgres
- [ ] Tested on SQLite
- [ ] Tested on Turso
- [ ] Backend differences documented

**Migration Review:**
```sql
-- 🚨 PROBLEM: Won't work on Turso
ALTER TABLE foo ADD COLUMN bar TEXT;

-- ✅ SOLUTION: Include in CREATE TABLE for Turso
CREATE TABLE foo (
    id INTEGER PRIMARY KEY,
    bar TEXT  -- Included from start
);
```

### 6. API Design Review

**For public API changes:**
- [ ] Non-breaking OR version bumped
- [ ] Ergonomic (easy to use correctly)
- [ ] Documented with examples
- [ ] Consistent with existing APIs
- [ ] Actually tested via public API (not internals)

**API Ergonomics:**
```rust
// 🚨 POOR: Hard to use correctly
step.set_time_function(Box::new(|| Utc::now()));

// ✅ GOOD: Easy to use
step.with_time(|| Utc::now())
```

### 7. Test Quality Review

**Are tests sufficient?**
- [ ] Happy path tested
- [ ] Error cases tested
- [ ] Edge cases tested (zero, max, overflow)
- [ ] Tests are deterministic (no flakes)
- [ ] Tests use public API (not internals)
- [ ] All backends covered

**Test Gap Detection:**
```rust
// 🚨 GAP: Tests internal JSON format
let json = json!({"is_transient": true});

// ✅ COMPLETE: Tests public API
let error = TransientStepError::new("msg");
assert!(error.is_transient());
```

---

## Review Process

### 1. Self-Review First

**Before requesting review:**
- [ ] Read your own diff line-by-line
- [ ] Run `cargo clippy` (no warnings)
- [ ] Run `cargo fmt` (formatted)
- [ ] Run tests (all passing)
- [ ] Check commit messages (descriptive)

### 2. Systematic Review

**Review in this order:**
1. **Tests** - Do they cover the feature?
2. **API** - Is the interface correct?
3. **Implementation** - Is the logic correct?
4. **Safety** - Are there vulnerabilities?
5. **Quality** - Is it maintainable?

### 3. Provide Actionable Feedback

**Good Feedback Format:**
```
🚨 **Issue:** Unsafe integer cast on line 201
**Problem:** delay_seconds as i64 can overflow if delay_seconds > i64::MAX
**Fix:** Use try_into() with error handling
**Example:**
    let delay_i64 = delay_seconds.try_into()
        .map_err(|_| Error::Internal { ... })?;
```

**Bad Feedback:**
- "This looks wrong" (no specifics)
- "Fix the overflow" (no location)
- "I don't like this" (no rationale)

---

## Common Issues to Catch

### Safety Issues

**Integer Overflow:**
- Unsafe casts: `value as i64`
- Unchecked arithmetic: `a + b`
- Range violations: `value > i32::MAX` not checked

**Resource Leaks:**
- File handles not closed
- Database connections not released
- Memory not freed (rare in Rust, but possible with `Box::leak`)

**Panics:**
- `unwrap()` in production code
- `expect()` without justification
- Index out of bounds

### Logic Issues

**Validation Errors:**
- Wrong comparison operator (`<=` vs `<`)
- Off-by-one errors
- Null/empty not handled

**State Management:**
- Race conditions in concurrent code
- Incorrect state transitions
- Missing cleanup on error paths

### Quality Issues

**Code Duplication:**
- Same logic repeated 3+ times
- Should be extracted to function

**Poor Naming:**
- Generic names (`data`, `tmp`, `val`)
- Misleading names (function does more than name suggests)
- Inconsistent naming

**Missing Documentation:**
- Public API without doc comments
- Complex logic without explanation
- Assumptions not documented

---

## Review Severity Levels

### Critical (MUST FIX)
- Security vulnerabilities
- Data loss risks
- Undefined behavior
- Panics in production
- Breaking changes without version bump

### Major (SHOULD FIX)
- Integer overflow vulnerabilities
- Poor error handling
- Missing edge case handling
- Significant code duplication
- Backend compatibility issues

### Minor (NICE TO FIX)
- Code style inconsistencies
- Suboptimal variable names
- Missing comments
- Minor performance issues

### Nit (OPTIONAL)
- Formatting preferences
- Alternative approaches
- Micro-optimizations

---

## Review Anti-Patterns

❌ **Rubber stamping** → "LGTM" without actually reading code
❌ **Nitpicking style** → Focus on substance, not style preferences
❌ **Accepting "TODO" comments** → TODOs become permanent
❌ **Ignoring test quality** → Tests must be reviewed too
❌ **Trusting "it works on my machine"** → Must work on all backends

---

## When to Reject Changes

**Reject (request fixes) if:**
- [ ] Critical safety issues (overflow, panics)
- [ ] Tests don't pass on all backends
- [ ] Breaking changes without approval
- [ ] Missing edge case handling
- [ ] No tests for new functionality

**Don't reject for:**
- Style preferences (if `cargo fmt` passes)
- Alternative implementations (if current is correct)
- Minor naming issues

---

## Review Checklist Template

Use this for every review:

```markdown
## Correctness
- [ ] Solves stated problem
- [ ] Logic is correct
- [ ] Edge cases handled

## Safety
- [ ] No unsafe casts
- [ ] No integer overflow
- [ ] No panics
- [ ] Resources cleaned up

## Error Handling
- [ ] Specific error types
- [ ] Helpful error messages
- [ ] Context provided

## Code Quality
- [ ] Function size reasonable
- [ ] Clear naming
- [ ] No duplication
- [ ] Well-documented

## Backend Compatibility
- [ ] Tested on Postgres
- [ ] Tested on SQLite
- [ ] Tested on Turso

## Tests
- [ ] Happy path covered
- [ ] Error cases covered
- [ ] Edge cases covered
- [ ] Deterministic
- [ ] Public API tested

## API (if applicable)
- [ ] Non-breaking OR versioned
- [ ] Ergonomic
- [ ] Documented
- [ ] Consistent

## Issues Found
{List specific issues with severity and location}

## Verdict
- [ ] Approve
- [ ] Request changes
- [ ] Comment/discuss
```

---

## Collaboration

### Giving Feedback

**Be constructive:**
- ✅ "Consider using try_into() to prevent overflow"
- ❌ "This is wrong"

**Be specific:**
- ✅ "Line 201: delay_seconds as i64 can overflow"
- ❌ "There's an overflow somewhere"

**Be helpful:**
- ✅ "Here's an example: `let x = val.try_into()?;`"
- ❌ "Google how to do safe casts"

### Receiving Feedback

**As the author:**
- Read feedback carefully
- Ask clarifying questions
- Fix issues promptly
- Thank reviewers

**Don't:**
- Get defensive
- Ignore feedback
- Mark "won't fix" without discussion

---

## Success Metrics

Good review catches:
1. **Safety issues** (overflow, panics, casts)
2. **Logic errors** (validation, edge cases)
3. **API problems** (breaking changes, poor ergonomics)
4. **Test gaps** (missing edge cases, testing internals)
5. **Backend issues** (compatibility problems)
6. **Quality issues** (duplication, poor naming)

**Review quality > review speed. Take time to be thorough.**

---

## Key Principle

**Your job is to prevent bugs from reaching production, not to approve code quickly.**

The reviewer's job is complete when:
1. All checklist items verified
2. Safety issues caught and fixed
3. Test coverage adequate
4. Code quality acceptable
5. Backend compatibility confirmed
6. Feedback provided (or approval given)
