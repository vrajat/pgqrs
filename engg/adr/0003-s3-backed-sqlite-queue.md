# ADR-0003: S3-Backed SQLite Queue

## Status

Proposed

## Context

pgqrs currently supports Postgres (utilizing `SKIP LOCKED` for robust transactional queues) and SQLite/Turso (polling-based). For deployments that require high scale and low cost without provisioning a persistent Postgres cluster, we want to adopt a brokered group-commit pattern against object storage.

Instead of maintaining a JSON array of queue states (which requires manual parsing and lacks query capabilities, as seen in alternative minimalist object storage queues like Turbopuffer's approach), we aim to maintain the state as a single `.sqlite` binary database file. This allows pgqrs to reuse its existing sophisticated relational models for routing, prioritization, and dead-letter queues.

## Decision

We will implement a Brokered Group Commit architecture against S3/Object Storage using a raw `.sqlite` binary file.

1. **In-Memory State**: The broker maintains the queue state in an in-memory SQLite database.
2. **Group Commit Loop**: The broker downloads the `.sqlite` file on startup (recording its `ETag`). It buffers traffic locally, modifying the in-memory database. Periodically, it serializes the entire in-memory SQLite database and uploads it to S3 using Conditional Writes (`If-Match: <ETag>`), then acknowledges pending client requests.
3. **Stateless High-Availability**: If the broker encounters a CAS failure (due to another broker taking over), it completely re-downloads the database and replays its pending operations.
4. **Tombstone Deflation**: The broker must continuously track database fragmentation and run `PRAGMA incremental_vacuum;` or `VACUUM` before shipping to S3 if the active row count drops significantly compared to the file size.
5. **Library-Only Implementation**: `pgqrs` itself remains purely a library. It will not spawn background daemons. Instead, it will expose `admin` functions (e.g., `pull_from_s3`, `batch_operations`, `flush_to_s3`) that allow an application to implement a broker as a thin wrapper loop.

## Consequences

### Positive
- **Cost & Scale**: Leverages cheap, infinitely scalable object storage.
- **Rich Querying**: Retains all the relational querying power of pgqrs (dead-letter queues, priorities) without reinventing parsing logic for JSON models.
- **Library-Only Philosophy Alignment**: The "broker" does not require a standalone daemon bundled by pgqrs. Any active pgqrs application instance can act as the leader broker by calling the exposed library functions in a loop, utilizing S3 CAS for leader election.

### Negative
- **Latency**: Every client request incurs the latency of an S3 PUT (~50-100ms) because requests block until the group commit lands.
- **Write Amplification**: When the queue is under load, the broker repeatedly uploads the *entire* database file, not just the diffs, which can waste S3 bandwidth if the database grows too large.

### Neutral
- Standard binary format guarantees high read performance on boot but requires explicit `VACUUM` commands to prevent file bloat on S3.

## Alternatives Considered

### Alternative 1: JSON Array on S3
- **Description**: Maintaining the queue state as a flat JSON array string in S3.
- **Pros**: Easy to parse, human-readable, shrinks automatically when elements are popped.
- **Cons**: Lacks query capabilities; requires manual parsing and filtering instead of standard SQL. CPU pressure to deserialize entire arrays under load.
- **Reason for rejection**: Fails to leverage the existing `pgqrs` relational models, reinventing robust algorithms that we already have in SQL.

### Alternative 2: libSQL/Bottomless WAL Shipping to S3
- **Description**: Using Turso's libSQL + Bottomless to continuously stream SQLite WAL diffs to S3 instead of replacing the entire file.
- **Pros**: Solves write amplification by only uploading diffs.
- **Cons**: Requires tighter coupling with libSQL specific extensions and infrastructure. Currently, we want to explicitly avoid exploring WAL shipping mechanisms.
- **Reason for rejection**: Excluded from scope based on current technical direction.

## References

- [Turbopuffer Object Storage Queue Blog](https://turbopuffer.com/blog/object-storage-queue)

---

**Date**: 2026-02-20
**Author(s)**: Antigravity
**Reviewers**: Rajat Venkatesh
