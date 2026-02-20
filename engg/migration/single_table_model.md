# Migration Note: Single-Table Message Model

## Overview
pgqrs has migrated from a two-table model (`pgqrs_messages` + `pgqrs_archive`) to a single-table model. All messages (ready, leased, and archived) now reside in the `pgqrs_messages` table.

## Changes
- Added `archived_at` column to `pgqrs_messages` (TIMESTAMP/TEXT depending on backend).
- `archived_at IS NULL` indicates "hot" messages (ready or leased).
- `archived_at IS NOT NULL` indicates archived messages (acked or moved to DLQ).
- `dequeued_at` now represents the timestamp of the *first* lease only and is never reset.
- `read_ct` remains the source of truth for poison message detection.

## Invariants
1. **Hot Set**: Dequeue and active message queries always filter for `archived_at IS NULL`.
2. **Poison Messages**: Dequeue filters for `read_ct < max_attempts` even if `archived_at` is null (DLQ is finalized asynchronously).
3. **Zombie Release**: Zombie worker reclamation clears `consumer_worker_id` and sets `vt=now()` for in-flight unarchived messages, but never touches archived rows or clears `dequeued_at`.
4. **Stable History**: `dequeued_at` preserves the first time a message was picked up, providing better visibility into message lifecycle.

## Backend Support
This change is implemented and verified across all supported backends:
- PostgreSQL
- SQLite
- Turso

## API Compatibility
The public API remains stable. The `ArchivedMessage` struct and related methods now read from the unified `pgqrs_messages` table where `archived_at` is set.
