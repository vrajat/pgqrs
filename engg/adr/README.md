# Architecture Decision Records (ADRs)

## About ADRs

Architecture Decision Records document significant architectural decisions made in the project. Each ADR:

- Is **numbered sequentially** (0001, 0002, etc.)
- Is **immutable** once accepted (create a new ADR to supersede)
- Captures **context, decision, and consequences**
- Includes **alternatives considered**

## Format

Use the template in `template.md` when creating a new ADR.

## Index

### Accepted

- [ADR-0001: Scheduled Retry with retry_at Timestamps](0001-scheduled-retry-timestamps.md) - Non-blocking workflow step retries using database timestamps

### Superseded

_(Superseded ADRs remain here for historical reference)_

## Creating a New ADR

1. Copy `template.md` to `NNNN-title.md` (use next sequential number)
2. Fill in all sections
3. Set status to "Proposed"
4. Create PR for review
5. Update status to "Accepted" when merged

## Statuses

- **Proposed**: Under discussion
- **Accepted**: Decision has been made and implemented
- **Superseded**: Replaced by a newer ADR (link to the new one)
- **Deprecated**: No longer relevant
