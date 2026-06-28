# Design Documents

Comprehensive technical design documentation for major features and components.

## What Goes Here

- Detailed system architecture
- API design specifications
- Data model designs
- Backend implementation details
- Integration patterns
- Performance considerations

## Current Documents

- [Postgres-Only Extension Runtime Implementation Plan](postgres-only-extension-runtime-implementation.md)
- [Postgres-Only Shared Worker Protocol Design](postgres-only-worker-protocol.md)
- [SQL API, Workflow Triggering, and Scheduler Design](pgqrs-sql-api-workflow-scheduler.md)
- [Durable Workflows Engineering Design](durable-workflows-engineering-design.md)
- [Durable Workflows Product Requirements](durable-workflows-product-requirements.md)
- [Queue and Workflow Benchmarking Strategy](queue-workflow-benchmarking.md)
- [pgqrs-admin Coordinator and Scheduler Design](pgqrs-admin-coordinator.md)
- [pgqrs-sql-worker External SQL Executor Design](pgqrs-sql-worker.md)
- [pgqrs-extension pgrx Scaffold and Install Path Design](pgqrs-extension-scaffold.md)




## Historical / Deprecated Documents (ADR-0004 Postgres-Only Transition)

- [Database Abstraction Design](database_abstraction_design.md)
- [Store Access and Serialized DB Design](store_access_and_serialized_db.md)
- [SQLite Store Implementation](sqlite_store.md)
- [Turso Store Implementation](turso_store.md)
- [Store SQL Dialect Constants](store-sql-dialect-constants.md)

## Design Doc Template

Each design doc should include:

1. **Overview** - What is being designed
2. **Goals & Non-Goals** - What's in/out of scope
3. **Architecture** - High-level design
4. **Detailed Design** - Implementation details
5. **API/Interface** - Public APIs
6. **Data Model** - Database schema, data structures
7. **Error Handling** - How errors are handled
8. **Testing Strategy** - How it will be tested
9. **Migration/Rollout** - How to deploy
10. **Alternatives Considered** - What else was evaluated
11. **Open Questions** - Unresolved issues

## Living Documents

Design docs are living documents - they can be updated as implementations evolve and new insights are gained.
