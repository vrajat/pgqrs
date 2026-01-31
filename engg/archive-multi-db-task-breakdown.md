# Task: Add Multiple Database Support (Parent Issue #112)

- [x] Explore codebase to understand current database usage and architecture <!-- id: 0 -->
- [x] Analyze abstraction options (Producer/Consumer/Admin vs DB Traits) <!-- id: 1 -->
- [x] Create Design Document <!-- id: 2 -->
- [ ] **Phase 1: Core Abstraction**
    - [x] Define `Store` and generic repository traits in `src/store/mod.rs` <!-- id: 3 --> (Issue #104)
    - [x] Create `src/store` module structure <!-- id: 4 -->
- [ ] **Phase 2: Postgres Refactor**
    - [x] Implement `PostgresStore` and repositories in `src/store/postgres/` <!-- id: 5 --> (Issue #107)

- [x] **Phase 2a: Extend Store Traits (Issue #115)**
    - [x] Add administrative & worker support methods to `Store` traits <!-- id: 10 -->
    - [x] Implement extensions in `PostgresStore` <!-- id: 11 -->
    - [x] Verify extensions with `make check` and tests <!-- id: 12 -->

- [ ] **Phase 2b: Refactor Workers (Issue #105)**
    - [ ] Refactor `WorkerLifecycle` to use `Store` trait <!-- id: 7 -->
    - [ ] Refactor `Producer` and `Consumer` to use `Store` trait <!-- id: 6 -->
    - [ ] Refactor `Admin` to use `Store` trait <!-- id: 13 -->
    - [ ] Update `main.rs` and `lib.rs` to use `PostgresStore` <!-- id: 8 -->
    - [ ] Verify existing tests pass with `PostgresStore` <!-- id: 9 --> (Issue #110)
- [ ] **Phase 3: New Backends**
    - [ ] Implement `SqliteStore` (`feature = "sqlite"`) <!-- id: 10 --> (Issue #106)
    - [ ] Implement `TursoStore` (`feature = "turso"`) <!-- id: 11 --> (Issue #108)
- [ ] **Phase 4: Bindings & Cleanup**
    - [ ] Update Python bindings with `PyStore` enum wrappers <!-- id: 12 --> (Issue #113)
    - [ ] Add integration tests for all backends <!-- id: 13 --> (Issue #110)
- [ ] **Phase 5: Documentation & Verification**
    - [ ] Document new architecture and usage <!-- id: 14 --> (Issue #111)
