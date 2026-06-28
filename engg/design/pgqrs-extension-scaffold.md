# Mini-Design: pgqrs-extension pgrx Scaffold and Install Path

## Overview
This document specifies the scaffolding and local installation/upgrading workflow for the `pgqrs-extension` PostgreSQL extension using `pgrx`. It establishes the build configuration, crate structure, control parameters, and versioning for self-hosted pgqrs deployments.

---

## 1. Crate Structure & Directory Layout

The extension lives in a new subdirectory under `crates/`:

```text
crates/pgqrs-extension/
├── Cargo.toml
├── pgqrs-extension.control
└── src/
    └── lib.rs
```

### pgqrs-extension.control
```ini
comment = 'pgqrs extension - Postgres-native durable task queues and workflows'
default_version = '0.1.0'
module_pathname = '$libdir/pgqrs_extension'
relocatable = false
superuser = false
```

---

## 2. Cargo Configuration

The crate `pgqrs-extension` uses `pgrx` to compile a dynamically loaded Postgres module.

### crates/pgqrs-extension/Cargo.toml
```toml
[package]
name = "pgqrs-extension"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib", "lib"]

[dependencies]
pgrx = { version = "0.12.6", features = ["pg15"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

[features]
default = ["pg15"]
pg15 = ["pgrx/pg15", "pgrx/pg15-test-utils"]
pg16 = ["pgrx/pg16", "pgrx/pg16-test-utils"]
```

We default to PostgreSQL 15 (`pg15`) to align with our local test environment (`postgres:15-alpine`).

---

## 3. SQL API & Version Verification

For this scaffolding phase, the extension exports a minimal health and version verification function:

```rust
use pgrx::prelude::*;

pgrx::pg_module_magic!();

/// Get the compiled version of pgqrs-extension
#[pg_extern]
fn pgqrs_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Simple health check function
#[pg_extern]
fn pgqrs_health_check() -> bool {
    true
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_pgqrs_version() {
        let version = crate::pgqrs_version();
        assert_eq!(version, "0.1.0");
    }

    #[pg_test]
    fn test_pgqrs_health_check() {
        assert!(crate::pgqrs_health_check());
    }
}
```

---

## 4. Local Development & Makefile Targets

We introduce targets in the main `Makefile` to simplify local compilation, testing, and deployment:

### Makefile Targets
- `pgrx-init`: Install `cargo-pgrx` and run `cargo pgrx init` targeting local postgres installations.
- `pgrx-build`: Build the extension using `cargo pgrx run`.
- `pgrx-test`: Run integration tests inside Postgres using `cargo pgrx test`.
- `pgrx-install`: Copy built binaries and control files to the local Postgres installation directory.

---

## 5. Architectural Coexistence & Evolution

- **Decoupled Architecture**: In hosted environments (e.g. AWS Aurora, Supabase, Neon), the `pgqrs-extension` is **not required**. Instead, `pgqrs-admin` and `pgqrs-sql-worker` run as external coordinator and executor processes.
- **Self-Hosted Architecture**: In self-hosted Postgres environments, compiling and installing `pgqrs-extension` enables native background workers and SQL interfaces directly inside the DB server, avoiding the need for external coordinator daemons.
- **Migration Coexistence**: The extension coexists with the standard library database migrations. The migrations bootstrap the underlying tables (`pgqrs_messages`, `pgqrs_workflow_runs`, etc.) and basic PL/pgSQL helpers, while the extension coordinates and automates execution natively.
