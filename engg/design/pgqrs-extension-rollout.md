# Mini-Design: pgqrs-extension Packaging, CI Integration, and Rollout Plan

## Overview
This document specifies the integration, packaging, automated CI testing, operational maintenance, and safe production rollout checklist for `pgqrs-extension`.

---

## 1. Extension Packaging

The extension uses the standard `pgrx` packaging toolset. For production distribution:
1. Run `cargo pgrx package` to compile and bundle the release assets.
2. The packaged assets consist of:
   - `pgqrs_extension.control`: The extension control file specifying version and module load path.
   - `pgqrs_extension--<version>.sql`: Auto-generated SQL schema file defining functions, types, and worker hooks.
   - `pgqrs_extension.so` (or `.dylib` on macOS): Shared object library.
3. These are copied into target directories on the database server:
   - Control & SQL files -> `/usr/share/postgresql/<version>/extension/`
   - Shared library -> `/usr/lib/postgresql/<version>/lib/`

---

## 2. CI Testing Architecture

To guarantee reliability across updates:
1. **GitHub Actions**: Manual workflow validation verifies syntax formatting and unit checks.
2. **Buildkite CI Pipeline**: The primary automated environment installs `cargo-pgrx` and executes all unit, integration, and E2E extension tests:
   ```bash
   make pgrx-init pgrx-test
   ```
   This ensures that any change in the Rust core or schema triggers test validation on a real Postgres engine.

---

## 3. Production Rollout Checklist

### Step 1: Pre-installation
* Verify that Postgres version is 12 or above.
* Ensure superuser access to the database server.
* Backup existing client tables and schemas.

### Step 2: Install Extension Assets
Copy control, SQL, and shared library files to the designated paths of the database server.

### Step 3: Configure Shared Preload Libraries
Modify `postgresql.conf` to load the extension library at server startup:
```ini
shared_preload_libraries = 'pgqrs_extension'
```
*Note: Modifying `shared_preload_libraries` requires a database server restart.*

### Step 4: Configure GUC parameters
Add these to `postgresql.conf` (or using `ALTER SYSTEM`):
```ini
pgqrs.coordinator_enabled = true
pgqrs.database = 'postgres'
pgqrs.user = 'postgres'
pgqrs.coordinator_interval_ms = 1000
pgqrs.builtin_queues = 'sql-tasks'
```

### Step 5: Restart Postgres
Perform a rolling restart of the database cluster.

### Step 6: Create/Upgrade SQL Extension
Run inside the target database:
```sql
CREATE EXTENSION pgqrs_extension;
```

---

## 4. Version Compatibility Matrix

| Client Version (Rust/Python) | Extension Version | Compatibility Status |
| --- | --- | --- |
| `0.15.x` | `0.15.y` | Fully Compatible (compatible protocol schemas) |
| `0.15.x` | `0.16.y` | Incompatible (requires extension upgrade) |

---

## 5. Operations: Logs & Metrics

* **Logs**: Check `pg_log/` or system journal logs for coordinator outputs prefixed with `pgqrs`.
* **Built-in Metrics**: Enqueue a message to any queue in `pgqrs.builtin_queues` with `{"capability": "metrics"}` to print queue statistics directly to the postgres log.
* **Failover / Rollback**: If the background worker encounters issues, set `pgqrs.coordinator_enabled = false` and reload configuration (`SELECT pg_reload_conf();`). The separate external `pgqrs-admin` coordinator daemon can then be spun up to resume scheduling without needing a server reboot.
