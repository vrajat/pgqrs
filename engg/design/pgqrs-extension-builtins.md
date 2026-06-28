# Mini-Design: pgqrs-extension Built-In Executor Capabilities

## Overview
This document specifies the design for executing SQL-shaped tasks, workflows, and built-in system capabilities directly within `pgqrs-extension` via the resident background worker. This provides a lightweight, self-contained executor architecture that functions without needing external Python or Rust daemon processes.

---

## 1. Built-In Capabilities Registry

We expose an inspection SQL function `pgqrs_builtins()` that lists the available built-in capabilities and their versions:

```sql
SELECT * FROM pgqrs_builtins();
```

| capability | version | description |
| --- | --- | --- |
| `sql` | `1.0.0` | Standalone raw SQL statement execution |
| `timer` | `1.0.0` | Non-blocking execution delays and sleeps |
| `maintenance` | `1.0.0` | Force execution of zombie lease reclamation and sweep |
| `metrics` | `1.0.0` | Log current queue size, worker status, and delay statistics |

---

## 2. GUC Configuration

To control which queues the built-in worker listens to:

| GUC Name | Type | Default | Description |
| --- | --- | --- | --- |
| `pgqrs.builtin_queues` | string | `""` | Comma-separated list of queues to poll and execute via built-in handler. |

If `pgqrs.builtin_queues` is configured (e.g. `"sql-tasks,sys-maintenance"`), the coordinator background worker registers itself as a consumer for these queues and dequeues messages in its main loop.

---

## 3. Dequeue and Execution Flow

In each iteration of the main loop, the worker:
1. Performs heartbeat update for each registered queue worker:
   `pgqrs-builtin-worker-<database>-<queue>`
2. Dequeues up to 1 message per queue.
3. Checks if the message corresponds to a workflow run (the queue name matches a registered workflow name in `pgqrs_workflows`).
   - If yes: Executes the workflow run function `SELECT {workflow_name}(run_id, input)`. On success, commits the run and archives the trigger message. On error, records error details.
   - If no: Processes the standalone capability defined by payload properties.

### Processing Standalone Capabilities
* **`sql`**:
  If payload contains `statement`:
  - Run transaction safety check (fail on `BEGIN`, `COMMIT`, `ROLLBACK`, `ABORT`, `SAVEPOINT`).
  - Set local statement timeout.
  - Bind dynamic JSON arguments (`params`) to SPI datums.
  - Execute statement, serialize output rows (capped at 100 rows), and archive message on success.
* **`timer`**:
  If payload contains `"capability": "timer"`:
  - Reads `"duration_ms"` (default: `1000`) and sleeps.
* **`maintenance`**:
  If payload contains `"capability": "maintenance"`:
  - Force triggers a maintenance sweep (marking stale workers, reclaiming leases).
* **`metrics`**:
  If payload contains `"capability": "metrics"`:
  - Computes counts of pending, active, and DLQ'd messages and writes to the server log.
