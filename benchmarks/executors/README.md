# Executors

Executors contain benchmark-specific hot-path logic.

Rules:

- keep orchestration out of executors
- keep result schema out of executors
- keep backend reset and environment setup out of executors

Executors should focus on running the scenario workload and returning samples.
