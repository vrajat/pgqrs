# Rust Executors

This directory is reserved for Rust benchmark executors.

It is intentionally not wired into the Cargo workspace yet.

When the first Rust executor is implemented:

1. decide whether it should become a workspace member
2. add a real `Cargo.toml`
3. keep Rust execution logic here, not in the Python orchestrator

The expected shape is:

- `main.rs` for a small executor binary
- shared queue helpers
- shared workflow helpers
