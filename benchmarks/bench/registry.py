"""Scenario registry.

The registry is the single place that maps scenario IDs to:
- valid backends
- valid bindings
- valid profiles
- executor choice
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScenarioRegistration:
    scenario_id: str
    backends: tuple[str, ...]
    bindings: tuple[str, ...]
    profiles: tuple[str, ...]
    executor_hint: str


SCENARIOS: tuple[ScenarioRegistration, ...] = (
    ScenarioRegistration(
        scenario_id="queue.e2e.steady_state",
        backends=("postgres", "sqlite", "turso"),
        bindings=("rust", "python"),
        profiles=("compat", "single_process"),
        executor_hint="queue",
    ),
    ScenarioRegistration(
        scenario_id="queue.drain_fixed_backlog",
        backends=("postgres", "sqlite", "turso"),
        bindings=("rust", "python"),
        profiles=("compat", "single_process"),
        executor_hint="queue",
    ),
    ScenarioRegistration(
        scenario_id="queue.e2e.capacity_knee",
        backends=("postgres", "sqlite", "turso"),
        bindings=("rust", "python"),
        profiles=("single_process",),
        executor_hint="queue",
    ),
)
