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
    scenario_path: str
    backends: tuple[str, ...]
    bindings: tuple[str, ...]
    profiles: tuple[str, ...]
    executor_hint: str


SCENARIOS: tuple[ScenarioRegistration, ...] = (
    ScenarioRegistration(
        scenario_id="queue.e2e.steady_state",
        scenario_path="benchmarks/scenarios/queue/e2e_steady_state.toml",
        backends=("postgres", "sqlite", "turso"),
        bindings=("rust", "python"),
        profiles=("compat", "single_process"),
        executor_hint="queue",
    ),
    ScenarioRegistration(
        scenario_id="queue.drain_fixed_backlog",
        scenario_path="benchmarks/scenarios/queue/drain_fixed_backlog.toml",
        backends=("postgres", "sqlite", "turso", "s3"),
        bindings=("rust", "python"),
        profiles=("compat", "single_process"),
        executor_hint="queue",
    ),
    ScenarioRegistration(
        scenario_id="queue.e2e.capacity_knee",
        scenario_path="benchmarks/scenarios/queue/e2e_capacity_knee.toml",
        backends=("postgres", "sqlite", "turso"),
        bindings=("rust", "python"),
        profiles=("single_process",),
        executor_hint="queue",
    ),
)


def get_registration(scenario_id: str) -> ScenarioRegistration:
    for scenario in SCENARIOS:
        if scenario.scenario_id == scenario_id:
            return scenario
    raise KeyError(f"Unknown scenario: {scenario_id}")
