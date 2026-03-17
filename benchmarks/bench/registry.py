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


SCENARIOS: tuple[ScenarioRegistration, ...] = ()
