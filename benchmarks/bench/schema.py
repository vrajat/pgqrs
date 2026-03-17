"""Canonical benchmark data structures.

These are intentionally minimal placeholders.
When the first real scenario is implemented, expand these types before
adding scenario-specific result formats.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RunSpec:
    scenario_id: str
    backend: str
    binding: str
    profile: str
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunResult:
    metadata: dict[str, Any]
    samples: list[dict[str, Any]]
    summary: dict[str, Any]
