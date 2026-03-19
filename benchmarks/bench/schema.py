"""Canonical benchmark data structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ScenarioAction:
    fixed: dict[str, Any]
    variables: dict[str, list[Any]]


@dataclass(frozen=True)
class ScenarioAnswer:
    kind: str
    primary: str
    series: list[str]
    vary_by: list[str]


@dataclass(frozen=True)
class ScenarioSpec:
    scenario_id: str
    kind: str
    question: str
    backends: tuple[str, ...]
    bindings: tuple[str, ...]
    profiles: tuple[str, ...]
    action: ScenarioAction
    answer: ScenarioAnswer
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    scenario_id: str
    backend: str
    binding: str
    profile: str
    question: str
    fixed_parameters: dict[str, Any] = field(default_factory=dict)
    point_parameters: dict[str, Any] = field(default_factory=dict)
    output_path: str | None = None


@dataclass(frozen=True)
class RunPointResult:
    metadata: dict[str, Any]
    summary: dict[str, Any]
    samples: list[dict[str, Any]] = field(default_factory=list)
