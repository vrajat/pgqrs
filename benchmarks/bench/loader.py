"""Load scenario specs and expand sweep points."""

from __future__ import annotations

import itertools
import tomllib
from pathlib import Path
from typing import Any

from benchmarks.bench.schema import ScenarioAction, ScenarioAnswer, ScenarioSpec


def load_scenario(path: str | Path) -> ScenarioSpec:
    raw = tomllib.loads(Path(path).read_text())

    return ScenarioSpec(
        scenario_id=raw["id"],
        kind=raw["kind"],
        question=raw["question"],
        backends=tuple(raw["backends"]),
        bindings=tuple(raw["bindings"]),
        profiles=tuple(raw["profiles"]),
        action=ScenarioAction(
            fixed=dict(raw.get("action", {}).get("fixed", {})),
            variables={
                key: list(values)
                for key, values in raw.get("action", {}).get("variables", {}).items()
            },
        ),
        answer=ScenarioAnswer(
            kind=raw["answer"]["kind"],
            primary=raw["answer"]["primary"],
            series=list(raw["answer"]["series"]),
            vary_by=list(raw["answer"]["vary_by"]),
        ),
        notes=dict(raw.get("notes", {})),
    )


def expand_points(spec: ScenarioSpec) -> list[dict[str, Any]]:
    if not spec.action.variables:
        return [{}]

    keys = tuple(spec.action.variables.keys())
    value_lists = [spec.action.variables[key] for key in keys]

    return [
        dict(zip(keys, combo, strict=True)) for combo in itertools.product(*value_lists)
    ]
