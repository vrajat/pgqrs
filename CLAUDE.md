# Claude Development Agent

This file configures Claude when working with this repository.

## Load Order

1. `AGENTS.md`
2. `agents/README.md`
3. `agents/process.md`
4. `agents/context/technical-domain.md`
5. `agents/context/living-notes.md`

## Role Selection

- Planner: use for non-trivial design or before large changes
- Builder: use for implementation work
- Tester: use when validating behavior or expanding coverage
- Reviewer: use for code review and regression hunting

## Claude-Specific Notes

- Read complete files before editing when the file is small enough to do so safely.
- Treat this file as a thin wrapper, not the main instruction body.
- If prompt docs conflict with current code, `README.md`, or `Makefile`, treat the live repo as authoritative.
- In reviews, cite file and line references and focus on correctness, safety, regressions, and missing tests.
