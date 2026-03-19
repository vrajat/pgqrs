"""Console reporting helpers for benchmark runs."""

from __future__ import annotations

import logging
from typing import Any, Protocol

import typer

from benchmarks.bench.schema import RunPointResult


class RunObserver(Protocol):
    """Observer interface for progress and summary reporting."""

    def point_started(
        self,
        *,
        index: int,
        total: int,
        point_parameters: dict[str, Any],
    ) -> None: ...

    def phase_started(self, *, name: str, total: int) -> None: ...

    def phase_progress(self, *, name: str, current: int, total: int) -> None: ...

    def phase_finished(self, *, name: str) -> None: ...

    def point_finished(self, *, result: RunPointResult) -> None: ...

    def close(self) -> None: ...


class TyperEchoHandler(logging.Handler):
    """Log handler that routes records through typer's terminal output."""

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        typer.echo(message, err=record.levelno >= logging.WARNING)


def configure_logging(*, verbose: bool) -> None:
    """Configure benchmark logging for terminal use."""

    logger = logging.getLogger("benchmarks")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO if verbose else logging.WARNING)

    handler = TyperEchoHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)


class NullObserver:
    """Observer that discards all events."""

    def point_started(
        self,
        *,
        index: int,
        total: int,
        point_parameters: dict[str, Any],
    ) -> None:
        del index, total, point_parameters

    def phase_started(self, *, name: str, total: int) -> None:
        del name, total

    def phase_progress(self, *, name: str, current: int, total: int) -> None:
        del name, current, total

    def phase_finished(self, *, name: str) -> None:
        del name

    def point_finished(self, *, result: RunPointResult) -> None:
        del result

    def close(self) -> None:
        return None


class TyperRunObserver:
    """Progress-bar and summary output for sequential benchmark points."""

    def __init__(self, *, show_progress: bool) -> None:
        self._show_progress = show_progress
        self._active_phase: str | None = None
        self._progress_cm: Any | None = None
        self._progress_bar: Any | None = None
        self._current = 0
        self._total = 0

    def point_started(
        self,
        *,
        index: int,
        total: int,
        point_parameters: dict[str, Any],
    ) -> None:
        params = ", ".join(f"{key}={value}" for key, value in point_parameters.items())
        typer.echo(f"[{index}/{total}] {params}")

    def phase_started(self, *, name: str, total: int) -> None:
        self.phase_finished(name=name)
        if not self._show_progress:
            return

        self._active_phase = name
        self._current = 0
        self._total = total
        self._progress_cm = typer.progressbar(length=total, label=f"{name:<8}")
        self._progress_bar = self._progress_cm.__enter__()

    def phase_progress(self, *, name: str, current: int, total: int) -> None:
        if not self._show_progress:
            return
        if self._active_phase != name:
            self.phase_started(name=name, total=total)
        if self._progress_bar is None:
            return

        target = max(0, min(current, total))
        delta = target - self._current
        if delta > 0:
            self._progress_bar.update(delta)
            self._current = target

    def phase_finished(self, *, name: str) -> None:
        del name
        if not self._show_progress or self._progress_bar is None:
            return

        if self._current < self._total:
            self._progress_bar.update(self._total - self._current)

        self._progress_cm.__exit__(None, None, None)
        self._active_phase = None
        self._progress_cm = None
        self._progress_bar = None
        self._current = 0
        self._total = 0

    def point_finished(self, *, result: RunPointResult) -> None:
        summary = result.summary
        typer.echo(
            "  "
            f"throughput={summary['drain_messages_per_second']} msg/s "
            f"drain_ms={summary['total_drain_time_ms']} "
            f"errors={summary['error_rate']}"
        )

    def close(self) -> None:
        self.phase_finished(name=self._active_phase or "")
