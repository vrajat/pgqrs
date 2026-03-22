"""Generate static benchmark SVG charts for the docs from curated JSONL baselines."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BASELINE_DIR = ROOT / "benchmarks" / "data" / "baselines" / "queue.drain_fixed_backlog"
OUTPUT_DIR = ROOT / "docs" / "assets" / "benchmarks" / "queue-drain-fixed-backlog"

SERIES_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c"]
DISPLAY_NAMES = {
    "postgres": "PostgreSQL",
    "sqlite": "SQLite",
}


@dataclass(frozen=True)
class Point:
    x: int
    y: float


def _load_backend(backend: str) -> list[dict]:
    path = BASELINE_DIR / f"{backend}-rust-compat-release-20260321.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    flattened = []
    for row in rows:
        metadata = row["metadata"]
        point = metadata["point_parameters"]
        summary = row["summary"]
        flattened.append(
            {
                "consumers": int(point["consumers"]),
                "dequeue_batch_size": int(point["dequeue_batch_size"]),
                "drain_messages_per_second": float(
                    summary["drain_messages_per_second"]
                ),
                "p95_dequeue_latency_ms": float(summary["p95_dequeue_latency_ms"]),
                "p95_archive_latency_ms": float(summary["p95_archive_latency_ms"]),
            }
        )
    return flattened


def _group_points(
    rows: list[dict],
    *,
    x_key: str,
    y_key: str,
    series_key: str,
) -> list[tuple[str, list[Point]]]:
    values: dict[int, list[Point]] = {}
    for row in rows:
        series_value = int(row[series_key])
        values.setdefault(series_value, []).append(
            Point(x=int(row[x_key]), y=float(row[y_key]))
        )
    grouped = []
    for key in sorted(values):
        grouped.append((str(key), sorted(values[key], key=lambda point: point.x)))
    return grouped


def _svg_line_chart(
    *,
    title: str,
    x_label: str,
    y_label: str,
    series_label: str,
    series: list[tuple[str, list[Point]]],
    width: int = 720,
    height: int = 420,
) -> str:
    left = 80
    right = 180
    top = 54
    bottom = 56
    plot_width = width - left - right
    plot_height = height - top - bottom

    x_values = sorted({point.x for _, points in series for point in points})
    y_max = max(point.y for _, points in series for point in points)
    y_top = y_max * 1.1 if y_max > 0 else 1.0

    def x_pos(value: int) -> float:
        if len(x_values) == 1:
            return left + plot_width / 2
        index = x_values.index(value)
        return left + (index / (len(x_values) - 1)) * plot_width

    def y_pos(value: float) -> float:
        if y_top == 0:
            return top + plot_height
        return top + plot_height - (value / y_top) * plot_height

    grid_lines = []
    y_ticks = 5
    for i in range(y_ticks + 1):
        value = (y_top / y_ticks) * i
        y = y_pos(value)
        label = f"{value:,.0f}" if y_top >= 100 else f"{value:.1f}"
        grid_lines.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}" '
            'stroke="#e5e7eb" stroke-width="1"/>'
        )
        grid_lines.append(
            f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'font-size="12" fill="#374151">{label}</text>'
        )

    x_ticks = []
    for value in x_values:
        x = x_pos(value)
        x_ticks.append(
            f'<line x1="{x:.1f}" y1="{top + plot_height}" x2="{x:.1f}" y2="{top + plot_height + 6}" '
            'stroke="#6b7280" stroke-width="1"/>'
        )
        x_ticks.append(
            f'<text x="{x:.1f}" y="{top + plot_height + 24}" text-anchor="middle" '
            f'font-size="12" fill="#374151">{value}</text>'
        )

    paths = []
    legend_items = []
    legend_x = left + plot_width + 20
    legend_y = top + 8
    for idx, (name, points) in enumerate(series):
        color = SERIES_COLORS[idx % len(SERIES_COLORS)]
        path = " ".join(
            (
                ("M" if point_idx == 0 else "L")
                + f" {x_pos(point.x):.1f} {y_pos(point.y):.1f}"
            )
            for point_idx, point in enumerate(points)
        )
        paths.append(
            f'<path d="{path}" fill="none" stroke="{color}" stroke-width="3" '
            'stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for point in points:
            paths.append(
                f'<circle cx="{x_pos(point.x):.1f}" cy="{y_pos(point.y):.1f}" r="4" fill="{color}"/>'
            )
        legend_items.append(
            f'<line x1="{legend_x}" y1="{legend_y + idx * 26:.1f}" '
            f'x2="{legend_x + 24}" y2="{legend_y + idx * 26:.1f}" '
            f'stroke="{color}" stroke-width="3"/>'
            f'<text x="{legend_x + 34}" y="{legend_y + idx * 26 + 4:.1f}" '
            f'font-size="12" fill="#111827">{series_label} {name}</text>'
        )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{title}">
  <rect width="{width}" height="{height}" fill="#ffffff"/>
  <text x="{left}" y="28" font-size="22" font-weight="700" fill="#111827">{title}</text>
  <line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#111827" stroke-width="1.5"/>
  <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#111827" stroke-width="1.5"/>
  {"".join(grid_lines)}
  {"".join(x_ticks)}
  {"".join(paths)}
  {"".join(legend_items)}
  <text x="{left + plot_width / 2:.1f}" y="{height - 14}" text-anchor="middle" font-size="13" fill="#111827">{x_label}</text>
  <text x="22" y="{top + plot_height / 2:.1f}" text-anchor="middle" font-size="13" fill="#111827" transform="rotate(-90 22 {top + plot_height / 2:.1f})">{y_label}</text>
</svg>
"""


def _write_chart(name: str, svg: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / name).write_text(svg)


def export_backend(backend: str) -> None:
    rows = _load_backend(backend)
    display_name = DISPLAY_NAMES[backend]

    _write_chart(
        f"{backend}-throughput-vs-consumers.svg",
        _svg_line_chart(
            title=f"{display_name}: throughput vs consumers",
            x_label="Consumers",
            y_label="Throughput (msg/s)",
            series_label="Batch",
            series=_group_points(
                rows,
                x_key="consumers",
                y_key="drain_messages_per_second",
                series_key="dequeue_batch_size",
            ),
        ),
    )
    _write_chart(
        f"{backend}-throughput-vs-batch-size.svg",
        _svg_line_chart(
            title=f"{display_name}: throughput vs batch size",
            x_label="Batch size",
            y_label="Throughput (msg/s)",
            series_label="Consumers",
            series=_group_points(
                rows,
                x_key="dequeue_batch_size",
                y_key="drain_messages_per_second",
                series_key="consumers",
            ),
        ),
    )
    _write_chart(
        f"{backend}-dequeue-latency-vs-consumers.svg",
        _svg_line_chart(
            title=f"{display_name}: p95 dequeue latency vs consumers",
            x_label="Consumers",
            y_label="P95 dequeue latency (ms)",
            series_label="Batch",
            series=_group_points(
                rows,
                x_key="consumers",
                y_key="p95_dequeue_latency_ms",
                series_key="dequeue_batch_size",
            ),
        ),
    )
    _write_chart(
        f"{backend}-archive-latency-vs-consumers.svg",
        _svg_line_chart(
            title=f"{display_name}: p95 archive latency vs consumers",
            x_label="Consumers",
            y_label="P95 archive latency (ms)",
            series_label="Batch",
            series=_group_points(
                rows,
                x_key="consumers",
                y_key="p95_archive_latency_ms",
                series_key="dequeue_batch_size",
            ),
        ),
    )


def main() -> None:
    export_backend("postgres")
    export_backend("sqlite")
    print(f"Wrote charts to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
