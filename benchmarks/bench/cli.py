"""CLI entrypoint for the benchmark program.

This module is intentionally lightweight for now.
It defines the shape of the command surface without locking us into
scenario-specific behavior too early.
"""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pgqrs-bench")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List known benchmark scenarios")

    run_parser = subparsers.add_parser("run", help="Run a benchmark scenario")
    run_parser.add_argument("--scenario", required=True)
    run_parser.add_argument("--backend", required=True)
    run_parser.add_argument("--binding", required=True)
    run_parser.add_argument("--profile", default="compat")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        print("TODO: list scenarios from benchmarks.bench.registry")
        return 0

    if args.command == "run":
        print(
            "TODO: run scenario="
            f"{args.scenario} backend={args.backend} binding={args.binding} profile={args.profile}"
        )
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
