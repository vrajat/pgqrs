#!/usr/bin/env bash
set -euo pipefail

if ! command -v mise >/dev/null 2>&1; then
  echo "mise is required on the Buildkite agent" >&2
  exit 1
fi

mise install
