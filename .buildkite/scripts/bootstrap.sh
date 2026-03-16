#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required on the Buildkite agent" >&2
  exit 1
fi

if ! command -v rustfmt >/dev/null 2>&1; then
  if command -v rustup >/dev/null 2>&1; then
    rustup component add rustfmt
  else
    echo "rustfmt is required and rustup is not available to install it" >&2
    exit 1
  fi
fi

if ! command -v cargo-nextest >/dev/null 2>&1; then
  cargo install cargo-nextest --locked
fi

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="${HOME}/.local/bin:${PATH}"
fi
