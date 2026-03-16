#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required on the Buildkite agent" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
  export PATH="${HOME}/.cargo/bin:${PATH}"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo could not be installed on the Buildkite agent" >&2
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

if ! command -v cargo-clippy >/dev/null 2>&1; then
  if command -v rustup >/dev/null 2>&1; then
    rustup component add clippy
  else
    echo "clippy is required and rustup is not available to install it" >&2
    exit 1
  fi
fi

if [[ "${MODE}" == "check" ]]; then
  exit 0
fi

if ! command -v cargo-nextest >/dev/null 2>&1; then
  cargo install cargo-nextest --locked
fi

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="${HOME}/.local/bin:${PATH}"
fi
