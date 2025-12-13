UV ?= uv

.venv:  ## Set up Python virtual environment
	$(UV) venv

requirements: .venv  ## Install Python requirements
	$(UV) pip install maturin "mkdocs-material[imaging]"
	$(UV) pip install -e "py-pgqrs[test]"

build: requirements  ## Build Rust and Python bindings
	cargo build -p pgqrs
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml

test-rust:  ## Run Rust tests only
	cargo test --workspace

test: build  ## Run all tests
	cargo test --workspace
	$(UV) run pytest py-pgqrs

test-py: build  ## Run Python tests only
	$(UV) run pytest py-pgqrs

fmt:  ## Format code
	cargo fmt --all

clippy:  ## Run clippy
	cargo clippy --workspace --all-targets --all-features

check:  ## Run all checks (fmt, clippy, deny)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features

clean:  ## Clean artifacts
	cargo clean
	rm -rf .venv
	rm -rf target
	rm -rf site

docs: requirements  ## Serve documentation
	$(UV) run mkdocs serve -f mkdocs.yml

docs-build: requirements  ## Build documentation
	$(UV) run mkdocs build --strict -f mkdocs.yml

help:  ## Display this help screen
	@echo "Usage: make [target]"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
