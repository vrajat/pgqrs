UV ?= uv

# Database backend for testing (postgres, sqlite, turso)
PGQRS_TEST_BACKEND ?= postgres

.venv:  ## Set up Python virtual environment
	$(UV) venv

docs-requirements: .venv  ## Install documentation dependencies only
	$(UV) pip install maturin "mkdocs-material[imaging]" mkdocs-catppuccin

requirements: docs-requirements  ## Install all Python requirements
	$(UV) pip install -e "py-pgqrs[test]"

build: requirements  ## Build Rust and Python bindings
	cargo build -p pgqrs $(CARGO_FEATURES)
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES)

test-rust:  ## Run Rust tests only
ifdef TEST
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo test --workspace $(CARGO_FEATURES) --test $(TEST)
else
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo test --workspace $(CARGO_FEATURES)
endif

test: build  ## Run all tests
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo test --workspace $(CARGO_FEATURES)
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest py-pgqrs

test-py: build  ## Run Python tests only
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest py-pgqrs

# Convenience targets for each backend
test-postgres:  ## Run tests on Postgres backend
	$(MAKE) test PGQRS_TEST_BACKEND=postgres

test-sqlite:  ## Run tests on SQLite backend
	$(MAKE) test PGQRS_TEST_BACKEND=sqlite

test-turso:  ## Run tests on Turso backend
	@if [ -z "$$PGQRS_TEST_TURSO_DSN" ]; then \
		echo "Error: PGQRS_TEST_TURSO_DSN must be set"; \
		exit 1; \
	fi
	$(MAKE) test PGQRS_TEST_BACKEND=turso

# Run on all available backends
test-all-backends:  ## Run tests on all available backends
	@echo "=== Testing on Postgres ==="
	$(MAKE) test-postgres
	@echo ""
	@echo "=== Testing on SQLite ==="
	$(MAKE) test-sqlite
	@if [ -n "$$PGQRS_TEST_TURSO_DSN" ]; then \
		echo ""; \
		echo "=== Testing on Turso ==="; \
		$(MAKE) test-turso; \
	else \
		echo ""; \
		echo "=== Skipping Turso (PGQRS_TEST_TURSO_DSN not set) ==="; \
	fi

# Run on a subset (comma-separated)
# Usage: make test-backends BACKENDS=postgres,sqlite
test-backends:  ## Run tests on specified backends (BACKENDS=postgres,sqlite)
	@for backend in $$(echo "$(BACKENDS)" | tr ',' ' '); do \
		echo "=== Testing on $$backend ==="; \
		$(MAKE) test PGQRS_TEST_BACKEND=$$backend; \
		echo ""; \
	done

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

docs: docs-requirements  ## Serve documentation
	$(UV) run mkdocs serve -f mkdocs.yml

docs-build: docs-requirements  ## Build documentation
	$(UV) run mkdocs build --strict -f mkdocs.yml

help:  ## Display this help screen
	@echo "Usage: make [target]"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

release-dry-run: requirements  ## Dry run of the release process
	@BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	cargo release $${LEVEL:-minor} --no-push --no-publish
	$(UV) run maturin build --release -m py-pgqrs/Cargo.toml

release: requirements  ## Execute the release process (LEVEL=patch|minor|major, default=minor)
	@BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$BRANCH" != "main" ]; then \
		echo "Error: Must be on main branch (currently on $$BRANCH)"; \
		exit 1; \
	fi
	@echo "Creating release with version bump: $${LEVEL:-minor}"
	@echo "Note: CI will build multi-platform wheels and publish to PyPI on tag push"
	cargo release $${LEVEL:-minor} --execute --no-publish

bump-version: ## Update version in documentation files (Usage: make bump-version VERSION=x.y.z)
	@if [ -z "$(VERSION)" ]; then echo "Error: VERSION not set"; exit 1; fi
	@echo "Bumping documentation versions to $(VERSION)..."
	@$(UV) run python3 -c "import re; \
		files = ['README.md', 'docs/user-guide/getting-started/installation.md', 'docs/user-guide/concepts/backends.md']; \
		pattern = r'(pgqrs(?:-macros)?\s*=\s*)\"[^\"]+\"'; \
		repl = r'\1\"$(VERSION)\"'; \
		[open(f, 'w').write(re.sub(pattern, repl, open(f).read())) for f in files]"
