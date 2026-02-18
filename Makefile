UV ?= uv

# Database backend for testing (postgres, sqlite, turso)
PGQRS_TEST_BACKEND ?= postgres

# Auto-set CARGO_FEATURES based on PGQRS_TEST_BACKEND if not explicitly provided
ifeq ($(PGQRS_TEST_BACKEND),postgres)
	CARGO_FEATURES ?= --no-default-features --features postgres
else ifeq ($(PGQRS_TEST_BACKEND),sqlite)
	CARGO_FEATURES ?= --no-default-features --features sqlite
else ifeq ($(PGQRS_TEST_BACKEND),turso)
	CARGO_FEATURES ?= --no-default-features --features turso
else
	CARGO_FEATURES ?=
endif

.venv:  ## Set up Python virtual environment
	$(UV) venv

docs-requirements: .venv  ## Install documentation dependencies only
	$(UV) pip install maturin "mkdocs-material[imaging]" mkdocs-catppuccin

requirements: .venv/requirements.timestamp  ## Install all Python requirements

.venv/requirements.timestamp: py-pgqrs/pyproject.toml py-pgqrs/Cargo.toml
	$(MAKE) docs-requirements
	$(UV) pip install -e "py-pgqrs[test]"
	@touch .venv/requirements.timestamp


build: requirements  ## Build Rust library and Python bindings
	cargo build -p pgqrs $(CARGO_FEATURES)
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES)

build-python: requirements  ## Build Python bindings only (for tests)
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES)

install-nextest: ## Install cargo-nextest
	cargo install cargo-nextest --locked

check-nextest:
	@which cargo-nextest >/dev/null || (echo "cargo-nextest not found. Run 'make install-nextest' or 'cargo install cargo-nextest'" && exit 1)

test-rust: check-nextest ## Run Rust tests only (using nextest)
ifdef TEST
ifdef FILTER
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev --workspace $(CARGO_FEATURES) --test $(TEST) -E '$(FILTER)'
else
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev --workspace $(CARGO_FEATURES) --test $(TEST)
endif
else
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev --workspace $(CARGO_FEATURES)
endif


test: build-python check-nextest  ## Run all tests
	PGQRS_TEST_DSN=$(PGQRS_TEST_DSN) PGBOUNCER_TEST_DSN=$(PGBOUNCER_TEST_DSN) PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev --workspace $(CARGO_FEATURES)
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest py-pgqrs

test-py: build-python  ## Run Python tests only
	PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest py-pgqrs

# Convenience targets for each backend
start-postgres: ## Start global Postgres container (skipped if CI_POSTGRES_RUNNING=true)
ifdef CI_POSTGRES_RUNNING
	@echo "Skipping Postgres container start (CI_POSTGRES_RUNNING=true)"
else
	docker rm -f pgqrs-test-db || true
	docker run -d --name pgqrs-test-db -p 5433:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres postgres:15-alpine
	@echo "Waiting for Postgres to be ready..."
	@until docker exec pgqrs-test-db pg_isready; do sleep 1; done
endif

start-pgbouncer: start-postgres ## Start PgBouncer container (skipped if CI_POSTGRES_RUNNING=true)
ifdef CI_POSTGRES_RUNNING
	@echo "Skipping PgBouncer container start (CI_POSTGRES_RUNNING=true)"
else
	docker rm -f pgqrs-test-pgbouncer || true
	docker run -d --name pgqrs-test-pgbouncer \
		--link pgqrs-test-db:postgres \
		-p 6433:5432 \
		-e DATABASE_URL="postgres://postgres:postgres@postgres:5432/postgres" \
		-e POOL_MODE=session \
		-e AUTH_TYPE=scram-sha-256 \
		-e MAX_CLIENT_CONN=100 \
		-e DEFAULT_POOL_SIZE=20 \
		-e ADMIN_USERS=postgres \
		-e STATS_USERS=postgres \
		edoburu/pgbouncer:latest
	@echo "Waiting for PgBouncer to be ready..."
	@sleep 3
endif

stop-postgres: ## Stop global Postgres and PgBouncer containers (skipped if CI_POSTGRES_RUNNING=true)
ifdef CI_POSTGRES_RUNNING
	@echo "Skipping container stop (CI_POSTGRES_RUNNING=true)"
else
	docker rm -f pgqrs-test-pgbouncer || true
	docker rm -f pgqrs-test-db || true
endif

test-setup-postgres: start-pgbouncer ## Provision schemas (uses CI database if available)
ifdef CI_POSTGRES_RUNNING
	@echo "Using CI Postgres database"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" cargo run -p pgqrs --bin setup_test_schemas
else
	@echo "Using local Postgres database"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" cargo run -p pgqrs --bin setup_test_schemas
endif

test-postgres: test-setup-postgres ## Run tests on Postgres backend (supports CI and local modes)
ifdef CI_POSTGRES_RUNNING
	@echo "Running tests with CI Postgres"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" \
	PGBOUNCER_TEST_DSN="$${PGBOUNCER_TEST_DSN:-postgres://postgres@localhost:6432/postgres}" \
	$(MAKE) test PGQRS_TEST_BACKEND=postgres CARGO_FEATURES="--no-default-features --features postgres"
	$(MAKE) test-cleanup-postgres
else
	@echo "Running tests with local Postgres"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" \
	PGBOUNCER_TEST_DSN="postgres://postgres:postgres@localhost:6433/postgres" \
	$(MAKE) test PGQRS_TEST_BACKEND=postgres CARGO_FEATURES="--no-default-features --features postgres"
	$(MAKE) test-cleanup-postgres
	$(MAKE) stop-postgres
endif

test-cleanup-postgres: ## Drop all test schemas (respects PGQRS_KEEP_TEST_DATA)
ifdef PGQRS_KEEP_TEST_DATA
	@echo "Skipping cleanup: PGQRS_KEEP_TEST_DATA is set"
else
ifdef CI_POSTGRES_RUNNING
	@echo "Cleaning up CI Postgres schemas"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" \
		cargo run -p pgqrs --bin setup_test_schemas --features postgres -- --cleanup
else
	@echo "Cleaning up local Postgres schemas"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" \
		cargo run -p pgqrs --bin setup_test_schemas --features postgres -- --cleanup
endif
endif

test-sqlite:  ## Run tests on SQLite backend
	$(MAKE) test PGQRS_TEST_BACKEND=sqlite CARGO_FEATURES="--no-default-features --features sqlite"

test-turso:  ## Run tests on Turso backend
	$(MAKE) test PGQRS_TEST_BACKEND=turso CARGO_FEATURES="--no-default-features --features turso"

# Run on all available backends
test-all-backends:  ## Run tests on all available backends
	@echo "=== Testing on Postgres ==="
	$(MAKE) test-postgres
	@echo ""
	@echo "=== Testing on SQLite ==="
	$(MAKE) test-sqlite
	echo "=== Testing on Turso ==="; \
	$(MAKE) test-turso; \

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
		files = ['README.md', 'docs/user-guide/getting-started/installation.md', 'docs/user-guide/concepts/backends.md', 'py-pgqrs/pyproject.toml']; \
		pattern = r'(pgqrs(?:-macros)?\s*=\s*|version\s*=\s*)\"[^\"]+\"'; \
		repl = r'\1\"$(VERSION)\"'; \
		[(lambda c: open(f, 'w').write(re.sub(pattern, repl, c)))(open(f, 'r').read()) for f in files]"
