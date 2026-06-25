UV ?= uv

CARGO_FEATURES ?= --no-default-features --features postgres
TEST_FEATURES ?= --features test-utils
CARGO_TARGET_DIR_EFFECTIVE := $(if $(strip $(CARGO_TARGET_DIR)),$(CARGO_TARGET_DIR),target)
CARGO_TARGET_TMPDIR ?= $(abspath $(CARGO_TARGET_DIR_EFFECTIVE)/tmp)
SETUP_TEST_SCHEMAS_BIN := $(CARGO_TARGET_DIR_EFFECTIVE)/debug/setup_test_schemas
PYTEST_TARGET ?= py-pgqrs
PYTEST_ARGS ?=

.venv:
	$(UV) venv

docs-requirements: .venv
	$(UV) pip install maturin "mkdocs-material[imaging]" mkdocs-catppuccin

requirements: test-requirements

test-requirements: .venv/test-requirements.timestamp

.venv/test-requirements.timestamp: py-pgqrs/pyproject.toml py-pgqrs/Cargo.toml
	$(MAKE) docs-requirements
	$(UV) pip install \
		"pytest>=7.0" \
		"pytest-asyncio>=0.21" \
		"testcontainers[postgres]>=3.7" \
		"urllib3<2.0"
	@touch .venv/test-requirements.timestamp

build: test-requirements
	cargo build -p pgqrs $(CARGO_FEATURES)
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES) $(TEST_FEATURES)

build-python: test-requirements
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES) $(TEST_FEATURES)

python-wheel: docs-requirements
	rm -rf dist/py-pgqrs
	mkdir -p dist/py-pgqrs
	$(UV) run maturin build --release --out dist/py-pgqrs -m py-pgqrs/Cargo.toml $(CARGO_FEATURES)

install-python-wheel: python-wheel
	$(UV) pip install --force-reinstall dist/py-pgqrs/pgqrs-*.whl

benchmark-bootstrap:
	$(MAKE) -C benchmarks bootstrap UV="$(UV)"

benchmark-list:
	$(MAKE) -C benchmarks list UV="$(UV)"

benchmark-run:
	$(MAKE) -C benchmarks run UV="$(UV)" SCENARIO="$(SCENARIO)" BACKEND="$(BACKEND)" BINDING="$(BINDING)" PROFILE="$(PROFILE)" PREFILL_JOBS="$(PREFILL_JOBS)"

benchmark-dashboard:
	$(MAKE) -C benchmarks dashboard UV="$(UV)"

benchmark-doc-charts:
	$(MAKE) -C benchmarks docs-charts UV="$(UV)"

install-nextest:
	cargo install cargo-nextest --locked

check-nextest:
	@which cargo-nextest >/dev/null || (echo "cargo-nextest not found. Run 'make install-nextest' or 'cargo install cargo-nextest'" && exit 1)

build-setup-test-schemas:
	cargo build -p pgqrs --bin setup_test_schemas $(CARGO_FEATURES)

test-rust: check-nextest
ifdef TEST
ifdef FILTER
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES) --test $(TEST) -E '$(FILTER)'
else
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES) --test $(TEST)
endif
else
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES)
endif

test: build-python check-nextest
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_DSN=$(PGQRS_TEST_DSN) PGBOUNCER_TEST_DSN=$(PGBOUNCER_TEST_DSN) cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES)
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" $(UV) run pytest py-pgqrs

test-py: build-python
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" $(UV) run pytest $(PYTEST_ARGS) $(PYTEST_TARGET)

start-postgres:
ifdef CI_POSTGRES_RUNNING
	@echo "Skipping Postgres container start (CI_POSTGRES_RUNNING=true)"
else
	docker rm -f pgqrs-test-db || true
	docker run -d --name pgqrs-test-db -p 5433:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres postgres:15-alpine
	@echo "Waiting for Postgres to be ready..."
	@until docker exec pgqrs-test-db pg_isready; do sleep 1; done
endif

start-pgbouncer: start-postgres
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

stop-postgres:
ifdef CI_POSTGRES_RUNNING
	@echo "Skipping container stop (CI_POSTGRES_RUNNING=true)"
else
	docker rm -f pgqrs-test-pgbouncer || true
	docker rm -f pgqrs-test-db || true
endif

test-setup-postgres: start-pgbouncer build-setup-test-schemas
ifdef CI_POSTGRES_RUNNING
	@echo "Using CI Postgres database"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" $(SETUP_TEST_SCHEMAS_BIN)
else
	@echo "Using local Postgres database"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" $(SETUP_TEST_SCHEMAS_BIN)
endif

test-postgres: test-setup-postgres
ifdef CI_POSTGRES_RUNNING
	@echo "Running tests with CI Postgres"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" \
	PGBOUNCER_TEST_DSN="$${PGBOUNCER_TEST_DSN:-postgres://postgres@localhost:6432/postgres}" \
	$(MAKE) test CARGO_FEATURES="--no-default-features --features postgres"
	$(MAKE) test-cleanup-postgres
else
	@echo "Running tests with local Postgres"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" \
	PGBOUNCER_TEST_DSN="postgres://postgres:postgres@localhost:6433/postgres" \
	$(MAKE) test CARGO_FEATURES="--no-default-features --features postgres"
	$(MAKE) test-cleanup-postgres
	$(MAKE) stop-postgres
endif

test-cleanup-postgres: build-setup-test-schemas
ifdef PGQRS_KEEP_TEST_DATA
	@echo "Skipping cleanup: PGQRS_KEEP_TEST_DATA is set"
else
ifdef CI_POSTGRES_RUNNING
	@echo "Cleaning up CI Postgres schemas"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" \
		$(SETUP_TEST_SCHEMAS_BIN) --cleanup
else
	@echo "Cleaning up local Postgres schemas"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" \
		$(SETUP_TEST_SCHEMAS_BIN) --cleanup
endif
endif

fmt:  ## Format code
	cargo fmt --all
	$(MAKE) -C benchmarks fmt UV="$(UV)"

clippy:  ## Run clippy
	cargo clippy --workspace --all-targets --all-features

check:  ## Run all checks (fmt, clippy, deny)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features
	$(MAKE) -C benchmarks check UV="$(UV)"

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

release-dry-run: docs-requirements  ## Dry run of the release process
	cargo release $${LEVEL:-minor} --no-push --no-publish
	$(UV) run maturin build --release -m py-pgqrs/Cargo.toml

release: docs-requirements  ## Execute the release process (LEVEL=patch|minor|major, default=minor)
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
	@$(UV) run python3 -c "from functools import reduce; from pathlib import Path; import re; \
		version = '$(VERSION)'; \
		dep_files = ['README.md', 'docs/user-guide/getting-started/installation.md', 'docs/user-guide/concepts/backends.md']; \
		dep_patterns = [(r'((?:pgqrs|pgqrs-macros)\s*=\s*)\"[^\"]+\"', rf'\1\"{version}\"'), (r'((?:pgqrs|pgqrs-macros)\s*=\s*\{{\s*version\s*=\s*)\"[^\"]+\"', rf'\1\"{version}\"')]; \
		[path.write_text(reduce(lambda content, pattern: re.sub(pattern[0], pattern[1], content), dep_patterns, path.read_text())) for path in map(Path, dep_files)]; \
		pyproject = Path('py-pgqrs/pyproject.toml'); \
		content = pyproject.read_text(); \
		content = re.sub(r'(?m)^version\s*=\s*\"[^\"]+\"', f'version = \"{version}\"', content, count=1); \
		pyproject.write_text(content)"

