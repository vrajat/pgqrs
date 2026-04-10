UV ?= uv

# Database backend for testing (postgres, sqlite, turso)
PGQRS_TEST_BACKEND ?= postgres

# Auto-set CARGO_FEATURES based on PGQRS_TEST_BACKEND if not explicitly provided
ifeq ($(PGQRS_TEST_BACKEND),postgres)
	CARGO_FEATURES ?= --no-default-features --features postgres
else ifeq ($(PGQRS_TEST_BACKEND),sqlite)
	CARGO_FEATURES ?= --no-default-features --features sqlite
else ifeq ($(PGQRS_TEST_BACKEND),s3)
	CARGO_FEATURES ?= --no-default-features --features s3
else ifeq ($(PGQRS_TEST_BACKEND),turso)
	CARGO_FEATURES ?= --no-default-features --features turso
else
	CARGO_FEATURES ?=
endif

# LocalStack test config (S3)
LOCALSTACK_IMAGE ?= localstack/localstack:3
LOCALSTACK_CONTAINER ?= pgqrs-test-localstack
LOCALSTACK_PORT ?= 4566
LOCALSTACK_REGION ?= us-east-1
PGQRS_S3_TEST_BUCKET ?= pgqrs-test-bucket
TOXIPROXY_IMAGE ?= ghcr.io/shopify/toxiproxy:2.12.0
TOXIPROXY_CONTAINER ?= pgqrs-bench-toxiproxy
TOXIPROXY_API_PORT ?= 8474
TOXIPROXY_LISTEN_PORT ?= 4567
TOXIPROXY_UPSTREAM ?= host.docker.internal:$(LOCALSTACK_PORT)

# Test-only features to always enable for test runs
TEST_FEATURES ?= --features test-utils
CARGO_TARGET_DIR_EFFECTIVE := $(if $(strip $(CARGO_TARGET_DIR)),$(CARGO_TARGET_DIR),target)
CARGO_TARGET_TMPDIR ?= $(abspath $(CARGO_TARGET_DIR_EFFECTIVE)/tmp)
SETUP_TEST_SCHEMAS_BIN := $(CARGO_TARGET_DIR_EFFECTIVE)/debug/setup_test_schemas

.venv:  ## Set up Python virtual environment
	$(UV) venv

docs-requirements: .venv  ## Install documentation/build dependencies
	$(UV) pip install maturin "mkdocs-material[imaging]" mkdocs-catppuccin

requirements: test-requirements  ## Backward-compatible alias for test Python requirements

test-requirements: .venv/test-requirements.timestamp  ## Install Python test requirements

.venv/test-requirements.timestamp: py-pgqrs/pyproject.toml py-pgqrs/Cargo.toml
	$(MAKE) docs-requirements
	$(UV) pip install \
		"pytest>=7.0" \
		"pytest-asyncio>=0.21" \
		"testcontainers[postgres]>=3.7" \
		"boto3>=1.34" \
		"urllib3<2.0"
	@touch .venv/test-requirements.timestamp


build: test-requirements  ## Build Rust library and Python bindings
	cargo build -p pgqrs $(CARGO_FEATURES)
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES) $(TEST_FEATURES)

build-python: test-requirements  ## Build Python bindings only (for tests)
	$(UV) run maturin develop -m py-pgqrs/Cargo.toml $(CARGO_FEATURES) $(TEST_FEATURES)

python-wheel: docs-requirements  ## Build a local Python wheel into dist/
	rm -rf dist/py-pgqrs
	mkdir -p dist/py-pgqrs
	$(UV) run maturin build --release --out dist/py-pgqrs -m py-pgqrs/Cargo.toml $(CARGO_FEATURES)

install-python-wheel: python-wheel  ## Install the built wheel into .venv
	$(UV) pip install --force-reinstall dist/py-pgqrs/pgqrs-*.whl

benchmark-bootstrap:  ## Prepare .venv for running benchmarks and dashboard
	$(MAKE) -C benchmarks bootstrap UV="$(UV)"

benchmark-list:  ## List available benchmark scenarios
	$(MAKE) -C benchmarks list UV="$(UV)"

benchmark-run:  ## Run a benchmark (SCENARIO=... BACKEND=... BINDING=... [PROFILE=compat] [PREFILL_JOBS=...])
	$(MAKE) -C benchmarks run UV="$(UV)" SCENARIO="$(SCENARIO)" BACKEND="$(BACKEND)" BINDING="$(BINDING)" PROFILE="$(PROFILE)" PREFILL_JOBS="$(PREFILL_JOBS)"

benchmark-s3-smoke:  ## Run the S3 benchmark smoke check through Toxiproxy
	$(MAKE) -C benchmarks s3-smoke UV="$(UV)" LATENCY_MS="$(LATENCY_MS)" JITTER_MS="$(JITTER_MS)" TOXIPROXY_API_URL="$(TOXIPROXY_API_URL)" TOXIPROXY_UPSTREAM="$(TOXIPROXY_UPSTREAM)"

benchmark-dashboard:  ## Run the Streamlit benchmark dashboard
	$(MAKE) -C benchmarks dashboard UV="$(UV)"

benchmark-doc-charts:  ## Generate static benchmark charts for docs
	$(MAKE) -C benchmarks docs-charts UV="$(UV)"

install-nextest: ## Install cargo-nextest
	cargo install cargo-nextest --locked

check-nextest:
	@which cargo-nextest >/dev/null || (echo "cargo-nextest not found. Run 'make install-nextest' or 'cargo install cargo-nextest'" && exit 1)

build-setup-test-schemas: ## Build the test schema helper for the active backend
	cargo build -p pgqrs --bin setup_test_schemas $(CARGO_FEATURES)

test-rust: check-nextest ## Run Rust tests only (using nextest)
ifdef TEST
ifdef FILTER
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES) --test $(TEST) -E '$(FILTER)'
else
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES) --test $(TEST)
endif
else
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES)
endif


test: build-python check-nextest  ## Run all tests
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_DSN=$(PGQRS_TEST_DSN) PGBOUNCER_TEST_DSN=$(PGBOUNCER_TEST_DSN) PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) cargo nextest run --cargo-profile dev -p pgqrs $(CARGO_FEATURES) $(TEST_FEATURES)
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest py-pgqrs

# Optional selectors for Python tests
# Usage examples:
#   make test-py PGQRS_TEST_BACKEND=sqlite PYTEST_TARGET=py-pgqrs/tests/test_guides.py
#   make test-py PYTEST_ARGS='-k guides -q'
PYTEST_TARGET ?= py-pgqrs
PYTEST_ARGS ?=

test-py: build-python  ## Run Python tests only
	CARGO_TARGET_TMPDIR="$(CARGO_TARGET_TMPDIR)" PGQRS_TEST_BACKEND=$(PGQRS_TEST_BACKEND) $(UV) run pytest $(PYTEST_ARGS) $(PYTEST_TARGET)

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

test-setup-postgres: start-pgbouncer build-setup-test-schemas ## Provision schemas (uses CI database if available)
ifdef CI_POSTGRES_RUNNING
	@echo "Using CI Postgres database"
	PGQRS_TEST_DSN="$${PGQRS_TEST_DSN:-postgres://postgres:postgres@localhost:5432/postgres}" $(SETUP_TEST_SCHEMAS_BIN)
else
	@echo "Using local Postgres database"
	PGQRS_TEST_DSN="postgres://postgres:postgres@localhost:5433/postgres" $(SETUP_TEST_SCHEMAS_BIN)
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

test-cleanup-postgres: build-setup-test-schemas ## Drop all test schemas (respects PGQRS_KEEP_TEST_DATA)
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

test-sqlite:  ## Run tests on SQLite backend
	$(MAKE) test PGQRS_TEST_BACKEND=sqlite CARGO_FEATURES="--no-default-features --features sqlite"

test-turso:  ## Run tests on Turso backend
	$(MAKE) test PGQRS_TEST_BACKEND=turso CARGO_FEATURES="--no-default-features --features turso"

start-localstack: ## Start LocalStack S3 container (skipped if CI_LOCALSTACK_RUNNING=true)
ifdef CI_LOCALSTACK_RUNNING
	@echo "Skipping LocalStack container start (CI_LOCALSTACK_RUNNING=true)"
else
	docker rm -f $(LOCALSTACK_CONTAINER) || true
	docker run -d --name $(LOCALSTACK_CONTAINER) \
		-p $(LOCALSTACK_PORT):4566 \
		-e SERVICES=s3 \
		-e AWS_DEFAULT_REGION=$(LOCALSTACK_REGION) \
		-e AWS_ACCESS_KEY_ID=test \
		-e AWS_SECRET_ACCESS_KEY=test \
		$(LOCALSTACK_IMAGE)
	@echo "Waiting for LocalStack S3 to be ready..."
	@until curl -fsS "http://localhost:$(LOCALSTACK_PORT)/_localstack/health" | grep -Eq '"s3"[[:space:]]*:[[:space:]]*"(running|available)"'; do sleep 1; done
	@docker exec $(LOCALSTACK_CONTAINER) awslocal s3api create-bucket --bucket $(PGQRS_S3_TEST_BUCKET) >/dev/null 2>&1 || true
endif

start-toxiproxy: ## Start Toxiproxy container for S3 benchmark latency injection
	docker rm -f $(TOXIPROXY_CONTAINER) || true
	docker run -d --name $(TOXIPROXY_CONTAINER) \
		-p $(TOXIPROXY_API_PORT):8474 \
		-p $(TOXIPROXY_LISTEN_PORT):$(TOXIPROXY_LISTEN_PORT) \
		$(TOXIPROXY_IMAGE)
	@echo "Waiting for Toxiproxy API to be ready..."
	@until curl -fsS "http://localhost:$(TOXIPROXY_API_PORT)/proxies" >/dev/null; do sleep 1; done

stop-toxiproxy: ## Stop Toxiproxy container
	docker rm -f $(TOXIPROXY_CONTAINER) || true

start-s3-bench-stack: start-localstack start-toxiproxy ## Start LocalStack and Toxiproxy for S3 benchmark work

stop-s3-bench-stack: stop-toxiproxy stop-localstack ## Stop LocalStack and Toxiproxy for S3 benchmark work

stop-localstack: ## Stop LocalStack S3 container (skipped if CI_LOCALSTACK_RUNNING=true)
ifdef CI_LOCALSTACK_RUNNING
	@echo "Skipping LocalStack container stop (CI_LOCALSTACK_RUNNING=true)"
else
	docker rm -f $(LOCALSTACK_CONTAINER) || true
endif

test-localstack: start-localstack ## Run full test suite against LocalStack-backed S3 backend
ifdef CI_LOCALSTACK_RUNNING
	@echo "Running full test suite with CI LocalStack (S3 backend)"
	$(MAKE) build-setup-test-schemas PGQRS_TEST_BACKEND=s3 CARGO_FEATURES="--no-default-features --features s3"
	AWS_ENDPOINT_URL="$${AWS_ENDPOINT_URL:-http://localhost:4566}" \
	AWS_REGION="$${AWS_REGION:-$(LOCALSTACK_REGION)}" \
	PGQRS_S3_BUCKET="$${PGQRS_S3_BUCKET:-$(PGQRS_S3_TEST_BUCKET)}" \
	AWS_ACCESS_KEY_ID="$${AWS_ACCESS_KEY_ID:-test}" \
	AWS_SECRET_ACCESS_KEY="$${AWS_SECRET_ACCESS_KEY:-test}" \
	$(MAKE) test PGQRS_TEST_BACKEND=s3 CARGO_FEATURES="--no-default-features --features s3"
	@echo "Listing sqlite objects in LocalStack after test run"
	AWS_ENDPOINT_URL="$${AWS_ENDPOINT_URL:-http://localhost:4566}" \
	AWS_REGION="$${AWS_REGION:-$(LOCALSTACK_REGION)}" \
	PGQRS_S3_BUCKET="$${PGQRS_S3_BUCKET:-$(PGQRS_S3_TEST_BUCKET)}" \
	AWS_ACCESS_KEY_ID="$${AWS_ACCESS_KEY_ID:-test}" \
	AWS_SECRET_ACCESS_KEY="$${AWS_SECRET_ACCESS_KEY:-test}" \
	$(SETUP_TEST_SCHEMAS_BIN) --list-s3-sqlite
else
	@echo "Running full test suite with local LocalStack (S3 backend)"
	$(MAKE) build-setup-test-schemas PGQRS_TEST_BACKEND=s3 CARGO_FEATURES="--no-default-features --features s3"
	@AWS_ENDPOINT_URL="http://localhost:$(LOCALSTACK_PORT)" \
	AWS_REGION="$(LOCALSTACK_REGION)" \
	PGQRS_S3_BUCKET="$(PGQRS_S3_TEST_BUCKET)" \
	AWS_ACCESS_KEY_ID="test" \
	AWS_SECRET_ACCESS_KEY="test" \
	$(MAKE) test PGQRS_TEST_BACKEND=s3 CARGO_FEATURES="--no-default-features --features s3"; \
	test_status=$$?; \
	AWS_ENDPOINT_URL="http://localhost:$(LOCALSTACK_PORT)" \
	AWS_REGION="$(LOCALSTACK_REGION)" \
	PGQRS_S3_BUCKET="$(PGQRS_S3_TEST_BUCKET)" \
	AWS_ACCESS_KEY_ID="test" \
	AWS_SECRET_ACCESS_KEY="test" \
	$(SETUP_TEST_SCHEMAS_BIN) --list-s3-sqlite; \
	list_status=$$?; \
	$(MAKE) stop-localstack; \
	if [ $$test_status -ne 0 ]; then exit $$test_status; fi; \
	exit $$list_status
endif

# Backward compatibility alias.
test-s3: test-localstack ## Alias for test-localstack

# Run on all available backends
test-all-backends:  ## Run tests on all available backends
	@echo "=== Testing on Postgres ==="
	$(MAKE) test-postgres
	@echo ""
	@echo "=== Testing on SQLite ==="
	$(MAKE) test-sqlite
	echo "=== Testing on Turso ==="; \
	$(MAKE) test-turso; \
	echo "=== Testing S3 (LocalStack full suite) ==="; \
	$(MAKE) test-localstack; \

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
