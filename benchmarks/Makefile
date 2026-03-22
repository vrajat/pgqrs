UV ?= uv

deps:  ## Install benchmark package and Python tooling into .venv
	$(MAKE) -C .. .venv UV="$(UV)"
	cd .. && $(UV) pip install --python .venv/bin/python -e './benchmarks[dev]'

bootstrap:  ## Prepare .venv for benchmark runs and dashboard use
	$(MAKE) -C .. install-python-wheel UV="$(UV)" PGQRS_TEST_BACKEND="$(if $(BACKEND),$(BACKEND),$(if $(PGQRS_TEST_BACKEND),$(PGQRS_TEST_BACKEND),postgres))"
	$(MAKE) deps UV="$(UV)"

list: deps  ## List available benchmark scenarios
	cd .. && .venv/bin/python -m benchmarks.bench.cli list

run: bootstrap  ## Run a benchmark (SCENARIO=... BACKEND=... BINDING=... [PROFILE=compat] [PREFILL_JOBS=...])
	@if [ -z "$(SCENARIO)" ]; then echo "Error: SCENARIO not set"; exit 1; fi
	@if [ -z "$(BACKEND)" ]; then echo "Error: BACKEND not set"; exit 1; fi
	@if [ -z "$(BINDING)" ]; then echo "Error: BINDING not set"; exit 1; fi
	cd .. && .venv/bin/python -m benchmarks.bench.cli run \
		--scenario "$(SCENARIO)" \
		--backend "$(BACKEND)" \
		--binding "$(BINDING)" \
		--profile "$${PROFILE:-compat}" \
		$(if $(PREFILL_JOBS),--prefill-jobs "$(PREFILL_JOBS)",)

dashboard: deps  ## Run the Streamlit benchmark dashboard
	cd .. && .venv/bin/streamlit run benchmarks/dashboard/app.py

docs-charts: deps  ## Generate static benchmark charts for docs
	cd .. && .venv/bin/python benchmarks/scripts/export_docs_charts.py

fmt: deps  ## Format benchmark Python code
	cd .. && .venv/bin/ruff check benchmarks --fix
	cd .. && .venv/bin/ruff format benchmarks

check: deps  ## Lint benchmark Python code
	cd .. && .venv/bin/ruff check benchmarks
	cd .. && .venv/bin/ruff format --check benchmarks
