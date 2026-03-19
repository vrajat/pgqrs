UV ?= uv

deps:  ## Install benchmark package and Python tooling into .venv
	cd .. && $(UV) pip install -e './benchmarks[dev]'

bootstrap:  ## Prepare .venv for benchmark runs and dashboard use
	$(MAKE) -C .. install-python-wheel UV="$(UV)" PGQRS_TEST_BACKEND="$(if $(BACKEND),$(BACKEND),$(if $(PGQRS_TEST_BACKEND),$(PGQRS_TEST_BACKEND),postgres))"
	$(MAKE) deps UV="$(UV)"

list: deps  ## List available benchmark scenarios
	cd .. && $(UV) run python -m benchmarks.bench.cli list

run: bootstrap  ## Run a benchmark (SCENARIO=... BACKEND=... BINDING=... [PROFILE=compat])
	@if [ -z "$(SCENARIO)" ]; then echo "Error: SCENARIO not set"; exit 1; fi
	@if [ -z "$(BACKEND)" ]; then echo "Error: BACKEND not set"; exit 1; fi
	@if [ -z "$(BINDING)" ]; then echo "Error: BINDING not set"; exit 1; fi
	cd .. && $(UV) run python -m benchmarks.bench.cli run \
		--scenario "$(SCENARIO)" \
		--backend "$(BACKEND)" \
		--binding "$(BINDING)" \
		--profile "$${PROFILE:-compat}"

dashboard: deps  ## Run the Streamlit benchmark dashboard
	cd .. && $(UV) run streamlit run benchmarks/dashboard/app.py

fmt: deps  ## Format benchmark Python code
	cd .. && $(UV) run ruff check benchmarks --fix
	cd .. && $(UV) run ruff format benchmarks

check: deps  ## Lint benchmark Python code
	cd .. && $(UV) run ruff check benchmarks
	cd .. && $(UV) run ruff format --check benchmarks
