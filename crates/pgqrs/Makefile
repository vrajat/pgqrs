docs:
# Makefile for pgqrs documentation

.PHONY: .venv docs docs-release clean-docs

MKDOCS=mkdocs

VENV_DIR=.venv
PYTHON=$(VENV_DIR)/bin/python
PIP=$(VENV_DIR)/bin/pip
MKDOCS=$(VENV_DIR)/bin/mkdocs
PYTHON_MIN_VERSION=3.11
REQUIRED_PACKAGES=mkdocs-material[imaging]

# Create a Python virtual environment and install required packages
.venv:
	@if ! command -v python3 >/dev/null; then \
		echo "Python 3 is required but not found."; exit 1; \
	fi
	@PY_VER=$$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])'); \
	REQ_VER=$(PYTHON_MIN_VERSION); \
	if [ "$$(printf '%s\n' "$$REQ_VER" "$$PY_VER" | sort -V | head -n1)" != "$$REQ_VER" ]; then \
		echo "Python >= $$REQ_VER is required, found $$PY_VER"; exit 1; \
	fi
	@if [ ! -d $(VENV_DIR) ]; then \
		python3 -m venv $(VENV_DIR); \
	fi
	@$(PIP) install --upgrade pip
	@$(PIP) install $(REQUIRED_PACKAGES)
	@echo "Virtual environment with required packages is ready."

# Build and serve MkDocs documentation using the venv
docs: .venv
	$(MKDOCS) serve

# Build the release version of the documentation
docs-release: .venv clean-docs
	$(MKDOCS) build --strict
	@echo "Release documentation built in site/."

# Clean all built documentation (site)
clean-docs:
	rm -rf site
	@echo "Removed site/ directory."
