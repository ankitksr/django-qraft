.PHONY: test test-cov test-watch test-parallel lint format install-test clean help

# Default target
.DEFAULT_GOAL := help

help:  ## Show this help message
	@echo "Django-Qraft Development Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install-test:  ## Install test dependencies
	uv pip install --group test

test:  ## Run all tests
	uv run pytest

test-cov:  ## Run tests with coverage report
	uv run pytest --cov=qraft --cov-report=html --cov-report=term

test-watch:  ## Run tests in watch mode (requires pytest-watch)
	uv run ptw

test-parallel:  ## Run tests in parallel
	uv run pytest -n auto

test-verbose:  ## Run tests with verbose output
	uv run pytest -vv -s

test-failed:  ## Re-run only failed tests
	uv run pytest --lf

test-specific:  ## Run specific test file (usage: make test-specific FILE=test_models.py)
	uv run pytest tests/$(FILE)

lint:  ## Run linter
	uv run ruff check qraft/

format:  ## Format code
	uv run ruff check --fix qraft/

clean:  ## Clean up generated files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf *.egg-info
	rm -rf dist
	rm -rf build

check:  ## Run all checks (lint + tests)
	$(MAKE) lint
	$(MAKE) test

ci:  ## Run CI pipeline (lint + test with coverage)
	$(MAKE) lint
	uv run pytest --cov=qraft --cov-report=xml --cov-report=term -n auto
