.PHONY: install test lint format type-check docker-build clean help

PYTHON := python3
PIP := pip
PYTEST := pytest
BLACK := black
ISORT := isort
FLAKE8 := flake8
MYPY := mypy

SRC_DIRS := src dags plugins tests
DOCKER_IMAGE := ad-data-pipeline
DOCKER_TAG := latest

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install all dependencies including dev
	$(PIP) install -e ".[dev]"
	pre-commit install

install-prod:  ## Install production dependencies only
	$(PIP) install -e .

test:  ## Run all tests with coverage
	$(PYTEST) tests/ --cov=src/ad_data_pipeline --cov-report=term-missing --cov-report=xml -v

test-unit:  ## Run unit tests only
	$(PYTEST) tests/unit/ -v -m unit

test-integration:  ## Run integration tests only
	$(PYTEST) tests/integration/ -v -m integration

test-fast:  ## Run tests excluding slow tests
	$(PYTEST) tests/ -v -m "not slow"

lint:  ## Run all linters
	$(FLAKE8) $(SRC_DIRS)
	$(BLACK) --check $(SRC_DIRS)
	$(ISORT) --check-only $(SRC_DIRS)

format:  ## Auto-format code with black and isort
	$(BLACK) $(SRC_DIRS)
	$(ISORT) $(SRC_DIRS)

type-check:  ## Run mypy type checking
	$(MYPY) src/

validate-dags:  ## Validate all DAG files
	bash scripts/validate-dags.sh

docker-build:  ## Build Docker image
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-up:  ## Start local Docker environment
	docker-compose up -d

docker-down:  ## Stop local Docker environment
	docker-compose down

docker-logs:  ## Show Docker logs
	docker-compose logs -f

clean:  ## Remove build artifacts and caches
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage coverage.xml

pre-commit:  ## Run pre-commit hooks on all files
	pre-commit run --all-files
