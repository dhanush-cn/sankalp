PYTHON  ?= python
COMPOSE ?= docker compose

.DEFAULT_GOAL := help
.PHONY: help install up down migrate test test-crash test-unwind-crash test-gates test-soak api worker logs psql psql-test clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install the package and dev extras in editable mode
	$(PYTHON) -m pip install -e ".[dev]"

up: ## Start Postgres (5432, both DBs) + Redis, wait for health, then migrate
	$(COMPOSE) up -d --wait
	$(MAKE) migrate

down: ## Stop containers (data volume is preserved)
	$(COMPOSE) down

migrate: ## Apply migrations/*.sql to both sankalp and sankalp_test
	$(PYTHON) -m sankalp.storage.migrate --target both

test: migrate ## Run the suite against sankalp_test
	SANKALP_ENVIRONMENT=test $(PYTHON) -m pytest

test-crash: migrate ## Crash recovery mid-STEP, 20x loop
	SANKALP_ENVIRONMENT=test $(PYTHON) -m pytest tests/test_crash.py --count=20

test-unwind-crash: migrate ## Crash recovery mid-COMPENSATION, 20x loop
	SANKALP_ENVIRONMENT=test $(PYTHON) -m pytest tests/test_compensation_crash.py --count=20

test-gates: test-crash test-unwind-crash ## Both crash gates, 20x each

test-soak: migrate ## Soak: 1000 workflows with workers being killed
	SANKALP_ENVIRONMENT=test $(PYTHON) -m pytest -m slow

api: ## Dev API server against the sankalp database
	$(PYTHON) -m uvicorn sankalp.api.main:app --reload --host 0.0.0.0 --port 8000

worker: ## Run one worker process against the sankalp database
	$(PYTHON) -m sankalp.engine.worker

logs: ## Tail container logs
	$(COMPOSE) logs -f --tail=100

psql: ## psql shell on the dev database
	$(COMPOSE) exec postgres psql -U sankalp -d sankalp

psql-test: ## psql shell on the test database
	$(COMPOSE) exec postgres psql -U sankalp -d sankalp_test

clean: ## Stop containers AND destroy the data volume
	$(COMPOSE) down -v
