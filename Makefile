# make up demo down — Kafka (3 brokers), Schema Registry, Flink (1 JM + 2 TM),
# ClickHouse, Iceberg on MinIO, Prometheus and Grafana; one container per node.
COMPOSE := docker compose
RUN := $(COMPOSE) run --rm --no-deps -T
RUNS ?= 3

.PHONY: help up jobs demo down ps lint test typecheck ci-local contract-check compose-check \
        measure-load measure-broker-kill measure-tm-kill measure-stall measure-contract \
        measure-query

help: ## list targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

up: ## build, start everything, wait for health, submit the Flink jobs
	$(COMPOSE) up -d --build --wait
	scripts/submit-jobs.sh
	@echo "Grafana http://localhost:13000  Flink http://localhost:18081  Prometheus http://localhost:19090"

jobs: ## (re)submit any Flink SQL job that is not running
	scripts/submit-jobs.sh

demo: ## 60 s of storefront traffic with faults, then reconcile every layer
	@mkdir -p runs
	$(COMPOSE) --profile load run --rm -T generator generate --rate 500 --duration 60 --run-id demo$$(date +%H%M%S) | tee runs/last.json
	$(RUN) -v $(CURDIR)/runs:/app/runs gatekeeper reconcile runs/$$(python3 -c 'import json;print(json.load(open("runs/last.json"))["run_id"])').json --wait 120

down: ## stop and remove everything, volumes included
	$(COMPOSE) --profile load down -v --remove-orphans

ps: ## container state
	$(COMPOSE) ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}'

lint: ## ruff (format and lint) + SQL lint
	uv run ruff format --check .
	uv run ruff check .

typecheck: ## mypy --strict
	uv run mypy src

test: ## unit tests (no services)
	uv run pytest

ci-local: lint typecheck test compose-check ## the static CI jobs, locally

compose-check: ## docker compose config -q
	$(COMPOSE) config -q

contract-check: ## against the running registry: compatible accepted, breaking refused
	$(RUN) gatekeeper contract-check

measure-load: ## throughput and end-to-end latency at three rates, RUNS times each
	uv run python scripts/measure.py load --runs $(RUNS)

measure-broker-kill: ## kill a Kafka broker under load, reconcile
	uv run python scripts/measure.py broker-kill --runs $(RUNS)

measure-tm-kill: ## kill a Flink TaskManager under load: restore time, reprocessing, reconcile
	uv run python scripts/measure.py tm-kill --runs $(RUNS)

measure-stall: ## pause both TaskManagers: time for the burn-rate alert to fire and clear
	uv run python scripts/measure.py stall --runs $(RUNS)

measure-contract: ## schema rejection, RUNS times
	uv run python scripts/measure.py contract --runs $(RUNS)

measure-query: ## ClickHouse query latency on fresh data
	uv run python scripts/measure.py query --runs $(RUNS)
