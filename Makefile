# Optional convenience commands; Python entry points are canonical.
.DEFAULT_GOAL := help
.PHONY: help up test cycle status logs stats down add-installments

ifeq ($(OS),Windows_NT)
PYTHON ?= py -3
else
PYTHON ?= python3
endif

COMPOSE := docker compose -f docker-compose.yml -f docker-compose.m2.yml -f docker-compose.m2b.yml -f docker-compose.m3a.yml

help:
	@echo "make up       Full T0-T5 startup (starts simulator)"
	@echo "make test     Strict acceptance (stop simulator and drain CDC first)"
	@echo "make cycle    One incremental processing/quality/health cycle"
	@echo "make status   Show full stack"
	@echo "make logs     Tail full stack logs"
	@echo "make stats    One-time Docker memory snapshot"
	@echo "make down     Stop containers; preserve named volumes"
	@echo "make add-installments   Idempotent source schema-evolution command"

up:
	$(PYTHON) scripts/start.py

test:
	$(PYTHON) scripts/verify.py

cycle:
	$(COMPOSE) --profile transform run --rm --no-deps dbt-runner python run_pipeline_cycle.py

status:
	$(COMPOSE) --profile cdc --profile transform ps -a

logs:
	$(COMPOSE) --profile cdc --profile transform logs --tail=80

stats:
	docker stats --no-stream

down:
	$(COMPOSE) down

add-installments:
	$(PYTHON) scripts/start.py --add-installments-only
