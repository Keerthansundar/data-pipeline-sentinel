# Makefile for PipelineSentinel (MVP)
SHELL := /bin/bash

.PHONY: help up down reset seed demo demo-all rerun apply-fix test lint logs open

help:
	@echo "PipelineSentinel Make Targets:"
	@echo "  make up                     - Start all Docker containers"
	@echo "  make down                   - Stop all Docker containers"
	@echo "  make reset                  - Stop containers and purge local data"
	@echo "  make seed                   - Generate 30 days of seeded synthetic historical data"
	@echo "  make demo FAILURE=<mode> BATCH=<date> - Inject failure and trigger pipeline run"
	@echo "  make demo-all               - Run healthy day followed by all failure scenarios"
	@echo "  make rerun BATCH=<date>     - Rerun pipeline for a specific batch date"
	@echo "  make apply-fix INCIDENT=<id> - Apply rules patch from incident PR preview"
	@echo "  make test                   - Run local pytest unit tests"
	@echo "  make lint                   - Check code formatting and linting via ruff"
	@echo "  make logs                   - Tail docker compose container logs"
	@echo "  make open                   - Print local access URLs"

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v
	rm -rf data/landing/* data/bronze/* data/silver/* data/gold/* data/quarantine/* data/sandbox/* data/pr_previews/*

seed:
	docker compose exec airflow-scheduler python -m simulator.generate_history

demo:
	docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=$(FAILURE) --batch-date=$(BATCH)
	docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "$(BATCH)"}'

demo-all:
	@echo "Running healthy day demo..."
	$(MAKE) demo FAILURE=none BATCH=2026-10-01

rerun:
	docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "$(BATCH)"}'

apply-fix:
	python -m responder.patcher --incident-id=$(INCIDENT)

test:
	python -m pytest tests/ -v

lint:
	python -m ruff check .
	python -m ruff format --check .

logs:
	docker compose logs -f

open:
	@echo "Airflow UI:    http://localhost:8080 (airflow / airflow)"
	@echo "Streamlit App: http://localhost:8501"
	@echo "Mailpit Web:   http://localhost:8025"
