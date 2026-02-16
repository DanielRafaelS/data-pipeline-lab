PROJECT=data-pipeline-lab

.PHONY: help up down build reset deploy run logs test

help:
	@echo "Available commands:"
	@echo "  make up       -> Start all services"
	@echo "  make down     -> Stop all services"
	@echo "  make build    -> Build Docker images"
	@echo "  make reset    -> Reset environment (delete volumes)"
	@echo "  make deploy   -> Deploy Prefect flow"
	@echo "  make run      -> Run deployed flow"
	@echo "  make logs     -> Tail logs"

up:
	docker compose -f docker-compose.win.yml up -d

down:
	docker compose -f docker-compose.win.yml down

build:
	docker compose -f docker-compose.win.yml build

reset:
	docker compose -f docker-compose.win.yml down -v
	docker compose -f docker-compose.win.yml up -d --build

deploy:
	prefect deploy

run:
	prefect deployment run medallion-etl/medallion-etl-local

logs:
	docker compose -f docker-compose.win.yml logs -f
