COMPOSE = docker compose -f docker-compose.infra.yml -f docker-compose.app.yml -f docker-compose.airflow.yml
TEST_COMPOSE = docker compose -p data-flow-tests -f docker-compose.tests.yml

.DEFAULT_GOAL := help

.PHONY: help up ps logs test down clean

help:
	@echo "Available commands:"
	@echo "  make up       Build and start the full local stack"
	@echo "  make ps       Show service status"
	@echo "  make logs     Follow logs for all services"
	@echo "  make test     Run the Docker test suite"
	@echo "  make down     Stop the local stack"
	@echo "  make clean    Stop stack and remove local runtime state (requires CONFIRM=1)"

up:
	$(COMPOSE) up -d --build

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

test:
	@$(TEST_COMPOSE) up --build --exit-code-from api_tests --remove-orphans; \
	status=$$?; \
	$(TEST_COMPOSE) down -v --remove-orphans; \
	exit $$status

down:
	$(COMPOSE) down --remove-orphans

clean:
	@test "$(CONFIRM)" = "1" || (echo "This removes Docker volumes and local runtime data. Run: make clean CONFIRM=1"; exit 1)
	$(COMPOSE) down -v --remove-orphans
	rm -rf pgdata minio-data shared_logs
