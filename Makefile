up:
	docker compose up -d --build

down:
	docker compose down

test:
	uv run $(if $(wildcard .env),--env-file .env,) pytest

.PHONY: up down test
