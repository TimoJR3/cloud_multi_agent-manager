.PHONY: init up down logs ps validate validate-runtime smoke integration test

init:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "[✓] Создан .env из .env.example"; \
	else \
		echo "[✓] .env уже существует, файл не перезаписан"; \
	fi
	python scripts/validate_env.py
	@echo ""
	@echo "Команды для продолжения:"
	@echo "  python scripts/check_compose.py"
	@echo "  docker compose config"
	@echo "  docker compose up --build -d"
	@echo "  python scripts/validate_runtime.py"

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

validate:
	python scripts/validate_infra.py

validate-runtime:
	python scripts/validate_runtime.py

smoke:
	pytest -q tests/smoke

integration:
	RUN_INTEGRATION=1 pytest -q tests/integration

test:
	pytest -q
