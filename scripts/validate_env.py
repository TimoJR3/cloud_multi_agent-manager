from __future__ import annotations

import os
import sys
from pathlib import Path


REQUIRED = [
    "COMPOSE_PROJECT_NAME",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "DATABASE_URL",
    "REDIS_URL",
    "KAFKA_BOOTSTRAP_SERVERS",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_MANAGEMENT_PORT",
    "RABBITMQ_DEFAULT_USER",
    "RABBITMQ_DEFAULT_PASS",
    "RABBITMQ_EXCHANGE",
    "RABBITMQ_DLX",
    "RABBITMQ_RETRY_EXCHANGE",
    "RABBITMQ_PREFETCH",
    "SERVICE_CONFIG",
]


def parse_env_file(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def get_value(values: dict[str, str], key: str) -> str:
    if key in os.environ:
        return os.environ[key].strip()
    return values.get(key, "").strip()


def print_empty_name_error(name: str) -> None:
    print(f"[!] {name} пустой")
    if name == "COMPOSE_PROJECT_NAME":
        print("Исправление: добавьте в .env строку:")
        print("COMPOSE_PROJECT_NAME=ahmed_cloud_mas")
    else:
        print("Исправление: задайте непустое значение в .env, например:")
        print("PROJECT_NAME=cloudrm")


def main() -> int:
    env_path = Path(".env")
    if not env_path.exists():
        print("[!] .env не найден. Выполните: make init")
        return 1
    values = parse_env_file(env_path)
    missing = [key for key in REQUIRED if not get_value(values, key)]
    if missing:
        print(f"[!] Не хватает обязательных переменных окружения: {', '.join(missing)}")
        for key in missing:
            if key in {"COMPOSE_PROJECT_NAME", "PROJECT_NAME"}:
                print_empty_name_error(key)
        if "COMPOSE_PROJECT_NAME" not in missing:
            print("Исправление: проверьте .env или выполните: make init")
        return 1
    for name in ("COMPOSE_PROJECT_NAME", "PROJECT_NAME"):
        if name in values or name in os.environ:
            if not get_value(values, name):
                print_empty_name_error(name)
                return 1
    if get_value(values, "COMPOSE_PROJECT_NAME") == ".":
        print("[!] COMPOSE_PROJECT_NAME не должен быть точкой")
        print("Исправление: добавьте в .env строку:")
        print("COMPOSE_PROJECT_NAME=ahmed_cloud_mas")
        return 1
    print("[✓] Файл .env содержит обязательные переменные")
    print(f"[✓] COMPOSE_PROJECT_NAME={get_value(values, 'COMPOSE_PROJECT_NAME')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
