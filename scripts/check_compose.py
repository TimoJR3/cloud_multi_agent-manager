from __future__ import annotations

import shutil
import subprocess


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, text=True, capture_output=True, timeout=30)


def print_compose_hint(output: str) -> None:
    lowered = output.lower()
    if "project name must not be empty" in lowered:
        print("[!] Docker Compose получил пустое имя проекта")
        print("Вероятная причина: COMPOSE_PROJECT_NAME или PROJECT_NAME пустой.")
        print("Исправление: добавьте в .env строку:")
        print("COMPOSE_PROJECT_NAME=ahmed_cloud_mas")
        print("Затем выполните:")
        print("  python scripts/check_compose.py")
        print("  docker compose config")


def main() -> int:
    if shutil.which("docker") is None:
        print("[!] Docker CLI не найден в текущем окружении")
        print("Проверка требует установленный и запущенный Docker Desktop или совместимый Docker CLI.")
        print("Команды после установки Docker:")
        print("  python scripts/check_compose.py")
        print("  docker compose config")
        print("  docker compose up --build -d")
        return 1

    print("[…] Проверка Docker Compose конфигурации: docker compose config")
    config_result = run_command(["docker", "compose", "config"])
    config_output = (config_result.stdout + config_result.stderr).strip()
    if config_result.returncode != 0:
        print("[!] Docker Compose конфигурация невалидна")
        if config_output:
            print(config_output)
        print_compose_hint(config_output)
        return 1

    print("[✓] Docker Compose конфигурация валидна")
    print("[…] Текущий статус контейнеров: docker compose ps")
    ps_result = run_command(["docker", "compose", "ps"])
    ps_output = (ps_result.stdout + ps_result.stderr).strip()
    if ps_result.returncode == 0:
        print(ps_output or "[✓] Контейнеры проекта пока не созданы")
    else:
        print("[!] Не удалось получить docker compose ps")
        print(ps_output)
        print("Конфигурация валидна, но Docker daemon или проект могут быть недоступны.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
