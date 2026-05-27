from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any


SERVICES = {
    "api-service": "http://localhost:8000",
    "queue-agent": "http://localhost:8011",
    "resource-agent": "http://localhost:8012",
    "sla-agent": "http://localhost:8013",
    "forecast-agent": "http://localhost:8014",
    "coordinator-agent": "http://localhost:8015",
    "executor-agent": "http://localhost:8016",
    "load-generator": "http://localhost:8017",
    "scale-agent": "http://localhost:8018",
}

COMPOSE_SERVICES = [
    "kafka",
    "rabbitmq",
    "postgres",
    "redis",
    "api-service",
    "queue-agent",
    "resource-agent",
    "sla-agent",
    "forecast-agent",
    "coordinator-agent",
    "executor-agent",
    "scale-agent",
    "load-generator",
    "prometheus",
    "grafana",
]


def run_command(command: list[str], timeout: int = 15) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, text=True, capture_output=True, timeout=timeout)


def check_compose_config() -> bool:
    if shutil.which("docker") is None:
        print("[!] Docker CLI не найден в текущем окружении")
        print("Проверка runtime требует Docker. Выполните локально:")
        print("  python scripts/check_compose.py")
        print("  docker compose up --build -d")
        print("  python scripts/validate_runtime.py")
        return False
    try:
        result = run_command(["docker", "compose", "config"], timeout=30)
    except Exception as exc:  # noqa: BLE001
        print(f"[!] Не удалось выполнить docker compose config: {exc}")
        return False
    output = (result.stdout + result.stderr).strip()
    if result.returncode == 0:
        print("[OK] Docker Compose конфигурация валидна")
        return True
    print("[!] Docker Compose конфигурация невалидна")
    if output:
        print(output)
    if "project name must not be empty" in output.lower():
        print("[!] Причина: пустое имя проекта Docker Compose")
        print("Исправление: добавьте в .env строку:")
        print("COMPOSE_PROJECT_NAME=ahmed_cloud_mas")
        print("И убедитесь, что docker-compose.yml содержит:")
        print("name: ${COMPOSE_PROJECT_NAME:-ahmed_cloud_mas}")
    print("После исправления выполните:")
    print("  python scripts/check_compose.py")
    print("  docker compose config")
    print("  docker compose up --build -d")
    return False


def check_compose_services_running() -> bool:
    try:
        result = run_command(["docker", "compose", "ps", "--services", "--filter", "status=running"], timeout=30)
    except Exception as exc:  # noqa: BLE001
        print(f"[!] Не удалось выполнить docker compose ps: {exc}")
        return False
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        print("[!] Не удалось получить статус Docker Compose сервисов")
        if output:
            print(output)
        print("Сначала запустите стек:")
        print("  docker compose up --build -d")
        return False
    running = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    missing = [service for service in COMPOSE_SERVICES if service not in running]
    if not missing:
        print("[OK] Все основные Docker Compose сервисы запущены")
        return True
    print("[!] Не все Docker Compose сервисы запущены")
    print(f"Не запущены: {', '.join(missing)}")
    print("Проверьте причину запуска:")
    print("  docker compose up --build -d")
    print("  docker compose ps")
    print("  docker compose logs --tail=200")
    return False


def http_request(method: str, url: str, payload: dict[str, Any] | None = None, timeout: int = 5) -> tuple[int, str]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return response.status, body


def http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: int = 5) -> tuple[int, Any]:
    status, body = http_request(method, url, payload, timeout)
    return status, json.loads(body) if body else {}


def check_http(name: str, url: str) -> bool:
    try:
        status, payload = http_json("GET", url)
        service_status = payload.get("status", "unknown")
        ok = 200 <= status < 300 and service_status == "ok"
        print(f"[{'OK' if ok else '!'}] {name}: HTTP {status}, статус={service_status}")
        if not ok:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return ok
    except Exception as exc:  # noqa: BLE001
        print(f"[!] {name}: недоступен: {exc}")
        return False


def check_plain_http(name: str, url: str) -> bool:
    try:
        status, _ = http_request("GET", url)
        ok = 200 <= status < 300
        print(f"[{'OK' if ok else '!'}] {name}: HTTP {status}")
        return ok
    except Exception as exc:  # noqa: BLE001
        print(f"[!] {name}: недоступен: {exc}")
        return False


def check_tcp_port(name: str, host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"[OK] {name}: TCP {host}:{port} доступен")
            return True
    except Exception as exc:  # noqa: BLE001
        print(f"[!] {name}: TCP {host}:{port} недоступен: {exc}")
        return False


def check_command(name: str, command: list[str], expected: str | None = None) -> bool:
    try:
        result = run_command(command)
        output = (result.stdout + result.stderr).strip()
        ok = result.returncode == 0 and (expected is None or expected in output)
        print(f"[{'OK' if ok else '!'}] {name}: код={result.returncode}")
        if not ok:
            print(output[-1000:])
        return ok
    except Exception as exc:  # noqa: BLE001
        print(f"[!] {name}: команда не выполнена: {exc}")
        return False


def check_event_flow() -> bool:
    task = {
        "task_type": "validation",
        "cpu_required": 1,
        "ram_required_mb": 512,
        "duration_seconds": 1,
        "priority": 3,
        "sla_deadline_seconds": 20,
    }
    try:
        status, created = http_json("POST", "http://localhost:8000/tasks", task)
        if status not in {200, 202}:
            print(f"[!] Поток событий: API вернул HTTP {status}")
            return False
        task_id = created["task_id"]
        print(f"[OK] Поток событий: задача принята API, task_id={task_id}")
        for _ in range(60):
            _, current = http_json("GET", f"http://localhost:8000/tasks/{task_id}")
            if current["status"] == "done":
                print("[OK] Поток событий: задача дошла до execution.done")
                return True
            if current["status"] == "failed":
                print(f"[!] Поток событий: задача завершилась ошибкой: {current.get('error')}")
                return False
            time.sleep(1)
        _, current = http_json("GET", f"http://localhost:8000/tasks/{task_id}")
        print(
            f"[!] Поток событий: задача не завершилась за 60 секунд, "
            f"last_status={current.get('status')}, error={current.get('error')}"
        )
        print("[!] Проверьте логи: docker compose logs --tail=200 coordinator-agent resource-agent executor-agent")
        return False
    except urllib.error.HTTPError as exc:
        print(f"[!] Поток событий: HTTP ошибка {exc.code}: {exc.read().decode('utf-8')}")
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[!] Поток событий: проверка не выполнена: {exc}")
        return False


def main() -> int:
    if not check_compose_config():
        print("[!] Runtime-проверки HTTP и зависимостей пропущены, потому что Docker Compose не готов")
        return 1
    if not check_compose_services_running():
        print("[!] Runtime-проверки HTTP и зависимостей пропущены, потому что контейнеры не запущены")
        return 1
    checks: list[bool] = []
    checks.append(
        check_command(
            "Kafka broker",
            ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list"],
            expected="request.created",
        )
    )
    checks.append(check_command("RabbitMQ container health", ["docker", "compose", "exec", "-T", "rabbitmq", "rabbitmq-diagnostics", "ping"], expected="Ping succeeded"))
    checks.append(check_tcp_port("RabbitMQ AMQP", "localhost", 5672))
    checks.append(check_tcp_port("RabbitMQ Management", "localhost", 15672))
    checks.append(check_command("PostgreSQL", ["docker", "compose", "exec", "-T", "postgres", "pg_isready", "-U", "cloudrm", "-d", "cloudrm"]))
    checks.append(check_command("Redis", ["docker", "compose", "exec", "-T", "redis", "redis-cli", "ping"], expected="PONG"))
    for name, base_url in SERVICES.items():
        checks.append(check_http(f"{name} /health", f"{base_url}/health"))
        checks.append(check_http(f"{name} /ready", f"{base_url}/ready"))
    checks.append(check_event_flow())
    checks.append(check_plain_http("Prometheus", "http://localhost:9090/-/healthy"))
    checks.append(check_plain_http("Grafana", "http://localhost:3000/api/health"))
    if all(checks):
        print("[OK] Итог: все обязательные runtime-проверки прошли")
        return 0
    print("[!] Итог: часть runtime-проверок не прошла")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
