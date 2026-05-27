from __future__ import annotations

import os
import time

import httpx
import pytest


pytestmark = pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="Интеграционные тесты требуют запущенный docker compose")


def test_task_reaches_done_status() -> None:
    payload = {
        "task_type": "pytest-flow",
        "cpu_required": 1,
        "ram_required_mb": 512,
        "duration_seconds": 1,
        "priority": 2,
        "sla_deadline_seconds": 20,
    }
    with httpx.Client(base_url="http://localhost:8000", timeout=5) as client:
        created = client.post("/tasks", json=payload)
        assert created.status_code == 202
        task_id = created.json()["task_id"]
        for _ in range(60):
            current = client.get(f"/tasks/{task_id}")
            assert current.status_code == 200
            status = current.json()["status"]
            if status == "done":
                return
            assert status != "failed", current.json()
            time.sleep(1)
    pytest.fail("Задача не дошла до статуса done за 60 секунд")
