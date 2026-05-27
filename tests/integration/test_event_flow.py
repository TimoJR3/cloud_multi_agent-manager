from __future__ import annotations

import os
import time

import httpx
import pytest


pytestmark = pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="integration tests require running docker compose")


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
    pytest.fail("task did not reach done status in 60 seconds")


def test_gpu_shortage_triggers_scale_decision() -> None:
    payload = {
        "task_type": "pytest-scale",
        "cpu_required": 32,
        "ram_required_mb": 131072,
        "duration_seconds": 1,
        "priority": 3,
        "sla_deadline_seconds": 20,
        "requires_gpu": True,
    }
    with httpx.Client(base_url="http://localhost:8000", timeout=5) as client:
        created = client.post("/tasks", json=payload)
        assert created.status_code == 202
        for _ in range(30):
            nodes = client.get("/nodes")
            assert nodes.status_code == 200
            if any(str(node.get("node_id", "")).startswith("node-auto-") for node in nodes.json()):
                return
            time.sleep(1)
    pytest.fail("decision.scale did not create an auto node")


def test_node_failure_endpoint_marks_node_failed() -> None:
    with httpx.Client(base_url="http://localhost:8000", timeout=5) as client:
        failed = client.post("/nodes/node-a/failure")
        assert failed.status_code == 200
        nodes = client.get("/nodes")
        assert nodes.status_code == 200
        node_a = [node for node in nodes.json() if node.get("node_id") == "node-a"]
        assert node_a and node_a[0].get("status") == "failed"
