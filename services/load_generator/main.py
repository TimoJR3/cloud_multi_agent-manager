from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from cloudrm.logging import configure_logging
from cloudrm.messages import HealthResponse
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("load-generator")

runtime = ServiceRuntime("load-generator")
worker_task: asyncio.Task[None] | None = None
GENERATED_TASKS = Counter("cloudrm_load_generator_tasks_total", "Количество задач, созданных генератором", ["scenario"])


SCENARIOS = {
    "normal": {"interval": 2.0, "cpu": 1, "ram": 512, "duration": 3, "priority": 2},
    "peak": {"interval": 0.8, "cpu": 2, "ram": 1024, "duration": 5, "priority": 4},
    "overload": {"interval": 0.2, "cpu": 4, "ram": 4096, "duration": 8, "priority": 7},
    "node_failure": {"interval": 1.0, "cpu": 2, "ram": 2048, "duration": 6, "priority": 5},
}


async def generator_loop() -> None:
    assert runtime.redis is not None
    api_url = os.getenv("API_URL", "http://api-service:8000")
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            scenario_name = await runtime.redis.get("experiment:active")
            if not scenario_name:
                await asyncio.sleep(1)
                continue
            scenario = SCENARIOS.get(scenario_name, SCENARIOS["normal"])
            payload = {
                "task_type": f"generated-{scenario_name}",
                "cpu_required": scenario["cpu"],
                "ram_required_mb": scenario["ram"],
                "duration_seconds": scenario["duration"],
                "priority": scenario["priority"],
                "sla_deadline_seconds": 20,
                "requires_gpu": False,
            }
            try:
                await client.post(f"{api_url}/tasks", json=payload)
                GENERATED_TASKS.labels(scenario_name).inc()
                logger.info("Сгенерирована задача", extra={"cloudrm_scenario": scenario_name})
            except Exception as exc:  # noqa: BLE001
                runtime.last_error = str(exc)
                logger.warning("Не удалось отправить задачу генератора", extra={"cloudrm_error": str(exc)})
            await asyncio.sleep(float(scenario["interval"]))


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_task
    await runtime.start()
    worker_task = asyncio.create_task(generator_loop())
    try:
        yield
    finally:
        if worker_task is not None:
            worker_task.cancel()
        await runtime.stop()


app = FastAPI(title="CloudRM Load Generator", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {
            "status": "ok",
            "service": "load-generator",
            "ready": True,
        }
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "load-generator",
            "ready": False,
        },
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
