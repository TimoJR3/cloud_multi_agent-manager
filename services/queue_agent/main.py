from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.config import get_nested
from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, QUEUE_LENGTH
from cloudrm.persistence import update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("queue-agent")

runtime = ServiceRuntime("queue-agent", ["request.created"])
worker_task: asyncio.Task[None] | None = None


def classify(payload: dict[str, Any]) -> str:
    if payload.get("requires_gpu"):
        return "gpu"
    if float(payload.get("cpu_required", 1)) >= 4:
        return "cpu-heavy"
    if int(payload.get("ram_required_mb", 512)) >= 8192:
        return "memory-heavy"
    return "standard"


def dynamic_priority(payload: dict[str, Any], created_at: float) -> int:
    aging_factor = float(get_nested(runtime.config, "agents", "queue", "aging_factor", default=0.05))
    waited = max(0.0, time.time() - created_at)
    base = int(payload.get("priority", 1))
    return min(10, int(base + waited * aging_factor))


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    assert runtime.db is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            payload = envelope.payload
            task_id = payload["task_id"]
            created_at = time.time()
            task_class = classify(payload)
            priority = dynamic_priority(payload, created_at)
            await runtime.redis.zadd("queue:waiting", {task_id: priority})
            await runtime.redis.hset(
                f"task:{task_id}",
                mapping={"status": "classified", "class": task_class, "priority": str(priority)},
            )
            queue_length = await runtime.redis.zcard("queue:waiting")
            QUEUE_LENGTH.labels("waiting").set(queue_length)
            await update_task(runtime.db, task_id, status="classified", priority=priority)

            classified_payload = {**payload, "class": task_class, "dynamic_priority": priority, "queue_length": queue_length}
            await runtime.publish(
                "request_classified",
                EventEnvelope(
                    event_type="request.classified",
                    correlation_id=envelope.correlation_id,
                    source="queue-agent",
                    payload=classified_payload,
                ),
            )
            await runtime.publish(
                "need_placement",
                EventEnvelope(
                    event_type="need_placement",
                    correlation_id=envelope.correlation_id,
                    source="queue-agent",
                    payload=classified_payload,
                ),
            )
            await runtime.consumer.commit()
            runtime.last_error = None
            logger.info("Задача классифицирована", extra={"cloudrm_task_id": task_id, "cloudrm_class": task_class})
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Ошибка обработки события очереди")
        finally:
            AGENT_LATENCY.labels("queue-agent").observe(time.perf_counter() - started)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_task
    await runtime.start()
    worker_task = asyncio.create_task(worker())
    try:
        yield
    finally:
        if worker_task is not None:
            worker_task.cancel()
        await runtime.stop()


app = FastAPI(title="CloudRM Queue Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {
            "status": "ok",
            "service": "queue-agent",
            "ready": True,
        }

    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "queue-agent",
            "ready": False,
        },
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
