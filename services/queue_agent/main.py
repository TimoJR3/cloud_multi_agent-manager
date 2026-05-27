from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.config import get_nested
from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, QUEUE_LENGTH
from cloudrm.persistence import update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("queue-agent")

runtime = ServiceRuntime("queue-agent", ["request.created", "sla.risk", "sla.boost", "scaling.done"])
worker_task: asyncio.Task[None] | None = None
aging_task: asyncio.Task[None] | None = None

TASK_CLASSES = ("standard", "cpu-heavy", "memory-heavy", "gpu", "critical")


def classify(payload: dict[str, Any]) -> str:
    if payload.get("task_type") == "critical" or int(payload.get("priority", 1)) >= 9:
        return "critical"
    if payload.get("requires_gpu"):
        return "gpu"
    if float(payload.get("cpu_required", 1)) >= 4:
        return "cpu-heavy"
    if int(payload.get("ram_required_mb", 512)) >= 8192:
        return "memory-heavy"
    return "standard"


def queue_key(task_class: str) -> str:
    return f"queue:waiting:{task_class}"


def dynamic_priority(payload: dict[str, Any], created_at: float, now: float | None = None) -> float:
    aging_factor = float(get_nested(runtime.config, "agents", "queue", "aging_factor", default=0.05))
    waited = max(0.0, (now or time.time()) - created_at)
    base = int(payload.get("base_priority", payload.get("priority", 1)))
    boost = int(payload.get("sla_boost", 0))
    return min(20.0, base + boost + waited * aging_factor)


async def set_queue_metrics() -> None:
    assert runtime.redis is not None
    QUEUE_LENGTH.labels("waiting").set(await runtime.redis.zcard("queue:waiting"))
    for task_class in TASK_CLASSES:
        QUEUE_LENGTH.labels(task_class).set(await runtime.redis.zcard(queue_key(task_class)))


async def enqueue_task(payload: dict[str, Any], correlation_id: str) -> None:
    assert runtime.redis is not None
    assert runtime.db is not None
    task_id = payload["task_id"]
    created_at = float(payload.get("created_at_ts", time.time()))
    task_class = classify(payload)
    score = dynamic_priority(payload, created_at)
    await runtime.redis.zadd("queue:waiting", {task_id: score})
    await runtime.redis.zadd(queue_key(task_class), {task_id: score})
    await runtime.redis.hset(
        f"task:{task_id}",
        mapping={
            "status": "classified",
            "class": task_class,
            "priority": str(int(min(10, score))),
            "base_priority": str(payload.get("priority", 1)),
            "created_at_ts": str(created_at),
            "sla_boost": str(payload.get("sla_boost", 0)),
        },
    )
    await set_queue_metrics()
    await update_task(runtime.db, task_id, status="classified", priority=int(min(10, score)))
    queue_length = await runtime.redis.zcard("queue:waiting")
    classified_payload = {**payload, "class": task_class, "dynamic_priority": int(min(10, score)), "queue_length": queue_length}
    await runtime.publish(
        "request_classified",
        EventEnvelope(
            event_type="request.classified",
            correlation_id=correlation_id,
            source="queue-agent",
            payload=classified_payload,
        ),
    )
    await runtime.publish(
        "need_placement",
        EventEnvelope(
            event_type="need_placement",
            correlation_id=correlation_id,
            source="queue-agent",
            payload=classified_payload,
        ),
    )


async def apply_sla_signal(payload: dict[str, Any], correlation_id: str) -> None:
    assert runtime.redis is not None
    task_id = payload.get("task_id")
    if not task_id:
        return
    if await runtime.redis.get("experiment:mode") == "baseline":
        return
    task = await runtime.redis.hgetall(f"task:{task_id}")
    if not task or task.get("status") not in {"classified", "waiting"}:
        return
    severity = str(payload.get("severity", payload.get("sla_severity", "low")))
    boost = {"low": 0, "medium": 1, "high": 2, "critical": 4}.get(severity, 1)
    boost = max(boost, int(payload.get("boost", 0)))
    if boost <= 0:
        return
    task_class = task.get("class", "standard")
    current_boost = int(task.get("sla_boost", 0))
    created_at = float(task.get("created_at_ts", time.time()))
    merged = {**task, "sla_boost": current_boost + boost}
    score = dynamic_priority(merged, created_at)
    await runtime.redis.hset(
        f"task:{task_id}",
        mapping={"sla_boost": str(current_boost + boost), "priority": str(int(min(10, score)))},
    )
    await runtime.redis.zadd("queue:waiting", {task_id: score})
    await runtime.redis.zadd(queue_key(task_class), {task_id: score})
    await set_queue_metrics()
    await runtime.publish(
        "need_placement",
        EventEnvelope(
            event_type="need_placement",
            correlation_id=correlation_id,
            source="queue-agent",
            payload={**payload, "class": task_class, "dynamic_priority": int(min(10, score))},
        ),
    )


async def handle_scaling_done(payload: dict[str, Any], correlation_id: str) -> None:
    assert runtime.redis is not None
    if not payload.get("requeue_after_scale"):
        return
    task_id = payload.get("task_id")
    if not task_id:
        return
    task = await runtime.redis.hgetall(f"task:{task_id}")
    if task.get("status") in {"running", "done", "dispatched"}:
        return
    task_class = task.get("class", payload.get("class", classify(payload)))
    score = float(task.get("priority", payload.get("dynamic_priority", payload.get("priority", 1))))
    await runtime.redis.zadd("queue:waiting", {task_id: score})
    await runtime.redis.zadd(queue_key(task_class), {task_id: score})
    await set_queue_metrics()
    await runtime.publish(
        "need_placement",
        EventEnvelope(
            event_type="need_placement",
            correlation_id=correlation_id,
            source="queue-agent",
            payload={**payload, **task, "task_id": task_id, "class": task_class, "dynamic_priority": int(min(10, score))},
        ),
    )
    logger.info("Task requeued after scaling", extra={"cloudrm_task_id": task_id, "cloudrm_node_id": payload.get("node_id")})


async def aging_loop() -> None:
    assert runtime.redis is not None
    interval = float(get_nested(runtime.config, "agents", "queue", "aging_interval_seconds", default=1.0))
    while True:
        try:
            now = time.time()
            for task_class in TASK_CLASSES:
                ids = await runtime.redis.zrange(queue_key(task_class), 0, -1)
                for task_id in ids:
                    task = await runtime.redis.hgetall(f"task:{task_id}")
                    if not task:
                        continue
                    created_at = float(task.get("created_at_ts", now))
                    score = dynamic_priority(task, created_at, now=now)
                    await runtime.redis.zadd(queue_key(task_class), {task_id: score})
                    await runtime.redis.zadd("queue:waiting", {task_id: score})
            await set_queue_metrics()
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Queue aging loop failed")
        await asyncio.sleep(interval)


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    assert runtime.db is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            if envelope.event_type == "request.created":
                await enqueue_task(envelope.payload, envelope.correlation_id)
                logger.info("Task classified", extra={"cloudrm_task_id": envelope.payload["task_id"], "cloudrm_class": classify(envelope.payload)})
            elif envelope.event_type in {"sla.boost", "sla.risk"}:
                await apply_sla_signal(envelope.payload, envelope.correlation_id)
            elif envelope.event_type == "scaling.done":
                await handle_scaling_done(envelope.payload, envelope.correlation_id)
            await runtime.consumer.commit()
            runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Queue event processing failed")
        finally:
            AGENT_LATENCY.labels("queue-agent").observe(time.perf_counter() - started)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_task, aging_task
    await runtime.start()
    worker_task = asyncio.create_task(worker())
    aging_task = asyncio.create_task(aging_loop())
    try:
        yield
    finally:
        if worker_task is not None:
            worker_task.cancel()
        if aging_task is not None:
            aging_task.cancel()
        await runtime.stop()


app = FastAPI(title="CloudRM Queue Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {"status": "ok", "service": "queue-agent", "ready": True}
    return JSONResponse(status_code=503, content={"status": "not_ready", "service": "queue-agent", "ready": False})


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
