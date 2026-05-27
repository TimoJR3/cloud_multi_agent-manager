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
from cloudrm.metrics import AGENT_LATENCY, SCALING_ACTIONS, SCALING_COOLDOWN_BLOCKS
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("scale-agent")

runtime = ServiceRuntime("scale-agent", ["decision.scale"])
worker_task: asyncio.Task[None] | None = None


def cooldown_seconds() -> int:
    return int(get_nested(runtime.config, "agents", "scale", "cooldown_seconds", default=30))


async def publish_scaling(event_type: str, correlation_id: str, payload: dict[str, Any]) -> None:
    topic_key = event_type.replace(".", "_")
    await runtime.publish(
        topic_key,
        EventEnvelope(event_type=event_type, correlation_id=correlation_id, source="scale-agent", payload=payload),
    )


async def blocked(correlation_id: str, payload: dict[str, Any], reason: str) -> None:
    SCALING_ACTIONS.labels(payload.get("action", "scale-out"), "blocked").inc()
    await publish_scaling("scaling.blocked", correlation_id, {**payload, "reason": reason})
    logger.info("Scaling blocked", extra={"cloudrm_reason": reason})


async def scale_out(correlation_id: str, payload: dict[str, Any]) -> None:
    assert runtime.redis is not None
    now = int(time.time())
    last = await runtime.redis.get("scaling:last_action_ts")
    mode = await runtime.redis.get("experiment:mode")
    if mode != "baseline" and last and now - int(float(last)) < cooldown_seconds():
        SCALING_COOLDOWN_BLOCKS.inc()
        await blocked(correlation_id, {**payload, "action": "scale-out"}, "cooldown")
        return
    await publish_scaling("scaling.started", correlation_id, {**payload, "action": "scale-out"})
    sequence = await runtime.redis.incr("scaling:auto_node_sequence")
    node_id = f"node-auto-{sequence}"
    defaults = get_nested(runtime.config, "agents", "scale", "emulator_node", default={})
    await runtime.redis.hset(
        f"node:{node_id}",
        mapping={
            "node_id": node_id,
            "status": "ready",
            "cpu_total": str(defaults.get("cpu_total", 8)),
            "ram_total_mb": str(defaults.get("ram_total_mb", 16384)),
            "cpu_used": "0",
            "ram_used_mb": "0",
            "gpu": str(defaults.get("gpu", "false")).lower(),
            "zone": str(defaults.get("zone", "auto")),
            "auto_scaled": "true",
        },
    )
    await runtime.redis.set("scaling:last_action_ts", str(now), ex=max(cooldown_seconds() * 2, 60))
    SCALING_ACTIONS.labels("scale-out", "done").inc()
    await publish_scaling("scaling.done", correlation_id, {**payload, "action": "scale-out", "node_id": node_id})
    logger.info("Scale-out done", extra={"cloudrm_node_id": node_id})


async def scale_in(correlation_id: str, payload: dict[str, Any]) -> None:
    assert runtime.redis is not None
    candidates = await runtime.redis.keys("node:node-auto-*")
    for key in candidates:
        node = await runtime.redis.hgetall(key)
        node_id = node.get("node_id")
        if not node_id:
            continue
        running = await runtime.redis.scard(f"node:{node_id}:running")
        cpu_used = float(node.get("cpu_used", 0))
        ram_used = float(node.get("ram_used_mb", 0))
        if running == 0 and cpu_used == 0 and ram_used == 0:
            await publish_scaling("scaling.started", correlation_id, {**payload, "action": "scale-in", "node_id": node_id})
            await runtime.redis.delete(key)
            SCALING_ACTIONS.labels("scale-in", "done").inc()
            await publish_scaling("scaling.done", correlation_id, {**payload, "action": "scale-in", "node_id": node_id})
            return
    await blocked(correlation_id, {**payload, "action": "scale-in"}, "no_safe_auto_nodes")


async def handle_decision(envelope: EventEnvelope) -> None:
    action = str(envelope.payload.get("action", "scale-out"))
    if action == "scale-in":
        await scale_in(envelope.correlation_id, envelope.payload)
    else:
        await scale_out(envelope.correlation_id, envelope.payload)


async def worker() -> None:
    assert runtime.consumer is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            if envelope.event_type == "decision.scale":
                await handle_decision(envelope)
            await runtime.consumer.commit()
            runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            SCALING_ACTIONS.labels("scale-out", "failed").inc()
            await publish_scaling("scaling.failed", envelope.correlation_id, {**envelope.payload, "error": str(exc)})
            logger.exception("Scaling failed")
        finally:
            AGENT_LATENCY.labels("scale-agent").observe(time.perf_counter() - started)


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


app = FastAPI(title="CloudRM Scale Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {"status": "ok", "service": "scale-agent", "ready": True}
    return JSONResponse(status_code=503, content={"status": "not_ready", "service": "scale-agent", "ready": False})


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
