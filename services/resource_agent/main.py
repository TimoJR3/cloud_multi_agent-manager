from __future__ import annotations

import asyncio
import logging
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
from cloudrm.metrics import AGENT_LATENCY, NODE_UTILIZATION
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("resource-agent")

runtime = ServiceRuntime("resource-agent", ["need_placement", "node.unavailable"])
worker_task: asyncio.Task[None] | None = None


async def initialize_nodes() -> None:
    assert runtime.redis is not None
    ttl = int(get_nested(runtime.config, "redis", "runtime_ttl_seconds", default=3600))
    for node in get_nested(runtime.config, "agents", "resource", "nodes", default=[]):
        key = f"node:{node['id']}"
        existing = await runtime.redis.hgetall(key)
        status = existing.get("status", "ready")
        await runtime.redis.hset(
            key,
            mapping={
                "node_id": node["id"],
                "status": status,
                "cpu_total": str(node["cpu_total"]),
                "ram_total_mb": str(node["ram_total_mb"]),
                "cpu_used": existing.get("cpu_used", "0"),
                "ram_used_mb": existing.get("ram_used_mb", "0"),
                "gpu": node.get("labels", {}).get("gpu", "false"),
                "zone": node.get("labels", {}).get("zone", "unknown"),
            },
        )
        await runtime.redis.expire(key, ttl)


def score_node(node: dict[str, str], task: dict[str, Any]) -> dict[str, Any] | None:
    if node.get("status") == "failed":
        return None
    cpu_total = float(node["cpu_total"])
    ram_total = float(node["ram_total_mb"])
    cpu_used = float(node.get("cpu_used", 0))
    ram_used = float(node.get("ram_used_mb", 0))
    cpu_required = float(task.get("cpu_required", 1))
    ram_required = float(task.get("ram_required_mb", 512))
    requires_gpu = bool(task.get("requires_gpu", False))
    if requires_gpu and node.get("gpu") != "true":
        return None
    if cpu_total - cpu_used < cpu_required or ram_total - ram_used < ram_required:
        return None
    cpu_after = (cpu_used + cpu_required) / cpu_total
    ram_after = (ram_used + ram_required) / ram_total
    balance = 1.0 - abs(cpu_after - ram_after)
    fit = max(0.0, 1.0 - ((cpu_after + ram_after) / 2.0))
    utility_score = round(0.7 * fit + 0.3 * balance, 4)
    return {
        "node_id": node["node_id"],
        "fit_score": utility_score,
        "balance_score": round(balance, 4),
        "cost": round((cpu_required / cpu_total) + (ram_required / ram_total), 4),
        "cpu_utilization_after": round(cpu_after, 4),
        "ram_utilization_after": round(ram_after, 4),
    }


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    await initialize_nodes()
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            task = envelope.payload
            if envelope.event_type == "node.unavailable":
                await runtime.redis.hset(f"node:{task['node_id']}", mapping={"status": "failed"})
                await runtime.consumer.commit()
                runtime.last_error = None
                continue
            keys = await runtime.redis.keys("node:*")
            proposals = 0
            for key in keys:
                node = await runtime.redis.hgetall(key)
                if not node or "cpu_total" not in node:
                    continue
                NODE_UTILIZATION.labels(node["node_id"], "cpu").set(float(node.get("cpu_used", 0)) / float(node["cpu_total"]))
                NODE_UTILIZATION.labels(node["node_id"], "ram").set(
                    float(node.get("ram_used_mb", 0)) / float(node["ram_total_mb"])
                )
                proposal = score_node(node, task)
                if proposal is None:
                    continue
                proposals += 1
                await runtime.publish(
                    "node_proposal_fit",
                    EventEnvelope(
                        event_type="node.proposal.fit",
                        correlation_id=envelope.correlation_id,
                        source="resource-agent",
                        payload={**task, **proposal, "unsafe": False, "resource_unsafe": False},
                    ),
                )
            if proposals == 0:
                await runtime.publish(
                    "node_proposal_fit",
                    EventEnvelope(
                        event_type="node.proposal.fit",
                        correlation_id=envelope.correlation_id,
                        source="resource-agent",
                        payload={**task, "node_id": None, "fit_score": 0.0, "unsafe": True, "reason": "нет подходящих узлов"},
                    ),
                )
            await runtime.consumer.commit()
            runtime.last_error = None
            logger.info("Сформированы предложения ресурсов", extra={"cloudrm_task_id": task.get("task_id"), "cloudrm_count": proposals})
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Ошибка агента ресурсов")
        finally:
            AGENT_LATENCY.labels("resource-agent").observe(time.perf_counter() - started)


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


app = FastAPI(title="CloudRM Resource Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()



@app.get("/ready")
async def ready():
    if runtime.ready:
        return {
            "status": "ok",
            "service": "resource-agent",
            "ready": True,
        }
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "resource-agent",
            "ready": False,
        },
    )

@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
