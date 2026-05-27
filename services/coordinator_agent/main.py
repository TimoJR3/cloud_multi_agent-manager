from __future__ import annotations

import asyncio
import json
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
from cloudrm.metrics import AGENT_LATENCY
from cloudrm.persistence import save_decision, update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("coordinator-agent")

runtime = ServiceRuntime("coordinator-agent", ["request.classified", "node.proposal.fit", "sla.risk", "forecast.queue"])
worker_task: asyncio.Task[None] | None = None


async def save_payload(prefix: str, task_id: str, payload: dict[str, Any]) -> None:
    assert runtime.redis is not None
    ttl = int(get_nested(runtime.config, "redis", "runtime_ttl_seconds", default=3600))
    await runtime.redis.set(f"{prefix}:{task_id}", json.dumps(payload, ensure_ascii=False), ex=ttl)


async def load_payload(prefix: str, task_id: str) -> dict[str, Any] | None:
    assert runtime.redis is not None
    raw = await runtime.redis.get(f"{prefix}:{task_id}")
    return json.loads(raw) if raw else None


def utility(proposal: dict[str, Any], sla: dict[str, Any] | None, forecast: dict[str, Any] | None) -> float:
    weights = get_nested(runtime.config, "agents", "coordinator", "weights", default={})
    fit = float(proposal.get("fit_score", 0))
    risk = float((sla or {}).get("sla_risk", 0.2))
    balance = float(proposal.get("balance_score", 0.5))
    overload = float((forecast or {}).get("overload_risk", 0.2))
    cost = float(proposal.get("cost", 0.1))
    value = (
        float(weights.get("fit", 0.35)) * fit
        + float(weights.get("sla", 0.25)) * (1 - risk)
        + float(weights.get("balance", 0.2)) * balance
        + float(weights.get("forecast", 0.1)) * (1 - overload)
        - float(weights.get("cost", 0.1)) * cost
    )
    return round(value, 4)


async def decide(task_id: str, correlation_id: str) -> None:
    assert runtime.redis is not None
    assert runtime.db is not None
    if await runtime.redis.get(f"decision:made:{task_id}"):
        return
    task = await load_payload("classified", task_id)
    if task is None:
        return
    sla = await load_payload("sla", task_id)
    forecast = await load_payload("forecast", task_id)
    proposal_keys = await runtime.redis.keys(f"proposal:{task_id}:*")
    proposals: list[dict[str, Any]] = []
    for key in proposal_keys:
        raw = await runtime.redis.get(key)
        if raw:
            proposals.append(json.loads(raw))
    if not proposals:
        return
    safe_proposals = [proposal for proposal in proposals if proposal.get("node_id") and not proposal.get("unsafe")]
    if sla and sla.get("unsafe") and not safe_proposals:
        await runtime.publish(
            "decision_scale",
            EventEnvelope(
                event_type="decision.scale",
                correlation_id=correlation_id,
                source="coordinator-agent",
                payload={**task, "reason": "SLA-риск и нет безопасного размещения", "recommended_nodes": 1},
            ),
        )
        await save_decision(
            runtime.db,
            task_id=task_id,
            correlation_id=correlation_id,
            node_id=None,
            utility=0.0,
            decision_type="scale",
            payload={"reason": "SLA-риск и нет безопасного размещения"},
        )
        await runtime.redis.set(f"decision:made:{task_id}", "scale", ex=3600)
        return
    candidates = safe_proposals or proposals
    ranked = sorted(candidates, key=lambda item: utility(item, sla, forecast), reverse=True)
    selected = ranked[0]
    selected_utility = utility(selected, sla, forecast)
    if not selected.get("node_id"):
        await runtime.publish(
            "decision_scale",
            EventEnvelope(
                event_type="decision.scale",
                correlation_id=correlation_id,
                source="coordinator-agent",
                payload={**task, "reason": selected.get("reason", "нет подходящего узла"), "recommended_nodes": 1},
            ),
        )
        await save_decision(
            runtime.db,
            task_id=task_id,
            correlation_id=correlation_id,
            node_id=None,
            utility=selected_utility,
            decision_type="scale",
            payload=selected,
        )
        await runtime.redis.set(f"decision:made:{task_id}", "scale", ex=3600)
        return
    decision_payload = {**task, **selected, "utility": selected_utility}
    await update_task(runtime.db, task_id, status="dispatched", assigned_node_id=selected["node_id"])
    await runtime.redis.hset(f"task:{task_id}", mapping={"status": "dispatched", "node_id": selected["node_id"]})
    await runtime.publish(
        "decision_dispatch",
        EventEnvelope(
            event_type="decision.dispatch",
            correlation_id=correlation_id,
            source="coordinator-agent",
            payload=decision_payload,
        ),
    )
    await save_decision(
        runtime.db,
        task_id=task_id,
        correlation_id=correlation_id,
        node_id=selected["node_id"],
        utility=selected_utility,
        decision_type="dispatch",
        payload=decision_payload,
    )
    await runtime.redis.set(f"decision:made:{task_id}", "dispatch", ex=3600)
    logger.info("Принято решение о размещении", extra={"cloudrm_task_id": task_id, "cloudrm_node_id": selected["node_id"]})


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            payload = envelope.payload
            task_id = payload.get("task_id")
            if not task_id:
                await runtime.consumer.commit()
                continue
            if envelope.event_type == "request.classified":
                await save_payload("classified", task_id, payload)
            elif envelope.event_type == "sla.risk":
                await save_payload("sla", task_id, payload)
            elif envelope.event_type == "forecast.queue":
                await save_payload("forecast", task_id, payload)
            elif envelope.event_type == "node.proposal.fit":
                node_id = payload.get("node_id") or "none"
                await save_payload(f"proposal:{task_id}", node_id, payload)
            await decide(task_id, envelope.correlation_id)
            await runtime.consumer.commit()
            runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Ошибка координатора")
        finally:
            AGENT_LATENCY.labels("coordinator-agent").observe(time.perf_counter() - started)


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


app = FastAPI(title="CloudRM Coordinator Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {
            "status": "ok",
            "service": "coordinator-agent",
            "ready": True,
        }
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "coordinator-agent",
            "ready": False,
        },
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
