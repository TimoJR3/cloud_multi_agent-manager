from __future__ import annotations

import asyncio
import json
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
from cloudrm.metrics import AGENT_LATENCY, DECISION_LATENCY_SECONDS
from cloudrm.persistence import save_decision, update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("coordinator-agent")

runtime = ServiceRuntime("coordinator-agent", ["request.classified", "node.proposal.fit", "sla.risk", "forecast.queue"])
worker_task: asyncio.Task[None] | None = None
pending_decisions: dict[str, asyncio.Task[None]] = {}


def proposal_ttl() -> int:
    return int(get_nested(runtime.config, "agents", "coordinator", "proposal_ttl_seconds", default=30))


async def save_payload(prefix: str, task_id: str, payload: dict[str, Any], ttl: int | None = None) -> None:
    assert runtime.redis is not None
    effective_ttl = ttl or int(get_nested(runtime.config, "redis", "runtime_ttl_seconds", default=3600))
    await runtime.redis.set(f"{prefix}:{task_id}", json.dumps(payload, ensure_ascii=False), ex=effective_ttl)


async def load_payload(prefix: str, task_id: str) -> dict[str, Any] | None:
    assert runtime.redis is not None
    raw = await runtime.redis.get(f"{prefix}:{task_id}")
    return json.loads(raw) if raw else None


def utility_breakdown(proposal: dict[str, Any], sla: dict[str, Any] | None, forecast: dict[str, Any] | None) -> dict[str, float]:
    weights = get_nested(runtime.config, "agents", "coordinator", "weights", default={})
    fit = float(proposal.get("fit_score", 0.0))
    risk = float((sla or {}).get("sla_risk", (sla or {}).get("risk", 0.2)))
    balance = float(proposal.get("balance_score", 0.5))
    overload = float((forecast or {}).get("overload_risk", 0.2))
    cost = float(proposal.get("cost", 0.1))
    return {
        "fit": round(float(weights.get("fit", 0.35)) * fit, 4),
        "sla": round(float(weights.get("sla", 0.25)) * (1 - risk), 4),
        "balance": round(float(weights.get("balance", 0.2)) * balance, 4),
        "forecast": round(float(weights.get("forecast", 0.1)) * (1 - overload), 4),
        "cost": round(-float(weights.get("cost", 0.1)) * cost, 4),
    }


def utility(proposal: dict[str, Any], sla: dict[str, Any] | None, forecast: dict[str, Any] | None) -> float:
    return round(sum(utility_breakdown(proposal, sla, forecast).values()), 4)


async def load_proposals(task_id: str) -> list[dict[str, Any]]:
    assert runtime.redis is not None
    proposal_keys = await runtime.redis.keys(f"proposal:{task_id}:*")
    proposals: list[dict[str, Any]] = []
    for key in proposal_keys:
        raw = await runtime.redis.get(key)
        if raw:
            proposals.append(json.loads(raw))
    return proposals


async def degrade_proposals(task: dict[str, Any]) -> list[dict[str, Any]]:
    assert runtime.redis is not None
    proposals: list[dict[str, Any]] = []
    for key in await runtime.redis.keys("node:*"):
        node = await runtime.redis.hgetall(key)
        if not node or node.get("status") == "failed" or "cpu_total" not in node:
            continue
        cpu_free = float(node["cpu_total"]) - float(node.get("cpu_used", 0))
        ram_free = float(node["ram_total_mb"]) - float(node.get("ram_used_mb", 0))
        if task.get("requires_gpu") and node.get("gpu") != "true":
            continue
        if cpu_free < float(task.get("cpu_required", 1)) or ram_free < float(task.get("ram_required_mb", 512)):
            continue
        proposals.append(
            {
                **task,
                "node_id": node["node_id"],
                "fit_score": round((cpu_free / float(node["cpu_total"]) + ram_free / float(node["ram_total_mb"])) / 2, 4),
                "balance_score": 0.5,
                "cost": 0.2,
                "degraded": True,
            }
        )
    return proposals


def build_explanation(
    decision_type: str,
    task: dict[str, Any],
    selected: dict[str, Any] | None,
    sla: dict[str, Any] | None,
    forecast: dict[str, Any] | None,
    proposals: list[dict[str, Any]],
    reason: str,
) -> dict[str, Any]:
    breakdown = utility_breakdown(selected, sla, forecast) if selected else {}
    return {
        "reason": reason,
        "used_signals": {
            "task": bool(task),
            "proposals": len(proposals),
            "sla": bool(sla),
            "forecast": bool(forecast),
            "degraded": bool(selected and selected.get("degraded")),
        },
        "utility_breakdown": breakdown,
        "decision_type": decision_type,
    }


async def publish_scale(task: dict[str, Any], correlation_id: str, reason: str, explanation: dict[str, Any], utility_value: float = 0.0) -> None:
    assert runtime.db is not None
    task_id = task["task_id"]
    payload = {**task, **explanation, "recommended_nodes": 1}
    await runtime.publish(
        "decision_scale",
        EventEnvelope(event_type="decision.scale", correlation_id=correlation_id, source="coordinator-agent", payload=payload),
    )
    await save_decision(
        runtime.db,
        task_id=task_id,
        correlation_id=correlation_id,
        node_id=None,
        utility=utility_value,
        decision_type="scale",
        payload=payload,
    )
    await runtime.redis.set(f"decision:made:{task_id}", "scale", ex=3600)  # type: ignore[union-attr]
    logger.info("Scale decision emitted", extra={"cloudrm_task_id": task_id, "cloudrm_reason": reason})


async def decide(task_id: str, correlation_id: str) -> None:
    assert runtime.redis is not None
    assert runtime.db is not None
    if await runtime.redis.get(f"decision:made:{task_id}"):
        return
    task = await load_payload("classified", task_id)
    if task is None:
        return
    started = float(task.get("classified_at_ts", time.time()))
    sla = await load_payload("sla", task_id)
    forecast = await load_payload("forecast", task_id)
    mode = await runtime.redis.get("experiment:mode")
    if mode == "baseline":
        sla = None
        forecast = None
    proposals = await load_proposals(task_id)
    if not proposals:
        proposals = await degrade_proposals(task)
    safe_proposals = [proposal for proposal in proposals if proposal.get("node_id") and not proposal.get("unsafe")]

    if sla and sla.get("unsafe"):
        explanation = build_explanation("scale", task, None, sla, forecast, proposals, "sla_policy_blocks_dispatch")
        await publish_scale(task, correlation_id, "sla_policy_blocks_dispatch", explanation)
        DECISION_LATENCY_SECONDS.observe(time.time() - started)
        return
    if not safe_proposals:
        explanation = build_explanation("scale", task, None, sla, forecast, proposals, "no_safe_nodes_available")
        await publish_scale(task, correlation_id, "no_safe_nodes_available", explanation)
        DECISION_LATENCY_SECONDS.observe(time.time() - started)
        return

    if mode == "baseline":
        ranked = sorted(safe_proposals, key=lambda item: (float(item.get("fit_score", 0.0)), -float(item.get("cost", 0.0))), reverse=True)
    else:
        ranked = sorted(safe_proposals, key=lambda item: utility(item, sla, forecast), reverse=True)
    selected = ranked[0]
    selected_utility = utility(selected, sla, forecast)
    reason = "best_utility"
    if selected.get("degraded"):
        reason = "degraded_policy_no_agent_proposals"
    explanation = build_explanation("dispatch", task, selected, sla, forecast, proposals, reason)
    decision_payload = {**task, **selected, "utility": selected_utility, **explanation}
    await update_task(runtime.db, task_id, status="dispatched", assigned_node_id=selected["node_id"])
    await runtime.redis.hset(f"task:{task_id}", mapping={"status": "dispatched", "node_id": selected["node_id"]})
    await runtime.publish(
        "decision_dispatch",
        EventEnvelope(event_type="decision.dispatch", correlation_id=correlation_id, source="coordinator-agent", payload=decision_payload),
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
    DECISION_LATENCY_SECONDS.observe(time.time() - started)
    logger.info("Dispatch decision emitted", extra={"cloudrm_task_id": task_id, "cloudrm_node_id": selected["node_id"]})


async def schedule_decision(task_id: str, correlation_id: str) -> None:
    if task_id in pending_decisions and not pending_decisions[task_id].done():
        return
    window_ms = int(get_nested(runtime.config, "agents", "coordinator", "decision_window_ms", default=500))

    async def delayed() -> None:
        try:
            await asyncio.sleep(window_ms / 1000)
            await decide(task_id, correlation_id)
        finally:
            pending_decisions.pop(task_id, None)

    pending_decisions[task_id] = asyncio.create_task(delayed())


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
                await save_payload("classified", task_id, {**payload, "classified_at_ts": time.time()})
            elif envelope.event_type == "sla.risk":
                await save_payload("sla", task_id, payload)
            elif envelope.event_type == "forecast.queue":
                await save_payload("forecast", task_id, payload)
            elif envelope.event_type == "node.proposal.fit":
                node_id = payload.get("node_id") or "none"
                await save_payload(f"proposal:{task_id}", node_id, payload, ttl=proposal_ttl())
            elif envelope.event_type == "node.unavailable":
                await runtime.redis.hset(f"node:{payload.get('node_id')}", mapping={"status": "failed"})
            await schedule_decision(task_id, envelope.correlation_id)
            await runtime.consumer.commit()
            runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Coordinator event processing failed")
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
        for task in pending_decisions.values():
            task.cancel()
        await runtime.stop()


app = FastAPI(title="CloudRM Coordinator Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {"status": "ok", "service": "coordinator-agent", "ready": True}
    return JSONResponse(status_code=503, content={"status": "not_ready", "service": "coordinator-agent", "ready": False})


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
