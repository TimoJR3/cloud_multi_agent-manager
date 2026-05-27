from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, QUEUE_LENGTH
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("forecast-agent")

runtime = ServiceRuntime("forecast-agent", ["request.classified", "execution.done", "execution.failed"])
worker_task: asyncio.Task[None] | None = None
window: deque[float] = deque(maxlen=20)


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            if envelope.event_type == "request.classified":
                window.append(time.time())
            queue_len = await runtime.redis.zcard("queue:waiting")
            intervals = [b - a for a, b in zip(list(window), list(window)[1:])]
            arrival_rate = 0.0 if not intervals else 1.0 / max(sum(intervals) / len(intervals), 0.001)
            predicted_queue = int(queue_len + arrival_rate * 5)
            overload_risk = min(1.0, predicted_queue / 20.0)
            QUEUE_LENGTH.labels("predicted").set(predicted_queue)
            await runtime.redis.hset(
                "forecast:latest",
                mapping={"predicted_queue": str(predicted_queue), "overload_risk": str(overload_risk), "arrival_rate": str(arrival_rate)},
            )
            await runtime.publish(
                "forecast_queue",
                EventEnvelope(
                    event_type="forecast.queue",
                    correlation_id=envelope.correlation_id,
                    source="forecast-agent",
                    payload={
                        **envelope.payload,
                        "predicted_queue": predicted_queue,
                        "overload_risk": round(overload_risk, 4),
                        "arrival_rate": round(arrival_rate, 4),
                    },
                ),
            )
            await runtime.consumer.commit()
            runtime.last_error = None
            logger.info("Сформирован прогноз очереди", extra={"cloudrm_predicted_queue": predicted_queue})
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Ошибка forecast-агента")
        finally:
            AGENT_LATENCY.labels("forecast-agent").observe(time.perf_counter() - started)


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


app = FastAPI(title="CloudRM Forecast Agent", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "forecast-agent",
    }

@app.get("/ready")
async def ready():
    if runtime.ready:
        return {
            "status": "ok",
            "service": "forecast-agent",
            "ready": True,
        }
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "forecast-agent",
            "ready": False,
        },
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
