from __future__ import annotations

import argparse
import asyncio
import csv
import os
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import httpx


SCENARIOS: dict[str, dict[str, Any]] = {
    "normal": {"rps": 1.0, "mix": {"standard": 0.7, "cpu-heavy": 0.2, "memory-heavy": 0.1}},
    "overload": {"rps": 5.0, "mix": {"standard": 0.4, "cpu-heavy": 0.3, "memory-heavy": 0.2, "critical": 0.1}},
    "peak": {"rps": 5.0, "mix": {"standard": 0.4, "cpu-heavy": 0.3, "memory-heavy": 0.2, "critical": 0.1}},
    "gpu_shortage": {"rps": 2.0, "mix": {"gpu": 0.8, "standard": 0.2}},
    "node_failure": {"rps": 2.0, "mix": {"standard": 0.6, "critical": 0.2, "cpu-heavy": 0.2}, "fail_node": "node-a"},
}


@dataclass
class SubmittedTask:
    task_id: str
    submitted_at: float
    final_status: str = "unknown"


def weighted_class(rng: random.Random, mix: dict[str, float]) -> str:
    value = rng.random()
    cumulative = 0.0
    for task_class, weight in mix.items():
        cumulative += weight
        if value <= cumulative:
            return task_class
    return next(reversed(mix))


def task_payload(task_class: str, rng: random.Random) -> dict[str, Any]:
    payload = {
        "task_type": task_class,
        "cpu_required": 1,
        "ram_required_mb": 512,
        "duration_seconds": rng.uniform(0.5, 2.0),
        "priority": 2,
        "sla_deadline_seconds": 20,
        "requires_gpu": False,
    }
    if task_class == "cpu-heavy":
        payload["cpu_required"] = 5
    elif task_class == "memory-heavy":
        payload["ram_required_mb"] = 12288
    elif task_class == "gpu":
        payload["requires_gpu"] = True
        payload["cpu_required"] = 2
    elif task_class == "critical":
        payload["priority"] = 9
        payload["sla_deadline_seconds"] = 8
    return payload


async def submit_load(client: httpx.AsyncClient, scenario: str, mode: str, seed: int, duration: int, rps: float, mix: dict[str, float]) -> list[SubmittedTask]:
    rng = random.Random(seed)
    await client.post("/experiments/start", json={"scenario": scenario, "mode": mode})
    submitted: list[SubmittedTask] = []
    deadline = time.time() + duration
    interval = 1.0 / max(rps, 0.001)
    while time.time() < deadline:
        task_class = weighted_class(rng, mix)
        response = await client.post("/tasks", json=task_payload(task_class, rng))
        response.raise_for_status()
        submitted.append(SubmittedTask(task_id=response.json()["task_id"], submitted_at=time.time()))
        await asyncio.sleep(interval)
    return submitted


async def wait_for_completion(client: httpx.AsyncClient, submitted: list[SubmittedTask], timeout: int) -> None:
    end = time.time() + timeout
    unfinished = {task.task_id: task for task in submitted}
    while unfinished and time.time() < end:
        for task_id in list(unfinished):
            response = await client.get(f"/tasks/{task_id}")
            if response.status_code != 200:
                continue
            status = response.json()["status"]
            if status in {"done", "failed"}:
                unfinished[task_id].final_status = status
                unfinished.pop(task_id)
        await asyncio.sleep(1)


def db_dsn() -> str | None:
    raw = os.getenv("DATABASE_URL")
    if not raw:
        return None
    return raw.replace("postgresql+asyncpg://", "postgresql://")


async def collect_db_metrics(task_ids: list[str]) -> dict[str, float] | None:
    dsn = db_dsn()
    if not dsn:
        return None
    try:
        conn = await asyncpg.connect(dsn)
    except Exception:
        return None
    try:
        rows = await conn.fetch(
            """
            SELECT t.task_id, t.status, t.created_at, e.started_at, e.finished_at
            FROM tasks t
            LEFT JOIN execution_history e ON e.task_id = t.task_id
            WHERE t.task_id::text = ANY($1::text[])
            """,
            task_ids,
        )
        waits = [
            (row["started_at"] - row["created_at"]).total_seconds()
            for row in rows
            if row["started_at"] is not None and row["created_at"] is not None
        ]
        violations = await conn.fetchval("SELECT count(*) FROM sla_violations WHERE task_id::text = ANY($1::text[])", task_ids)
        decisions = await conn.fetch(
            "SELECT EXTRACT(EPOCH FROM (d.created_at - t.created_at)) * 1000 AS latency_ms FROM decisions d JOIN tasks t ON t.task_id = d.task_id WHERE d.task_id::text = ANY($1::text[])",
            task_ids,
        )
        events = await conn.fetchval("SELECT count(*) FROM events WHERE payload->>'task_id' = ANY($1::text[])", task_ids)
        scale_actions = await conn.fetchval("SELECT count(*) FROM events WHERE event_type LIKE 'scaling.%'")
        return {
            "average_wait_seconds": statistics.fmean(waits) if waits else 0.0,
            "p95_wait_seconds": percentile(waits, 95),
            "sla_violations_percent": (float(violations or 0) / max(len(task_ids), 1)) * 100,
            "messages_per_task": float(events or 0) / max(len(task_ids), 1),
            "average_decision_latency_ms": statistics.fmean([float(row["latency_ms"]) for row in decisions if row["latency_ms"] is not None]) if decisions else 0.0,
            "scaling_actions_count": float(scale_actions or 0),
        }
    finally:
        await conn.close()


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


async def node_metrics(client: httpx.AsyncClient) -> dict[str, float]:
    response = await client.get("/nodes")
    response.raise_for_status()
    nodes = response.json()
    cpu_utils = []
    imbalances = []
    for node in nodes:
        cpu_total = float(node.get("cpu_total", 0) or 0)
        ram_total = float(node.get("ram_total_mb", 0) or 0)
        if cpu_total <= 0 or ram_total <= 0:
            continue
        cpu_util = float(node.get("cpu_used", 0) or 0) / cpu_total
        ram_util = float(node.get("ram_used_mb", 0) or 0) / ram_total
        cpu_utils.append(cpu_util)
        imbalances.append(abs(cpu_util - ram_util))
    return {
        "average_cpu_utilization": statistics.fmean(cpu_utils) if cpu_utils else 0.0,
        "resource_imbalance_index": statistics.fmean(imbalances) if imbalances else 0.0,
    }


async def run_one(args: argparse.Namespace, scenario: str, mode: str) -> dict[str, Any]:
    settings = SCENARIOS[scenario]
    rps = args.requests_per_second or float(settings["rps"])
    mix = settings["mix"]
    async with httpx.AsyncClient(base_url=args.api_url, timeout=10) as client:
        submitted = await submit_load(client, scenario, mode, args.seed, args.duration_seconds, rps, mix)
        if settings.get("fail_node"):
            await asyncio.sleep(max(1, args.duration_seconds // 3))
            await client.post(f"/nodes/{settings['fail_node']}/failure")
        await wait_for_completion(client, submitted, args.completion_timeout)
        db_metrics = await collect_db_metrics([task.task_id for task in submitted])
        metrics = db_metrics or {
            "average_wait_seconds": 0.0,
            "p95_wait_seconds": 0.0,
            "sla_violations_percent": 0.0,
            "messages_per_task": 0.0,
            "average_decision_latency_ms": 0.0,
            "scaling_actions_count": 0.0,
        }
        metrics.update(await node_metrics(client))
        await client.post("/experiments/stop")
    return {"scenario": scenario, "mode": mode, "tasks": len(submitted), **metrics}


def write_outputs(rows: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    fields = [
        "scenario",
        "mode",
        "tasks",
        "average_wait_seconds",
        "p95_wait_seconds",
        "sla_violations_percent",
        "average_cpu_utilization",
        "resource_imbalance_index",
        "scaling_actions_count",
        "messages_per_task",
        "average_decision_latency_ms",
    ]
    with (output_dir / "results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Experiment Summary", "", "| Scenario | Mode | Tasks | Avg wait | P95 wait | SLA violations % | Avg CPU | Imbalance | Scaling | Msg/task | Decision ms |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        lines.append(
            f"| {row['scenario']} | {row['mode']} | {row['tasks']} | {row['average_wait_seconds']:.3f} | {row['p95_wait_seconds']:.3f} | {row['sla_violations_percent']:.2f} | {row['average_cpu_utilization']:.3f} | {row['resource_imbalance_index']:.3f} | {row['scaling_actions_count']:.0f} | {row['messages_per_task']:.2f} | {row['average_decision_latency_ms']:.2f} |"
        )
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default=os.getenv("API_URL", "http://localhost:8000"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--completion-timeout", type=int, default=120)
    parser.add_argument("--requests-per-second", type=float)
    parser.add_argument("--scenarios", nargs="+", default=["normal", "overload", "gpu_shortage", "node_failure"])
    parser.add_argument("--modes", nargs="+", default=["baseline", "mas"])
    parser.add_argument("--output-dir", default="artifacts/experiments")
    args = parser.parse_args()
    rows = []
    for scenario in args.scenarios:
        for mode in args.modes:
            rows.append(await run_one(args, scenario, mode))
    write_outputs(rows, Path(args.output_dir))


if __name__ == "__main__":
    asyncio.run(main())
