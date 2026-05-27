import pytest

from services.scale_agent import main as scale_agent


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sequence = 0

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.values[key] = value

    async def incr(self, key: str):
        self.sequence += 1
        self.values[key] = str(self.sequence)
        return self.sequence

    async def hset(self, key: str, mapping: dict[str, str]):
        self.hashes[key] = mapping


@pytest.mark.asyncio
async def test_scale_out_respects_cooldown(monkeypatch) -> None:
    published: list[str] = []
    fake_redis = FakeRedis()
    scale_agent.runtime.redis = fake_redis
    scale_agent.runtime.config = {"agents": {"scale": {"cooldown_seconds": 60, "emulator_node": {"cpu_total": 4, "ram_total_mb": 4096}}}}

    async def fake_publish(topic_key, envelope):
        published.append(envelope.event_type)

    monkeypatch.setattr(scale_agent.runtime, "publish", fake_publish)

    await scale_agent.scale_out("c1", {"task_id": "t1"})
    await scale_agent.scale_out("c2", {"task_id": "t2"})

    assert "node:node-auto-1" in fake_redis.hashes
    assert published.count("scaling.done") == 1
    assert "scaling.blocked" in published
