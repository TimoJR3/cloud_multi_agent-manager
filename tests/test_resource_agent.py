import pytest

from cloudrm.messages import EventEnvelope
from services.resource_agent import main as resource_agent
from services.resource_agent.main import score_node


def test_resource_scoring_rejects_failed_node() -> None:
    assert score_node({"status": "failed"}, {"cpu_required": 1}) is None


def test_resource_scoring_checks_cpu_ram_and_gpu() -> None:
    node = {
        "node_id": "node-gpu",
        "status": "ready",
        "cpu_total": "8",
        "ram_total_mb": "16384",
        "cpu_used": "2",
        "ram_used_mb": "1024",
        "gpu": "true",
    }

    proposal = score_node(node, {"cpu_required": 2, "ram_required_mb": 2048, "requires_gpu": True})

    assert proposal is not None
    assert proposal["node_id"] == "node-gpu"
    assert proposal["fit_score"] > 0
    assert score_node({**node, "gpu": "false"}, {"cpu_required": 1, "ram_required_mb": 512, "requires_gpu": True}) is None
    assert score_node(node, {"cpu_required": 99, "ram_required_mb": 512}) is None
    assert score_node(node, {"cpu_required": 1, "ram_required_mb": 999999}) is None


class FakeRedis:
    def __init__(self) -> None:
        self.types = {
            "node:bad_key": "string",
            "node:valid": "hash",
            "node:incomplete": "hash",
        }
        self.hashes = {
            "node:valid": {
                "node_id": "node-valid",
                "status": "ready",
                "cpu_total": "8",
                "ram_total_mb": "16384",
                "cpu_used": "0",
                "ram_used_mb": "0",
                "gpu": "false",
            },
            "node:incomplete": {"node_id": "node-incomplete"},
        }
        self.hgetall_calls: list[str] = []

    async def scan_iter(self, match: str):
        for key in self.types:
            yield key

    async def type(self, key: str):
        return self.types[key]

    async def hgetall(self, key: str):
        self.hgetall_calls.append(key)
        if key == "node:bad_key":
            raise AssertionError("hgetall must not be called for non-hash keys")
        return self.hashes[key]


@pytest.mark.asyncio
async def test_resource_agent_skips_non_hash_node_keys_and_publishes_proposal(monkeypatch) -> None:
    fake_redis = FakeRedis()
    published: list[EventEnvelope] = []
    resource_agent.runtime.redis = fake_redis

    async def fake_publish(topic_key: str, envelope: EventEnvelope) -> None:
        published.append(envelope)

    monkeypatch.setattr(resource_agent.runtime, "publish", fake_publish)
    envelope = EventEnvelope(
        event_type="need_placement",
        correlation_id="c1",
        source="queue-agent",
        payload={"task_id": "t1", "cpu_required": 1, "ram_required_mb": 512, "requires_gpu": False},
    )

    proposals = await resource_agent.handle_need_placement(envelope)

    assert proposals == 1
    assert fake_redis.hgetall_calls == ["node:valid", "node:incomplete"]
    assert len(published) == 1
    assert published[0].event_type == "node.proposal.fit"
    assert published[0].payload["node_id"] == "node-valid"
    assert published[0].payload["unsafe"] is False
