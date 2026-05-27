from services.queue_agent import main as queue_agent


def test_queue_classification_covers_required_classes() -> None:
    assert queue_agent.classify({"priority": 9}) == "critical"
    assert queue_agent.classify({"requires_gpu": True}) == "gpu"
    assert queue_agent.classify({"cpu_required": 4}) == "cpu-heavy"
    assert queue_agent.classify({"ram_required_mb": 8192}) == "memory-heavy"
    assert queue_agent.classify({}) == "standard"


def test_dynamic_priority_ages_with_actual_wait_time() -> None:
    queue_agent.runtime.config = {"agents": {"queue": {"aging_factor": 0.5}}}

    score = queue_agent.dynamic_priority({"priority": 2}, created_at=100.0, now=106.0)

    assert score == 5.0


def test_dynamic_priority_includes_sla_boost() -> None:
    queue_agent.runtime.config = {"agents": {"queue": {"aging_factor": 0.0}}}

    score = queue_agent.dynamic_priority({"priority": 2, "sla_boost": 3}, created_at=100.0, now=100.0)

    assert score == 5
