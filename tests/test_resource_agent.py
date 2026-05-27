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
