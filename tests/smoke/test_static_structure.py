from pathlib import Path


def test_required_files_exist() -> None:
    root = Path(__file__).resolve().parents[2]
    required = [
        "docker-compose.yml",
        "docker/api-service.Dockerfile",
        "docker/queue-agent.Dockerfile",
        "docker/resource-agent.Dockerfile",
        "docker/sla-agent.Dockerfile",
        "docker/forecast-agent.Dockerfile",
        "docker/coordinator-agent.Dockerfile",
        "docker/executor-agent.Dockerfile",
        "docker/load-generator.Dockerfile",
        "common/cloudrm/kafka.py",
        "common/cloudrm/rabbit.py",
        "services/api_service/main.py",
        "services/queue_agent/main.py",
        "services/resource_agent/main.py",
        "services/sla_agent/main.py",
        "services/forecast_agent/main.py",
        "services/coordinator_agent/main.py",
        "services/executor_agent/main.py",
        "services/load_generator/main.py",
        "config/services.yaml",
        "prometheus/prometheus.yml",
        "grafana/dashboards/cloudrm-overview.json",
        "scripts/validate_brokers.py",
    ]
    for path in required:
        assert (root / path).exists(), path


def test_service_modules_are_importable() -> None:
    import services.api_service.main  # noqa: F401
    import services.coordinator_agent.main  # noqa: F401
    import services.executor_agent.main  # noqa: F401
    import services.forecast_agent.main  # noqa: F401
    import services.load_generator.main  # noqa: F401
    import services.queue_agent.main  # noqa: F401
    import services.resource_agent.main  # noqa: F401
    import services.sla_agent.main  # noqa: F401


def test_rabbitmq_env_example_exists() -> None:
    root = Path(__file__).resolve().parents[2]
    content = (root / ".env.example").read_text(encoding="utf-8")
    required = [
        "RABBITMQ_HOST=rabbitmq",
        "RABBITMQ_PORT=5672",
        "RABBITMQ_MANAGEMENT_PORT=15672",
        "RABBITMQ_DEFAULT_USER=mas",
        "RABBITMQ_DEFAULT_PASS=mas_password",
        "RABBITMQ_EXCHANGE=mas.events",
        "RABBITMQ_DLX=mas.dlx",
        "RABBITMQ_RETRY_EXCHANGE=mas.retry",
        "RABBITMQ_PREFETCH=10",
    ]
    for item in required:
        assert item in content
