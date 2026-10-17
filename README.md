# CloudRM: мультиагентное управление очередями и ресурсами облачного ЦОД

[![CI](https://github.com/TimoJR3/cloud_multi_agent-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/TimoJR3/cloud_multi_agent-manager/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-3776AB)
![AsyncAPI](https://img.shields.io/badge/AsyncAPI-3.0-6F42C1)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-основной_брокер-FF6600)
![Kafka](https://img.shields.io/badge/Kafka-audit-231F20)

Событийная система из 7 агентов: они принимают заявки на вычисления, распределяют
их по узлам облачного ЦОД, следят за SLA и масштабируют ресурсы. Главный артефакт
для системного анализа — **контракт событий в AsyncAPI 3.0**, который автотест сверяет
с реальной топологией брокера.

**Роль:** системный аналитик и разработчик прототипа (ВКР) · **Стек:** Python, FastAPI,
RabbitMQ, Kafka, PostgreSQL, Redis, Prometheus, Grafana, Docker Compose, pytest

## Главное

| | |
|---|---|
| **Контракт событий** | [AsyncAPI 3.0](docs/asyncapi.yaml): 16 типов событий, 7 очередей агентов, продюсеры и консьюмеры каждого события |
| **Отказоустойчивость** | повторная доставка через `mas.retry` (TTL 10 с, до 3 попыток), затем очередь недоставленных сообщений `queue.dead` |
| **Контракт = код** | тест `tests/test_asyncapi_contract.py` сверяет спецификацию с топологией RabbitMQ и подписками сервисов — расхождение ломает CI |
| **Эксперименты** | сценарии `normal`, `overload`, `gpu_shortage`, `node_failure` в режимах `baseline` и `mas` |
| **Наблюдаемость** | 10 метрик Prometheus: ожидание в очереди, задержка решения, нарушения SLA, DLQ, масштабирование |

## Как устроено

```mermaid
flowchart LR
    API[api-service] -- request.created --> Q[queue-agent]
    Q -- need_placement --> R[resource-agent]
    Q -- request.classified --> C[coordinator-agent]
    R -- node.proposal.fit --> C
    S[sla-agent] -- "sla.risk / sla.boost" --> C
    F[forecast-agent] -- forecast.queue --> C
    C -- decision.dispatch --> E[executor-agent]
    C -- decision.scale --> SC[scale-agent]
    E -. "execution.done / failed" .-> S
    SC -. scaling.done .-> Q
```

Все события идут через exchange `mas.events` в RabbitMQ. Полный список привязок
очередей — в [AsyncAPI-спецификации](docs/asyncapi.yaml).

## О проекте


Прототип к выпускной квалификационной работе (ВКР) «Разработка мультиагентной системы для управления очередями и ресурсами в облачных ЦОД». Система моделирует событийный контур: API принимает заявки, queue-agent классифицирует их и ведет очереди, resource-agent формирует предложения размещения, SLA-agent оценивает риск и фактические нарушения, forecast-agent дает краткосрочный прогноз, coordinator-agent принимает объяснимое решение, executor-agent эмулирует исполнение, scale-agent эмулирует scale-out/scale-in.

Стек: Python 3.11, FastAPI, RabbitMQ (aio-pika), Kafka (aiokafka), PostgreSQL, Redis, Prometheus, Grafana, Docker Compose.

**Контракт событий:** [docs/asyncapi.yaml](docs/asyncapi.yaml) — спецификация AsyncAPI 3.0: 16 типов событий, очереди агентов, повторная доставка через `mas.retry` и DLQ `queue.dead`. Тест `tests/test_asyncapi_contract.py` сверяет её с топологией RabbitMQ и подписками сервисов. Открыть визуально: [AsyncAPI Studio](https://studio.asyncapi.com/) → *Import* файла.

RabbitMQ теперь является основным runtime broker для агентного контура. Kafka сохранена как опциональная audit/telemetry шина через `EVENT_BACKEND=kafka|dual`.

## Быстрый запуск

```bash
make init
docker compose up --build -d
docker compose ps
```

Проверки после запуска:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
python scripts/validate_brokers.py
python scripts/validate_runtime.py
RUN_INTEGRATION=1 pytest -q tests/integration
```

Локальные unit/smoke тесты (их же запускает CI):

```bash
py -3.11 -m pytest -q --ignore=tests/integration
```

## Конфигурация брокера

`EVENT_BACKEND` задает режим событий:

- `rabbitmq` - основной режим по умолчанию, агенты потребляют RabbitMQ queues.
- `kafka` - совместимый Kafka-only режим.
- `dual` - publish в RabbitMQ и Kafka, consumption через RabbitMQ для агентов.

RabbitMQ topology:

- exchange: `mas.events`
- retry exchange: `mas.retry`
- DLX: `mas.dlx`
- queues: `queue.requests`, `queue.resources`, `queue.sla`, `queue.forecast`, `queue.coordinator`, `queue.executor`, `queue.scale`, `queue.dead`, `queue.retry`
- повтор: необработанное сообщение уходит в `mas.retry` → `queue.retry` (TTL 10 с) → обратно в `mas.events`; после 3 попыток — в `mas.dlx` → `queue.dead`
- в Kafka повторной доставки и DLQ нет

Подробно — в [AsyncAPI-спецификации](docs/asyncapi.yaml).

## Сервисы

| Сервис | Порт | Роль |
| --- | ---: | --- |
| `api-service` | 8000 | прием задач, статусы, эксперименты, отказ узла |
| `queue-agent` | 8011 | классовые очереди Redis, aging, SLA boost |
| `resource-agent` | 8012 | CPU/RAM/GPU scoring и `node.proposal.fit` |
| `sla-agent` | 8013 | `sla.risk`, `sla.boost`, фактические `sla_violations` |
| `forecast-agent` | 8014 | moving-average прогноз очереди |
| `coordinator-agent` | 8015 | decision window, utility, fallback dispatch/scale |
| `executor-agent` | 8016 | emulator execution, node failure handling |
| `load-generator` | 8017 | генератор нагрузки |
| `scale-agent` | 8018 | emulator autoscaling по `decision.scale` |
| `prometheus` | 9090 | метрики |
| `grafana` | 3000 | dashboard |

## Основные сценарии

Создать задачу:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"batch","cpu_required":2,"ram_required_mb":1024,"duration_seconds":3,"priority":4,"sla_deadline_seconds":20}'
```

GPU-задача:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"gpu","cpu_required":2,"ram_required_mb":4096,"duration_seconds":3,"priority":5,"sla_deadline_seconds":20,"requires_gpu":true}'
```

Отказ узла:

```bash
curl -X POST http://localhost:8000/nodes/node-a/failure
```

## Эксперименты

Скрипт запускает воспроизводимые сценарии `normal`, `overload`, `gpu_shortage`, `node_failure` в режимах `baseline` и `mas`, затем пишет:

- `artifacts/experiments/results.csv`
- `artifacts/experiments/summary.md`

Команда:

```bash
py -3.11 scripts/run_experiments.py --duration-seconds 30 --seed 42
```

Метрики summary считаются по PostgreSQL при доступности `DATABASE_URL`; если БД недоступна с хоста, скрипт использует только факты, собранные через API, и не подставляет синтетические значения.

## Метрики

Добавлены метрики:

- `cloudrm_request_wait_seconds`
- `cloudrm_decision_latency_seconds`
- `cloudrm_execution_failures_total`
- `cloudrm_consumed_messages_total`
- `cloudrm_dead_letter_messages_total`
- `cloudrm_sla_risks_total`
- `cloudrm_sla_violations_total`
- `cloudrm_scaling_actions_total`
- `cloudrm_scaling_cooldown_blocks_total`
- `cloudrm_queue_length{class=...}`

## Ограничения

Это исследовательский emulator, а не Kubernetes operator. `scale-agent` добавляет виртуальные Redis-узлы `node-auto-N`; future Kubernetes adapter можно подключить за тем же контуром `decision.scale -> scaling.*`. Forecast остается moving-average без тяжелой ML-модели. Авторизация API намеренно не добавлена.
