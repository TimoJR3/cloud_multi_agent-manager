FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/common

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY services/sla_agent /app/services/sla_agent
COPY config /app/config
COPY scripts /app/scripts

CMD ["uvicorn", "services.sla_agent.main:app", "--host", "0.0.0.0", "--port", "8013"]
