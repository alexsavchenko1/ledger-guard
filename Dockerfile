FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml .
COPY src ./src

RUN python -m pip install --no-cache-dir .

CMD ["python", "-m", "ledger_guard.infrastructure.kafka_consumer"]
