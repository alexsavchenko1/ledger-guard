import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from ledger_guard.domain.enums import EventType
from ledger_guard.domain.models import OperationEvent


def read_events(path: str) -> list[OperationEvent]:
    file_path = Path(path)

    with file_path.open(encoding="utf-8") as file:
        raw_events = json.load(file)

    if not isinstance(raw_events, list):
        raise ValueError("JSON должен содержать список событий")

    events = []

    for item in raw_events:
        if not isinstance(item, dict):
            raise ValueError("Каждое событие должно быть JSON-объектом")

        event_id = item["event_id"]
        operation_id = item["operation_id"]
        client_id = item["client_id"]

        if not event_id or not operation_id or not client_id:
            raise ValueError("Идентификаторы события не должны быть пустыми")

        amount = Decimal(item["amount"])

        if amount <= 0:
            raise ValueError("Сумма события должна быть больше нуля")

        currency = item["currency"]
        source = item["source"]

        if currency != "RUB":
            raise ValueError("Поддерживается только валюта RUB")

        if not source:
            raise ValueError("Источник события не должен быть пустым")

        event = OperationEvent(
            event_id=event_id,
            operation_id=operation_id,
            event_type=EventType(item["event_type"]),
            source=source,
            client_id=client_id,
            amount=amount,
            currency=currency,
            occurred_at=datetime.fromisoformat(item["occurred_at"]),
        )
        events.append(event)

    return events
