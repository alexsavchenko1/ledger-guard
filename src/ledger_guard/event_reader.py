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

    events = []

    for item in raw_events:
        event = OperationEvent(
            event_id=item["event_id"],
            operation_id=item["operation_id"],
            event_type=EventType(item["event_type"]),
            source=item["source"],
            client_id=item["client_id"],
            amount=Decimal(item["amount"]),
            currency=item["currency"],
            occurred_at=datetime.fromisoformat(item["occurred_at"]),
        )
        events.append(event)

    return events
