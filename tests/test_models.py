from datetime import UTC, datetime
from decimal import Decimal

from ledger_guard.domain.enums import EventType
from ledger_guard.domain.models import OperationEvent


def test_operation_event_creation() -> None:
    event = OperationEvent(
        event_id="event-1",
        operation_id="operation-1",
        event_type=EventType.DEPOSIT_CREATED,
        source="funding_service",
        client_id="client-1",
        amount=Decimal("15000.00"),
        currency="RUB",
        occurred_at=datetime.now(UTC),
    )

    assert event.event_id == "event-1"
    assert event.amount == Decimal("15000.00")
    assert event.event_type == EventType.DEPOSIT_CREATED
