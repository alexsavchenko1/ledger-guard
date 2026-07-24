from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.domain.enums import EventType
from ledger_guard.domain.models import OperationEvent


def create_event(
    event_id: str,
    event_type: EventType,
    source: str,
    occurred_at: datetime,
) -> OperationEvent:
    return OperationEvent(
        event_id=event_id,
        operation_id="operation-1",
        event_type=event_type,
        source=source,
        client_id="client-1",
        amount=Decimal("15000.00"),
        currency="RUB",
        occurred_at=occurred_at,
    )


def main() -> None:
    start_time = datetime.now(UTC)

    events = [
        create_event(
            "event-1",
            EventType.DEPOSIT_CREATED,
            "funding_service",
            start_time,
        ),
        create_event(
            "event-2",
            EventType.MONEY_DEBITED,
            "bank_service",
            start_time + timedelta(seconds=1),
        ),
        create_event(
            "event-3",
            EventType.TRANSFER_COMPLETED,
            "payment_service",
            start_time + timedelta(seconds=2),
        ),
        create_event(
            "event-4",
            EventType.FUNDS_CREDITED,
            "investment_ledger",
            start_time + timedelta(seconds=3),
        ),
    ]

    engine = ReconciliationEngine()
    result = engine.reconcile(events)

    print(f"Операция: {events[0].operation_id}")
    print(f"Получено событий: {len(events)}")
    print(f"Результат сверки: {result}")


if __name__ == "__main__":
    main()
