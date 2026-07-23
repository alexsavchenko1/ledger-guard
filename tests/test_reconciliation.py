from datetime import UTC, datetime
from decimal import Decimal

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.domain.enums import EventType, ReconciliationStatus
from ledger_guard.domain.models import OperationEvent


def make_event(
    event_type: EventType,
    amount: Decimal = Decimal("15000.00"),
) -> OperationEvent:
    return OperationEvent(
        event_id=f"event-{event_type}",
        operation_id="operation-1",
        event_type=event_type,
        source="test_service",
        client_id="client-1",
        amount=amount,
        currency="RUB",
        occurred_at=datetime.now(UTC),
    )



def make_valid_events() -> list[OperationEvent]:
    return [
        make_event(EventType.DEPOSIT_CREATED),
        make_event(EventType.MONEY_DEBITED),
        make_event(EventType.TRANSFER_COMPLETED),
        make_event(EventType.FUNDS_CREDITED),
    ]

def test_returns_pending_when_events_are_missing() -> None:
    engine = ReconciliationEngine()

    events = [
        make_event(EventType.DEPOSIT_CREATED),
        make_event(EventType.MONEY_DEBITED),
    ]

    result = engine.reconcile(events)

    assert result == ReconciliationStatus.PENDING


def test_returns_matched_when_all_events_are_received() -> None:
    engine = ReconciliationEngine()

    events = [
        make_event(EventType.DEPOSIT_CREATED),
        make_event(EventType.MONEY_DEBITED),
        make_event(EventType.TRANSFER_COMPLETED),
        make_event(EventType.FUNDS_CREDITED),
    ]

    result = engine.reconcile(events)

    assert result == ReconciliationStatus.MATCHED


def test_returns_amount_mismatch_when_amounts_are_different() -> None:
    engine = ReconciliationEngine()

    events = [
        make_event(EventType.DEPOSIT_CREATED),
        make_event(EventType.MONEY_DEBITED),
        make_event(
            EventType.TRANSFER_COMPLETED,
            amount=Decimal("14900.00"),
        ),
        make_event(EventType.FUNDS_CREDITED),
    ]

    result = engine.reconcile(events)

    assert result == ReconciliationStatus.AMOUNT_MISMATCH


def test_duplicate_event_with_different_amount_returns_mismatch() -> None:
    engine = ReconciliationEngine()
    events = make_valid_events()

    duplicate_event = make_event(
        EventType.TRANSFER_COMPLETED,
        amount=Decimal("14900.00"),
    )
    duplicate_event.event_id = "duplicate-event"

    events.append(duplicate_event)

    result = engine.reconcile(events)

    assert result == ReconciliationStatus.AMOUNT_MISMATCH
