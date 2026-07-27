import json
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.domain.enums import EventType, ReconciliationStatus
from ledger_guard.infrastructure.event_repository import EventRepository
from ledger_guard.infrastructure.kafka_consumer import (
    process_message,
)
from ledger_guard.infrastructure.result_repository import ResultRepository

DATABASE_URL = "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard"


@pytest.fixture
def operation_id() -> Iterator[str]:
    operation_id = f"op-kafka-test-{uuid4()}"

    yield operation_id

    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(
            """
            DELETE FROM operation_events
            WHERE operation_id = %s
            """,
            (operation_id,),
        )
        connection.execute(
            """
            DELETE FROM reconciliation_results
            WHERE operation_id = %s
            """,
            (operation_id,),
        )


def make_message(operation_id: str) -> bytes:
    event_data = {
        "event_id": f"evt-kafka-test-{uuid4()}",
        "operation_id": operation_id,
        "event_type": "DEPOSIT_CREATED",
        "source": "funding_service",
        "client_id": "client-001",
        "amount": "1000.00",
        "currency": "RUB",
        "occurred_at": "2026-07-26T12:00:00+00:00",
    }

    return json.dumps(event_data).encode("utf-8")


def test_process_message_saves_event_and_result(
    operation_id: str,
) -> None:
    event_repository = EventRepository(DATABASE_URL)
    result_repository = ResultRepository(DATABASE_URL)
    engine = ReconciliationEngine()

    event, saved, events_count, status = process_message(
        raw_value=make_message(operation_id),
        event_repository=event_repository,
        result_repository=result_repository,
        engine=engine,
    )

    stored_events = event_repository.get_by_operation_id(operation_id)
    stored_result = result_repository.get_by_operation_id(operation_id)

    assert event.event_type == EventType.DEPOSIT_CREATED
    assert saved is True
    assert events_count == 1
    assert status == ReconciliationStatus.PENDING
    assert stored_events == [event]

    assert stored_result is not None

    stored_status, stored_events_count, _ = stored_result

    assert stored_status == ReconciliationStatus.PENDING.value
    assert stored_events_count == 1


def test_process_message_rejects_invalid_json() -> None:
    event_repository = EventRepository(DATABASE_URL)
    result_repository = ResultRepository(DATABASE_URL)
    engine = ReconciliationEngine()

    with pytest.raises(json.JSONDecodeError):
        process_message(
            raw_value=b"{broken-json",
            event_repository=event_repository,
            result_repository=result_repository,
            engine=engine,
        )


def make_operation_message(
    operation_id: str,
    event_type: EventType,
    source: str,
    occurred_at: str,
) -> bytes:
    event_data = {
        "event_id": f"evt-kafka-test-{uuid4()}",
        "operation_id": operation_id,
        "event_type": event_type.value,
        "source": source,
        "client_id": "client-001",
        "amount": "1000.00",
        "currency": "RUB",
        "occurred_at": occurred_at,
    }

    return json.dumps(event_data).encode("utf-8")


def test_four_messages_complete_reconciliation(
    operation_id: str,
) -> None:
    event_repository = EventRepository(DATABASE_URL)
    result_repository = ResultRepository(DATABASE_URL)
    engine = ReconciliationEngine()

    messages = [
        make_operation_message(
            operation_id=operation_id,
            event_type=EventType.DEPOSIT_CREATED,
            source="funding_service",
            occurred_at="2026-07-26T12:00:00+00:00",
        ),
        make_operation_message(
            operation_id=operation_id,
            event_type=EventType.MONEY_DEBITED,
            source="bank_service",
            occurred_at="2026-07-26T12:01:00+00:00",
        ),
        make_operation_message(
            operation_id=operation_id,
            event_type=EventType.TRANSFER_COMPLETED,
            source="payment_service",
            occurred_at="2026-07-26T12:02:00+00:00",
        ),
        make_operation_message(
            operation_id=operation_id,
            event_type=EventType.FUNDS_CREDITED,
            source="investment_ledger",
            occurred_at="2026-07-26T12:03:00+00:00",
        ),
    ]

    expected_statuses = [
        ReconciliationStatus.PENDING,
        ReconciliationStatus.PENDING,
        ReconciliationStatus.PENDING,
        ReconciliationStatus.MATCHED,
    ]

    for expected_count, (message, expected_status) in enumerate(
        zip(messages, expected_statuses, strict=True),
        start=1,
    ):
        _, saved, events_count, status = process_message(
            raw_value=message,
            event_repository=event_repository,
            result_repository=result_repository,
            engine=engine,
        )

        assert saved is True
        assert events_count == expected_count
        assert status == expected_status

    stored_events = event_repository.get_by_operation_id(operation_id)
    stored_result = result_repository.get_by_operation_id(operation_id)

    assert len(stored_events) == 4
    assert stored_result is not None

    stored_status, stored_events_count, _ = stored_result

    assert stored_status == ReconciliationStatus.MATCHED.value
    assert stored_events_count == 4
