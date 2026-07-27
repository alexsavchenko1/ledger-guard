from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest

from ledger_guard.domain.enums import EventType
from ledger_guard.domain.models import OperationEvent
from ledger_guard.infrastructure.event_repository import EventRepository

DATABASE_URL = "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard"


@pytest.fixture
def repository() -> EventRepository:
    return EventRepository(DATABASE_URL)


@pytest.fixture
def operation_id() -> Iterator[str]:
    operation_id = f"op-test-{uuid4()}"

    yield operation_id

    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(
            """
            DELETE FROM operation_events
            WHERE operation_id = %s
            """,
            (operation_id,),
        )


def make_event(
    operation_id: str,
    *,
    event_id: str | None = None,
    amount: str = "1000.00",
) -> OperationEvent:
    return OperationEvent(
        event_id=event_id or f"evt-test-{uuid4()}",
        operation_id=operation_id,
        event_type=EventType.DEPOSIT_CREATED,
        source="funding_service",
        client_id="client-001",
        amount=Decimal(amount),
        currency="RUB",
        occurred_at=datetime(
            2026,
            7,
            26,
            12,
            0,
            tzinfo=UTC,
        ),
    )


def test_save_and_get_event(
    repository: EventRepository,
    operation_id: str,
) -> None:
    event = make_event(operation_id)

    saved = repository.save(event)
    events = repository.get_by_operation_id(operation_id)

    assert saved is True
    assert events == [event]


def test_exact_duplicate_is_not_saved_twice(
    repository: EventRepository,
    operation_id: str,
) -> None:
    event = make_event(operation_id)

    first_save = repository.save(event)
    second_save = repository.save(event)
    events = repository.get_by_operation_id(operation_id)

    assert first_save is True
    assert second_save is False
    assert events == [event]


def test_conflicting_event_with_same_id_is_saved(
    repository: EventRepository,
    operation_id: str,
) -> None:
    event_id = f"evt-test-{uuid4()}"

    first_event = make_event(
        operation_id,
        event_id=event_id,
        amount="1000.00",
    )
    conflicting_event = make_event(
        operation_id,
        event_id=event_id,
        amount="1500.00",
    )

    first_save = repository.save(first_event)
    second_save = repository.save(conflicting_event)
    events = repository.get_by_operation_id(operation_id)

    assert first_save is True
    assert second_save is True
    assert events == [first_event, conflicting_event]
