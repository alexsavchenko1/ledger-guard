from decimal import Decimal

from ledger_guard.domain.enums import EventType
from ledger_guard.event_reader import read_events


def test_read_events_from_json() -> None:
    events = read_events("examples/events.json")

    assert len(events) == 4
    assert events[0].event_type == EventType.DEPOSIT_CREATED
    assert events[0].amount == Decimal("15000.00")
    assert events[3].source == "investment_ledger"
