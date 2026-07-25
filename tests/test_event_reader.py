from decimal import Decimal

from ledger_guard.domain.enums import EventType
from ledger_guard.event_reader import read_events


def test_read_events_from_json() -> None:
    events = read_events("examples/events.json")

    assert len(events) == 4
    assert events[0].event_type == EventType.DEPOSIT_CREATED
    assert events[0].amount == Decimal("15000.00")
    assert events[3].source == "investment_ledger"


def test_read_events_rejects_json_object(tmp_path) -> None:
    file_path = tmp_path / "events.json"
    file_path.write_text(
        '{"event_id": "event-1"}',
        encoding="utf-8",
    )

    try:
        read_events(str(file_path))
    except ValueError as error:
        assert str(error) == "JSON должен содержать список событий"
    else:
        raise AssertionError("Ожидалась ошибка ValueError")


def test_read_events_rejects_non_object_item(tmp_path) -> None:
    file_path = tmp_path / "events.json"
    file_path.write_text(
        '["event-1"]',
        encoding="utf-8",
    )

    try:
        read_events(str(file_path))
    except ValueError as error:
        assert str(error) == "Каждое событие должно быть JSON-объектом"
    else:
        raise AssertionError("Ожидалась ошибка ValueError")
