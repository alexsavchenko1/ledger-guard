from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.event_reader import read_events


def main() -> None:
    events = read_events("examples/events.json")

    engine = ReconciliationEngine()
    result = engine.reconcile(events)

    print(f"Операция: {events[0].operation_id}")
    print(f"Получено событий: {len(events)}")
    print(f"Результат сверки: {result}")


if __name__ == "__main__":
    main()
