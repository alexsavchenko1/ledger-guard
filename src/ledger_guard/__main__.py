import sys

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.event_reader import read_events


def main() -> None:
    path = "examples/events.json"

    if len(sys.argv) > 1:
        path = sys.argv[1]

    events = read_events(path)

    engine = ReconciliationEngine()
    result = engine.reconcile(events)

    print(f"Файл: {path}")
    print(f"Операция: {events[0].operation_id}")
    print(f"Получено событий: {len(events)}")
    print(f"Результат сверки: {result}")


if __name__ == "__main__":
    main()
