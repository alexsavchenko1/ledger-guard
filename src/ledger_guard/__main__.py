import json
import os
import sys

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.event_reader import read_events
from ledger_guard.infrastructure.result_repository import ResultRepository


def main() -> None:
    file_path = sys.argv[1] if len(sys.argv) > 1 else "examples/events.json"

    try:
        events = read_events(file_path)
    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
        return
    except json.JSONDecodeError:
        print(f"Некорректный JSON в файле: {file_path}")
        return
    except KeyError as error:
        print(f"В событии отсутствует поле: {error}")
        return
    except ValueError as error:
        print(f"Некорректное значение в событии: {error}")
        return

    if not events:
        print(f"В файле нет событий: {file_path}")
        return

    engine = ReconciliationEngine()
    status = engine.reconcile(events)

    operation_id = events[0].operation_id

    print(f"Файл: {file_path}")
    print(f"Операция: {operation_id}")
    print(f"Получено событий: {len(events)}")
    print(f"Результат сверки: {status.value}")

    database_url = os.getenv("DATABASE_URL")

    if database_url:
        repository = ResultRepository(database_url)
        repository.save(
            operation_id=operation_id,
            status=status,
            events_count=len(events),
        )
        print("Результат сохранён в PostgreSQL")


if __name__ == "__main__":
    main()
