import json
import os
from decimal import InvalidOperation

from confluent_kafka import Consumer

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.event_reader import parse_event
from ledger_guard.infrastructure.event_repository import EventRepository
from ledger_guard.infrastructure.result_repository import ResultRepository


TOPIC = "operation-events"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard",
)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "ledger-guard-debug",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    event_repository = EventRepository(DATABASE_URL)
    result_repository = ResultRepository(DATABASE_URL)
    engine = ReconciliationEngine()

    consumer.subscribe([TOPIC])

    print(f"Ожидаю сообщения из топика {TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Ошибка Kafka: {message.error()}")
                continue

            raw_value = message.value()

            if raw_value is None:
                print("Получено сообщение без value")
                continue

            try:
                json_text = raw_value.decode("utf-8")
                event_data = json.loads(json_text)
                event = parse_event(event_data)
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError,
                InvalidOperation,
            ) as error:
                print(f"Не удалось разобрать сообщение: {error}")
                continue

            saved = event_repository.save(event)

            operation_events = event_repository.get_by_operation_id(
                event.operation_id
            )

            status = engine.reconcile(operation_events)

            result_repository.save(
                operation_id=event.operation_id,
                status=status,
                events_count=len(operation_events),
            )

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            print()
            print("Получено событие:")
            print(event)

            print("Событие сохранено:", saved)
            print("Количество событий операции:", len(operation_events))
            print("Статус сверки:", status.value)
            print(
                "Kafka offset подтверждён:",
                message.offset(),
            )

    except KeyboardInterrupt:
        print("\nConsumer остановлен")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
