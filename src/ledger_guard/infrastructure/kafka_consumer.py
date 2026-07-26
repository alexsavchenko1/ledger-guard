import json
import os
from decimal import InvalidOperation
from typing import Protocol

from confluent_kafka import Consumer, Producer

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.domain.enums import ReconciliationStatus
from ledger_guard.domain.models import OperationEvent
from ledger_guard.event_reader import parse_event
from ledger_guard.infrastructure.event_repository import EventRepository
from ledger_guard.infrastructure.result_repository import ResultRepository


TOPIC = "operation-events"
DLQ_TOPIC = "operation-events-dlq"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)


class DlqProducer(Protocol):
    def produce(self, topic: str, value: bytes) -> None:
        pass

    def flush(self, timeout: float) -> int:
        pass


def send_to_dlq(
    raw_value: bytes,
    error: Exception,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    producer: DlqProducer,
) -> None:
    dlq_message = {
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "original_message": raw_value.decode(
            "utf-8",
            errors="replace",
        ),
    }

    serialized_message = json.dumps(
        dlq_message,
        ensure_ascii=False,
    ).encode("utf-8")

    producer.produce(
        DLQ_TOPIC,
        value=serialized_message,
    )

    messages_left = producer.flush(10.0)

    if messages_left != 0:
        raise RuntimeError(
            "Не удалось отправить сообщение в DLQ"
        )


def process_message(
    raw_value: bytes,
    event_repository: EventRepository,
    result_repository: ResultRepository,
    engine: ReconciliationEngine,
) -> tuple[OperationEvent, bool, int, ReconciliationStatus]:
    json_text = raw_value.decode("utf-8")
    event_data = json.loads(json_text)
    event = parse_event(event_data)

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

    return event, saved, len(operation_events), status


def main() -> None:
    kafka_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }

    consumer = Consumer(
        {
            **kafka_config,
            "group.id": "ledger-guard-debug",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    producer = Producer(kafka_config)

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
                event, saved, events_count, status = process_message(
                    raw_value=raw_value,
                    event_repository=event_repository,
                    result_repository=result_repository,
                    engine=engine,
                )
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError,
                InvalidOperation,
            ) as error:
                send_to_dlq(
                    raw_value=raw_value,
                    error=error,
                    source_topic=message.topic(),
                    source_partition=message.partition(),
                    source_offset=message.offset(),
                    producer=producer,
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print()
                print("Битое сообщение отправлено в DLQ")
                print("Ошибка:", error)
                print("Kafka offset подтверждён:", message.offset())
                continue

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            print()
            print("Получено событие:")
            print(event)
            print("Событие сохранено:", saved)
            print("Количество событий операции:", events_count)
            print("Статус сверки:", status.value)
            print(
                "Kafka offset подтверждён:",
                message.offset(),
            )

    except KeyboardInterrupt:
        print("\nConsumer остановлен")
    finally:
        producer.flush(5.0)
        consumer.close()


if __name__ == "__main__":
    main()
