import json
import logging
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

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "operation-events",
)

DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "operation-events-dlq",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "ledger-guard-consumer",
)

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO",
).upper()


logging.basicConfig(
    level=LOG_LEVEL,
    format=("%(asctime)s %(levelname)s %(name)s %(message)s"),
)

logger = logging.getLogger(__name__)


class DlqProducer(Protocol):
    def produce(self, topic: str, value: bytes) -> None: ...

    def flush(self, timeout: float) -> int: ...


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
        raise RuntimeError("Не удалось отправить сообщение в DLQ")


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

    operation_events = event_repository.get_by_operation_id(event.operation_id)

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
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    producer = Producer(kafka_config)

    event_repository = EventRepository(DATABASE_URL)
    result_repository = ResultRepository(DATABASE_URL)
    engine = ReconciliationEngine()

    consumer.subscribe([KAFKA_TOPIC])

    logger.info(
        "Consumer запущен: topic=%s group_id=%s broker=%s",
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error(
                    "Ошибка получения Kafka-сообщения: %s",
                    message.error(),
                )
                continue

            raw_value = message.value()
            source_topic = message.topic()
            source_partition = message.partition()
            source_offset = message.offset()

            if (
                source_topic is None
                or source_partition is None
                or source_offset is None
            ):
                logger.error("Получено Kafka-сообщение без метаданных")
                continue

            if raw_value is None:
                logger.warning(
                    "Получено сообщение без value: topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
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
                    source_topic=source_topic,
                    source_partition=source_partition,
                    source_offset=source_offset,
                    producer=producer,
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                logger.warning(
                    "Сообщение отправлено в DLQ: "
                    "topic=%s partition=%s offset=%s error=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    error,
                )
                continue

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.info(
                "Событие обработано: "
                "operation_id=%s event_id=%s saved=%s "
                "events_count=%s status=%s "
                "partition=%s offset=%s",
                event.operation_id,
                event.event_id,
                saved,
                events_count,
                status.value,
                message.partition(),
                message.offset(),
            )

    except KeyboardInterrupt:
        logger.info("Consumer остановлен пользователем")
    finally:
        messages_left = producer.flush(5.0)

        if messages_left:
            logger.warning(
                "При завершении не отправлено сообщений: %s",
                messages_left,
            )

        consumer.close()
        logger.info("Kafka consumer закрыт")


if __name__ == "__main__":
    main()
