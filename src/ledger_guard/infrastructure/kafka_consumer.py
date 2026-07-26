import json

from confluent_kafka import Consumer


TOPIC = "operation-events"


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "ledger-guard-debug",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

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
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                print(f"Не удалось разобрать сообщение: {error}")
                continue

            print()
            print("1. Kafka вернула байты:")
            print(type(raw_value))
            print(raw_value)

            print()
            print("2. Байты декодированы в строку:")
            print(type(json_text))
            print(json_text)

            print()
            print("3. JSON-строка преобразована в словарь:")
            print(type(event_data))
            print(event_data)

    except KeyboardInterrupt:
        print("\nConsumer остановлен")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
