# Ledger Guard

Ledger Guard - сервис, который собирает события финансовой операции из разных систем и проверяет, что цепочка сошлась.

Идею проекта я взял из своих рабочих задач в Т-Банке. Там одна операция часто проходит через несколько систем, и потом приходится разбираться, почему цепочка не сошлась: событие потерялось, пришло дважды или данные отличаются. В Ledger Guard я взял эту механику за основу и собрал сервис, который такие расхождения находит.

Конкретную рабочую систему я сюда не переносил. Сценарий и реализацию придумал отдельно, но саму задачу взял из практики.

## Что здесь происходит

В качестве примера я взял пополнение инвестиционного счёта.

Полная операция состоит из четырёх этапов:

1. `DEPOSIT_CREATED` - создано пополнение
2. `MONEY_DEBITED` - деньги списаны со счёта клиента
3. `TRANSFER_COMPLETED` - перевод выполнен
4. `FUNDS_CREDITED` - средства зачислены на инвестиционный счёт

Каждое событие приходит от своей системы:

| Событие | Источник |
|---|---|
| `DEPOSIT_CREATED` | `funding_service` |
| `MONEY_DEBITED` | `bank_service` |
| `TRANSFER_COMPLETED` | `payment_service` |
| `FUNDS_CREDITED` | `investment_ledger` |

После получения нового события Ledger Guard собирает всё, что уже известно об операции, и заново запускает сверку.

Сервис проверяет:

- пришли ли все четыре события
- совпадают ли сумма, клиент и валюта
- от той ли системы пришло каждое событие
- не нарушен ли порядок этапов
- нет ли дублей или конфликтующих событий

## Статусы

По результатам сверки операция получает один из статусов:

- `PENDING` - цепочка ещё не собрана полностью
- `MATCHED` - всё сошлось
- `AMOUNT_MISMATCH` - в событиях отличаются суммы
- `DATA_MISMATCH` - найдено другое расхождение
- `INVALID_SEQUENCE` - нарушен временной порядок этапов

## Как всё связано

```text
Producer -> Apache Kafka -> Python consumer ->
сохраняет событие в PostgreSQL ->
запускает Reconciliation Engine ->
сохраняет результат сверки в PostgreSQL

Клиент -> FastAPI -> получает сохранённый результат

Некорректное сообщение -> operation-events-dlq
```

События приходят в Kafka. Consumer читает сообщение, преобразует его в доменную модель и сохраняет в PostgreSQL.

После этого он загружает все события с тем же `operation_id`, запускает сверку и сохраняет актуальный статус операции.

Через FastAPI можно получить последний результат.

Если сообщение сломано или в нём не хватает обязательных полей, оно отправляется в отдельный топик `operation-events-dlq`. Вместе с исходным сообщением туда попадают текст ошибки, исходный топик, partition и offset.

## Повторная доставка

Kafka может передать одно сообщение повторно. Поэтому точная копия уже сохранённого события не создаёт ещё одну строку в базе.

Consumer всё равно повторно собирает состояние операции и получает тот же результат.

Offset фиксируется только после успешной обработки сообщения или после его отправки в DLQ. Если consumer упадёт раньше, Kafka сможет передать сообщение ещё раз.

## Пример события

```json
{
  "event_id": "event-1",
  "operation_id": "operation-1",
  "event_type": "DEPOSIT_CREATED",
  "source": "funding_service",
  "client_id": "client-1",
  "amount": "15000.00",
  "currency": "RUB",
  "occurred_at": "2026-07-24T12:00:00+00:00"
}
```

## Запуск

Для запуска нужен Docker.

```bash
docker compose up -d --build
```

После запуска доступны:

- API: `http://localhost:8000`
- Swagger UI: `http://localhost:8000/docs`
- Kafka: `localhost:9092`
- PostgreSQL: `localhost:5433`

Проверить состояние контейнеров:

```bash
docker compose ps -a
```

`postgres` и `kafka` должны быть в состоянии `healthy`.

Контейнеры `kafka-volume-init` и `kafka-init` выполняют разовую настройку и после этого завершаются с кодом `0`.

Остановить проект:

```bash
docker compose down
```

Данные Kafka и PostgreSQL при этом останутся в Docker volumes.

Полностью удалить проект вместе с данными:

```bash
docker compose down -v
```

## Отправка событий через Kafka

Создадим уникальный идентификатор операции:

```bash
OPERATION_ID="operation-$(date +%s)"
```

Отправим четыре события:

```bash
cat <<JSON | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 \
  --topic operation-events
{"event_id":"${OPERATION_ID}-1","operation_id":"${OPERATION_ID}","event_type":"DEPOSIT_CREATED","source":"funding_service","client_id":"client-1","amount":"15000.00","currency":"RUB","occurred_at":"2026-07-24T12:00:00+00:00"}
{"event_id":"${OPERATION_ID}-2","operation_id":"${OPERATION_ID}","event_type":"MONEY_DEBITED","source":"bank_service","client_id":"client-1","amount":"15000.00","currency":"RUB","occurred_at":"2026-07-24T12:01:00+00:00"}
{"event_id":"${OPERATION_ID}-3","operation_id":"${OPERATION_ID}","event_type":"TRANSFER_COMPLETED","source":"payment_service","client_id":"client-1","amount":"15000.00","currency":"RUB","occurred_at":"2026-07-24T12:02:00+00:00"}
{"event_id":"${OPERATION_ID}-4","operation_id":"${OPERATION_ID}","event_type":"FUNDS_CREDITED","source":"investment_ledger","client_id":"client-1","amount":"15000.00","currency":"RUB","occurred_at":"2026-07-24T12:03:00+00:00"}
JSON
```

Через пару секунд результат можно получить через API:

```bash
curl -s "http://localhost:8000/results/$OPERATION_ID" \
  | python -m json.tool
```

Пример ответа:

```json
{
  "operation_id": "operation-123",
  "status": "MATCHED",
  "events_count": 4,
  "checked_at": "2026-07-27T20:00:00+00:00"
}
```

Логи consumer:

```bash
docker compose logs consumer
```

## API

Получить сохранённый результат:

```http
GET /results/{operation_id}
```

Запустить сверку напрямую, без Kafka:

```http
POST /reconcile
```

`POST /reconcile` принимает массив событий, запускает тот же движок сверки и сохраняет результат в PostgreSQL.

Все контракты можно посмотреть в Swagger:

```text
http://localhost:8000/docs
```

## CLI

Я оставил и первоначальный режим работы с JSON-файлом. Он позволяет отдельно запускать логику сверки без Kafka и PostgreSQL.

Проект использует Python 3.13.

```bash
python3.13 -m venv .ledgerguard
source .ledgerguard/bin/activate
python -m pip install -e ".[dev]"
```

Запуск со стандартным примером:

```bash
python -m ledger_guard
```

Запуск со своим файлом:

```bash
python -m ledger_guard path/to/events.json
```

## Проверки

Интеграционные тесты используют PostgreSQL на порту `5433`.

Поднять только базу:

```bash
docker compose up -d postgres
```

Запустить все проверки:

```bash
ruff check .
ruff format --check .
pyright
pytest
```

Сейчас в проекте 31 тест. Они проверяют:

- чтение и валидацию событий
- правила сверки
- сохранение событий и результатов
- обработку Kafka-сообщений
- повторную доставку
- API

## Структура проекта

```text
.
├── compose.yaml
├── Dockerfile
├── examples
├── scripts
│   └── create_kafka_topics.sh
├── sql
│   └── init.sql
├── src
│   └── ledger_guard
│       ├── application
│       │   └── reconciliation.py
│       ├── domain
│       │   ├── enums.py
│       │   └── models.py
│       ├── infrastructure
│       │   ├── event_repository.py
│       │   ├── kafka_consumer.py
│       │   └── result_repository.py
│       ├── api.py
│       ├── event_reader.py
│       └── __main__.py
└── tests
```

## Что я не стал сюда добавлять

Это локальный проект, а не попытка собрать промышленную платформу в одном репозитории.

Здесь один Kafka broker, одна partition и один consumer. Нет Kubernetes, авторизации, метрик, трассировки и отдельного сервиса миграций.

Я не стал добавлять всё это только ради количества технологий. Мне было важнее собрать рабочую цепочку целиком: получить событие, сохранить его, пересчитать состояние операции, пережить повторную доставку и отдельно обработать некорректное сообщение.
