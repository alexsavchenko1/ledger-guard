import psycopg

from ledger_guard.domain.enums import EventType
from ledger_guard.domain.models import OperationEvent


class EventRepository:
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def save(self, event: OperationEvent) -> bool:
        query = """
            INSERT INTO operation_events (
                event_id,
                operation_id,
                event_type,
                source,
                client_id,
                amount,
                currency,
                occurred_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """

        with psycopg.connect(self.connection_string) as connection:
            result = connection.execute(
                query,
                (
                    event.event_id,
                    event.operation_id,
                    event.event_type.value,
                    event.source,
                    event.client_id,
                    event.amount,
                    event.currency,
                    event.occurred_at,
                ),
            ).fetchone()

        return result is not None

    def get_by_operation_id(
        self,
        operation_id: str,
    ) -> list[OperationEvent]:
        query = """
            SELECT
                event_id,
                operation_id,
                event_type,
                source,
                client_id,
                amount,
                currency,
                occurred_at
            FROM operation_events
            WHERE operation_id = %s
            ORDER BY id
        """

        with psycopg.connect(self.connection_string) as connection:
            rows = connection.execute(
                query,
                (operation_id,),
            ).fetchall()

        return [
            OperationEvent(
                event_id=row[0],
                operation_id=row[1],
                event_type=EventType(row[2]),
                source=row[3],
                client_id=row[4],
                amount=row[5],
                currency=row[6],
                occurred_at=row[7],
            )
            for row in rows
        ]
