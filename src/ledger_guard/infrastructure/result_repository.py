from datetime import datetime

import psycopg

from ledger_guard.domain.enums import ReconciliationStatus


class ResultRepository:
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def save(
        self,
        operation_id: str,
        status: ReconciliationStatus,
        events_count: int,
    ) -> None:
        query = """
            INSERT INTO reconciliation_results (
                operation_id,
                status,
                events_count,
                checked_at
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (operation_id)
            DO UPDATE SET
                status = EXCLUDED.status,
                events_count = EXCLUDED.events_count,
                checked_at = EXCLUDED.checked_at
        """

        with psycopg.connect(self.connection_string) as connection:
            connection.execute(
                query,
                (
                    operation_id,
                    status.value,
                    events_count,
                    datetime.now().astimezone(),
                ),
            )

    def get_by_operation_id(
        self,
        operation_id: str,
    ) -> tuple[str, int, datetime] | None:
        query = """
            SELECT status, events_count, checked_at
            FROM reconciliation_results
            WHERE operation_id = %s
        """

        with psycopg.connect(self.connection_string) as connection:
            result = connection.execute(
                query,
                (operation_id,),
            ).fetchone()

        return result
