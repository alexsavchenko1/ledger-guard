import os

import psycopg

from ledger_guard.domain.enums import ReconciliationStatus
from ledger_guard.infrastructure.result_repository import ResultRepository


CONNECTION_STRING = os.getenv(
    "DATABASE_URL",
    "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard",
)


def delete_result(operation_id: str) -> None:
    with psycopg.connect(CONNECTION_STRING) as connection:
        connection.execute(
            """
            DELETE FROM reconciliation_results
            WHERE operation_id = %s
            """,
            (operation_id,),
        )


def test_repository_saves_reconciliation_result() -> None:
    repository = ResultRepository(CONNECTION_STRING)

    repository.save(
        operation_id="operation-save-test",
        status=ReconciliationStatus.MATCHED,
        events_count=4,
    )

    with psycopg.connect(CONNECTION_STRING) as connection:
        result = connection.execute(
            """
            SELECT status, events_count
            FROM reconciliation_results
            WHERE operation_id = %s
            """,
            ("operation-save-test",),
        ).fetchone()

    delete_result("operation-save-test")

    assert result == ("MATCHED", 4)


def test_repository_reads_reconciliation_result() -> None:
    repository = ResultRepository(CONNECTION_STRING)

    repository.save(
        operation_id="operation-read-test",
        status=ReconciliationStatus.PENDING,
        events_count=2,
    )

    result = repository.get_by_operation_id("operation-read-test")

    delete_result("operation-read-test")

    assert result is not None
    assert result[0] == "PENDING"
    assert result[1] == 2


def test_repository_returns_none_for_unknown_operation() -> None:
    repository = ResultRepository(CONNECTION_STRING)

    delete_result("unknown-operation")

    result = repository.get_by_operation_id("unknown-operation")

    assert result is None
