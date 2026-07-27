import os

import psycopg
from fastapi.testclient import TestClient

from ledger_guard.api import app
from ledger_guard.domain.enums import ReconciliationStatus
from ledger_guard.infrastructure.result_repository import ResultRepository

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard",
)

client = TestClient(app)


def delete_result(operation_id: str) -> None:
    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(
            """
            DELETE FROM reconciliation_results
            WHERE operation_id = %s
            """,
            (operation_id,),
        )


def test_get_existing_result() -> None:
    repository = ResultRepository(DATABASE_URL)

    repository.save(
        operation_id="operation-api-test",
        status=ReconciliationStatus.MATCHED,
        events_count=4,
    )

    response = client.get("/results/operation-api-test")

    assert response.status_code == 200
    assert response.json()["operation_id"] == "operation-api-test"
    assert response.json()["status"] == "MATCHED"
    assert response.json()["events_count"] == 4


def test_get_unknown_result_returns_404() -> None:
    response = client.get("/results/unknown-api-operation")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "Результат сверки не найден",
    }


def test_reconcile_events_returns_and_saves_result() -> None:
    response = client.post(
        "/reconcile",
        json=[
            {
                "event_id": "api-event-1",
                "operation_id": "operation-reconcile-test",
                "event_type": "DEPOSIT_CREATED",
                "source": "funding_service",
                "client_id": "client-1",
                "amount": "15000.00",
                "currency": "RUB",
                "occurred_at": "2026-07-24T12:00:00+00:00",
            },
            {
                "event_id": "api-event-2",
                "operation_id": "operation-reconcile-test",
                "event_type": "MONEY_DEBITED",
                "source": "bank_service",
                "client_id": "client-1",
                "amount": "15000.00",
                "currency": "RUB",
                "occurred_at": "2026-07-24T12:01:00+00:00",
            },
            {
                "event_id": "api-event-3",
                "operation_id": "operation-reconcile-test",
                "event_type": "TRANSFER_COMPLETED",
                "source": "payment_service",
                "client_id": "client-1",
                "amount": "15000.00",
                "currency": "RUB",
                "occurred_at": "2026-07-24T12:02:00+00:00",
            },
            {
                "event_id": "api-event-4",
                "operation_id": "operation-reconcile-test",
                "event_type": "FUNDS_CREDITED",
                "source": "investment_ledger",
                "client_id": "client-1",
                "amount": "15000.00",
                "currency": "RUB",
                "occurred_at": "2026-07-24T12:03:00+00:00",
            },
        ],
    )

    assert response.status_code == 200
    assert response.json() == {
        "operation_id": "operation-reconcile-test",
        "status": "MATCHED",
        "events_count": 4,
    }

    repository = ResultRepository(DATABASE_URL)
    saved_result = repository.get_by_operation_id(
        "operation-reconcile-test",
    )

    delete_result("operation-reconcile-test")

    assert saved_result is not None
    assert saved_result[0] == "MATCHED"
    assert saved_result[1] == 4


def test_reconcile_events_rejects_empty_list() -> None:
    response = client.post(
        "/reconcile",
        json=[],
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Список событий не должен быть пустым",
    }
