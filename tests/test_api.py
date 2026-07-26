import os

from fastapi.testclient import TestClient

from ledger_guard.api import app
from ledger_guard.domain.enums import ReconciliationStatus
from ledger_guard.infrastructure.result_repository import ResultRepository


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ledger_guard:ledger_guard@localhost:5433/ledger_guard",
)

client = TestClient(app)


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
