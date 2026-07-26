import os

from fastapi import FastAPI, HTTPException

from ledger_guard.infrastructure.result_repository import ResultRepository


app = FastAPI(title="Ledger Guard")


def get_repository() -> ResultRepository:
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("Переменная DATABASE_URL не задана")

    return ResultRepository(database_url)


@app.get("/results/{operation_id}")
def get_result(operation_id: str) -> dict:
    repository = get_repository()
    result = repository.get_by_operation_id(operation_id)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Результат сверки не найден",
        )

    status, events_count, checked_at = result

    return {
        "operation_id": operation_id,
        "status": status,
        "events_count": events_count,
        "checked_at": checked_at,
    }
