import os
from datetime import datetime
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ledger_guard.application.reconciliation import ReconciliationEngine
from ledger_guard.domain.enums import EventType
from ledger_guard.domain.models import OperationEvent
from ledger_guard.infrastructure.result_repository import ResultRepository

app = FastAPI(title="Ledger Guard")


class EventRequest(BaseModel):
    event_id: str
    operation_id: str
    event_type: EventType
    source: str
    client_id: str
    amount: Decimal
    currency: str
    occurred_at: datetime


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


@app.post("/reconcile")
def reconcile_events(request_events: list[EventRequest]) -> dict:
    if not request_events:
        raise HTTPException(
            status_code=400,
            detail="Список событий не должен быть пустым",
        )

    events = [
        OperationEvent(
            event_id=item.event_id,
            operation_id=item.operation_id,
            event_type=item.event_type,
            source=item.source,
            client_id=item.client_id,
            amount=item.amount,
            currency=item.currency,
            occurred_at=item.occurred_at,
        )
        for item in request_events
    ]

    engine = ReconciliationEngine()
    status = engine.reconcile(events)

    operation_id = events[0].operation_id
    repository = get_repository()

    repository.save(
        operation_id=operation_id,
        status=status,
        events_count=len(events),
    )

    return {
        "operation_id": operation_id,
        "status": status.value,
        "events_count": len(events),
    }
