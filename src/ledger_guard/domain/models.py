from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from ledger_guard.domain.enums import EventType


@dataclass
class OperationEvent:
    event_id: str
    operation_id: str
    event_type: EventType
    source: str
    client_id: str
    amount: Decimal
    currency: str
    occurred_at: datetime