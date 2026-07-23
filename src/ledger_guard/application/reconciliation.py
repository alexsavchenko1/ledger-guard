from ledger_guard.domain.enums import EventType, ReconciliationStatus
from ledger_guard.domain.models import OperationEvent


class ReconciliationEngine:
    def reconcile(self, events: list[OperationEvent]) -> ReconciliationStatus:
        received_types = {event.event_type for event in events}

        required_types = {
            EventType.DEPOSIT_CREATED,
            EventType.MONEY_DEBITED,
            EventType.TRANSFER_COMPLETED,
            EventType.FUNDS_CREDITED,
        }

        if not required_types.issubset(received_types):
            return ReconciliationStatus.PENDING

        amounts = {event.amount for event in events}

        if len(amounts) > 1:
            return ReconciliationStatus.AMOUNT_MISMATCH

        return ReconciliationStatus.MATCHED
