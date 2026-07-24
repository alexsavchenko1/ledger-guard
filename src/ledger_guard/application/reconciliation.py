from ledger_guard.domain.enums import EventType, ReconciliationStatus
from ledger_guard.domain.models import OperationEvent


class ReconciliationEngine:
    def reconcile(self, events: list[OperationEvent]) -> ReconciliationStatus:
        unique_events = []
        seen_event_ids = set()

        for event in events:
            if event.event_id not in seen_event_ids:
                unique_events.append(event)
                seen_event_ids.add(event.event_id)

        received_types = {event.event_type for event in unique_events}

        required_types = {
            EventType.DEPOSIT_CREATED,
            EventType.MONEY_DEBITED,
            EventType.TRANSFER_COMPLETED,
            EventType.FUNDS_CREDITED,
        }

        if not required_types.issubset(received_types):
            return ReconciliationStatus.PENDING

        operation_ids = {event.operation_id for event in unique_events}

        if len(operation_ids) > 1:
            return ReconciliationStatus.DATA_MISMATCH

        amounts = {event.amount for event in unique_events}

        if len(amounts) > 1:
            return ReconciliationStatus.AMOUNT_MISMATCH

        client_ids = {event.client_id for event in unique_events}
        currencies = {event.currency for event in unique_events}

        if len(client_ids) > 1 or len(currencies) > 1:
            return ReconciliationStatus.DATA_MISMATCH

        return ReconciliationStatus.MATCHED
