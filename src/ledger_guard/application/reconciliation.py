from ledger_guard.domain.enums import EventType, ReconciliationStatus
from ledger_guard.domain.models import OperationEvent


class ReconciliationEngine:
    def reconcile(self, events: list[OperationEvent]) -> ReconciliationStatus:
        unique_events = []
        events_by_id = {}

        for event in events:
            existing_event = events_by_id.get(event.event_id)

            if existing_event is not None:
                if existing_event != event:
                    return ReconciliationStatus.DATA_MISMATCH

                continue

            events_by_id[event.event_id] = event
            unique_events.append(event)

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
