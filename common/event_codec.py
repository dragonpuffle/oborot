"""Existing JSON wire contract translated to a validated integration DTO."""

import json
from uuid import UUID

from common.contracts import InventoryEventV1
from common.errors import InvalidEvent
from common.values import MovementKind


class InventoryEventCodec:
    def from_mapping(self, payload: object) -> InventoryEventV1:
        if not isinstance(payload, dict):
            raise InvalidEvent("body must be a JSON object")
        try:
            event_id = UUID(payload["id"])
        except (KeyError, ValueError, TypeError, AttributeError) as error:
            raise InvalidEvent("id must be a UUID string") from error
        try:
            kind = MovementKind(payload.get("event_type"))
        except (ValueError, TypeError) as error:
            raise InvalidEvent("unknown event_type") from error
        return InventoryEventV1(
            event_id, kind, payload.get("sku"), payload.get("qty"), payload.get("warehouse")
        )

    def to_mapping(self, event: InventoryEventV1) -> dict:
        return dict(
            id=str(event.id),
            event_type=event.event_type.value,
            sku=event.sku,
            qty=event.qty,
            warehouse=event.warehouse,
        )

    def decode(self, data: str) -> InventoryEventV1:
        try:
            return self.from_mapping(json.loads(data))
        except (ValueError, TypeError, RecursionError) as error:
            raise InvalidEvent(str(error)) from error

    def encode(self, event: InventoryEventV1) -> str:
        return json.dumps(self.to_mapping(event))
