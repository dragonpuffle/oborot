"""Validated integration DTO; wire encoding belongs to event_codec."""

from dataclasses import dataclass
from uuid import UUID

from common.errors import InvalidEvent, ValidationError
from common.values import MovementKind, Quantity, StockKey


@dataclass(frozen=True)
class InventoryEventV1:
    id: UUID
    event_type: MovementKind
    sku: str
    qty: int
    warehouse: str

    def __post_init__(self):
        if not isinstance(self.id, UUID):
            raise InvalidEvent("id must be a UUID")
        if not isinstance(self.event_type, MovementKind):
            raise InvalidEvent("unknown event_type")
        try:
            StockKey(self.warehouse, self.sku)
            Quantity(self.qty)
        except ValidationError as error:
            raise InvalidEvent(str(error)) from error
