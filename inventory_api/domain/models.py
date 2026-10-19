from dataclasses import dataclass
from uuid import UUID

from common.errors import ValidationError
from common.values import MovementKind, Quantity, StockKey


@dataclass(frozen=True)
class StockMovement:
    id: UUID
    key: StockKey
    quantity: Quantity
    kind: MovementKind

    def __post_init__(self):
        if (
            not isinstance(self.id, UUID)
            or not isinstance(self.key, StockKey)
            or not isinstance(self.quantity, Quantity)
            or not isinstance(self.kind, MovementKind)
        ):
            raise ValidationError("movement requires a UUID and validated stock values")

    @property
    def delta(self) -> int:
        return self.quantity.value if self.kind is MovementKind.RECEIPT else -self.quantity.value
