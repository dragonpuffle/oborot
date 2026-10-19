"""Small immutable warehouse values, independent of transport and storage."""

from dataclasses import dataclass
from enum import Enum

from common.errors import ValidationError


def validate_code(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 100
        or any(
            ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in value
        )
    ):
        raise ValidationError(
            f"{field} must be a valid nonempty Unicode code of at most 100 characters"
        )
    return value


class MovementKind(str, Enum):
    RECEIPT = "inventory.receipt"
    ISSUE = "inventory.issue"


@dataclass(frozen=True)
class StockKey:
    warehouse: str
    sku: str

    def __post_init__(self):
        validate_code(self.warehouse, "warehouse")
        validate_code(self.sku, "sku")


@dataclass(frozen=True)
class Quantity:
    value: int

    def __post_init__(self):
        if type(self.value) is not int or not 1 <= self.value <= 1000000000:
            raise ValidationError("qty must be an integer between 1 and 1000000000")
