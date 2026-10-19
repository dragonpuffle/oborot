"""A signed change to one warehouse/SKU projection."""

from dataclasses import dataclass

from common.values import Quantity, StockKey


@dataclass(frozen=True)
class ProjectionChange:
    key: StockKey
    delta: int

    def __post_init__(self):
        Quantity(self.delta if self.delta >= 0 else -self.delta)
