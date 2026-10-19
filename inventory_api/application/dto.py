from dataclasses import dataclass
from uuid import UUID

from common.contracts import InventoryEventV1
from common.values import Quantity, StockKey


@dataclass(frozen=True)
class MovementCommand:
    key: StockKey
    quantity: Quantity


@dataclass(frozen=True)
class StockItem:
    warehouse: str
    sku: str
    qty: int


@dataclass(frozen=True)
class WarehouseSummary:
    warehouse: str
    total_qty: int
    sku_count: int


@dataclass(frozen=True)
class SkuSummary:
    sku: str
    total_qty: int


@dataclass(frozen=True)
class StockSummary:
    warehouses: tuple[WarehouseSummary, ...]
    top_skus: tuple[SkuSummary, ...]


@dataclass(frozen=True)
class OutboxMessage:
    event_id: UUID
    event: InventoryEventV1
