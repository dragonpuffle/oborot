from collections.abc import Sequence
from types import TracebackType
from typing import Protocol
from uuid import UUID

from common.contracts import InventoryEventV1
from common.values import Quantity, StockKey
from inventory_api.application.dto import OutboxMessage, StockItem, StockSummary
from inventory_api.domain.models import StockMovement


class BalanceRepository(Protocol):
    async def increase(self, key: StockKey, quantity: Quantity) -> None: ...

    async def try_decrease(self, key: StockKey, quantity: Quantity) -> bool: ...


class StockEventRepository(Protocol):
    async def append(self, movement: StockMovement) -> None: ...


class OutboxWriter(Protocol):
    async def append(self, event: InventoryEventV1) -> None: ...


class OutboxRepository(Protocol):
    async def lock_pending(self, limit: int) -> Sequence[OutboxMessage]: ...

    async def mark_published(self, event_id: UUID) -> None: ...


class StockQueries(Protocol):
    async def stock(self, warehouse: str, sku: str | None = None) -> Sequence[StockItem]: ...

    async def summary(self, top_n: int) -> StockSummary: ...


class HealthCheck(Protocol):
    async def check(self) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, event: InventoryEventV1) -> None: ...


class InventoryUnitOfWork(Protocol):
    balances: BalanceRepository
    events: StockEventRepository
    outbox: OutboxWriter

    async def __aenter__(self) -> "InventoryUnitOfWork": ...

    async def commit(self) -> None: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class OutboxUnitOfWork(Protocol):
    outbox: OutboxRepository

    async def __aenter__(self) -> "OutboxUnitOfWork": ...

    async def commit(self) -> None: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
