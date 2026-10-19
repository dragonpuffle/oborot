from collections.abc import Callable, Sequence
from uuid import uuid4

from common.contracts import InventoryEventV1
from common.values import MovementKind
from inventory_api.application.dto import MovementCommand, StockItem, StockSummary
from inventory_api.application.ports import InventoryUnitOfWork, StockQueries
from inventory_api.domain.errors import InsufficientStock
from inventory_api.domain.models import StockMovement


class InventoryService:
    def __init__(
        self, inventory_uow_factory: Callable[[], InventoryUnitOfWork], queries: StockQueries
    ):
        self._uow_factory = inventory_uow_factory
        self._queries = queries

    async def receive(self, command: MovementCommand) -> InventoryEventV1:
        return await self._record(command, MovementKind.RECEIPT)

    async def issue(self, command: MovementCommand) -> InventoryEventV1:
        return await self._record(command, MovementKind.ISSUE)

    async def _record(self, command: MovementCommand, kind: MovementKind) -> InventoryEventV1:
        movement = StockMovement(uuid4(), command.key, command.quantity, kind)
        event = InventoryEventV1(
            movement.id, kind, command.key.sku, command.quantity.value, command.key.warehouse
        )
        async with self._uow_factory() as uow:
            if kind is MovementKind.RECEIPT:
                await uow.balances.increase(command.key, command.quantity)
            elif not await uow.balances.try_decrease(command.key, command.quantity):
                raise InsufficientStock
            await uow.events.append(movement)
            await uow.outbox.append(event)
            await uow.commit()
        return event

    async def stock(self, warehouse: str, sku: str | None = None) -> Sequence[StockItem]:
        return await self._queries.stock(warehouse, sku)

    async def summary(self, top_n: int) -> StockSummary:
        return await self._queries.summary(top_n)
