"""Atomic, replay-safe application of inventory events."""

from analytics_worker.application.ports import ProjectionUowFactory
from analytics_worker.domain.projection import ProjectionChange
from common.contracts import InventoryEventV1
from common.values import MovementKind, StockKey


class ApplyInventoryEvent:
    def __init__(self, projection_uow_factory: ProjectionUowFactory):
        self._uow_factory = projection_uow_factory

    async def execute(self, event: InventoryEventV1) -> bool:
        change = ProjectionChange(
            StockKey(event.warehouse, event.sku),
            event.qty if event.event_type == MovementKind.RECEIPT else -event.qty,
        )
        async with self._uow_factory() as uow:
            applied = await uow.processed.mark_if_new(event.id)
            if applied:
                await uow.projection.add_delta(change.key, change.delta)
            await uow.commit()
        return applied
