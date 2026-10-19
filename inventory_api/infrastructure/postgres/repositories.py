import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from common.contracts import InventoryEventV1
from common.event_codec import InventoryEventCodec
from common.schema import inventory_balances, outbox, stock_events
from common.values import Quantity, StockKey
from inventory_api.application.dto import OutboxMessage
from inventory_api.domain.models import StockMovement
from inventory_api.infrastructure.postgres.transactions import execute


class PgBalanceRepository:
    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    async def increase(self, key: StockKey, quantity: Quantity) -> None:
        statement = insert(inventory_balances).values(
            warehouse=key.warehouse, sku=key.sku, qty=quantity.value
        )
        await execute(
            self._connection,
            statement.on_conflict_do_update(
                index_elements=[inventory_balances.c.warehouse, inventory_balances.c.sku],
                set_={"qty": inventory_balances.c.qty + quantity.value},
            ),
        )

    async def try_decrease(self, key: StockKey, quantity: Quantity) -> bool:
        # The condition and subtraction share PostgreSQL's row lock across API instances.
        updated = await execute(
            self._connection,
            inventory_balances.update()
            .where(
                inventory_balances.c.warehouse == key.warehouse,
                inventory_balances.c.sku == key.sku,
                inventory_balances.c.qty >= quantity.value,
            )
            .values(qty=inventory_balances.c.qty - quantity.value)
            .returning(inventory_balances.c.qty),
        )
        return updated.scalar_one_or_none() is not None


class PgStockEventRepository:
    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    async def append(self, movement: StockMovement) -> None:
        await execute(
            self._connection,
            stock_events.insert().values(
                id=movement.id,
                event_type=movement.kind.value,
                sku=movement.key.sku,
                qty=movement.quantity.value,
                warehouse=movement.key.warehouse,
            ),
        )


class PgOutboxRepository:
    def __init__(self, connection: AsyncConnection, codec: InventoryEventCodec):
        self._connection = connection
        self._codec = codec

    async def append(self, event: InventoryEventV1) -> None:
        await execute(
            self._connection,
            outbox.insert().values(event_id=event.id, payload=self._codec.to_mapping(event)),
        )

    async def lock_pending(self, limit: int) -> list[OutboxMessage]:
        rows = await execute(
            self._connection,
            sa.select(outbox.c.event_id, outbox.c.payload)
            .where(outbox.c.published_at.is_(None))
            .order_by(outbox.c.event_id)
            .with_for_update(skip_locked=True)
            .limit(limit),
        )
        return [
            OutboxMessage(event_id, self._codec.from_mapping(payload)) for event_id, payload in rows
        ]

    async def mark_published(self, event_id) -> None:
        await execute(
            self._connection,
            outbox.update().where(outbox.c.event_id == event_id).values(published_at=sa.func.now()),
        )
