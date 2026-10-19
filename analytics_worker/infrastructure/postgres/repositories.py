"""Atomic PostgreSQL statements on the UoW connection."""

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from analytics_worker.infrastructure.postgres.errors import BACKEND_ERRORS
from common.errors import DependencyUnavailable
from common.schema import processed_events, stock_agg
from common.values import StockKey


class PgProcessedEventsRepository:
    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    async def mark_if_new(self, event_id: UUID) -> bool:
        try:
            result = await self._connection.execute(
                insert(processed_events)
                .values(event_id=event_id)
                .on_conflict_do_nothing(index_elements=[processed_events.c.event_id])
                .returning(processed_events.c.event_id)
            )
            return result.scalar_one_or_none() is not None
        except BACKEND_ERRORS as error:
            raise DependencyUnavailable(str(error)) from error


class PgProjectionRepository:
    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    async def add_delta(self, key: StockKey, delta: int) -> None:
        statement = insert(stock_agg).values(warehouse=key.warehouse, sku=key.sku, qty=delta)
        try:
            await self._connection.execute(
                statement.on_conflict_do_update(
                    index_elements=[stock_agg.c.warehouse, stock_agg.c.sku],
                    set_={"qty": stock_agg.c.qty + statement.excluded.qty},
                )
            )
        except BACKEND_ERRORS as error:
            raise DependencyUnavailable(str(error)) from error
