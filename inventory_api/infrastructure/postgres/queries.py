"""Journal read models execute their aggregation in PostgreSQL."""

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from common.errors import DependencyUnavailable
from common.schema import stock_events
from inventory_api.application.dto import SkuSummary, StockItem, StockSummary, WarehouseSummary
from inventory_api.infrastructure.postgres.transactions import BACKEND_ERRORS


def journal_stock():
    """Group the source journal before warehouse and global aggregations."""
    delta = sa.case(
        (stock_events.c.event_type == "inventory.receipt", stock_events.c.qty),
        else_=-stock_events.c.qty,
    )
    return (
        sa.select(
            stock_events.c.warehouse,
            stock_events.c.sku,
            sa.func.sum(delta).label("qty"),
        )
        .group_by(stock_events.c.warehouse, stock_events.c.sku)
        .subquery("journal_stock")
    )


def summary_query(top_n: int):
    stock = journal_stock()
    warehouses = sa.select(
        sa.literal("warehouse").label("kind"),
        stock.c.warehouse,
        sa.cast(sa.null(), sa.String).label("sku"),
        sa.func.sum(stock.c.qty).label("total_qty"),
        sa.func.count().label("sku_count"),
    ).group_by(stock.c.warehouse)
    top_skus = (
        sa.select(
            stock.c.sku,
            sa.func.sum(stock.c.qty).label("total_qty"),
        )
        .group_by(stock.c.sku)
        .order_by(sa.desc("total_qty"), stock.c.sku)
        .limit(top_n)
        .subquery("top_skus")
    )
    # A single statement gives both views the same MVCC snapshot even while writes continue.
    return sa.union_all(
        warehouses,
        sa.select(
            sa.literal("sku").label("kind"),
            sa.cast(sa.null(), sa.String).label("warehouse"),
            top_skus.c.sku,
            top_skus.c.total_qty,
            sa.cast(sa.null(), sa.BigInteger).label("sku_count"),
        ),
    )


class PgStockQueries:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def stock(self, warehouse: str, sku: str | None = None) -> list[StockItem]:
        stock = journal_stock()
        query = sa.select(stock).where(stock.c.warehouse == warehouse).order_by(stock.c.sku)
        if sku is not None:
            query = query.where(stock.c.sku == sku)
        rows = await self._rows(query)
        return [StockItem(row["warehouse"], row["sku"], int(row["qty"])) for row in rows]

    async def summary(self, top_n: int) -> StockSummary:
        rows = await self._rows(summary_query(top_n))
        warehouses = [
            WarehouseSummary(row["warehouse"], int(row["total_qty"]), int(row["sku_count"]))
            for row in rows
            if row["kind"] == "warehouse"
        ]
        top_skus = [
            SkuSummary(row["sku"], int(row["total_qty"])) for row in rows if row["kind"] == "sku"
        ]
        return StockSummary(
            tuple(sorted(warehouses, key=lambda item: item.warehouse)),
            tuple(sorted(top_skus, key=lambda item: (-item.total_qty, item.sku))),
        )

    async def _rows(self, statement):
        try:
            async with self._engine.connect() as connection:
                return (await connection.execute(statement)).mappings().all()
        except BACKEND_ERRORS as error:
            raise DependencyUnavailable("Database unavailable") from error


class PgDatabaseHealthCheck:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

    async def check(self) -> None:
        try:
            async with self._engine.connect() as connection:
                await connection.execute(sa.select(1))
        except BACKEND_ERRORS as error:
            raise DependencyUnavailable("Database unavailable") from error
