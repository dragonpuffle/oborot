"""Database metadata. Schema changes are applied only through Alembic."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = sa.MetaData()


def stock_table(name: str, *, nonnegative: bool = False) -> sa.Table:
    constraints = (
        [sa.CheckConstraint("qty >= 0", name=f"{name}_qty_nonnegative")] if nonnegative else []
    )
    return sa.Table(
        name,
        metadata,
        sa.Column("warehouse", sa.String(100), primary_key=True),
        sa.Column("sku", sa.String(100), primary_key=True),
        sa.Column("qty", sa.BigInteger, nullable=False),
        *constraints,
    )


inventory_balances = stock_table("inventory_balances", nonnegative=True)
stock_agg = stock_table("stock_agg")
stock_events = sa.Table(
    "stock_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("event_type", sa.String(30), nullable=False),
    sa.Column("sku", sa.String(100), nullable=False),
    sa.Column("warehouse", sa.String(100), nullable=False),
    sa.Column("qty", sa.BigInteger, nullable=False),
    sa.Column(
        "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    ),
    sa.CheckConstraint("qty > 0 AND qty <= 1000000000", name="stock_events_qty_positive"),
    sa.CheckConstraint(
        "event_type IN ('inventory.receipt','inventory.issue')", name="stock_events_type"
    ),
)
sa.Index("ix_stock_events_warehouse_sku", stock_events.c.warehouse, stock_events.c.sku)
outbox = sa.Table(
    "outbox",
    metadata,
    sa.Column("event_id", UUID(as_uuid=True), sa.ForeignKey("stock_events.id"), primary_key=True),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("published_at", sa.DateTime(timezone=True)),
)
sa.Index(
    "ix_outbox_unpublished", outbox.c.event_id, postgresql_where=outbox.c.published_at.is_(None)
)
processed_events = sa.Table(
    "processed_events",
    metadata,
    sa.Column("event_id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "processed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    ),
)
