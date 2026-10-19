"""Initial inventory schema.

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "inventory_balances",
        sa.Column("warehouse", sa.String(100), primary_key=True),
        sa.Column("sku", sa.String(100), primary_key=True),
        sa.Column("qty", sa.BigInteger, nullable=False),
        sa.CheckConstraint("qty >= 0", name="inventory_balances_qty_nonnegative"),
    )
    op.create_table(
        "stock_agg",
        sa.Column("warehouse", sa.String(100), primary_key=True),
        sa.Column("sku", sa.String(100), primary_key=True),
        sa.Column("qty", sa.BigInteger, nullable=False),
    )
    op.create_table(
        "stock_events",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
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
    op.create_index("ix_stock_events_warehouse_sku", "stock_events", ["warehouse", "sku"])
    op.create_table(
        "outbox",
        sa.Column(
            "event_id", pg.UUID(as_uuid=True), sa.ForeignKey("stock_events.id"), primary_key=True
        ),
        sa.Column("payload", pg.JSONB, nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["event_id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "processed_events",
        sa.Column("event_id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )


def downgrade():
    for table in ("processed_events", "outbox", "stock_events", "stock_agg", "inventory_balances"):
        op.drop_table(table)
