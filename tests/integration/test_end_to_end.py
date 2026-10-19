"""Combined API -> committed outbox -> Redis -> SQL analytics contract."""

import asyncio
import json

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from analytics_worker.bootstrap import create_worker
from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec
from common.schema import outbox, processed_events, stock_agg
from inventory_api.app import create_app
from inventory_api.application.outbox import PublishOutboxBatch
from inventory_api.infrastructure.postgres.uow import PgOutboxUnitOfWork
from inventory_api.infrastructure.redis.publisher import RedisEventPublisher


async def test_api_background_publisher_to_worker_matches_source(
    db_engine,
    redis_client,
    settings,
    aiohttp_client,
):
    # The API's actual background publisher runs. Start the group only after
    # publication to exercise reliable first startup on the combined path.
    client = await aiohttp_client(
        create_app(settings, engine=db_engine, redis=redis_client, publish=True)
    )
    for path, qty in (("/receipts", 50), ("/issues", 20)):
        response = await client.post(path, json=dict(sku="A-100", warehouse="WH-1", qty=qty))
        assert response.status == 201

    async def published():
        while True:
            async with db_engine.connect() as connection:
                remaining = await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(outbox)
                    .where(outbox.c.published_at.is_(None))
                )
            if remaining == 0 and await redis_client.xlen(settings.stream) == 2:
                return
            await asyncio.sleep(0.01)

    await asyncio.wait_for(published(), timeout=5)
    async with create_worker(settings, engine=db_engine, redis=redis_client) as worker:
        assert await worker.run_once() == 2
        assert await worker.run_once() == 0
    async with db_engine.connect() as connection:
        rows = (await connection.execute(sa.select(stock_agg))).mappings().all()
        assert [dict(row) for row in rows] == [dict(warehouse="WH-1", sku="A-100", qty=30)]
        assert (
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(outbox)
                .where(outbox.c.published_at.is_(None))
            )
            == 0
        )
    response = await client.get("/stock?warehouse=WH-1")
    assert (await response.json())["items"] == [dict(warehouse="WH-1", sku="A-100", qty=30)]
    response = await client.get("/stock/summary")
    assert await response.json() == dict(
        warehouses=[dict(warehouse="WH-1", total_qty=30, sku_count=1)],
        top_skus=[dict(sku="A-100", total_qty=30)],
    )
    # Stop publisher before fixtures dispose the externally owned dependencies.
    await client.close()


async def test_xadd_before_mark_failure_replays_without_double_projection(
    db_engine, redis_client, settings, aiohttp_client
):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    response = await client.post("/receipts", json=dict(sku="A", warehouse="W", qty=7))
    assert response.status == 201
    event = await response.json()
    codec = InventoryEventCodec()
    batch = PublishOutboxBatch(
        lambda: PgOutboxUnitOfWork(db_engine, codec),
        RedisEventPublisher(redis_client, settings.stream, codec),
        settings.batch_size,
    )
    async with db_engine.begin() as connection:
        await connection.execute(
            sa.text(
                "ALTER TABLE outbox ADD CONSTRAINT injected_failure CHECK (published_at IS NULL)"
            )
        )
    with pytest.raises(DependencyUnavailable) as caught:
        await batch.execute()
    assert isinstance(caught.value.__cause__, IntegrityError)
    assert await redis_client.xlen(settings.stream) == 1
    async with db_engine.begin() as connection:
        assert await connection.scalar(sa.select(outbox.c.published_at)) is None
        await connection.execute(sa.text("ALTER TABLE outbox DROP CONSTRAINT injected_failure"))
    assert await batch.execute() == 1
    assert await batch.execute() == 0
    entries = await redis_client.xrange(settings.stream)
    assert [json.loads(fields["data"]) for _, fields in entries] == [event, event]
    async with db_engine.connect() as connection:
        assert await connection.scalar(sa.select(outbox.c.published_at)) is not None
    async with create_worker(settings, engine=db_engine, redis=redis_client) as worker:
        assert await worker.run_once() == 2
        assert await worker.run_once() == 0
    async with db_engine.connect() as connection:
        assert await connection.scalar(sa.select(stock_agg.c.qty)) == 7
        assert (
            await connection.scalar(sa.select(sa.func.count()).select_from(processed_events)) == 1
        )
        marker = await connection.scalar(sa.select(processed_events.c.event_id))
        assert str(marker) == event["id"]
    response = await client.get("/stock?warehouse=W")
    assert await response.json() == {"items": [dict(warehouse="W", sku="A", qty=7)]}
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 0
    await client.close()
