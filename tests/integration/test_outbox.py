"""API-owned outbox delivery against real PostgreSQL and Redis adapters."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from redis.exceptions import RedisError

from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec
from common.schema import outbox, stock_events
from inventory_api.application.outbox import PublishOutboxBatch
from inventory_api.infrastructure.postgres.uow import PgOutboxUnitOfWork
from inventory_api.infrastructure.redis.publisher import RedisEventPublisher


def batch(engine, redis, settings, *, timeout=5):
    codec = InventoryEventCodec()
    return PublishOutboxBatch(
        lambda: PgOutboxUnitOfWork(engine, codec),
        RedisEventPublisher(redis, settings.stream, codec, timeout=timeout),
        settings.batch_size,
    )


async def add_outbox(engine):
    data = {
        "id": str(uuid4()),
        "event_type": "inventory.receipt",
        "sku": "A-100",
        "qty": 7,
        "warehouse": "WH-1",
    }
    event = InventoryEventCodec().from_mapping(data)
    async with engine.begin() as connection:
        await connection.execute(
            stock_events.insert().values(
                id=event.id,
                event_type=event.event_type.value,
                sku=event.sku,
                warehouse=event.warehouse,
                qty=event.qty,
            )
        )
        await connection.execute(outbox.insert().values(event_id=event.id, payload=data))
    return data


async def test_redis_outage_keeps_outbox_then_recovers(db_engine, redis_client, settings):
    data = await add_outbox(db_engine)
    unavailable = Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.1)
    try:
        with pytest.raises(DependencyUnavailable) as caught:
            await batch(db_engine, unavailable, settings).execute()
        assert isinstance(caught.value.__cause__, RedisError)
    finally:
        await unavailable.aclose()
    async with db_engine.connect() as connection:
        assert await connection.scalar(sa.select(outbox.c.published_at)) is None
    assert await batch(db_engine, redis_client, settings).execute() == 1
    assert await batch(db_engine, redis_client, settings).execute() == 0
    entries = await redis_client.xrange(settings.stream)
    assert len(entries) == 1
    assert json.loads(entries[0][1]["data"]) == data


async def test_parallel_publishers_skip_locked_rows(db_engine, redis_client, settings, monkeypatch):
    await add_outbox(db_engine)
    await add_outbox(db_engine)
    settings = replace(settings, batch_size=1)
    both_publishing = asyncio.Event()
    entered = 0
    original_xadd = redis_client.xadd

    async def concurrent_xadd(*args, **kwargs):
        nonlocal entered
        entered += 1
        if entered == 2:
            both_publishing.set()
        await asyncio.wait_for(both_publishing.wait(), timeout=3)
        return await original_xadd(*args, **kwargs)

    monkeypatch.setattr(redis_client, "xadd", concurrent_xadd)
    assert await asyncio.gather(
        batch(db_engine, redis_client, settings).execute(),
        batch(db_engine, redis_client, settings).execute(),
    ) == [1, 1]
    assert await redis_client.xlen(settings.stream) == 2
    async with db_engine.connect() as connection:
        assert (
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(outbox)
                .where(outbox.c.published_at.is_not(None))
            )
            == 2
        )


async def test_outbox_network_wait_is_bounded(db_engine, redis_client, settings, monkeypatch):
    await add_outbox(db_engine)

    async def stalled_xadd(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(redis_client, "xadd", stalled_xadd)
    with pytest.raises(DependencyUnavailable) as caught:
        await batch(db_engine, redis_client, settings, timeout=0.01).execute()
    assert isinstance(caught.value.__cause__, asyncio.TimeoutError)
    async with db_engine.connect() as connection:
        assert await connection.scalar(sa.select(outbox.c.published_at)) is None
