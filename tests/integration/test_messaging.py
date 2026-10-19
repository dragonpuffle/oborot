"""Delivery contracts exercised against migrated PostgreSQL and real Redis."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from redis.exceptions import ConnectionError, ResponseError
from sqlalchemy.exc import IntegrityError

from analytics_worker.application.handlers import ApplyInventoryEvent
from analytics_worker.bootstrap import create_worker
from analytics_worker.infrastructure.postgres.uow import PgProjectionUnitOfWork
from analytics_worker.infrastructure.redis.transport import RedisStreamTransport
from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec
from common.schema import processed_events, stock_agg


def payload(**changes):
    return {
        "id": str(uuid4()),
        "event_type": "inventory.receipt",
        "sku": "A-100",
        "qty": 7,
        "warehouse": "WH-1",
        **changes,
    }


async def aggregate(engine):
    async with engine.connect() as connection:
        return (await connection.execute(sa.select(stock_agg.c.qty))).scalar_one_or_none()


async def row_count(engine, table):
    async with engine.connect() as connection:
        return await connection.scalar(sa.select(sa.func.count()).select_from(table))


async def test_messages_published_before_first_group_are_consumed(
    worker_runtime,
    db_engine,
    redis_client,
    settings,
):
    await redis_client.xadd(settings.stream, {"data": json.dumps(payload())})
    await RedisStreamTransport(redis_client, settings).ensure_group()

    assert await worker_runtime.run_once() == 1
    assert await aggregate(db_engine) == 7
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 0


async def test_group_creation_is_idempotent(redis_client, settings):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    await RedisStreamTransport(redis_client, settings).ensure_group()


async def test_claims_messages_from_a_crashed_consumer(
    worker_runtime, db_engine, redis_client, settings
):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    await redis_client.xadd(settings.stream, {"data": json.dumps(payload())})
    await redis_client.xreadgroup(settings.group, "crashed", {settings.stream: ">"}, count=1)

    assert await worker_runtime.run_once() == 1
    assert await aggregate(db_engine) == 7
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 0


async def test_commit_before_ack_failure_is_replayed_once(
    worker_runtime,
    db_engine,
    redis_client,
    settings,
    monkeypatch,
):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    await redis_client.xadd(settings.stream, {"data": json.dumps(payload())})
    original_ack = redis_client.xack

    async def fail_ack(*args, **kwargs):
        raise ConnectionError("ack response lost")

    monkeypatch.setattr(redis_client, "xack", fail_ack)
    with pytest.raises(DependencyUnavailable, match="ack response lost") as failure:
        await worker_runtime.run_once()
    assert isinstance(failure.value.__cause__, ConnectionError)
    assert await aggregate(db_engine) == 7
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 1

    monkeypatch.setattr(redis_client, "xack", original_ack)
    assert await worker_runtime.run_once() == 1
    assert await aggregate(db_engine) == 7
    assert await row_count(db_engine, processed_events) == 1
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 0


async def test_database_failure_rolls_back_marker_and_leaves_pending(
    worker_runtime,
    db_engine,
    redis_client,
    settings,
):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    await redis_client.xadd(settings.stream, {"data": json.dumps(payload())})
    async with db_engine.begin() as connection:
        await connection.execute(
            sa.text("ALTER TABLE stock_agg ADD CONSTRAINT injected_failure CHECK (qty < 0)")
        )
    with pytest.raises(DependencyUnavailable) as failure:
        await worker_runtime.run_once()
    assert isinstance(failure.value.__cause__, IntegrityError)
    assert await aggregate(db_engine) is None
    assert await row_count(db_engine, processed_events) == 0
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 1

    async with db_engine.begin() as connection:
        await connection.execute(sa.text("ALTER TABLE stock_agg DROP CONSTRAINT injected_failure"))
    assert await worker_runtime.run_once() == 1
    assert await aggregate(db_engine) == 7


async def test_concurrent_duplicate_delivery_applies_one_delta(db_engine):
    event = InventoryEventCodec().from_mapping(payload())
    results = await asyncio.gather(
        *(
            ApplyInventoryEvent(lambda: PgProjectionUnitOfWork(db_engine)).execute(event)
            for _ in range(20)
        )
    )
    assert sum(results) == 1
    assert await aggregate(db_engine) == 7
    assert await row_count(db_engine, processed_events) == 1


async def test_concurrent_distinct_events_and_out_of_order_issue(db_engine):
    issue = InventoryEventCodec().from_mapping(payload(event_type="inventory.issue", qty=5))
    await ApplyInventoryEvent(lambda: PgProjectionUnitOfWork(db_engine)).execute(issue)
    assert await aggregate(db_engine) == -5
    events = [InventoryEventCodec().from_mapping(payload(qty=1)) for _ in range(20)]
    assert all(
        await asyncio.gather(
            *(
                ApplyInventoryEvent(lambda: PgProjectionUnitOfWork(db_engine)).execute(event)
                for event in events
            )
        )
    )
    assert await aggregate(db_engine) == 15


@pytest.mark.parametrize(
    "data",
    [
        "broken{",
        "[]",
        "null",
        "1",
        '"text"',
        "{}",
        pytest.param("[" * 10000 + "0" + "]" * 10000, id="deeply-nested"),
        pytest.param(json.dumps(payload(sku="\ud800")), id="unicode-surrogate"),
    ],
)
async def test_invalid_message_goes_to_dead_letter_and_next_message_progresses(
    worker_runtime,
    db_engine,
    redis_client,
    settings,
    data,
):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    bad_id = await redis_client.xadd(settings.stream, {"data": data})
    await redis_client.xadd(settings.stream, {"data": json.dumps(payload())})

    assert await worker_runtime.run_once() == 2
    assert await aggregate(db_engine) == 7
    assert await row_count(db_engine, processed_events) == 1
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 0
    letters = await redis_client.xrange(settings.dead_letter_stream)
    assert len(letters) == 1
    assert letters[0][1]["source_id"] == bad_id
    assert letters[0][1]["error"]
    assert json.loads(letters[0][1]["data"]) == {"data": data}


async def test_dead_letter_wrong_type_does_not_ack(
    worker_runtime, db_engine, redis_client, settings
):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    await redis_client.set(settings.dead_letter_stream, "wrong type")
    await redis_client.xadd(settings.stream, {"data": "{"})
    with pytest.raises(DependencyUnavailable, match="WRONGTYPE") as failure:
        await worker_runtime.run_once()
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 1
    assert isinstance(failure.value.__cause__, ResponseError)
    await redis_client.delete(settings.dead_letter_stream)
    assert await worker_runtime.run_once() == 1
    assert await redis_client.xlen(settings.dead_letter_stream) == 1


async def test_pending_scan_advances_past_busy_entries_and_reads_new_events(
    db_engine,
    redis_client,
    settings,
):
    await RedisStreamTransport(redis_client, settings).ensure_group()
    ids = [
        await redis_client.xadd(settings.stream, {"data": json.dumps(payload(qty=1))})
        for _ in range(31)
    ]
    await redis_client.xreadgroup(settings.group, "crashed", {settings.stream: ">"}, count=31)
    # Make only the last entry eligible without timing-dependent sleeps. Redis
    # scans at most COUNT * 10 pending IDs, so the cursor needs multiple calls.
    await redis_client.xclaim(
        settings.stream,
        settings.group,
        "crashed",
        0,
        [ids[-1]],
        idle=600000,
    )
    settings = replace(settings, batch_size=1, claim_idle_ms=300000)
    async with create_worker(settings, engine=db_engine, redis=redis_client) as worker_runtime:
        for _ in range(4):
            await redis_client.xadd(settings.stream, {"data": json.dumps(payload(qty=1))})
            assert await worker_runtime.run_once() >= 1
    assert await aggregate(db_engine) == 5
    assert (await redis_client.xpending(settings.stream, settings.group))["pending"] == 30


@pytest.fixture
async def worker_runtime(db_engine, redis_client, settings):
    async with create_worker(settings, engine=db_engine, redis=redis_client) as runtime:
        yield runtime
