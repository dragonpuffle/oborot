"""HTTP contracts against a migrated PostgreSQL database."""

import asyncio
import json

import pytest
from sqlalchemy import event, func, select, text

from common.event_codec import InventoryEventCodec
from common.schema import inventory_balances, outbox, stock_agg, stock_events
from common.values import Quantity, StockKey
from inventory_api import create_app
from inventory_api.infrastructure.postgres.uow import PgInventoryUnitOfWork


async def test_receipt_commits_event_balance_and_outbox(aiohttp_client, db_engine, settings):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    response = await client.post("/receipts", json={"sku": "A-100", "qty": 7, "warehouse": "WH-1"})
    assert response.status == 201
    payload = await response.json()
    assert payload | {"id": "generated"} == {
        "id": "generated",
        "event_type": "inventory.receipt",
        "sku": "A-100",
        "qty": 7,
        "warehouse": "WH-1",
    }
    async with db_engine.connect() as connection:
        event = (await connection.execute(select(stock_events))).mappings().one()
        message = (await connection.execute(select(outbox))).mappings().one()
        balance = (await connection.execute(select(inventory_balances))).mappings().one()
    assert str(event["id"]) == payload["id"]
    assert message["payload"] == payload
    assert message["published_at"] is None
    assert balance == {"warehouse": "WH-1", "sku": "A-100", "qty": 7}


async def post(client, path, sku="A", qty=1, warehouse="WH-1"):
    response = await client.post(path, json={"sku": sku, "qty": qty, "warehouse": warehouse})
    return response.status, await response.json()


async def table_counts(engine):
    async with engine.connect() as connection:
        return [
            (await connection.execute(select(func.count()).select_from(table))).scalar_one()
            for table in (inventory_balances, stock_events, outbox)
        ]


@pytest.mark.parametrize("existing_stock", [False, True])
async def test_rejected_issue_has_no_side_effects(
    aiohttp_client, db_engine, settings, existing_stock
):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    if existing_stock:
        assert (await post(client, "/receipts", qty=3))[0] == 201
    before = await table_counts(db_engine)
    status, payload = await post(client, "/issues", qty=4)
    assert status == 409
    assert payload == {"error": "Insufficient stock"}
    assert await table_counts(db_engine) == before
    response = await client.get("/stock", params={"warehouse": "WH-1"})
    assert await response.json() == {
        "items": [{"warehouse": "WH-1", "sku": "A", "qty": 3}] if existing_stock else []
    }


async def test_parallel_issues_across_api_instances_never_overspend(
    aiohttp_client, db_engine, settings
):
    clients = [
        await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
        for _ in range(2)
    ]
    assert (await post(clients[0], "/receipts", qty=10))[0] == 201
    results = await asyncio.gather(*(post(clients[index % 2], "/issues") for index in range(24)))
    statuses = [status for status, _ in results]
    assert statuses.count(201) == 10
    assert statuses.count(409) == 14
    response = await clients[0].get("/stock?warehouse=WH-1&sku=A")
    assert await response.json() == {"items": [{"warehouse": "WH-1", "sku": "A", "qty": 0}]}
    async with db_engine.connect() as connection:
        assert (await connection.execute(select(inventory_balances.c.qty))).scalar_one() == 0
    assert await table_counts(db_engine) == [1, 11, 11]


async def test_parallel_first_receipts_and_distinct_pairs(aiohttp_client, db_engine, settings):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    receipts = await asyncio.gather(
        *(
            post(client, "/receipts", sku=sku, warehouse=warehouse)
            for warehouse, sku in [("WH-1", "A"), ("WH-1", "B"), ("WH-2", "A")]
            for _ in range(8)
        )
    )
    assert all(status == 201 for status, _ in receipts)
    issues = await asyncio.gather(
        *(
            post(client, "/issues", sku=sku, qty=8, warehouse=warehouse)
            for warehouse, sku in [("WH-1", "A"), ("WH-1", "B"), ("WH-2", "A")]
        )
    )
    assert all(status == 201 for status, _ in issues)
    async with db_engine.connect() as connection:
        balances = (await connection.execute(select(inventory_balances.c.qty))).scalars().all()
    assert balances == [0, 0, 0]
    assert await table_counts(db_engine) == [3, 27, 27]


@pytest.mark.parametrize("operation", ["/receipts", "/issues"])
async def test_outbox_insert_failure_rolls_back_entire_operation(
    aiohttp_client, db_engine, settings, operation
):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    if operation == "/issues":
        assert (await post(client, "/receipts", qty=5))[0] == 201
    before = await table_counts(db_engine)
    async with db_engine.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION reject_outbox()
        """)
        )
    status, payload = await post(client, operation)
    assert status == 503
    assert payload == {"error": "Database unavailable"}
    assert await table_counts(db_engine) == before
    async with db_engine.connect() as connection:
        balance = (await connection.execute(select(inventory_balances.c.qty))).scalar_one_or_none()
    assert balance == (5 if operation == "/issues" else None)


async def test_stock_and_summary_aggregate_journal_with_zeroes_and_ties(
    aiohttp_client, db_engine, settings
):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    for path, sku, qty, warehouse in [
        ("/receipts", "A", 10, "WH-1"),
        ("/receipts", "B", 4, "WH-1"),
        ("/issues", "A", 3, "WH-1"),
        ("/receipts", "A", 7, "WH-2"),
        ("/receipts", "C", 14, "WH-2"),
        ("/issues", "B", 4, "WH-1"),
    ]:
        assert (await post(client, path, sku, qty, warehouse))[0] == 201
    # These projections are deliberately inconsistent: the API must query stock_events.
    async with db_engine.begin() as connection:
        await connection.execute(inventory_balances.update().values(qty=999))
        await connection.execute(stock_agg.insert().values(warehouse="WH-1", sku="FAKE", qty=999))

    response = await client.get("/stock?warehouse=WH-1")
    assert await response.json() == {
        "items": [
            {"warehouse": "WH-1", "sku": "A", "qty": 7},
            {"warehouse": "WH-1", "sku": "B", "qty": 0},
        ]
    }
    response = await client.get("/stock?warehouse=WH-2&sku=C")
    assert await response.json() == {"items": [{"warehouse": "WH-2", "sku": "C", "qty": 14}]}
    response = await client.get("/stock?warehouse=WH-2&sku=missing")
    assert await response.json() == {"items": []}
    response = await client.get("/stock?warehouse=missing")
    assert await response.json() == {"items": []}

    statements = []

    def capture_statement(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(db_engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        response = await client.get("/stock/summary?top_n=2")
        assert response.status == 200
        assert await response.json() == {
            "warehouses": [
                {"warehouse": "WH-1", "total_qty": 7, "sku_count": 2},
                {"warehouse": "WH-2", "total_qty": 21, "sku_count": 2},
            ],
            "top_skus": [{"sku": "A", "total_qty": 14}, {"sku": "C", "total_qty": 14}],
        }
    finally:
        event.remove(db_engine.sync_engine, "before_cursor_execute", capture_statement)
    assert len(statements) == 1
    assert "GROUP BY" in statements[0]
    assert "FROM (SELECT" in statements[0]

    response = await client.get("/stock/summary")
    assert (await response.json())["top_skus"] == [
        {"sku": "A", "total_qty": 14},
        {"sku": "C", "total_qty": 14},
        {"sku": "B", "total_qty": 0},
    ]


async def test_empty_reads_health_and_borrowed_engine_lifecycle(
    aiohttp_client, db_engine, settings
):
    client = await aiohttp_client(create_app(settings, engine=db_engine, publish=False))
    response = await client.get("/stock?warehouse=WH-1")
    assert await response.json() == {"items": []}
    response = await client.get("/stock/summary")
    assert await response.json() == {"warehouses": [], "top_skus": []}
    response = await client.get("/health")
    assert response.status == 200
    assert await response.json() == {"status": "ok"}
    pool = db_engine.pool
    await client.close()
    assert db_engine.pool is pool  # Disposing a borrowed engine would replace its pool.
    async with db_engine.connect() as connection:
        assert (await connection.execute(select(1))).scalar_one() == 1


async def test_application_publisher_delivers_outbox_and_preserves_borrowed_redis(
    aiohttp_client, db_engine, redis_client, settings
):
    client = await aiohttp_client(
        create_app(settings, engine=db_engine, redis=redis_client, publish=True)
    )
    status, payload = await post(client, "/receipts")
    assert status == 201
    for _ in range(200):
        async with db_engine.connect() as connection:
            published_at = (await connection.execute(select(outbox.c.published_at))).scalar_one()
        if published_at is not None:
            break
        await asyncio.sleep(0.01)
    assert published_at is not None, "API did not publish its committed outbox event"
    entries = await redis_client.xrange(settings.stream)
    assert len(entries) == 1
    assert json.loads(entries[0][1]["data"]) == payload
    await client.close()
    assert await redis_client.ping()


@pytest.mark.parametrize("cancelled", [False, True])
async def test_uow_without_commit_rolls_back_and_returns_connection(db_engine, cancelled):
    async def change_balance():
        async with PgInventoryUnitOfWork(db_engine, InventoryEventCodec()) as uow:
            await uow.balances.increase(StockKey("WH", "A"), Quantity(7))
            if cancelled:
                raise asyncio.CancelledError

    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await change_balance()
    else:
        await change_balance()
    assert await table_counts(db_engine) == [0, 0, 0]
    async with PgInventoryUnitOfWork(db_engine, InventoryEventCodec()) as uow:
        await uow.balances.increase(StockKey("WH", "A"), Quantity(3))
        await uow.commit()
    async with db_engine.connect() as connection:
        assert await connection.scalar(select(inventory_balances.c.qty)) == 3
