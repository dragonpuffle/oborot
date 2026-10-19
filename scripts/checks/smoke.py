"""Exercise the running Compose HTTP/publisher/worker path and duplicate replay.

Run from the repository root. Each run writes a unique warehouse; it never deletes
existing data. --recheck reads the report after a Compose stop/start.
"""

import argparse
import asyncio
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine

from common.config import Settings
from common.schema import outbox, processed_events, stock_agg


def http(path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(
        "http://localhost:8080" + path,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


async def wait_for(check):
    for _ in range(300):
        if await check():
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("Compose smoke did not converge")


async def run(report, recheck):
    settings = Settings.from_env()
    engine = create_async_engine(settings.database_url)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        assert await asyncio.to_thread(http, "/health") == (200, {"status": "ok"})
        if recheck:
            evidence = json.loads(report.read_text(encoding="utf-8"))
            warehouse = evidence["warehouse"]
        else:
            warehouse = "smoke-" + uuid4().hex
            events = []
            for path, qty in (("/receipts", 50), ("/issues", 20)):
                status, event = await asyncio.to_thread(
                    http, path, dict(warehouse=warehouse, sku="SMOKE", qty=qty)
                )
                assert status == 201, (status, event)
                events.append(event)
            status, error = await asyncio.to_thread(
                http, "/issues", dict(warehouse=warehouse, sku="SMOKE", qty=31)
            )
            assert (status, error) == (409, {"error": "Insufficient stock"})
            evidence = dict(warehouse=warehouse, events=events)

        event_ids = [UUID(event["id"]) for event in evidence["events"]]

        async def delivered():
            async with engine.connect() as connection:
                marked = await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(outbox)
                    .where(outbox.c.event_id.in_(event_ids), outbox.c.published_at.is_not(None))
                )
                applied = await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(processed_events)
                    .where(processed_events.c.event_id.in_(event_ids))
                )
                return marked == applied == 2

        await wait_for(delivered)
        if not recheck:
            message_id = await redis.xadd(
                settings.stream, {"data": json.dumps(evidence["events"][0])}
            )

            async def acknowledged():
                groups = await redis.xinfo_groups(settings.stream)
                group = next(item for item in groups if item["name"] == settings.group)
                # Compare stream IDs numerically, then require this entry is no longer pending.
                reached = tuple(map(int, group["last-delivered-id"].split("-"))) >= tuple(
                    map(int, message_id.split("-"))
                )
                pending = await redis.xpending_range(
                    settings.stream, settings.group, message_id, message_id, 1
                )
                return reached and not pending

            await wait_for(acknowledged)
            evidence["replay_id"] = message_id

        status, stock = await asyncio.to_thread(http, "/stock?warehouse=" + warehouse)
        assert status == 200
        assert stock == {"items": [dict(warehouse=warehouse, sku="SMOKE", qty=30)]}
        status, summary = await asyncio.to_thread(http, "/stock/summary?top_n=100")
        assert status == 200
        assert dict(warehouse=warehouse, total_qty=30, sku_count=1) in summary["warehouses"]
        async with engine.connect() as connection:
            qty = await connection.scalar(
                sa.select(stock_agg.c.qty).where(
                    stock_agg.c.warehouse == warehouse, stock_agg.c.sku == "SMOKE"
                )
            )
        assert qty == 30, qty
        evidence["restart_rechecked" if recheck else "passed"] = True
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        print(json.dumps(evidence))
    finally:
        try:
            await redis.aclose()
        finally:
            await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("tmp/architecture-smoke.json"))
    parser.add_argument("--recheck", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.report, args.recheck))
