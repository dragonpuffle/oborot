"""Validation runs at the HTTP boundary before any database access."""

import asyncio

import pytest

from inventory_api import create_app


@pytest.fixture
async def client(aiohttp_client):
    return await aiohttp_client(create_app(engine=object(), publish=False))


async def test_extremely_large_top_n_is_bad_request(client):
    response = await client.get("/stock/summary", params={"top_n": "9" * 4500})
    assert response.status == 400
    assert "error" in await response.json()


async def test_deeply_nested_json_is_bad_request(client):
    response = await client.post(
        "/receipts",
        data="[" * 10000 + "0" + "]" * 10000,
        headers={"Content-Type": "application/json"},
    )
    assert response.status == 400
    assert "error" in await response.json()


async def test_missing_fields_preserve_validation_order_and_error_text(client):
    response = await client.post("/receipts", json={"sku": "A"})
    assert response.status == 400
    assert await response.json() == {
        "error": "warehouse must be a valid nonempty Unicode code of at most 100 characters"
    }


@pytest.mark.parametrize("route", ["/receipts", "/issues"])
@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        "text",
        {},
        {"sku": "A", "qty": 0, "warehouse": "WH"},
        {"sku": "A", "qty": -1, "warehouse": "WH"},
        {"sku": "A", "qty": True, "warehouse": "WH"},
        {"sku": "A", "qty": 1.5, "warehouse": "WH"},
        {"sku": "A", "qty": "1", "warehouse": "WH"},
        {"sku": "A", "qty": 1_000_000_001, "warehouse": "WH"},
        {"sku": "", "qty": 1, "warehouse": "WH"},
        {"sku": " A", "qty": 1, "warehouse": "WH"},
        {"sku": "A" * 101, "qty": 1, "warehouse": "WH"},
        {"sku": "A\x00", "qty": 1, "warehouse": "WH"},
        {"sku": "\ud800", "qty": 1, "warehouse": "WH"},
        {"sku": "A", "qty": 1, "warehouse": None},
        {"sku": "A", "qty": 1, "warehouse": "WH "},
        {"sku": "A", "qty": 1, "warehouse": "W" * 101},
        {"sku": "A", "qty": 1, "warehouse": "W\nH"},
        {"sku": "A", "qty": 1, "warehouse": "WH", "extra": 1},
    ],
)
async def test_invalid_operation_is_json_bad_request(client, route, payload):
    response = await client.post(route, json=payload)
    assert response.status == 400
    assert "error" in await response.json()


@pytest.mark.parametrize(
    "body,content_type",
    [
        ("{broken", "application/json"),
        ("{}", "text/plain"),
        (b"\xff", "application/json"),
        (b"{}", "application/json; charset=invalid-charset"),
    ],
)
async def test_malformed_body_is_json_bad_request(client, body, content_type):
    response = await client.post("/receipts", data=body, headers={"Content-Type": content_type})
    assert response.status == 400
    assert "error" in await response.json()


@pytest.mark.parametrize(
    "path",
    [
        "/stock",
        "/stock?warehouse=",
        "/stock?warehouse=+WH",
        "/stock?warehouse=WH&sku=",
        "/stock?warehouse=WH&warehouse=OTHER",
        "/stock?warehouse=WH&unknown=1",
        "/stock/summary?top_n=0",
        "/stock/summary?top_n=101",
        "/stock/summary?top_n=1.5",
        "/stock/summary?top_n=-1",
        "/stock/summary?top_n=+1",
        "/stock/summary?top_n=",
        "/stock/summary?top_n=1&top_n=2",
        "/stock/summary?unknown=1",
        "/stock/summary?top_n=١",
        "/stock?warehouse=WH%00",
        "/stock?warehouse=" + "W" * 101,
    ],
)
async def test_invalid_query_is_json_bad_request(client, path):
    response = await client.get(path)
    assert response.status == 400
    assert "error" in await response.json()


async def test_router_errors_preserve_json_and_allowed_methods(client):
    missing = await client.get("/missing")
    assert missing.status == 404
    assert "error" in await missing.json()
    wrong_method = await client.get("/receipts")
    assert wrong_method.status == 405
    assert wrong_method.headers["Allow"] == "POST"
    assert "error" in await wrong_method.json()


class Resource:
    """Small disposable external dependency for lifecycle tests."""

    closed = False

    async def dispose(self):
        self.closed = True

    async def aclose(self):
        self.closed = True


def install_publisher(monkeypatch, publisher):
    monkeypatch.setattr("inventory_api.bootstrap.OutboxRunner.run", publisher)


async def test_owned_resources_close_when_publisher_has_crashed(aiohttp_client, monkeypatch):
    engine, redis = Resource(), Resource()
    crashed = asyncio.Event()

    async def publisher(*args):
        crashed.set()
        raise RuntimeError("injected publisher failure")

    install_publisher(monkeypatch, publisher)
    monkeypatch.setattr("inventory_api.bootstrap.create_engine", lambda settings: engine)
    monkeypatch.setattr("inventory_api.bootstrap.Redis.from_url", lambda *args, **kwargs: redis)
    client = await aiohttp_client(create_app())
    await asyncio.wait_for(crashed.wait(), timeout=1)
    with pytest.raises(RuntimeError, match="injected publisher failure"):
        await client.close()
    assert engine.closed
    assert redis.closed


@pytest.mark.parametrize("owned", [True, False])
async def test_publisher_is_cancelled_and_resource_ownership_respected(
    aiohttp_client, monkeypatch, owned
):
    engine, redis = Resource(), Resource()
    started, stopped = asyncio.Event(), asyncio.Event()

    async def publisher(self):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    install_publisher(monkeypatch, publisher)
    monkeypatch.setattr("inventory_api.bootstrap.create_engine", lambda settings: engine)
    monkeypatch.setattr("inventory_api.bootstrap.Redis.from_url", lambda *args, **kwargs: redis)
    app = create_app() if owned else create_app(engine=engine, redis=redis)
    client = await aiohttp_client(app)
    await asyncio.wait_for(started.wait(), timeout=1)
    await client.close()
    assert stopped.is_set()
    assert engine.closed is owned
    assert redis.closed is owned
