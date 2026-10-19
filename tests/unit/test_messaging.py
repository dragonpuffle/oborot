"""Consumer retry/cancellation and actual bootstrap resource ownership."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError

from analytics_worker.__main__ import main
from analytics_worker.bootstrap import create_worker
from analytics_worker.presentation.consumer import InventoryConsumer
from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec


async def test_consumer_retries_dependencies_and_propagates_cancellation(settings):
    transport = AsyncMock()
    transport.read_batch.side_effect = [
        DependencyUnavailable("offline"),
        [],
        asyncio.CancelledError(),
    ]
    consumer = InventoryConsumer(
        transport, InventoryEventCodec(), AsyncMock(), poll_interval=settings.poll_interval
    )
    with pytest.raises(asyncio.CancelledError):
        await consumer.run()
    assert transport.read_batch.await_count == 3
    assert transport.ensure_group.await_count == 3


@pytest.mark.parametrize("failure", [asyncio.CancelledError(), RuntimeError("consumer crashed")])
async def test_main_closes_dependencies_on_cancellation_or_crash(settings, monkeypatch, failure):
    import analytics_worker.bootstrap as bootstrap

    engine = AsyncMock()
    redis = AsyncMock()
    redis.xgroup_create.side_effect = failure
    monkeypatch.setattr(bootstrap.Settings, "from_env", lambda: settings)
    monkeypatch.setattr(bootstrap, "create_engine", lambda _: engine)
    monkeypatch.setattr(bootstrap.Redis, "from_url", lambda *args, **kwargs: redis)
    with pytest.raises(type(failure)):
        await main()
    redis.aclose.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_runtime_keeps_borrowed_dependencies_open_on_cancellation(settings):
    engine = AsyncMock()
    redis = AsyncMock()
    redis.xgroup_create.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        async with create_worker(settings, engine=engine, redis=redis) as runtime:
            await runtime.run()
    redis.aclose.assert_not_awaited()
    engine.dispose.assert_not_awaited()


async def test_runtime_disposes_engine_when_client_construction_fails(settings, monkeypatch):
    import analytics_worker.bootstrap as bootstrap

    engine = AsyncMock()
    monkeypatch.setattr(bootstrap, "create_engine", lambda _: engine)

    def fail_client(*args, **kwargs):
        raise RuntimeError("client construction failed")

    monkeypatch.setattr(bootstrap.Redis, "from_url", fail_client)
    with pytest.raises(RuntimeError, match="client construction failed"):
        async with create_worker(settings):
            pytest.fail("construction should have failed")
    engine.dispose.assert_awaited_once()


async def test_owned_engine_cleanup_survives_client_close_error(settings, monkeypatch):
    import analytics_worker.bootstrap as bootstrap

    engine = AsyncMock()
    redis = AsyncMock()
    redis.aclose.side_effect = RuntimeError("close failed")
    monkeypatch.setattr(bootstrap, "create_engine", lambda _: engine)
    monkeypatch.setattr(bootstrap.Redis, "from_url", lambda *args, **kwargs: redis)
    with pytest.raises(RuntimeError, match="close failed"):
        async with create_worker(settings):
            pass
    engine.dispose.assert_awaited_once()


async def test_cleanup_failure_does_not_replace_cancellation(settings, monkeypatch):
    import analytics_worker.bootstrap as bootstrap

    engine = AsyncMock()
    redis = AsyncMock()
    redis.xgroup_create.side_effect = asyncio.CancelledError()
    redis.aclose.side_effect = ConnectionError("close failed")
    monkeypatch.setattr(bootstrap, "create_engine", lambda _: engine)
    monkeypatch.setattr(bootstrap.Redis, "from_url", lambda *args, **kwargs: redis)
    with pytest.raises(asyncio.CancelledError):
        async with create_worker(settings) as runtime:
            await runtime.run()
    redis.aclose.assert_awaited_once()
    engine.dispose.assert_awaited_once()
