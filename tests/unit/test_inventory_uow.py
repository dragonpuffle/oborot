"""UoW cleanup preserves cancellation, including a broken backend cleanup."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from asyncpg import InvalidPasswordError
from sqlalchemy.exc import OperationalError

from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec
from inventory_api.infrastructure.postgres.queries import PgDatabaseHealthCheck, PgStockQueries
from inventory_api.infrastructure.postgres.transactions import execute
from inventory_api.infrastructure.postgres.uow import PgInventoryUnitOfWork


@pytest.mark.parametrize("cleanup", ["rollback", "close"])
async def test_cancellation_survives_backend_cleanup_failure(cleanup):
    transaction = AsyncMock()
    connection = AsyncMock()
    connection.begin.return_value = transaction
    engine = AsyncMock()
    engine.connect.return_value = connection
    target = transaction.rollback if cleanup == "rollback" else connection.close
    target.side_effect = OperationalError("cleanup", {}, None)
    with pytest.raises(asyncio.CancelledError):
        async with PgInventoryUnitOfWork(engine, InventoryEventCodec()):
            raise asyncio.CancelledError
    transaction.rollback.assert_awaited_once()
    connection.close.assert_awaited_once()


async def test_raw_driver_connection_failure_is_translated_with_original_cause():
    error = InvalidPasswordError("injected authentication failure")
    engine = AsyncMock()
    engine.connect.side_effect = error
    with pytest.raises(DependencyUnavailable) as caught:
        async with PgInventoryUnitOfWork(engine, InventoryEventCodec()):
            pytest.fail("connection failure must not enter")
    assert caught.value.__cause__ is error


@pytest.mark.parametrize("operation", ["stock", "summary", "health"])
async def test_query_connection_failure_is_translated_with_original_cause(operation):
    error = InvalidPasswordError("injected authentication failure")
    connection_context = AsyncMock()
    connection_context.__aenter__.side_effect = error
    engine = MagicMock()
    engine.connect.return_value = connection_context
    with pytest.raises(DependencyUnavailable) as caught:
        if operation == "stock":
            await PgStockQueries(engine).stock("WH")
        elif operation == "summary":
            await PgStockQueries(engine).summary(5)
        else:
            await PgDatabaseHealthCheck(engine).check()
    assert caught.value.__cause__ is error


async def test_raw_driver_statement_failure_is_translated_with_original_cause():
    error = InvalidPasswordError("injected connection failure")
    connection = AsyncMock()
    connection.execute.side_effect = error
    with pytest.raises(DependencyUnavailable) as caught:
        await execute(connection, object())
    assert caught.value.__cause__ is error


async def test_cancelled_begin_closes_connection_and_preserves_cancellation():
    connection = AsyncMock()
    connection.begin.side_effect = asyncio.CancelledError
    connection.close.side_effect = OperationalError("cleanup", {}, None)
    engine = AsyncMock()
    engine.connect.return_value = connection
    with pytest.raises(asyncio.CancelledError):
        async with PgInventoryUnitOfWork(engine, InventoryEventCodec()):
            pytest.fail("cancelled transaction must not enter")
    connection.close.assert_awaited_once()
