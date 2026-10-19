"""UoW rollback/cleanup and cancellation are explicit application guarantees."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from asyncpg import InterfaceError, InvalidPasswordError
from sqlalchemy.exc import OperationalError

from analytics_worker.infrastructure.postgres.repositories import PgProjectionRepository
from analytics_worker.infrastructure.postgres.uow import PgProjectionUnitOfWork
from common.errors import DependencyUnavailable
from common.values import StockKey


async def test_exit_without_commit_rolls_back_and_closes():
    engine = AsyncMock()
    connection = engine.connect.return_value
    transaction = connection.begin.return_value
    transaction.is_active = True
    async with PgProjectionUnitOfWork(engine):
        pass
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()
    connection.close.assert_awaited_once()


@pytest.mark.parametrize("error_type", [InvalidPasswordError, InterfaceError])
@pytest.mark.parametrize("operation", ["connect", "statement"])
async def test_driver_errors_are_translated_with_original_cause(error_type, operation):
    error = error_type("backend unavailable")
    engine = AsyncMock()
    connection = AsyncMock()
    engine.connect.side_effect = error
    connection.execute.side_effect = error
    with pytest.raises(DependencyUnavailable) as caught:
        if operation == "connect":
            async with PgProjectionUnitOfWork(engine):
                pytest.fail("failed connection cannot enter")
        else:
            await PgProjectionRepository(connection).add_delta(StockKey("W", "A"), 7)
    assert caught.value.__cause__ is error


async def test_cancellation_survives_rollback_error_and_still_closes():
    engine = AsyncMock()
    connection = engine.connect.return_value
    transaction = connection.begin.return_value
    transaction.is_active = True
    transaction.rollback.side_effect = OperationalError("rollback failed", {}, None)
    with pytest.raises(asyncio.CancelledError):
        async with PgProjectionUnitOfWork(engine):
            raise asyncio.CancelledError()
    transaction.rollback.assert_awaited_once()
    connection.close.assert_awaited_once()
