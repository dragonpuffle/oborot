"""A fresh connection and explicit transaction per scenario invocation."""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncEngine

from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec
from inventory_api.infrastructure.postgres.repositories import (
    PgBalanceRepository,
    PgOutboxRepository,
    PgStockEventRepository,
)
from inventory_api.infrastructure.postgres.transactions import BACKEND_ERRORS

logger = logging.getLogger(__name__)


class _PgUnitOfWork:
    def __init__(self, engine: AsyncEngine, codec: InventoryEventCodec):
        self._engine = engine
        self._codec = codec
        self._connection = None
        self._transaction = None
        self._committed = False

    async def __aenter__(self):
        try:
            self._connection = await self._engine.connect()
            self._transaction = await self._connection.begin()
        except BaseException as error:
            if self._connection is not None:
                try:
                    await self._connection.close()
                except BACKEND_ERRORS:
                    # Preserve the initiating failure, especially cancellation.
                    logger.exception("Connection cleanup failed after transaction startup failure")
            if isinstance(error, BACKEND_ERRORS):
                raise DependencyUnavailable("Database unavailable") from error
            raise
        return self

    async def commit(self) -> None:
        try:
            await self._transaction.commit()
            self._committed = True
        except BACKEND_ERRORS as error:
            raise DependencyUnavailable("Database unavailable") from error

    async def __aexit__(self, exc_type, exc, traceback):
        try:
            try:
                if not self._committed:
                    await self._transaction.rollback()
            finally:
                await self._connection.close()
        except BACKEND_ERRORS as error:
            if isinstance(exc, asyncio.CancelledError):
                logger.exception("Transaction cleanup failed during cancellation")
                return
            raise DependencyUnavailable("Database unavailable") from error


class PgInventoryUnitOfWork(_PgUnitOfWork):
    async def __aenter__(self):
        await super().__aenter__()
        self.balances = PgBalanceRepository(self._connection)
        self.events = PgStockEventRepository(self._connection)
        self.outbox = PgOutboxRepository(self._connection, self._codec)
        return self


class PgOutboxUnitOfWork(_PgUnitOfWork):
    async def __aenter__(self):
        await super().__aenter__()
        self.outbox = PgOutboxRepository(self._connection, self._codec)
        return self
