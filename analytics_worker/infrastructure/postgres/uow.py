"""One connection and explicit transaction per event application."""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncEngine

from analytics_worker.infrastructure.postgres.errors import BACKEND_ERRORS
from analytics_worker.infrastructure.postgres.repositories import (
    PgProcessedEventsRepository,
    PgProjectionRepository,
)
from common.errors import DependencyUnavailable

logger = logging.getLogger(__name__)


class PgProjectionUnitOfWork:
    def __init__(self, engine: AsyncEngine):
        self._engine = engine
        self._connection = None
        self._transaction = None

    async def __aenter__(self):
        try:
            self._connection = await self._engine.connect()
            self._transaction = await self._connection.begin()
            self.processed = PgProcessedEventsRepository(self._connection)
            self.projection = PgProjectionRepository(self._connection)
            return self
        except BaseException as error:
            if self._connection is not None:
                try:
                    await self._connection.close()
                except BACKEND_ERRORS:
                    logger.exception("Connection cleanup failed after transaction startup failure")
            if isinstance(error, BACKEND_ERRORS):
                raise DependencyUnavailable(str(error)) from error
            raise

    async def commit(self):
        try:
            await self._transaction.commit()
        except BACKEND_ERRORS as error:
            raise DependencyUnavailable(str(error)) from error

    async def __aexit__(self, exc_type, exc, traceback):
        try:
            try:
                if self._transaction.is_active:
                    await self._transaction.rollback()
            finally:
                await self._connection.close()
        except BACKEND_ERRORS as error:
            if isinstance(exc, asyncio.CancelledError):
                logger.exception("Transaction cleanup failed during cancellation")
                return
            raise DependencyUnavailable(str(error)) from error
