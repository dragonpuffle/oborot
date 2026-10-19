"""Composition and ownership of worker dependencies."""

import asyncio
import logging

from redis.asyncio import Redis

from analytics_worker.application.handlers import ApplyInventoryEvent
from analytics_worker.infrastructure.postgres.uow import PgProjectionUnitOfWork
from analytics_worker.infrastructure.redis.transport import RedisStreamTransport
from analytics_worker.presentation.consumer import InventoryConsumer
from common.config import Settings
from common.db import create_engine
from common.event_codec import InventoryEventCodec

logger = logging.getLogger(__name__)


class WorkerRuntime:
    def __init__(self, settings, *, engine=None, redis=None):
        self._settings = settings
        self._engine = engine
        self._redis = redis
        self._owns_engine = engine is None
        self._owns_redis = redis is None
        self._consumer = None

    async def __aenter__(self):
        try:
            if self._owns_engine:
                self._engine = create_engine(self._settings)
            if self._owns_redis:
                self._redis = Redis.from_url(
                    self._settings.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=max(5, self._settings.block_ms / 1000 + 1),
                )
            self._consumer = InventoryConsumer(
                RedisStreamTransport(self._redis, self._settings),
                InventoryEventCodec(),
                ApplyInventoryEvent(lambda: PgProjectionUnitOfWork(self._engine)),
                poll_interval=self._settings.poll_interval,
            )
            return self
        except BaseException as error:
            await self.__aexit__(type(error), error, error.__traceback__)
            raise

    async def __aexit__(self, exc_type, exc, traceback):
        try:
            try:
                if self._owns_redis and self._redis is not None:
                    await self._redis.aclose()
            finally:
                try:
                    if self._owns_engine and self._engine is not None:
                        await self._engine.dispose()
                finally:
                    self._consumer = None
        except Exception:
            if not isinstance(exc, asyncio.CancelledError):
                raise
            logger.exception("Worker cleanup failed during cancellation")

    async def run_once(self) -> int:
        if self._consumer is None:
            raise RuntimeError("worker must be entered as an async context manager")
        return await self._consumer.run_once()

    async def run(self) -> None:
        if self._consumer is None:
            raise RuntimeError("worker must be entered as an async context manager")
        await self._consumer.run()


def create_worker(settings=None, *, engine=None, redis=None) -> WorkerRuntime:
    return WorkerRuntime(settings or Settings.from_env(), engine=engine, redis=redis)
