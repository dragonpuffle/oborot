"""Explicit composition and ownership of HTTP and outbox dependencies."""

import asyncio
from contextlib import suppress
from functools import partial

from aiohttp import web
from redis.asyncio import Redis

from common.config import Settings
from common.db import create_engine
from common.event_codec import InventoryEventCodec
from inventory_api.application.outbox import PublishOutboxBatch
from inventory_api.application.services import InventoryService
from inventory_api.infrastructure.postgres.queries import PgDatabaseHealthCheck, PgStockQueries
from inventory_api.infrastructure.postgres.uow import PgInventoryUnitOfWork, PgOutboxUnitOfWork
from inventory_api.infrastructure.redis.publisher import RedisEventPublisher
from inventory_api.presentation.http.controller import InventoryController, error_responses
from inventory_api.presentation.outbox_runner import OutboxRunner


def create_app(settings=None, *, engine=None, redis=None, publish=True):
    settings = settings if settings is not None else Settings.from_env()
    database = engine if engine is not None else create_engine(settings)
    codec = InventoryEventCodec()
    service = InventoryService(
        partial(PgInventoryUnitOfWork, database, codec), PgStockQueries(database)
    )
    controller = InventoryController(service, PgDatabaseHealthCheck(database))
    app = web.Application(middlewares=[error_responses])
    controller.register(app)

    async def resources(application):
        publisher = None
        client = redis
        try:
            if publish:
                if client is None:
                    client = Redis.from_url(
                        settings.redis_url,
                        decode_responses=True,
                        socket_connect_timeout=5,
                        socket_timeout=5,
                    )
                batch = PublishOutboxBatch(
                    partial(PgOutboxUnitOfWork, database, codec),
                    RedisEventPublisher(client, settings.stream, codec),
                    settings.batch_size,
                )
                runner = OutboxRunner(batch, settings.poll_interval)
                publisher = asyncio.create_task(runner.run(), name="outbox-publisher")
            yield
        finally:
            try:
                if publisher is not None:
                    publisher.cancel()
                    with suppress(asyncio.CancelledError):
                        await publisher
            finally:
                try:
                    if client is not None and redis is None:
                        await client.aclose()
                finally:
                    if engine is None:
                        await database.dispose()

    app.cleanup_ctx.append(resources)
    return app
