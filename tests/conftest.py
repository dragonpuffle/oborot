"""Real services, migrated per-test schemas, and uniquely owned Redis keys."""

import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from common.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def run_migration(connection, revision="head", *, downgrade=False):
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["connection"] = connection
    (command.downgrade if downgrade else command.upgrade)(config, revision)


@pytest.fixture
def settings():
    prefix = f"test-inventory-{uuid4().hex}"
    return replace(
        Settings(),
        database_url=os.getenv("TEST_DATABASE_URL", Settings.database_url),
        redis_url=os.getenv("TEST_REDIS_URL", "redis://localhost:56379/15"),
        stream=f"{prefix}:events",
        group=f"{prefix}:group",
        consumer="test-consumer",
        dead_letter_stream=f"{prefix}:dlq",
        claim_idle_ms=0,
        block_ms=10,
        poll_interval=0.01,
    )


@pytest.fixture
async def db_engine(settings):
    schema = f"test_{uuid4().hex}"
    admin = create_async_engine(settings.database_url)
    engine = create_async_engine(
        settings.database_url,
        connect_args={"server_settings": {"search_path": schema}},
    )
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with engine.begin() as connection:
            await connection.run_sync(run_migration)
        yield engine
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def redis_client(settings):
    client = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=5)
    try:
        await client.ping()  # Missing infrastructure is a failure, never a skip.
        yield client
    finally:
        await client.delete(settings.stream, settings.dead_letter_stream)
        await client.aclose()
