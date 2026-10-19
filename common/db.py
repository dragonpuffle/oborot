"""SQLAlchemy Core engine construction; no implicit table creation."""

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from common.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(settings.database_url, pool_pre_ping=True)
