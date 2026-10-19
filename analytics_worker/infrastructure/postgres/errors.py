"""Known driver and SQLAlchemy failures translated at PostgreSQL adapters."""

import asyncio

from asyncpg import InterfaceError, PostgresError
from sqlalchemy.exc import SQLAlchemyError

BACKEND_ERRORS = (SQLAlchemyError, OSError, PostgresError, InterfaceError, asyncio.TimeoutError)
