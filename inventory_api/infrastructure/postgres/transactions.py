"""Translate known SQL failures at the infrastructure seam."""

import asyncio

from asyncpg import InterfaceError, PostgresError
from sqlalchemy.exc import SQLAlchemyError

from common.errors import DependencyUnavailable

BACKEND_ERRORS = (SQLAlchemyError, OSError, PostgresError, InterfaceError, asyncio.TimeoutError)


async def execute(connection, statement):
    try:
        return await connection.execute(statement)
    except BACKEND_ERRORS as error:
        raise DependencyUnavailable("Database unavailable") from error
