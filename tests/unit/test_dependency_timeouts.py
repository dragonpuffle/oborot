"""Model Python 3.10's distinct asyncio.TimeoutError on the configured SDK."""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


@pytest.mark.parametrize("service", ["inventory", "analytics"])
def test_distinct_asyncio_connection_timeout_is_translated_for_retry(service):
    # Isolate the compatibility model so other tests retain the real exception
    # hierarchy. This checks adapter behavior; it is not a Python 3.10 test run.
    probe = textwrap.dedent(
        f"""
        import asyncio
        from unittest.mock import AsyncMock

        class LegacyTimeout(Exception):
            pass

        asyncio.TimeoutError = LegacyTimeout
        from common.errors import DependencyUnavailable
        if {service!r} == "inventory":
            from common.event_codec import InventoryEventCodec
            from inventory_api.infrastructure.postgres.uow import PgOutboxUnitOfWork
            make_uow = lambda engine: PgOutboxUnitOfWork(engine, InventoryEventCodec())
        else:
            from analytics_worker.infrastructure.postgres.uow import PgProjectionUnitOfWork
            make_uow = PgProjectionUnitOfWork

        async def main():
            engine = AsyncMock()
            timeout = LegacyTimeout("transient database connection timeout")
            engine.connect.side_effect = timeout
            try:
                async with make_uow(engine):
                    raise AssertionError("failed connection cannot enter")
            except DependencyUnavailable as error:
                assert error.__cause__ is timeout

        asyncio.run(main())
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
