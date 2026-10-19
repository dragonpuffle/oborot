from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import inspect

from common.schema import metadata
from tests.conftest import run_migration


async def test_migration_matches_metadata_and_round_trip(db_engine):
    async with db_engine.begin() as connection:
        differences = await connection.run_sync(
            lambda conn: compare_metadata(MigrationContext.configure(conn), metadata)
        )
        assert differences == []
        await connection.run_sync(lambda conn: run_migration(conn, "base", downgrade=True))
        tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        assert set(tables) <= {"alembic_version"}
        await connection.run_sync(run_migration)
        tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        assert set(metadata.tables) <= set(tables)
