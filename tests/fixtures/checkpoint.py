from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from langgraph_surrealdb.checkpoint import SurrealSaver
from langgraph_surrealdb.checkpoint.settings import SurrealCheckpointSettings
from langgraph_surrealdb.settings import SurrealDatabaseSettings
from tests.fixtures.database import clear_tables, drop_database, init_database


@pytest.fixture(scope="module")
async def root_checkpoint_settings(
    root_db_settings: SurrealDatabaseSettings,
) -> SurrealCheckpointSettings:
    checkpoint_settings = SurrealCheckpointSettings.from_env()
    return SurrealCheckpointSettings(
        db=root_db_settings,
        checkpoints_table=checkpoint_settings.checkpoints_table,
        writes_table=checkpoint_settings.writes_table,
    )


@pytest.fixture(scope="module")
async def checkpoint_database(
    root_db_settings: SurrealDatabaseSettings,
) -> AsyncGenerator[None, None]:
    await init_database(root_db_settings)
    yield
    await drop_database(root_db_settings)


@pytest.fixture
async def checkpoint_settings(
    root_checkpoint_settings: SurrealCheckpointSettings,
    db_module_name: str,
    checkpoint_database: None,
) -> AsyncGenerator[SurrealCheckpointSettings, None]:
    checkpoint_settings = SurrealCheckpointSettings.from_env()
    checkpoint_settings.db.database = db_module_name

    with SurrealSaver.from_settings(root_checkpoint_settings) as saver:
        saver.setup()
    tables = [checkpoint_settings.checkpoints_table, checkpoint_settings.writes_table]
    await clear_tables(root_checkpoint_settings.db, tables)
    try:
        yield checkpoint_settings
    finally:
        await clear_tables(root_checkpoint_settings.db, tables)
