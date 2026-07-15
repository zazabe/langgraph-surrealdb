from __future__ import annotations

import os
import re
import uuid
from collections.abc import AsyncGenerator
from typing import Annotated, Self

import pytest
from pydantic import BaseModel, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from surrealdb import SurrealError

from langgraph_surrealdb import RootAuth, SurrealSaverDatabaseSettings
from langgraph_surrealdb.checkpoint import SurrealSaver
from langgraph_surrealdb.database import async_surreal_client
from langgraph_surrealdb.database.common import SurrealSaverSettings

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class SurrealEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LANGGRAPH_SURREALDB_TEST_ROOT_",
        extra="ignore",
    )

    user: NonEmpty
    password: NonEmpty


class TestSettings(BaseModel):
    test_root_user: str
    test_root_password: str
    user_settings: SurrealSaverSettings

    @classmethod
    def from_env(cls) -> Self:
        test_cfg: SurrealEnvSettings = SurrealEnvSettings.model_validate({})
        user_settings: SurrealSaverSettings = SurrealSaverSettings.from_env()
        return cls(
            test_root_user=test_cfg.user,
            test_root_password=test_cfg.password,
            user_settings=user_settings,
        )


def _module_slug(module_name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", module_name).strip("_").lower()


@pytest.fixture(scope="session")
def test_settings() -> TestSettings:
    try:
        return TestSettings.from_env()
    except ValueError as e:
        pytest.skip(
            f"Set .env.test (see .env.test.example) variables to run integration tests: {e}"
        )


@pytest.fixture(scope="session")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
async def db_module_name(
    test_settings: TestSettings,
    run_id: str,
    request: pytest.FixtureRequest,
) -> str:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    module_name = request.module.__name__
    db_name = test_settings.user_settings.db.database
    return f"{db_name}_{run_id}_{worker_id}_{_module_slug(module_name)}"


@pytest.fixture(scope="module")
async def root_settings(
    test_settings: TestSettings,
    db_module_name: str,
) -> SurrealSaverSettings:
    return SurrealSaverSettings(
        db=SurrealSaverDatabaseSettings(
            url=test_settings.user_settings.db.url,
            namespace=test_settings.user_settings.db.namespace,
            database=db_module_name,
            auth=RootAuth(
                username=test_settings.test_root_user,
                password=test_settings.test_root_password,
            ),
        ),
    )


@pytest.fixture(scope="module")
async def settings(
    root_settings: SurrealSaverSettings,
    test_settings: TestSettings,
    db_module_name: str,
) -> AsyncGenerator[SurrealSaverSettings, None]:
    user_settings = SurrealSaverSettings(
        db=SurrealSaverDatabaseSettings(
            url=test_settings.user_settings.db.url,
            namespace=test_settings.user_settings.db.namespace,
            database=db_module_name,
            auth=test_settings.user_settings.db.auth,
        ),
    )
    await _init_database(root_settings, db_module_name)
    yield user_settings
    await _drop_database(root_settings, db_module_name)


async def _ensure_database(
    root_settings: SurrealSaverSettings,
    database_name: str,
) -> None:
    async with async_surreal_client(root_settings) as conn:
        try:
            await conn.query(f"DEFINE DATABASE `{database_name}`;")
        except SurrealError as e:
            if "already exists" in str(e):
                await conn.query(f"REMOVE DATABASE `{database_name}`;")
                await conn.query(f"DEFINE DATABASE `{database_name}`;")
            else:
                raise


async def _init_database(
    root_settings: SurrealSaverSettings,
    database_name: str,
) -> None:
    await _ensure_database(root_settings, database_name)
    db_settings = root_settings.model_copy()
    db_settings.db.database = database_name

    async with async_surreal_client(db_settings) as conn:
        await conn.query("""
            DEFINE TABLE IF NOT EXISTS user SCHEMALESS;
            DEFINE FIELD IF NOT EXISTS username ON user TYPE string;
            DEFINE FIELD IF NOT EXISTS password ON user TYPE string;
            DEFINE INDEX IF NOT EXISTS idx_username ON user FIELDS username UNIQUE;
            DEFINE ACCESS IF NOT EXISTS auth ON DATABASE TYPE RECORD
            SIGNIN (
                SELECT * FROM user
                WHERE username = $username AND password = $password
                LIMIT 1
            );
            """)
        await conn.query(
            """
            UPSERT user:pytest CONTENT { username: $username, password: $password };
            """,
            {"username": "pytest", "password": "pytest"},
        )


async def _drop_database(
    root_settings: SurrealSaverSettings,
    database_name: str,
) -> None:
    async with async_surreal_client(root_settings) as conn:
        await conn.query(f"REMOVE DATABASE IF EXISTS`{database_name}`;")


async def _clear_tables(root_settings: SurrealSaverSettings) -> None:
    async with async_surreal_client(root_settings) as conn:
        await conn.query("DELETE checkpoints;")
        await conn.query("DELETE writes;")


@pytest.fixture(autouse=True)
async def cleanup_checkpoint_tables(
    root_settings: SurrealSaverSettings,
):
    with SurrealSaver.from_settings(root_settings) as saver:
        saver.setup()
    await _clear_tables(root_settings)
    yield
    await _clear_tables(root_settings)
