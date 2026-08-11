from __future__ import annotations

import os
import re
import uuid
from typing import Annotated

import pytest
from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from surrealdb import SurrealError

from langgraph_surrealdb.database import async_surreal_client
from langgraph_surrealdb.settings import RootAuth, SurrealDatabaseSettings

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class SurrealEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LANGGRAPH_SURREALDB_TEST_ROOT_",
        extra="ignore",
    )

    user: NonEmpty
    password: NonEmpty


def _module_slug(module_name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", module_name).strip("_").lower()


@pytest.fixture(scope="session")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
async def db_module_name(
    run_id: str,
    request: pytest.FixtureRequest,
) -> str:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    module_name = request.module.__name__
    return f"{run_id}_{worker_id}_{_module_slug(module_name)}"


@pytest.fixture(scope="module")
async def root_db_settings(
    db_module_name: str,
) -> SurrealDatabaseSettings:
    try:
        test_cfg: SurrealEnvSettings = SurrealEnvSettings.model_validate({})
        db_settings = SurrealDatabaseSettings.from_env()
        return SurrealDatabaseSettings(
            url=db_settings.url,
            namespace=db_settings.namespace,
            database=db_module_name,
            auth=RootAuth(
                username=test_cfg.user,
                password=test_cfg.password,
            ),
        )
    except ValueError as e:
        pytest.skip(
            f"Set .env.test (see .env.test.example) variables to run integration tests: {e}"
        )


async def _ensure_database(
    root_db_settings: SurrealDatabaseSettings,
) -> None:
    database_name = root_db_settings.database
    async with async_surreal_client(root_db_settings) as conn:
        try:
            await conn.query_raw(f"DEFINE DATABASE `{database_name}`;")
        except SurrealError as e:
            if "already exists" in str(e):
                await conn.query_raw(f"REMOVE DATABASE `{database_name}`;")
                await conn.query_raw(f"DEFINE DATABASE `{database_name}`;")
            else:
                raise


async def init_database(
    root_db_settings: SurrealDatabaseSettings,
) -> None:
    await _ensure_database(root_db_settings)

    async with async_surreal_client(root_db_settings) as conn:
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


async def drop_database(
    root_db_settings: SurrealDatabaseSettings,
) -> None:
    database_name = root_db_settings.database
    async with async_surreal_client(root_db_settings) as conn:
        await conn.query(f"REMOVE DATABASE IF EXISTS`{database_name}`;")


async def clear_tables(
    root_db_settings: SurrealDatabaseSettings, tables: list[str]
) -> None:
    async with async_surreal_client(root_db_settings) as conn:
        for table in tables:
            try:
                await conn.query(f"DELETE {table};")
            except Exception as e:
                if "does not exist" in str(e):
                    continue
                raise
