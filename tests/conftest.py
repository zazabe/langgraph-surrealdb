from __future__ import annotations

import os
import re
import uuid
from dataclasses import replace

import pytest

from langgraph_surrealdb.checkpoint import SurrealSaver
from langgraph_surrealdb.database import surreal_client
from langgraph_surrealdb.database.common import SurrealConnSettings


def get_surreal_settings() -> SurrealConnSettings:
    try:
        return SurrealConnSettings.from_env()
    except ValueError as e:
        pytest.skip(
            f"Set SURREAL_URL, SURREAL_NS, SURREAL_DB, SURREAL_USER, and SURREAL_PASS to run SurrealDB integration tests: {e}"
        )


def _module_slug(module_name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", module_name).strip("_").lower()


@pytest.fixture(scope="session")
def base_settings() -> SurrealConnSettings:
    return get_surreal_settings()


@pytest.fixture(scope="session")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def settings(
    base_settings: SurrealConnSettings,
    run_id: str,
    request: pytest.FixtureRequest,
) -> SurrealConnSettings:
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    module_name = request.module.__name__
    module_db = (
        f"{base_settings.database}_{run_id}_{worker_id}_{_module_slug(module_name)}"
    )
    return replace(base_settings, database=module_db)


def _clear_tables(settings: SurrealConnSettings) -> None:
    with surreal_client(settings) as conn:
        conn.query("DELETE checkpoints;")
        conn.query("DELETE writes;")


@pytest.fixture(autouse=True)
def cleanup_checkpoint_tables(
    settings: SurrealConnSettings,
):
    with SurrealSaver.from_settings(settings) as saver:
        saver.setup()
    _clear_tables(settings)
    yield
    _clear_tables(settings)
