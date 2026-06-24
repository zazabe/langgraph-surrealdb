from __future__ import annotations

import pytest

from langgraph_checkpoint_surreal.database import surreal_client
from langgraph_checkpoint_surreal.database.common import SurrealConnSettings


def get_surreal_settings() -> SurrealConnSettings:
    try:
        return SurrealConnSettings.from_env()
    except ValueError as e:
        pytest.skip(
            f"Set SURREAL_URL, SURREAL_NS, SURREAL_DB, SURREAL_USER, and SURREAL_PASS to run SurrealDB integration tests: {e}"
        )


@pytest.fixture(scope="function")
def settings() -> SurrealConnSettings:
    return get_surreal_settings()


REMOVE_ALL_TABLES_QUERY = """
LET $db = INFO FOR DB;
LET $tables =  $db.tables.keys();

FOR $table IN $tables {
    REMOVE TABLE IF EXISTS $table;
};
"""


@pytest.fixture(autouse=True)
def reset_graph_tables(
    settings: SurrealConnSettings,
):
    client = surreal_client(settings)
    with client as conn:
        conn.query(REMOVE_ALL_TABLES_QUERY)
