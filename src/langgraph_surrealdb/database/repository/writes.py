from abc import ABC
from typing import Any

from langgraph_surrealdb.database.client.interface import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.database.models.write import (
    DbWrite,
    DbWriteId,
    DbWritesModelFactory,
)

SETUP_QUERY = """
DEFINE TABLE IF NOT EXISTS {table} SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD IF NOT EXISTS thread_id ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_ns ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_id ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS task_id ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS idx ON {table} TYPE int;
DEFINE FIELD IF NOT EXISTS channel ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS value ON {table} TYPE bytes;
DEFINE INDEX IF NOT EXISTS writes_lookup ON {table} FIELDS thread_id, checkpoint_ns, checkpoint_id, task_id, idx UNIQUE;
"""

PROBE_QUERY = """
SELECT count() FROM {table} GROUP ALL;
"""

SELECT_QUERY = """
SELECT
    id,
    thread_id,
    checkpoint_ns,
    checkpoint_id,
    task_id,
    idx,
    channel,
    type,
    value
FROM {table}
WHERE thread_id = $thread_id
    AND checkpoint_ns = $checkpoint_ns
    AND checkpoint_id = $checkpoint_id
ORDER BY task_id ASC, idx ASC
"""


class BaseDbWritesRepository(ABC):
    _model_factory: DbWritesModelFactory

    def _sql(self, query: str, params: dict[str, Any] | None = None) -> str:
        return self._model_factory.sql(query, params)


class DbWritesRepository(BaseDbWritesRepository):
    def __init__(self, conn: SurrealConnection, model_factory: DbWritesModelFactory):
        self._conn = conn.with_validation_context(
            {
                "expected_table": model_factory.table,
            }
        )
        self._model_factory = model_factory

    def setup(self) -> None:
        self._conn.query(self._sql(SETUP_QUERY))

    def probe(self) -> None:
        try:
            self._conn.query_raw(self._sql(PROBE_QUERY)).check()
        except Exception as e:
            raise RuntimeError(
                "Failed to probe checkpoints table. Call setup() first"
            ) from e

    def get_by_id(self, id: DbWriteId) -> DbWrite | None:
        return self._conn.select(id)

    def upsert(self, write: DbWrite) -> None:
        self._conn.upsert(write)

    def fetch(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[DbWrite]:
        return self._conn.query(
            self._sql(SELECT_QUERY),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
            result_type=list[DbWrite],
        )

    def delete_thread(self, thread_id: str) -> None:
        self._conn.query(
            self._sql("DELETE FROM {table} WHERE thread_id = $thread_id"),
            {"thread_id": thread_id},
        )


class DbAsyncWritesRepository(BaseDbWritesRepository):
    def __init__(
        self, conn: SurrealAsyncConnection, model_factory: DbWritesModelFactory
    ):
        self._conn = conn.with_validation_context(
            {
                "expected_table": model_factory.table,
            }
        )
        self._model_factory = model_factory

    async def setup(self) -> None:
        await self._conn.query(self._sql(SETUP_QUERY))

    async def probe(self) -> None:
        try:
            (await self._conn.query_raw(self._sql(PROBE_QUERY))).check()
        except Exception as e:
            raise RuntimeError(
                "Failed to probe checkpoints table. Call setup() first"
            ) from e

    async def get_by_id(self, id: DbWriteId) -> DbWrite | None:
        return await self._conn.select(id)

    async def upsert(self, write: DbWrite) -> None:
        await self._conn.upsert(write)

    async def fetch(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[DbWrite]:
        return await self._conn.query(
            self._sql(SELECT_QUERY),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
            result_type=list[DbWrite],
        )

    async def delete_thread(self, thread_id: str) -> None:
        await self._conn.query(
            self._sql("DELETE FROM {table} WHERE thread_id = $thread_id"),
            {"thread_id": thread_id},
        )
