from typing import Any

from langgraph_surrealdb.database import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.database.common import select_result
from langgraph_surrealdb.database.interface import QueryRawResult
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


class DbWritesRepository:
    def __init__(self, conn: SurrealConnection, model_factory: DbWritesModelFactory):
        self._conn = conn
        self._model_factory = model_factory

    def setup(self) -> None:
        self._conn.query(self._sql(SETUP_QUERY))

    def probe(self) -> None:
        raw: QueryRawResult[dict[str, int]] = self._conn.query_raw(
            self._sql(PROBE_QUERY)
        )
        first = raw.first()
        if not first or first.status == "ERR":
            error = first.result if first else "Unknown error"
            raise RuntimeError(
                f"Failed to probe writes table. Call setup() first, error: {error}"
            )

    def create(self, write: DbWrite) -> None:
        self._conn.create(write.id, write.model_dump())

    def get_by_id(self, id: DbWriteId) -> DbWrite | None:
        raw = self._conn.select(id.record_id)
        results = select_result(raw)
        return self._model_factory.parse(results[0]) if results else None

    def upsert(self, write: DbWrite) -> None:
        id = write.id.record_id
        data = write.model_dump()
        self._conn.upsert(id, data)

    def fetch(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[DbWrite]:
        raw = self._conn.query(
            self._sql(SELECT_QUERY),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
        )
        result = select_result(raw)
        return [self._model_factory.parse(row) for row in result or []]

    def delete_thread(self, thread_id: str) -> None:
        self._conn.query(
            self._sql("DELETE FROM {table} WHERE thread_id = $thread_id"),
            {"thread_id": thread_id},
        )

    def _sql(self, query: str, params: dict[str, Any] | None = None) -> str:
        return self._model_factory.sql(query, params)


class DbAsyncWritesRepository:
    def __init__(
        self, conn: SurrealAsyncConnection, model_factory: DbWritesModelFactory
    ):
        self._conn = conn
        self._model_factory = model_factory

    async def setup(self) -> None:
        await self._conn.query(self._sql(SETUP_QUERY))

    async def probe(self) -> None:
        raw: QueryRawResult[dict[str, int]] = await self._conn.query_raw(
            self._sql(PROBE_QUERY)
        )
        first = raw.first()
        if not first or first.status == "ERR":
            error = first.result if first else "Unknown error"
            raise RuntimeError(
                f"Failed to probe writes table. Call setup() first, error: {error}"
            )

    async def create(self, write: DbWrite) -> None:
        await self._conn.create(write.id, write.model_dump())

    async def get_by_id(self, id: DbWriteId) -> DbWrite | None:
        raw = await self._conn.select(id.record_id)
        results = select_result(raw)
        return self._model_factory.parse(results[0]) if results else None

    async def upsert(self, write: DbWrite) -> None:
        await self._conn.upsert(write.id, write.model_dump())

    async def fetch(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[DbWrite]:
        raw = await self._conn.query(
            self._sql(SELECT_QUERY),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
        )
        result = select_result(raw)
        return [self._model_factory.parse(row) for row in result or []]

    async def delete_thread(self, thread_id: str) -> None:
        await self._conn.query(
            self._sql("DELETE FROM {table} WHERE thread_id = $thread_id"),
            {"thread_id": thread_id},
        )

    def _sql(self, query: str, params: dict[str, Any] | None = None) -> str:
        return self._model_factory.sql(query, params)
