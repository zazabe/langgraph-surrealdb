import re
from abc import ABC
from typing import Any

from langgraph_surrealdb.database.client.interface import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.database.models.checkpoint import (
    DbCheckpoint,
    DbCheckpointId,
    DbCheckpointsModelFactory,
)

SETUP_QUERY = """
DEFINE TABLE IF NOT EXISTS {table} SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD IF NOT EXISTS thread_id ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_ns ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_id ON {table} TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint ON {table} TYPE bytes;
DEFINE INDEX IF NOT EXISTS checkpoints_lookup ON {table} FIELDS thread_id, checkpoint_ns, checkpoint_id UNIQUE;
"""

PROBE_QUERY = """
SELECT count() FROM {table} GROUP ALL;
"""

SELECT_QUERY = """
SELECT
    id,
    checkpoint,
    checkpoint_id,
    checkpoint_ns,
    thread_id,
    metadata,
    parent_checkpoint_id,
    type
FROM {table}
WHERE {where}
ORDER BY checkpoint_id DESC
{limit}
"""


class BaseDbCheckpointsRepository(ABC):
    _model_factory: DbCheckpointsModelFactory

    def _sql(self, query: str, params: dict[str, Any] | None = None) -> str:
        return self._model_factory.sql(query, params)


class DbCheckpointsRepository(BaseDbCheckpointsRepository):
    def __init__(
        self, conn: SurrealConnection, model_factory: DbCheckpointsModelFactory
    ):
        self._conn = conn
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

    def upsert(self, checkpoint: DbCheckpoint) -> None:
        self._conn.upsert(checkpoint)

    def get_by_id(self, id: DbCheckpointId) -> DbCheckpoint | None:
        return self._conn.select(id)

    def get_latest(self, thread_id: str, checkpoint_ns: str) -> DbCheckpoint | None:
        result = self._conn.query(
            self._sql(
                SELECT_QUERY,
                {
                    "where": "thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns",
                    "limit": "LIMIT 1",
                },
            ),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            },
            result_type=list[DbCheckpoint],
        )
        return result[0] if len(result) > 0 else None

    def list(
        self,
        thread_id: str | None,
        checkpoint_ns: str | None,
        checkpoint_id: str | None,
        filter: dict[str, Any] | None,
        before_checkpoint_id: str | None = None,
        limit: int | None = None,
    ) -> list[DbCheckpoint]:
        where, params = _search_where(
            thread_id, checkpoint_ns, checkpoint_id, filter, before_checkpoint_id
        )
        query = self._sql(
            SELECT_QUERY,
            {
                "where": where,
                "limit": f"LIMIT {limit}" if limit is not None else "",
            },
        )
        return self._conn.query(query, params, result_type=list[DbCheckpoint])

    def delete_thread(self, thread_id: str) -> None:
        self._conn.query(
            self._sql("DELETE FROM {table} WHERE thread_id = $thread_id"),
            {"thread_id": thread_id},
        )


class DbAsyncCheckpointsRepository(BaseDbCheckpointsRepository):
    def __init__(
        self, conn: SurrealAsyncConnection, model_factory: DbCheckpointsModelFactory
    ):
        self._conn = conn
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

    async def upsert(self, checkpoint: DbCheckpoint) -> None:
        await self._conn.upsert(checkpoint)

    async def get_by_id(self, id: DbCheckpointId) -> DbCheckpoint | None:
        return await self._conn.select(id)

    async def get_latest(
        self, thread_id: str, checkpoint_ns: str
    ) -> DbCheckpoint | None:
        result = await self._conn.query(
            self._sql(
                SELECT_QUERY,
                {
                    "where": "thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns",
                    "limit": "LIMIT 1",
                },
            ),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            },
            result_type=list[DbCheckpoint],
        )
        return result[0] if len(result) > 0 else None

    async def list(
        self,
        thread_id: str | None,
        checkpoint_ns: str | None,
        checkpoint_id: str | None,
        filter: dict[str, Any] | None,
        before_checkpoint_id: str | None = None,
        limit: int | None = None,
    ) -> list[DbCheckpoint]:
        where, params = _search_where(
            thread_id, checkpoint_ns, checkpoint_id, filter, before_checkpoint_id
        )
        query = self._sql(
            SELECT_QUERY,
            {
                "where": where,
                "limit": f"LIMIT {limit}" if limit is not None else "",
            },
        )
        return await self._conn.query(query, params, result_type=list[DbCheckpoint])

    async def delete_thread(self, thread_id: str) -> None:
        await self._conn.query(
            self._sql("DELETE FROM {table} WHERE thread_id = $thread_id"),
            {
                "thread_id": thread_id,
            },
        )


_FILTER_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]+$")


def _validate_filter_key(key: str) -> None:
    if not _FILTER_PATTERN.match(key):
        raise ValueError(
            f"Invalid filter key: '{key}'. Filter keys must contain only alphanumeric characters, underscores, dots, and hyphens."
        )


def _where_value(query_value: Any) -> tuple[str, Any]:
    if query_value is None:
        return ("IS NONE", None)
    if isinstance(query_value, bool):
        return ("= $", query_value)
    if isinstance(query_value, (str, int, float)):
        return ("= $", query_value)
    if isinstance(query_value, (dict, list)):
        return ("= $", query_value)
    return ("= $", query_value)


class Clause:
    def __init__(self) -> None:
        self.list: list[str] = []

    def add(self, field: str, op: str, value: str) -> None:
        terms = ".".join([f"`{term}`" for term in field.split(".")])
        self.list.append(f"{terms} {op} {value}")

    def __str__(self) -> str:
        if not self.list:
            return "TRUE"
        return " AND ".join(self.list)


def _search_where(
    thread_id: str | None,
    checkpoint_ns: str | None,
    checkpoint_id: str | None,
    filter: dict[str, Any] | None,
    before_checkpoint_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    clauses: Clause = Clause()
    params: dict[str, Any] = {}

    if thread_id is not None:
        clauses.add("thread_id", "=", "$thread_id")
        params["thread_id"] = thread_id
    if checkpoint_ns is not None:
        clauses.add("checkpoint_ns", "=", "$checkpoint_ns")
        params["checkpoint_ns"] = checkpoint_ns
    if checkpoint_id is not None:
        clauses.add("checkpoint_id", "=", "$checkpoint_id")
        params["checkpoint_id"] = checkpoint_id
    if before_checkpoint_id is not None:
        clauses.add("checkpoint_id", "<", "$before_checkpoint_id")
        params["before_checkpoint_id"] = before_checkpoint_id

    if filter is not None:
        for idx, (key, value) in enumerate(filter.items()):
            _validate_filter_key(key)
            pname = f"m{idx}"
            op, parsed = _where_value(value)
            if op == "IS NONE":
                clauses.add(f"metadata.{key}", "=", "NONE")
            else:
                clauses.add(f"metadata.{key}", "=", f"${pname}")
                params[pname] = parsed

    return str(clauses), params
