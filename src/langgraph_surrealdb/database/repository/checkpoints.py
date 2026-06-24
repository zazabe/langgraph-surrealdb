import re
from typing import Any

from langgraph_surrealdb.database import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_surrealdb.database.common import select_result
from langgraph_surrealdb.database.models.checkpoint import (
    DbCheckpoint,
    DbCheckpointId,
)

SETUP_QUERY = """
DEFINE TABLE IF NOT EXISTS checkpoints SCHEMALESS;
DEFINE FIELD IF NOT EXISTS thread_id ON checkpoints TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_ns ON checkpoints TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_id ON checkpoints TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint ON checkpoints TYPE bytes;
DEFINE INDEX IF NOT EXISTS checkpoints_lookup ON checkpoints FIELDS thread_id, checkpoint_ns, checkpoint_id UNIQUE;
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
FROM checkpoints
WHERE {where}
ORDER BY checkpoint_id DESC
{limit}
"""


class DbCheckpointsRepository:
    def __init__(self, conn: SurrealConnection):
        self._conn = conn

    def setup(self) -> None:
        self._conn.query(SETUP_QUERY)

    def upsert(self, checkpoint: DbCheckpoint) -> None:
        self._conn.upsert(checkpoint.id, checkpoint.model_dump())

    def get_by_id(self, id: DbCheckpointId) -> DbCheckpoint | None:
        raw = self._conn.select(id.to_record_id())
        results = select_result(raw)
        return DbCheckpoint.model_validate(results[0]) if results else None

    def get_latest(self, thread_id: str, checkpoint_ns: str) -> DbCheckpoint | None:
        raw = self._conn.query(
            SELECT_QUERY.format(
                where="thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns",
                limit="LIMIT 1",
            ),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            },
        )
        result = select_result(raw)
        return DbCheckpoint.model_validate(result[0]) if result else None

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
        query = SELECT_QUERY.format(
            where=where, limit=f"LIMIT {limit}" if limit is not None else ""
        )
        raw = self._conn.query(query, params)
        return [DbCheckpoint.model_validate(row) for row in select_result(raw) or []]

    def delete_thread(self, thread_id: str) -> None:
        self._conn.query(
            "DELETE FROM checkpoints WHERE thread_id = $thread_id",
            {"thread_id": thread_id},
        )


class DbAsyncCheckpointsRepository:
    def __init__(self, conn: SurrealAsyncConnection):
        self._conn = conn

    async def setup(self) -> None:
        await self._conn.query(SETUP_QUERY)

    async def upsert(self, checkpoint: DbCheckpoint) -> None:
        id = checkpoint.id.to_record_id()
        data = checkpoint.model_dump()
        await self._conn.upsert(id, data)

    async def get_by_id(self, id: DbCheckpointId) -> DbCheckpoint | None:
        raw = await self._conn.select(id.to_record_id())
        results = select_result(raw)
        return DbCheckpoint.model_validate(results[0]) if results else None

    async def get_latest(
        self, thread_id: str, checkpoint_ns: str
    ) -> DbCheckpoint | None:
        raw = await self._conn.query(
            SELECT_QUERY.format(
                where="thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns",
                limit="LIMIT 1",
            ),
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            },
        )
        result = select_result(raw)
        return DbCheckpoint.model_validate(result[0]) if result else None

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
        query = SELECT_QUERY.format(
            where=where, limit=f"LIMIT {limit}" if limit is not None else ""
        )
        raw = await self._conn.query(query, params)
        return [DbCheckpoint.model_validate(row) for row in select_result(raw) or []]

    async def delete_thread(self, thread_id: str) -> None:
        await self._conn.query(
            "DELETE FROM checkpoints WHERE thread_id = $thread_id",
            {"thread_id": thread_id},
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
