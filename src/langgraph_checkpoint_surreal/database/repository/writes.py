from langgraph_checkpoint_surreal.database import (
    SurrealAsyncConnection,
    SurrealConnection,
)
from langgraph_checkpoint_surreal.database.common import select_result
from langgraph_checkpoint_surreal.database.models.write import DbWrite, DbWriteId

SETUP_QUERY = """
DEFINE TABLE IF NOT EXISTS writes SCHEMALESS;
DEFINE FIELD IF NOT EXISTS thread_id ON writes TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_ns ON writes TYPE string;
DEFINE FIELD IF NOT EXISTS checkpoint_id ON writes TYPE string;
DEFINE FIELD IF NOT EXISTS task_id ON writes TYPE string;
DEFINE FIELD IF NOT EXISTS idx ON writes TYPE int;
DEFINE FIELD IF NOT EXISTS channel ON writes TYPE string;
DEFINE FIELD IF NOT EXISTS value ON writes TYPE bytes;
DEFINE INDEX IF NOT EXISTS writes_lookup ON writes FIELDS thread_id, checkpoint_ns, checkpoint_id, task_id, idx UNIQUE;
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
FROM writes
WHERE thread_id = $thread_id
    AND checkpoint_ns = $checkpoint_ns
    AND checkpoint_id = $checkpoint_id
ORDER BY task_id ASC, idx ASC
"""


class DbWritesRepository:
    def __init__(self, conn: SurrealConnection):
        self._conn = conn

    def setup(self) -> None:
        self._conn.query(SETUP_QUERY)

    def create(self, write: DbWrite) -> None:
        self._conn.create(write.id, write.model_dump())

    def get_by_id(self, id: DbWriteId) -> DbWrite | None:
        raw = self._conn.select(id.to_record_id())
        results = select_result(raw)
        return DbWrite.model_validate(results[0]) if results else None

    def upsert(self, write: DbWrite) -> None:
        id = write.id.to_record_id()
        data = write.model_dump()
        self._conn.upsert(id, data)

    def fetch(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[DbWrite]:
        raw = self._conn.query(
            SELECT_QUERY,
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
        )
        result = select_result(raw)
        return [DbWrite.model_validate(row) for row in result or []]

    def delete_thread(self, thread_id: str) -> None:
        self._conn.query(
            "DELETE FROM writes WHERE thread_id = $thread_id",
            {"thread_id": thread_id},
        )


class DbAsyncWritesRepository:
    def __init__(self, conn: SurrealAsyncConnection):
        self._conn = conn

    async def setup(self) -> None:
        await self._conn.query(SETUP_QUERY)

    async def create(self, write: DbWrite) -> None:
        await self._conn.create(write.id, write.model_dump())

    async def get_by_id(self, id: DbWriteId) -> DbWrite | None:
        raw = await self._conn.select(id.to_record_id())
        results = select_result(raw)
        return DbWrite.model_validate(results[0]) if results else None

    async def upsert(self, write: DbWrite) -> None:
        await self._conn.upsert(write.id, write.model_dump())

    async def fetch(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[DbWrite]:
        raw = await self._conn.query(
            SELECT_QUERY,
            {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            },
        )
        result = select_result(raw)
        return [DbWrite.model_validate(row) for row in result or []]

    async def delete_thread(self, thread_id: str) -> None:
        await self._conn.query(
            "DELETE FROM writes WHERE thread_id = $thread_id",
            {"thread_id": thread_id},
        )
