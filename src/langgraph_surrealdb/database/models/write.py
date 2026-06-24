from typing import Any

from langgraph.checkpoint.base import PendingWrite
from langgraph.checkpoint.serde.base import SerializerProtocol
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self

from langgraph_surrealdb.database.models import DbRecordId


class DbWriteId(DbRecordId):
    prefix = "writes"

    @classmethod
    def from_ids(
        cls,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
    ) -> Self:
        return cls.from_raw(thread_id, checkpoint_ns, checkpoint_id, task_id, str(idx))


class DbWrite(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    id: DbWriteId = Field(exclude=True)
    thread_id: str
    checkpoint_ns: str
    checkpoint_id: str
    task_id: str
    idx: int
    channel: str
    type: str
    value: bytes

    @classmethod
    def create(
        cls,
        serde: SerializerProtocol,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
        channel: str,
        value: Any,
    ) -> Self:
        type_, encoded = serde.dumps_typed(value)
        id = DbWriteId.from_ids(thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
        return cls(
            id=id,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            idx=idx,
            channel=channel,
            type=type_,
            value=encoded,
        )

    def to_pending_write(self, serde: SerializerProtocol) -> PendingWrite:
        return PendingWrite(
            (
                self.task_id,
                self.channel,
                serde.loads_typed((self.type, self.value)),
            )
        )
