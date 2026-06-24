from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self

from langgraph_checkpoint_surreal.database.models import DbRecordId


class DbCheckpointId(DbRecordId):
    prefix = "checkpoints"

    @classmethod
    def from_ids(cls, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> Self:
        return cls.from_raw(
            thread_id,
            checkpoint_ns,
            checkpoint_id,
        )


class DbCheckpoint(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    id: DbCheckpointId = Field(exclude=True)
    thread_id: str
    checkpoint: bytes
    checkpoint_id: str
    checkpoint_ns: str
    metadata: CheckpointMetadata
    parent_checkpoint_id: str = Field(default="")
    type: str

    @classmethod
    def create(
        cls,
        serde: SerializerProtocol,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> Self:
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id", "")
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        parent_checkpoint_id = configurable.get("checkpoint_id", "")
        checkpoint_id = checkpoint["id"]
        type_, serialized_checkpoint = serde.dumps_typed(checkpoint)
        id = DbCheckpointId.from_ids(thread_id, checkpoint_ns, checkpoint_id)
        metadata = get_checkpoint_metadata(config, metadata)
        return cls(
            id=id,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            type=type_,
            checkpoint=serialized_checkpoint,
            metadata=metadata,
            parent_checkpoint_id=parent_checkpoint_id,
        )

    def to_config(self) -> RunnableConfig:
        return RunnableConfig(
            configurable={
                "thread_id": self.thread_id,
                "checkpoint_ns": self.checkpoint_ns,
                "checkpoint_id": self.checkpoint_id,
            }
        )

    def to_parent_config(self) -> RunnableConfig | None:
        if self.parent_checkpoint_id:
            return RunnableConfig(
                configurable={
                    "thread_id": self.thread_id,
                    "checkpoint_ns": self.checkpoint_ns,
                    "checkpoint_id": self.parent_checkpoint_id,
                }
            )
        return None

    def to_checkpoint(self, serde: SerializerProtocol) -> Checkpoint:
        return serde.loads_typed((self.type, self.checkpoint))
