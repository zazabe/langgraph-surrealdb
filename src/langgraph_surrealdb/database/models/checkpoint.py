from typing import Any, Self

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from pydantic import BaseModel, ConfigDict, Field, GetCoreSchemaHandler, ValidationInfo
from pydantic_core import CoreSchema, core_schema

from langgraph_surrealdb.checkpoint.config import FullCheckpointConfig
from langgraph_surrealdb.database.models import DbRecordId


class DbCheckpointId(DbRecordId):
    @classmethod
    def from_ids(
        cls, *, table: str, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> Self:
        return cls.from_raw(
            table,
            thread_id,
            checkpoint_ns,
            checkpoint_id,
        )

    @classmethod
    def _coerce_with_info(cls, value: object, info: ValidationInfo) -> str:
        s = DbRecordId._coerce_prefixed_input(value)
        expected = (info.context or {}).get("expected_table")
        if expected is not None:
            table, _ = s.split(":", 1)
            if table != expected:
                raise ValueError(
                    f"{cls.__name__} must use table '{expected}', got '{table}'"
                )
        return s

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: type, _handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.with_info_before_validator_function(
            cls._coerce_with_info,
            core_schema.no_info_after_validator_function(cls, core_schema.str_schema()),
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
        *,
        table: str,
        serde: SerializerProtocol,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> Self:
        checkpoint_config = FullCheckpointConfig.from_config(config)
        thread_id = checkpoint_config.thread_id
        checkpoint_ns = checkpoint_config.checkpoint_ns
        parent_checkpoint_id = checkpoint_config.checkpoint_id or ""
        checkpoint_id = checkpoint["id"]
        type_, serialized_checkpoint = serde.dumps_typed(checkpoint)
        id = DbCheckpointId.from_ids(
            table=table,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
        )
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


class DbCheckpointsModelFactory:
    def __init__(self, table: str = "checkpoints"):
        self._table = table

    def create_record(
        self,
        *,
        serde: SerializerProtocol,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> DbCheckpoint:
        return DbCheckpoint.create(
            table=self._table,
            serde=serde,
            config=config,
            checkpoint=checkpoint,
            metadata=metadata,
        )

    def create_id(
        self, *, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> DbCheckpointId:
        return DbCheckpointId.from_ids(
            table=self._table,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
        )

    def parse(self, raw: Any) -> DbCheckpoint:
        return DbCheckpoint.model_validate(raw, context={"expected_table": self._table})

    def sql(self, query: str, params: dict[str, Any] | None = None) -> str:
        return query.format(table=self._table, **(params or {}))
