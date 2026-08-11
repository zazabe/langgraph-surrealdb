from typing import Any, Self

from langgraph.checkpoint.base import PendingWrite
from langgraph.checkpoint.serde.base import SerializerProtocol
from pydantic import Field, GetCoreSchemaHandler, ValidationInfo
from pydantic_core import CoreSchema, core_schema

from langgraph_surrealdb.database.client.common import validate_table_name
from langgraph_surrealdb.database.models.common import DbRecordId, SurrealModel


class DbWriteId(DbRecordId):
    @property
    def record_type(self) -> type["DbWrite"]:
        return DbWrite

    @classmethod
    def from_ids(
        cls,
        *,
        table: str,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
    ) -> Self:
        return cls.from_raw(
            table, thread_id, checkpoint_ns, checkpoint_id, task_id, str(idx)
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


class DbWrite(SurrealModel):
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
        *,
        table: str,
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
        id = DbWriteId.from_ids(
            table=table,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            idx=idx,
        )
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


class DbWritesModelFactory:
    def __init__(self, table: str = "writes"):
        validate_table_name(table)
        self._table = table

    @property
    def table(self) -> str:
        return self._table

    def create_record(
        self,
        *,
        serde: SerializerProtocol,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
        channel: str,
        value: Any,
    ) -> DbWrite:
        return DbWrite.create(
            table=self._table,
            serde=serde,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            idx=idx,
            channel=channel,
            value=value,
        )

    def create_id(
        self,
        *,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
    ) -> DbWriteId:
        return DbWriteId.from_ids(
            table=self._table,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            idx=idx,
        )
