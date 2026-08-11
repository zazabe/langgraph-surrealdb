from __future__ import annotations

from datetime import UTC, datetime

from langgraph.store.base import get_text_at_path
from pydantic import Field, GetCoreSchemaHandler, ValidationInfo
from pydantic_core import CoreSchema, core_schema

from langgraph_surrealdb.database.client.common import validate_table_name
from langgraph_surrealdb.database.models.common import DbRecordId, SurrealModel
from langgraph_surrealdb.database.models.store import DbStoreItem, DbStoreItemId


class DbStoreVectorId(DbRecordId):
    @property
    def record_type(self) -> type[DbStoreVector]:
        return DbStoreVector

    @classmethod
    def from_ids(
        cls, *, table: str, namespace: list[str], key: str, field_name: str
    ) -> DbStoreVectorId:
        return cls.from_raw(table, namespace, key, field_name)

    @classmethod
    def _coerce_with_info(cls, value: object, info: ValidationInfo) -> str:
        value = DbRecordId._coerce_prefixed_input(value)
        expected = (info.context or {}).get("expected_table")
        if expected is not None:
            table, _ = value.split(":", 1)
            if table != expected:
                raise ValueError(
                    f"{cls.__name__} must use table '{expected}', got '{table}'"
                )
        return value

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: type, _handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.with_info_before_validator_function(
            cls._coerce_with_info,
            core_schema.no_info_after_validator_function(cls, core_schema.str_schema()),
        )


class DbStoreVector(SurrealModel):
    id: DbStoreVectorId = Field(exclude=True)
    item: DbStoreItemId
    namespace: list[str]
    key: str
    field_name: str
    indexed_text: str
    embedding: list[float]
    created_at: datetime
    updated_at: datetime


class DbStoreVectorRequest:
    def __init__(self, field: str, text: str):
        self.field = field
        self.text = text


class DbStoreItemWithVectorRequests:
    item: DbStoreItem
    vector_requests: list[DbStoreVectorRequest]

    def __init__(self, item: DbStoreItem, field_list: list[str]):
        vector_requests: list[DbStoreVectorRequest] = []
        for path in field_list:
            extracted = get_text_at_path(item.value, path)
            for index, text in enumerate(extracted):
                field = f"{path}.{index}" if len(extracted) > 1 else path
                vector_requests.append(DbStoreVectorRequest(field, text))
        self.item = item
        self.vector_requests = vector_requests

    def with_vectors(self, vectors: list[DbStoreVector]) -> DbStoreItemWithVectors:
        return DbStoreItemWithVectors(self.item, vectors)


class DbStoreItemWithVectors:
    item: DbStoreItem
    vectors: list[DbStoreVector]

    def __init__(self, item: DbStoreItem, vectors: list[DbStoreVector]):
        self.item = item
        self.vectors = vectors


class DbStoreVectorModelFactory:
    def __init__(self, table: str):
        validate_table_name(table)
        self._table = table

    @property
    def table(self) -> str:
        return self._table

    def create_id(
        self, *, namespace: list[str], key: str, field_name: str
    ) -> DbStoreVectorId:
        return DbStoreVectorId.from_ids(
            table=self.table, namespace=namespace, key=key, field_name=field_name
        )

    def create_record(
        self, item: DbStoreItem, field: str, indexed_text: str, embedding: list[float]
    ) -> DbStoreVector:
        return DbStoreVector(
            id=self.create_id(namespace=item.namespace, key=item.key, field_name=field),
            item=item.id,
            namespace=item.namespace,
            key=item.key,
            field_name=field,
            indexed_text=indexed_text,
            embedding=embedding,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
