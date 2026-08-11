from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Self

from langgraph.store.base import Item, SearchItem
from pydantic import BaseModel, Field, GetCoreSchemaHandler, ValidationInfo
from pydantic_core import CoreSchema, core_schema

from langgraph_surrealdb.database.client.common import validate_table_name
from langgraph_surrealdb.database.models.common import DbRecordId, SurrealModel


class DbStoreItemId(DbRecordId):
    @property
    def record_type(self) -> type[DbStoreItem]:
        return DbStoreItem

    @classmethod
    def from_ids(cls, *, table: str, namespace: tuple[str, ...], key: str) -> Self:
        return cls.from_raw(table, namespace, key)

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


class DbStoreItem(SurrealModel):
    id: DbStoreItemId = Field(exclude=True)
    namespace: list[str]
    key: str
    value: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    expires_at: datetime | None = None
    ttl_minutes: float | None = None

    def to_item(self) -> Item:
        return Item(
            namespace=tuple(self.namespace),
            key=self.key,
            value=self.value,
            created_at=self.created_at,
            updated_at=self.updated_at,
        )

    def update(self, *, value: dict[str, Any], ttl: float | None):
        now = datetime.now(UTC)
        expires_at = None
        if ttl is not None:
            expires_at = now + timedelta(minutes=ttl)

        self.value = value
        self.updated_at = now
        self.expires_at = expires_at
        self.ttl_minutes = ttl


class DbStoreItemScored(BaseModel):
    item: DbStoreItem
    score: float

    def to_item(self) -> SearchItem:
        return SearchItem(
            namespace=tuple(self.item.namespace),
            key=self.item.key,
            value=self.item.value,
            created_at=self.item.created_at,
            updated_at=self.item.updated_at,
            score=self.score,
        )


class DbStoreModelFactory:
    def __init__(self, table: str = "store"):
        validate_table_name(table)
        self._table = table

    @property
    def table(self) -> str:
        return self._table

    def create_id(self, *, namespace: tuple[str, ...], key: str) -> DbStoreItemId:
        return DbStoreItemId.from_ids(table=self._table, namespace=namespace, key=key)

    def create_record(
        self,
        *,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        ttl: float | None,
        created_at: datetime | None = None,
    ) -> DbStoreItem:
        now = datetime.now(UTC)
        return DbStoreItem(
            id=self.create_id(namespace=namespace, key=key),
            namespace=list(namespace),
            key=key,
            value=value,
            created_at=created_at or now,
            updated_at=now,
            expires_at=now + timedelta(minutes=ttl) if ttl is not None else None,
            ttl_minutes=ttl,
        )
