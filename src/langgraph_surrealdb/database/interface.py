from typing import Any, Protocol

from surrealdb.data.types.record_id import RecordIdType
from surrealdb.data.types.table import Table
from surrealdb.types import Value


class SurrealConnection(Protocol):
    def query(
        self,
        query: str,
        vars: dict[str, Value] | None = None,
    ) -> Value: ...

    def select(
        self,
        record: RecordIdType,
    ) -> Any: ...

    def create(
        self,
        record: RecordIdType,
        data: Value | None = None,
    ) -> Value: ...

    def insert(
        self,
        table: str | Table,
        data: Value,
    ) -> Value: ...

    def upsert(
        self,
        record: RecordIdType,
        data: Value | None = None,
    ) -> Value: ...

    def delete(self, record: RecordIdType) -> Value: ...

    def close(self) -> None: ...


class SurrealAsyncConnection(Protocol):
    async def query(
        self,
        query: str,
        vars: dict[str, Value] | None = None,
    ) -> Value: ...

    async def select(
        self,
        record: RecordIdType,
    ) -> Any: ...

    async def create(
        self,
        record: RecordIdType,
        data: Value | None = None,
    ) -> Value: ...

    async def insert(
        self,
        table: str | Table,
        data: Value,
    ) -> Value: ...

    async def upsert(
        self,
        record: RecordIdType,
        data: Value | None = None,
    ) -> Value: ...

    async def delete(self, record: RecordIdType) -> Value: ...

    async def close(self) -> None: ...
