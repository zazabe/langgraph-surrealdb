from typing import Any, Literal, Protocol

from pydantic import BaseModel
from surrealdb.data.types.record_id import RecordIdType
from surrealdb.data.types.table import Table
from surrealdb.types import Value


class QueryRawItemResult[T: Any](BaseModel):
    result: T
    status: Literal["OK", "ERROR"]


class QueryRawResult[T: Any](BaseModel):
    id: str
    result: list[QueryRawItemResult[T]]

    def len(self) -> int:
        return len(self.result)

    def first_success(self) -> QueryRawItemResult[T] | None:
        if self.len() == 0:
            return None
        for item in self.result:
            if item.status == "OK":
                return item
        return None


class SurrealConnectionProtocol(Protocol):
    def query(
        self,
        query: str,
        vars: dict[str, Value] | None = None,
    ) -> Value: ...

    def query_raw(
        self,
        query: str,
        params: dict[str, Value] | None = None,
    ) -> dict[str, Any]: ...

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


class SurrealConnection:
    conn: SurrealConnectionProtocol

    def __init__(self, conn: SurrealConnectionProtocol):
        self.conn = conn

    def query(self, query: str, vars: dict[str, Value] | None = None) -> Value:
        return self.conn.query(query, vars)

    def query_raw[T: Any](
        self,
        query: str,
        params: dict[str, Value] | None = None,
    ) -> QueryRawResult[T]:
        raw = self.conn.query_raw(query, params)
        return QueryRawResult.model_validate(raw)

    def select(self, record: RecordIdType) -> Any:
        return self.conn.select(record)

    def create(self, record: RecordIdType, data: Value | None = None) -> Value:
        return self.conn.create(record, data)

    def insert(self, table: str | Table, data: Value) -> Value:
        return self.conn.insert(table, data)

    def upsert(self, record: RecordIdType, data: Value | None = None) -> Value:
        return self.conn.upsert(record, data)

    def delete(self, record: RecordIdType) -> Value:
        return self.conn.delete(record)

    def close(self) -> None:
        return self.conn.close()


class SurrealAsyncConnectionProtocol(Protocol):
    async def query(
        self,
        query: str,
        vars: dict[str, Value] | None = None,
    ) -> Value: ...

    async def query_raw(
        self,
        query: str,
        params: dict[str, Value] | None = None,
    ) -> dict[str, Any]: ...

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


class SurrealAsyncConnection:
    conn: SurrealAsyncConnectionProtocol

    def __init__(self, conn: SurrealAsyncConnectionProtocol):
        self.conn = conn

    async def query(self, query: str, vars: dict[str, Value] | None = None) -> Value:
        return await self.conn.query(query, vars)

    async def query_raw[T: Any](
        self, query: str, params: dict[str, Value] | None = None
    ) -> QueryRawResult[T]:
        raw = await self.conn.query_raw(query, params)
        return QueryRawResult.model_validate(raw)

    async def select(self, record: RecordIdType) -> Any:
        return await self.conn.select(record)

    async def create(self, record: RecordIdType, data: Value | None = None) -> Value:
        return await self.conn.create(record, data)

    async def insert(self, table: str | Table, data: Value) -> Value:
        return await self.conn.insert(table, data)

    async def upsert(self, record: RecordIdType, data: Value | None = None) -> Value:
        return await self.conn.upsert(record, data)

    async def delete(self, record: RecordIdType) -> Value:
        return await self.conn.delete(record)

    async def close(self) -> None:
        return await self.conn.close()
