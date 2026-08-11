from __future__ import annotations

import asyncio
from abc import ABC
from datetime import datetime
from functools import cached_property
from typing import Any

from pydantic import BaseModel

from langgraph_surrealdb.assets import render_schema
from langgraph_surrealdb.database import SurrealConnection
from langgraph_surrealdb.database.client.interface import SurrealAsyncConnection
from langgraph_surrealdb.database.models.store import (
    DbStoreItem,
    DbStoreItemId,
    DbStoreItemScored,
    DbStoreModelFactory,
)
from langgraph_surrealdb.store.settings import SurrealStoreTTLSettings

LIST_NAMESPACES_QUERY = "SELECT VALUE namespace FROM type::table($table)"
UPDATE_EXPIRY_QUERY = "UPDATE ONLY $record_id SET expires_at = $expires_at"
DELETE_EXPIRED_ITEMS_QUERY = "DELETE FROM type::table($table) WHERE expires_at IS NOT NONE AND expires_at <= time::now() RETURN BEFORE"


class SearchQueryIndex(BaseModel):
    text: str
    embedding: list[float]


class SearchQuery(BaseModel):
    namespace: list[str]
    query: SearchQueryIndex | None = None
    filters: dict[str, Any] | None = None
    limit: int = 10
    offset: int = 0

    def embedding(self) -> list[float] | None:
        if self.query is None:
            return None
        return self.query.embedding

    def lexical(self) -> str | None:
        if self.query is None:
            return None
        return self.query.text


class BaseDbStoreRepository(ABC):
    _model_factory: DbStoreModelFactory


class DbAsyncStoreRepository(BaseDbStoreRepository):
    def __init__(
        self,
        conn: SurrealAsyncConnection,
        model_factory: DbStoreModelFactory,
        ttl_settings: SurrealStoreTTLSettings | None = None,
    ):
        self._tasks: dict[str, asyncio.Task[bool]] = {}
        self._conn = conn
        self._model_factory = model_factory

    async def setup(self) -> None:
        setup_query = render_schema(
            "schemas/store.surql", store_table=self._model_factory.table
        )
        await self._conn.query(setup_query)

    async def probe(self) -> None:
        try:
            installed = await self._is_installed()
            if not installed:
                raise RuntimeError("Langgraph store is not installed")
        except Exception as exc:
            raise RuntimeError(
                "Failed to probe store table. Call setup() first"
            ) from exc

    async def get_by_id(self, id: DbStoreItemId) -> DbStoreItem | None:
        return await self._conn.select(id)

    async def upsert(self, item: DbStoreItem) -> None:
        await self._conn.upsert(item)

    async def delete(self, id: DbStoreItemId) -> None:
        await self._conn.delete(id)

    async def search(
        self, query: SearchQuery
    ) -> list[DbStoreItem] | list[DbStoreItemScored]:
        if await self._is_index_enabled():
            return await self._conn.call(
                "fn::langgraph::store::search_indexed",
                {
                    "namespace": query.namespace,
                    "limit": query.limit,
                    "offset": query.offset,
                    "filters": query.filters,
                    "query": query.lexical(),
                    "embedding": query.embedding(),
                },
                result_type=list[DbStoreItemScored],
            )
        elif query.query is not None:
            raise Warning("Indexed vector search is required to use query parameter")

        return await self._conn.call(
            "fn::langgraph::store::search",
            {
                "namespace": query.namespace,
                "limit": query.limit,
                "offset": query.offset,
                "filters": query.filters,
            },
            result_type=list[DbStoreItem],
        )

    async def list_namespaces(self) -> list[list[str]]:
        return await self._conn.query(
            LIST_NAMESPACES_QUERY,
            {
                "table": self._model_factory.table,
            },
            result_type=list[list[str]],
        )

    async def refresh_expiry(self, id: DbStoreItemId, expires_at: datetime) -> None:
        await self._conn.query(
            UPDATE_EXPIRY_QUERY,
            {
                "record_id": id,
                "expires_at": expires_at,
            },
        )

    async def sweep_ttl(self) -> list[DbStoreItem]:
        return await self._conn.query(
            DELETE_EXPIRED_ITEMS_QUERY,
            {
                "table": self._model_factory.table,
            },
            result_type=list[DbStoreItem],
        )

    async def _is_installed(self) -> bool:
        return await self._lazy_call(
            "fn::langgraph::store::is_installed", result_type=bool
        )

    async def _is_index_enabled(self) -> bool:
        return await self._lazy_call(
            "fn::langgraph::store::is_index_enabled", result_type=bool
        )

    async def _lazy_call(
        self,
        function: str,
        args: dict[str, Any] | None = None,
        result_type: object | None = None,
    ) -> Any:
        task = self._tasks.get(
            function,
            asyncio.create_task(
                self._conn.call(function, args, result_type=result_type)
            ),
        )
        self._tasks[function] = task
        try:
            return await task
        except Exception:
            del self._tasks[function]
            raise


class DbStoreRepository(BaseDbStoreRepository):
    def __init__(
        self,
        conn: SurrealConnection,
        model_factory: DbStoreModelFactory,
        ttl_settings: SurrealStoreTTLSettings | None = None,
    ):
        self._tasks: dict[str, asyncio.Task[bool]] = {}
        self._conn = conn
        self._model_factory = model_factory

    def setup(self) -> None:
        setup_query = render_schema(
            "schemas/store.surql", store_table=self._model_factory.table
        )
        self._conn.query(setup_query)

    def probe(self) -> None:
        try:
            if not self.is_installed:
                raise RuntimeError("Langgraph store is not installed")
        except Exception as exc:
            raise RuntimeError(
                "Failed to probe store table. Call setup() first"
            ) from exc

    def get_by_id(self, id: DbStoreItemId) -> DbStoreItem | None:
        return self._conn.select(id)

    def upsert(self, item: DbStoreItem) -> None:
        self._conn.upsert(item)

    def delete(self, id: DbStoreItemId) -> None:
        self._conn.delete(id)

    def search(self, query: SearchQuery) -> list[DbStoreItem] | list[DbStoreItemScored]:
        if self.is_index_enabled:
            return self._conn.call(
                "fn::langgraph::store::search_indexed",
                {
                    "namespace": query.namespace,
                    "limit": query.limit,
                    "offset": query.offset,
                    "filters": query.filters,
                    "query": query.lexical(),
                    "embedding": query.embedding(),
                },
                result_type=list[DbStoreItemScored],
            )
        elif query.query is not None:
            raise Warning("Indexed vector search is required to use query parameter")

        return self._conn.call(
            "fn::langgraph::store::search",
            {
                "namespace": query.namespace,
                "limit": query.limit,
                "offset": query.offset,
                "filters": query.filters,
            },
            result_type=list[DbStoreItem],
        )

    def list_namespaces(self) -> list[list[str]]:
        return self._conn.query(
            LIST_NAMESPACES_QUERY,
            {
                "table": self._model_factory.table,
            },
            result_type=list[list[str]],
        )

    def refresh_expiry(self, id: DbStoreItemId, expires_at: datetime) -> None:
        self._conn.query(
            UPDATE_EXPIRY_QUERY,
            {
                "record_id": id,
                "expires_at": expires_at,
            },
        )

    def sweep_ttl(self) -> list[DbStoreItem]:
        return self._conn.query(
            DELETE_EXPIRED_ITEMS_QUERY,
            {
                "table": self._model_factory.table,
            },
            result_type=list[DbStoreItem],
        )

    @cached_property
    def is_installed(self) -> bool:
        return self._conn.call(
            "fn::langgraph::store::is_installed", {}, result_type=bool
        )

    @cached_property
    def is_index_enabled(self) -> bool:
        return self._conn.call(
            "fn::langgraph::store::is_index_enabled", {}, result_type=bool
        )
