from __future__ import annotations

import asyncio
from abc import ABC
from functools import lru_cache
from typing import Any

from langgraph_surrealdb.assets import render_schema
from langgraph_surrealdb.database import SurrealConnection
from langgraph_surrealdb.database.client.interface import SurrealAsyncConnection
from langgraph_surrealdb.database.models.store_vector import (
    DbStoreVector,
    DbStoreVectorId,
    DbStoreVectorModelFactory,
)
from langgraph_surrealdb.store.settings import SurrealStoreIndexSettings


class BaseDbStoreVectorRepository(ABC):
    _model_factory: DbStoreVectorModelFactory
    _index_settings: SurrealStoreIndexSettings


class DbAsyncStoreVectorRepository(BaseDbStoreVectorRepository):
    def __init__(
        self,
        conn: SurrealAsyncConnection,
        model_factory: DbStoreVectorModelFactory,
        index_settings: SurrealStoreIndexSettings,
    ):
        self._tasks: dict[str, asyncio.Task[bool]] = {}
        self._conn = conn
        self._model_factory = model_factory
        self._index_settings = index_settings

    async def setup(self, *, store_table: str) -> None:
        if self._index_settings.enabled:
            setup_query = render_schema(
                "schemas/store_vector.surql",
                store_table=store_table,
                vector_table=self._model_factory.table,
                dimensions=self._index_settings.dimensions,
                distance_type=self._index_settings.distance_type,
            )
            await self._conn.query(setup_query)

    async def probe(self) -> None:
        if self._index_settings.enabled:
            try:
                installed = await self._is_index_enabled()
                if not installed:
                    raise RuntimeError("Langgraph store is not installed")
            except Exception as exc:
                raise RuntimeError(
                    "Failed to probe store table. Call setup() first"
                ) from exc

    async def upsert(self, item: DbStoreVector) -> None:
        if not self._index_settings.enabled:
            raise Warning("Indexed vector search is not enabled")
        await self._conn.upsert(item)

    async def delete(self, id: DbStoreVectorId) -> None:
        if not self._index_settings.enabled:
            raise Warning("Indexed vector search is not enabled")
        await self._conn.delete(id)

    async def _is_index_enabled(self) -> bool:
        return await self._lazy_call(
            "fn::langgraph::store::is_index_enabled", result_type=bool
        )

    async def _lazy_call(
        self,
        function: str,
        args: dict[str, Any] | None = None,
        result_type: object | None = None,
    ) -> bool:
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


class DbStoreVectorRepository(BaseDbStoreVectorRepository):
    def __init__(
        self,
        conn: SurrealConnection,
        model_factory: DbStoreVectorModelFactory,
        index_settings: SurrealStoreIndexSettings,
    ):
        self._tasks: dict[str, asyncio.Task[bool]] = {}
        self._conn = conn
        self._model_factory = model_factory
        self._index_settings = index_settings

    def setup(self, *, store_table: str) -> None:
        if self._index_settings.enabled:
            setup_query = render_schema(
                "schemas/store_vector.surql",
                store_table=store_table,
                vector_table=self._model_factory.table,
                dimensions=self._index_settings.dimensions,
                distance_type=self._index_settings.distance_type,
            )
            self._conn.query(setup_query)

    def probe(self) -> None:
        if self._index_settings.enabled:
            try:
                installed = self._is_index_enabled()
                if not installed:
                    raise RuntimeError("Langgraph store is not installed")
            except Exception as exc:
                raise RuntimeError(
                    "Failed to probe store table. Call setup() first"
                ) from exc

    def upsert(self, item: DbStoreVector) -> None:
        if not self._index_settings.enabled:
            raise Warning("Indexed vector search is not enabled")
        self._conn.upsert(item)

    def delete(self, id: DbStoreVectorId) -> None:
        if not self._index_settings.enabled:
            raise Warning("Indexed vector search is not enabled")
        self._conn.delete(id)

    @lru_cache(maxsize=1)
    def _is_index_enabled(self) -> bool:
        return self._conn.call(
            "fn::langgraph::store::is_index_enabled", {}, result_type=bool
        )
