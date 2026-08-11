from __future__ import annotations

import math
from collections import Counter
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest
from langgraph.store.base import Embeddings

from langgraph_surrealdb import (
    AsyncSurrealStore,
    SurrealStore,
    SurrealStoreIndexSettings,
    SurrealStoreTTLSettings,
)
from langgraph_surrealdb.settings import SurrealDatabaseSettings
from langgraph_surrealdb.store.settings import SurrealStoreSettings
from tests.fixtures.database import clear_tables, drop_database, init_database


class CharacterEmbeddings(Embeddings):
    def __init__(self, dims: int = 32):
        self.dims = dims

    def _embed(self, text: str) -> list[float]:
        counts = Counter(text.casefold())
        vector = [0.0] * self.dims
        for character, count in counts.items():
            vector[ord(character) % self.dims] += count
        norm = math.sqrt(sum(component * component for component in vector))
        return [component / norm for component in vector] if norm else vector

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)


@pytest.fixture(scope="module")
async def root_store_settings(
    root_db_settings: SurrealDatabaseSettings,
) -> SurrealStoreSettings:
    store_settings = SurrealStoreSettings.from_env()
    return SurrealStoreSettings(
        db=root_db_settings,
        store_table=store_settings.store_table,
        index=store_settings.index,
        ttl=store_settings.ttl,
    )


@pytest.fixture(scope="module")
async def store_settings(
    root_db_settings: SurrealDatabaseSettings,
    db_module_name: str,
) -> AsyncGenerator[SurrealStoreSettings, None]:
    store_settings = SurrealStoreSettings.from_env()
    store_settings.db.database = db_module_name
    await init_database(root_db_settings)
    yield store_settings
    await drop_database(root_db_settings)


StoreFactory = Callable[
    ...,
    AbstractAsyncContextManager[SurrealStore],
]


@pytest.fixture
def store_factory(
    root_store_settings: SurrealStoreSettings,
    store_settings: SurrealStoreSettings,
) -> StoreFactory:
    @asynccontextmanager
    async def create(
        *,
        index: SurrealStoreIndexSettings | None = None,
        ttl: SurrealStoreTTLSettings | None = None,
        embed: Embeddings | None = None,
    ) -> AsyncIterator[SurrealStore]:
        overrides = {}
        if index is not None:
            overrides["index"] = index
        if ttl is not None:
            overrides["ttl"] = ttl
        root = root_store_settings.model_copy(update=overrides, deep=True)
        settings = store_settings.model_copy(update=overrides, deep=True)
        tables = [settings.store_table, settings.vector_table]

        with SurrealStore.from_settings(root, embed=embed) as setup_store:
            setup_store.setup()

        await clear_tables(root.db, tables)
        try:
            with SurrealStore.from_settings(settings, embed=embed) as store:
                yield store
        finally:
            await clear_tables(root.db, tables)

    return create


AsyncStoreFactory = Callable[
    ...,
    AbstractAsyncContextManager[AsyncSurrealStore],
]


@pytest.fixture
def async_store_factory(
    root_store_settings: SurrealStoreSettings,
    store_settings: SurrealStoreSettings,
) -> AsyncStoreFactory:
    @asynccontextmanager
    async def create(
        *,
        index: SurrealStoreIndexSettings | None = None,
        ttl: SurrealStoreTTLSettings | None = None,
        embed: Embeddings | None = None,
    ) -> AsyncIterator[AsyncSurrealStore]:
        overrides = {}
        if index is not None:
            overrides["index"] = index
        if ttl is not None:
            overrides["ttl"] = ttl
        root = root_store_settings.model_copy(update=overrides, deep=True)
        settings = store_settings.model_copy(update=overrides, deep=True)
        tables = [settings.store_table, settings.vector_table]

        async with AsyncSurrealStore.from_settings(root, embed=embed) as setup_store:
            await setup_store.setup()

        await clear_tables(root.db, tables)
        try:
            async with AsyncSurrealStore.from_settings(settings, embed=embed) as store:
                yield store
        finally:
            await clear_tables(root.db, tables)

    return create
