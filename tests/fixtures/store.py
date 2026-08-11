from __future__ import annotations

import math
from collections import Counter
from collections.abc import AsyncGenerator, AsyncIterator

import pytest
from langgraph.store.base import Embeddings

from langgraph_surrealdb import AsyncSurrealStore, SurrealStoreIndexSettings
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


@pytest.fixture(scope="function")
async def store(
    root_store_settings: SurrealStoreSettings,
    store_settings: SurrealStoreSettings,
) -> AsyncIterator[AsyncSurrealStore]:
    async with AsyncSurrealStore.from_settings(root_store_settings) as store:
        await store.setup()

    tables = [store_settings.store_table, f"{store_settings.store_table}_index"]
    await clear_tables(root_store_settings.db, tables)
    async with AsyncSurrealStore.from_settings(store_settings) as instance:
        yield instance
    await clear_tables(root_store_settings.db, tables)


@pytest.fixture(scope="function")
async def store_vector(
    root_store_settings: SurrealStoreSettings,
    store_settings: SurrealStoreSettings,
) -> AsyncIterator[AsyncSurrealStore]:
    index_settings = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["text"],
        distance_type="cosine",
    )
    store_settings.index = index_settings
    root_store_settings.index = index_settings
    emded = CharacterEmbeddings()

    async with AsyncSurrealStore.from_settings(
        root_store_settings, embed=emded
    ) as store:
        await store.setup()

    tables = [store_settings.store_table, f"{store_settings.store_table}_index"]
    await clear_tables(root_store_settings.db, tables)
    async with AsyncSurrealStore.from_settings(store_settings, embed=emded) as instance:
        yield instance
    await clear_tables(root_store_settings.db, tables)
