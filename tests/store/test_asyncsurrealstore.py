from __future__ import annotations

import asyncio

from langgraph.store.base import GetOp, PutOp

from langgraph_surrealdb import SurrealStoreIndexSettings, SurrealStoreTTLSettings
from tests.fixtures.store import AsyncStoreFactory, CharacterEmbeddings


async def test_crud_search_and_batch_order(
    async_store_factory: AsyncStoreFactory,
) -> None:
    async with async_store_factory() as store:
        await store.aput(("users", "one"), "a", {"name": "Alice", "score": 2})
        await store.aput(("users", "two"), "b", {"name": "Bob", "score": 4})

        item = await store.aget(("users", "one"), "a")
        assert item is not None
        assert item.value == {"name": "Alice", "score": 2}
        await store.aput(("users", "one"), "a", {"name": "Alice", "score": 3})
        updated = await store.aget(("users", "one"), "a")
        assert updated is not None
        assert updated.created_at == item.created_at
        assert updated.updated_at >= item.updated_at

        results = await store.asearch(("users",), filter={"score": {"$gte": 3}})
        assert {result.key for result in results} == {"a", "b"}

        batch_results = await store.abatch(
            [
                PutOp(("batch",), "new", {"value": 1}),
                GetOp(("batch",), "new"),
            ]
        )
        assert batch_results == [None, None]
        assert await store.aget(("batch",), "new") is not None

        await store.adelete(("users", "one"), "a")
        assert await store.aget(("users", "one"), "a") is None


async def test_namespace_matching_and_truncation(
    async_store_factory: AsyncStoreFactory,
) -> None:
    async with async_store_factory() as store:
        namespaces = [
            ("tenant", "docs", "public"),
            ("tenant", "docs", "private"),
            ("tenant", "images", "public"),
            ("other", "docs", "public"),
        ]
        for index, namespace in enumerate(namespaces):
            await store.aput(namespace, str(index), {"index": index})

        assert await store.alist_namespaces(prefix=("tenant", "docs")) == [
            ("tenant", "docs", "private"),
            ("tenant", "docs", "public"),
        ]
        assert await store.alist_namespaces(
            prefix=("tenant", "*"), suffix=("public",)
        ) == [
            ("tenant", "docs", "public"),
            ("tenant", "images", "public"),
        ]
        assert await store.alist_namespaces(prefix=("tenant",), max_depth=2) == [
            ("tenant", "docs"),
            ("tenant", "images"),
        ]


async def test_ttl_expiry_and_refresh(async_store_factory: AsyncStoreFactory) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=True)
    async with async_store_factory(ttl=ttl_settings) as store:
        ttl = 0.01
        await store.aput(("ttl",), "item", {"value": 1}, ttl=ttl)
        await asyncio.sleep(0.35)
        assert await store.aget(("ttl",), "item", refresh_ttl=True) is not None
        await asyncio.sleep(0.35)
        assert await store.aget(("ttl",), "item", refresh_ttl=False) is not None
        await asyncio.sleep(0.35)
        assert await store.aget(("ttl",), "item", refresh_ttl=False) is not None
        assert await store.sweep_ttl() == 1
        assert await store.aget(("ttl",), "item", refresh_ttl=False) is None


async def test_ttl_sweeper_deletes_expired_items(
    async_store_factory: AsyncStoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=False)
    async with async_store_factory(ttl=ttl_settings) as store:
        await store.aput(("ttl-sweeper",), "item", {"value": 1}, ttl=1 / 60)
        assert (
            await store.aget(("ttl-sweeper",), "item", refresh_ttl=False)
            is not None
        )

        sweeper = await store.start_ttl_sweeper(sweep_interval_minutes=0.001)
        try:
            async with asyncio.timeout(2):
                while (
                    await store.aget(
                        ("ttl-sweeper",), "item", refresh_ttl=False
                    )
                    is not None
                ):
                    await asyncio.sleep(0.02)
        finally:
            assert await store.stop_ttl_sweeper(timeout=1)

        assert sweeper.done()
        assert await store.aget(("ttl-sweeper",), "item", refresh_ttl=False) is None


async def test_vector_search(
    async_store_factory: AsyncStoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["text"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)

    async with async_store_factory(index=index, embed=embed) as store:
        await store.aput(("docs",), "apple", {"text": "red apple"})
        await store.aput(("docs",), "car", {"text": "blue car"})
        results = await store.asearch(("docs",), query="apple")
        assert [result.key for result in results] == ["apple", "car"]
        assert results[0].score is not None
        assert results[1].score is not None
        assert results[0].score > results[1].score

        await store.aput(("docs",), "apple", {"text": "train engine"})
        updated = await store.asearch(("docs",), query="apple")
        assert updated[0].key == "car"

        await store.aput(
            ("docs",), "hidden", {"text": "apple apple apple"}, index=False
        )
        vector_results = await store.asearch(("docs",), query="apple")
        assert all(result.key != "hidden" for result in vector_results)
        ordinary_results = await store.asearch(("docs",))
        assert any(result.key == "hidden" for result in ordinary_results)
