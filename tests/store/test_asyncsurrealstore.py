from __future__ import annotations

import asyncio
from typing import cast

import pytest
from langgraph.store.base import (
    GetOp,
    Item,
    ListNamespacesOp,
    PutOp,
    SearchItem,
    SearchOp,
)

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


async def test_mixed_batch_preserves_result_order(
    async_store_factory: AsyncStoreFactory,
) -> None:
    async with async_store_factory() as store:
        await store.aput(("batch-order", "one"), "a", {"group": "match"})
        results = await store.abatch(
            [
                GetOp(("batch-order", "one"), "a"),
                PutOp(("batch-order", "two"), "b", {"group": "match"}),
                SearchOp(("batch-order",), {"group": "match"}, 10, 0),
                ListNamespacesOp(None, None, 10, 0),
                GetOp(("batch-order",), "missing"),
            ]
        )

        first_item = cast(Item, results[0])
        assert first_item.key == "a"
        assert results[1] is None
        assert isinstance(results[2], list)
        search_results = cast(list[SearchItem], results[2])
        assert [item.key for item in search_results] == ["a"]
        assert isinstance(results[3], list)
        namespaces = cast(list[tuple[str, ...]], results[3])
        assert ("batch-order", "one") in namespaces
        assert results[4] is None
        assert await store.aget(("batch-order", "two"), "b") is not None


async def test_batch_put_last_write_wins(
    async_store_factory: AsyncStoreFactory,
) -> None:
    async with async_store_factory() as store:
        results = await store.abatch(
            [
                PutOp(("dedupe",), "item", {"version": 1}),
                PutOp(("dedupe",), "item", {"version": 2}),
                PutOp(("dedupe",), "item", {"version": 3}),
            ]
        )

        assert results == [None, None, None]
        item = await store.aget(("dedupe",), "item")
        assert item is not None
        assert item.value == {"version": 3}


async def test_sync_methods_rejected_in_store_event_loop(
    async_store_factory: AsyncStoreFactory,
) -> None:
    async with async_store_factory() as store:
        with pytest.raises(asyncio.InvalidStateError):
            store.put(("sync-guard",), "item", {"value": 1})
        with pytest.raises(asyncio.InvalidStateError):
            store.get(("sync-guard",), "item")
        with pytest.raises(asyncio.InvalidStateError):
            store.search(("sync-guard",))
        with pytest.raises(asyncio.InvalidStateError):
            store.list_namespaces(prefix=("sync-guard",))
        with pytest.raises(asyncio.InvalidStateError):
            store.delete(("sync-guard",), "item")
        with pytest.raises(asyncio.InvalidStateError):
            store.batch([GetOp(("sync-guard",), "item")])


async def test_concurrent_operations(async_store_factory: AsyncStoreFactory) -> None:
    async with async_store_factory() as store:
        await asyncio.gather(
            *(
                store.aput(("concurrent",), str(index), {"index": index})
                for index in range(25)
            )
        )
        items = await asyncio.gather(
            *(store.aget(("concurrent",), str(index)) for index in range(25))
        )
        assert [item.value["index"] for item in items if item is not None] == list(
            range(25)
        )

        searches = await asyncio.gather(
            *(store.asearch(("concurrent",), limit=25) for _ in range(5))
        )
        assert all(len(results) == 25 for results in searches)

        await asyncio.gather(
            *(store.adelete(("concurrent",), str(index)) for index in range(25))
        )
        assert await store.asearch(("concurrent",), limit=25) == []


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

        first_page = await store.alist_namespaces(prefix=("tenant",), limit=2)
        second_page = await store.alist_namespaces(
            prefix=("tenant",), limit=2, offset=2
        )
        assert len(first_page) == 2
        assert set(first_page).isdisjoint(second_page)


async def test_search_filters_and_pagination(
    async_store_factory: AsyncStoreFactory,
) -> None:
    async with async_store_factory() as store:
        for score in range(1, 6):
            await store.aput(
                ("filters",),
                str(score),
                {"score": score, "kind": "odd" if score % 2 else "even"},
            )

        expectations = {
            "$eq": {"3"},
            "$ne": {"1", "2", "4", "5"},
            "$gt": {"4", "5"},
            "$gte": {"3", "4", "5"},
            "$lt": {"1", "2"},
            "$lte": {"1", "2", "3"},
        }
        operands = {"$eq": 3, "$ne": 3, "$gt": 3, "$gte": 3, "$lt": 3, "$lte": 3}
        for operator, expected in expectations.items():
            results = await store.asearch(
                ("filters",), filter={"score": {operator: operands[operator]}}
            )
            assert {item.key for item in results} == expected

        combined = await store.asearch(
            ("filters",), filter={"score": {"$gte": 2}, "kind": "even"}
        )
        assert {item.key for item in combined} == {"2", "4"}

        first_page = await store.asearch(("filters",), limit=2)
        second_page = await store.asearch(("filters",), limit=2, offset=2)
        assert len(first_page) == len(second_page) == 2
        assert {item.key for item in first_page}.isdisjoint(
            item.key for item in second_page
        )


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


async def test_get_can_enable_ttl_refresh(
    async_store_factory: AsyncStoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=False)
    async with async_store_factory(ttl=ttl_settings) as store:
        await store.aput(("ttl-get-override",), "item", {"value": 1}, ttl=0.01)
        await asyncio.sleep(0.35)

        assert (
            await store.aget(("ttl-get-override",), "item", refresh_ttl=True)
            is not None
        )
        await asyncio.sleep(0.35)

        assert await store.sweep_ttl() == 0


@pytest.mark.parametrize("indexed", [False, True])
async def test_search_ttl_refresh(
    async_store_factory: AsyncStoreFactory,
    indexed: bool,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=True)
    index = SurrealStoreIndexSettings(
        enabled=indexed,
        dimensions=32,
        fields=["text"],
    )
    embed = CharacterEmbeddings(dims=32) if indexed else None
    async with async_store_factory(ttl=ttl_settings, index=index, embed=embed) as store:
        await store.aput(("ttl-search-default",), "item", {"text": "apple"}, ttl=0.01)
        await store.aput(("ttl-search-disabled",), "item", {"text": "apple"}, ttl=0.01)
        await asyncio.sleep(0.35)

        query = "apple" if indexed else None
        assert await store.asearch(("ttl-search-default",), query=query)
        assert await store.asearch(
            ("ttl-search-disabled",), query=query, refresh_ttl=False
        )
        await asyncio.sleep(0.35)

        assert await store.sweep_ttl() == 1
        assert (
            await store.aget(("ttl-search-default",), "item", refresh_ttl=False)
            is not None
        )
        assert (
            await store.aget(("ttl-search-disabled",), "item", refresh_ttl=False)
            is None
        )


async def test_ttl_sweeper_deletes_expired_items(
    async_store_factory: AsyncStoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=False)
    async with async_store_factory(ttl=ttl_settings) as store:
        await store.aput(("ttl-sweeper",), "item", {"value": 1}, ttl=1 / 60)
        assert await store.aget(("ttl-sweeper",), "item", refresh_ttl=False) is not None

        sweeper = await store.start_ttl_sweeper(sweep_interval_minutes=0.001)
        try:
            async with asyncio.timeout(2):
                while (
                    await store.aget(("ttl-sweeper",), "item", refresh_ttl=False)
                    is not None
                ):
                    await asyncio.sleep(0.02)
        finally:
            assert await store.stop_ttl_sweeper(timeout=1)

        assert sweeper.done()
        assert await store.aget(("ttl-sweeper",), "item", refresh_ttl=False) is None


async def test_ttl_defaults_overrides_and_removal(
    async_store_factory: AsyncStoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(
        enabled=True,
        refresh_on_read=False,
        default_ttl=0.001,
    )
    async with async_store_factory(ttl=ttl_settings) as store:
        await store.aput(("ttl-config",), "default", {"value": 1})
        await store.aput(("ttl-config",), "override", {"value": 2}, ttl=0.003)
        await store.aput(("ttl-config",), "never", {"value": 3}, ttl=None)

        await asyncio.sleep(0.1)
        assert await store.sweep_ttl() == 1
        assert await store.aget(("ttl-config",), "default", refresh_ttl=False) is None
        assert (
            await store.aget(("ttl-config",), "override", refresh_ttl=False) is not None
        )
        assert await store.aget(("ttl-config",), "never", refresh_ttl=False) is not None

        await store.aput(("ttl-config",), "override", {"value": 4}, ttl=None)
        await asyncio.sleep(0.15)
        assert await store.sweep_ttl() == 0
        assert (
            await store.aget(("ttl-config",), "override", refresh_ttl=False) is not None
        )


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


async def test_vector_field_override_and_stale_vector_cleanup(
    async_store_factory: AsyncStoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["title"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)

    async with async_store_factory(index=index, embed=embed) as store:
        await store.aput(
            ("vector-fields",),
            "item",
            {"title": "haystack", "body": "unique needle"},
            index=["body"],
        )
        results = await store.asearch(("vector-fields",), query="unique needle")
        assert results[0].key == "item"

        await store.aput(
            ("vector-fields",),
            "item",
            {"title": "haystack", "body": "unique needle"},
            index=False,
        )
        assert await store.asearch(("vector-fields",), query="unique needle") == []

        await store.aput(
            ("vector-fields",),
            "item",
            {"title": "unique needle", "body": "haystack"},
        )
        assert await store.asearch(("vector-fields",), query="unique needle")
        await store.adelete(("vector-fields",), "item")
        assert await store.asearch(("vector-fields",), query="unique needle") == []


async def test_multi_field_indexing_and_partial_field_removal(
    async_store_factory: AsyncStoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["title", "body"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)
    value = {"title": "aaaaaaaa", "body": "zzzzzzzz"}

    async with async_store_factory(index=index, embed=embed) as store:
        await store.aput(("multi-field",), "item", value)

        title_results = await store.asearch(("multi-field",), query=value["title"])
        body_results = await store.asearch(("multi-field",), query=value["body"])
        assert title_results[0].key == body_results[0].key == "item"
        assert title_results[0].score is not None
        assert body_results[0].score is not None
        body_score_with_both_fields = body_results[0].score

        await store.aput(("multi-field",), "item", value, index=["title"])

        title_results = await store.asearch(("multi-field",), query=value["title"])
        body_results = await store.asearch(("multi-field",), query=value["body"])
        assert title_results[0].key == "item"
        assert body_results[0].score is not None
        assert body_results[0].score < body_score_with_both_fields


async def test_embedding_dimension_mismatch(
    async_store_factory: AsyncStoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=8,
        fields=["text"],
        distance_type="cosine",
    )
    async with async_store_factory(
        index=index, embed=CharacterEmbeddings(dims=4)
    ) as store:
        with pytest.raises(ValueError, match="Embedding dimension mismatch"):
            await store.aput(("dimensions",), "item", {"text": "wrong dimensions"})
