import time
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
from tests.fixtures.store import CharacterEmbeddings, StoreFactory


async def test_crud_search_and_batch_order(store_factory: StoreFactory) -> None:
    async with store_factory() as store:
        store.put(("users", "one"), "a", {"name": "Alice", "score": 2})
        store.put(("users", "two"), "b", {"name": "Bob", "score": 4})

        item = store.get(("users", "one"), "a")
        assert item is not None
        assert item.value == {"name": "Alice", "score": 2}
        store.put(("users", "one"), "a", {"name": "Alice", "score": 3})
        updated = store.get(("users", "one"), "a")
        assert updated is not None
        assert updated.created_at == item.created_at
        assert updated.updated_at >= item.updated_at

        results = store.search(("users",), filter={"score": {"$gte": 3}})
        assert {result.key for result in results} == {"a", "b"}

        batch_results = store.batch(
            [
                PutOp(("batch",), "new", {"value": 1}),
                GetOp(("batch",), "new"),
            ]
        )
        assert batch_results == [None, None]
        assert store.get(("batch",), "new") is not None

        store.delete(("users", "one"), "a")
        assert store.get(("users", "one"), "a") is None


async def test_mixed_batch_preserves_result_order(store_factory: StoreFactory) -> None:
    async with store_factory() as store:
        store.put(("batch-order", "one"), "a", {"group": "match"})
        results = store.batch(
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
        assert store.get(("batch-order", "two"), "b") is not None


async def test_batch_put_last_write_wins(store_factory: StoreFactory) -> None:
    async with store_factory() as store:
        results = store.batch(
            [
                PutOp(("dedupe",), "item", {"version": 1}),
                PutOp(("dedupe",), "item", {"version": 2}),
                PutOp(("dedupe",), "item", {"version": 3}),
            ]
        )

        assert results == [None, None, None]
        item = store.get(("dedupe",), "item")
        assert item is not None
        assert item.value == {"version": 3}


async def test_namespace_matching_and_truncation(
    store_factory: StoreFactory,
) -> None:
    async with store_factory() as store:
        namespaces = [
            ("tenant", "docs", "public"),
            ("tenant", "docs", "private"),
            ("tenant", "images", "public"),
            ("other", "docs", "public"),
        ]
        for index, namespace in enumerate(namespaces):
            store.put(namespace, str(index), {"index": index})

        assert store.list_namespaces(prefix=("tenant", "docs")) == [
            ("tenant", "docs", "private"),
            ("tenant", "docs", "public"),
        ]
        assert store.list_namespaces(prefix=("tenant", "*"), suffix=("public",)) == [
            ("tenant", "docs", "public"),
            ("tenant", "images", "public"),
        ]
        assert store.list_namespaces(prefix=("tenant",), max_depth=2) == [
            ("tenant", "docs"),
            ("tenant", "images"),
        ]

        first_page = store.list_namespaces(prefix=("tenant",), limit=2)
        second_page = store.list_namespaces(prefix=("tenant",), limit=2, offset=2)
        assert len(first_page) == 2
        assert set(first_page).isdisjoint(second_page)


async def test_search_filters_and_pagination(store_factory: StoreFactory) -> None:
    async with store_factory() as store:
        for score in range(1, 6):
            store.put(
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
            results = store.search(
                ("filters",), filter={"score": {operator: operands[operator]}}
            )
            assert {item.key for item in results} == expected

        combined = store.search(
            ("filters",), filter={"score": {"$gte": 2}, "kind": "even"}
        )
        assert {item.key for item in combined} == {"2", "4"}

        first_page = store.search(("filters",), limit=2)
        second_page = store.search(("filters",), limit=2, offset=2)
        assert len(first_page) == len(second_page) == 2
        assert {item.key for item in first_page}.isdisjoint(
            item.key for item in second_page
        )


async def test_ttl_expiry_and_refresh(store_factory: StoreFactory) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=True)
    async with store_factory(ttl=ttl_settings) as store:
        ttl = 0.01
        store.put(("ttl",), "item", {"value": 1}, ttl=ttl)
        time.sleep(0.35)
        assert store.get(("ttl",), "item", refresh_ttl=True) is not None
        time.sleep(0.35)
        assert store.get(("ttl",), "item", refresh_ttl=False) is not None
        time.sleep(0.35)
        assert store.get(("ttl",), "item", refresh_ttl=False) is not None
        assert store.sweep_ttl() == 1
        assert store.get(("ttl",), "item", refresh_ttl=False) is None


async def test_get_can_enable_ttl_refresh(
    store_factory: StoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=False)
    async with store_factory(ttl=ttl_settings) as store:
        store.put(("ttl-get-override",), "item", {"value": 1}, ttl=0.01)
        time.sleep(0.35)

        assert store.get(("ttl-get-override",), "item", refresh_ttl=True) is not None
        time.sleep(0.35)

        assert store.sweep_ttl() == 0


@pytest.mark.parametrize("indexed", [False, True])
async def test_search_ttl_refresh(
    store_factory: StoreFactory,
    indexed: bool,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=True)
    index = SurrealStoreIndexSettings(
        enabled=indexed,
        dimensions=32,
        fields=["text"],
    )
    embed = CharacterEmbeddings(dims=32) if indexed else None
    async with store_factory(ttl=ttl_settings, index=index, embed=embed) as store:
        store.put(("ttl-search-default",), "item", {"text": "apple"}, ttl=0.01)
        store.put(("ttl-search-disabled",), "item", {"text": "apple"}, ttl=0.01)
        time.sleep(0.35)

        query = "apple" if indexed else None
        assert store.search(("ttl-search-default",), query=query)
        assert store.search(
            ("ttl-search-disabled",), query=query, refresh_ttl=False
        )
        time.sleep(0.35)

        assert store.sweep_ttl() == 1
        assert store.get(
            ("ttl-search-default",), "item", refresh_ttl=False
        ) is not None
        assert (
            store.get(("ttl-search-disabled",), "item", refresh_ttl=False) is None
        )


async def test_ttl_sweeper_deletes_expired_items(
    store_factory: StoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(enabled=True, refresh_on_read=False)
    async with store_factory(ttl=ttl_settings) as store:
        store.put(("ttl-sweeper",), "item", {"value": 1}, ttl=1 / 60)
        assert store.get(("ttl-sweeper",), "item", refresh_ttl=False) is not None

        sweeper = store.start_ttl_sweeper(sweep_interval_minutes=0.001)
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if store.get(("ttl-sweeper",), "item", refresh_ttl=False) is None:
                    break
                time.sleep(0.02)

            assert store.get(("ttl-sweeper",), "item", refresh_ttl=False) is None
        finally:
            assert store.stop_ttl_sweeper(timeout=1)
            sweeper.result(timeout=1)


async def test_ttl_defaults_overrides_and_removal(
    store_factory: StoreFactory,
) -> None:
    ttl_settings = SurrealStoreTTLSettings(
        enabled=True,
        refresh_on_read=False,
        default_ttl=0.001,
    )
    async with store_factory(ttl=ttl_settings) as store:
        store.put(("ttl-config",), "default", {"value": 1})
        store.put(("ttl-config",), "override", {"value": 2}, ttl=0.003)
        store.put(("ttl-config",), "never", {"value": 3}, ttl=None)

        time.sleep(0.1)
        assert store.sweep_ttl() == 1
        assert store.get(("ttl-config",), "default", refresh_ttl=False) is None
        assert store.get(("ttl-config",), "override", refresh_ttl=False) is not None
        assert store.get(("ttl-config",), "never", refresh_ttl=False) is not None

        store.put(("ttl-config",), "override", {"value": 4}, ttl=None)
        time.sleep(0.15)
        assert store.sweep_ttl() == 0
        assert store.get(("ttl-config",), "override", refresh_ttl=False) is not None


async def test_vector_search(
    store_factory: StoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["text"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)

    async with store_factory(index=index, embed=embed) as store:
        store.put(("docs",), "apple", {"text": "red apple"})
        store.put(("docs",), "car", {"text": "blue car"})
        results = store.search(("docs",), query="apple")
        assert [result.key for result in results] == ["apple", "car"]
        assert results[0].score is not None
        assert results[1].score is not None
        assert results[0].score > results[1].score

        store.put(("docs",), "apple", {"text": "train engine"})
        updated = store.search(("docs",), query="apple")
        assert updated[0].key == "car"

        store.put(("docs",), "hidden", {"text": "apple apple apple"}, index=False)
        vector_results = store.search(("docs",), query="apple")
        assert all(result.key != "hidden" for result in vector_results)
        ordinary_results = store.search(("docs",))
        assert any(result.key == "hidden" for result in ordinary_results)


async def test_vector_field_override_and_stale_vector_cleanup(
    store_factory: StoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["title"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)

    async with store_factory(index=index, embed=embed) as store:
        store.put(
            ("vector-fields",),
            "item",
            {"title": "haystack", "body": "unique needle"},
            index=["body"],
        )
        results = store.search(("vector-fields",), query="unique needle")
        assert results[0].key == "item"

        store.put(
            ("vector-fields",),
            "item",
            {"title": "haystack", "body": "unique needle"},
            index=False,
        )
        assert store.search(("vector-fields",), query="unique needle") == []

        store.put(
            ("vector-fields",),
            "item",
            {"title": "unique needle", "body": "haystack"},
        )
        assert store.search(("vector-fields",), query="unique needle")
        store.delete(("vector-fields",), "item")
        assert store.search(("vector-fields",), query="unique needle") == []


async def test_multi_field_indexing_and_partial_field_removal(
    store_factory: StoreFactory,
) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=32,
        fields=["title", "body"],
        distance_type="cosine",
    )
    embed = CharacterEmbeddings(dims=32)
    value = {"title": "aaaaaaaa", "body": "zzzzzzzz"}

    async with store_factory(index=index, embed=embed) as store:
        store.put(("multi-field",), "item", value)

        title_results = store.search(("multi-field",), query=value["title"])
        body_results = store.search(("multi-field",), query=value["body"])
        assert title_results[0].key == body_results[0].key == "item"
        assert title_results[0].score is not None
        assert body_results[0].score is not None
        body_score_with_both_fields = body_results[0].score

        store.put(("multi-field",), "item", value, index=["title"])

        title_results = store.search(("multi-field",), query=value["title"])
        body_results = store.search(("multi-field",), query=value["body"])
        assert title_results[0].key == "item"
        assert body_results[0].score is not None
        assert body_results[0].score < body_score_with_both_fields


async def test_embedding_dimension_mismatch(store_factory: StoreFactory) -> None:
    index = SurrealStoreIndexSettings(
        enabled=True,
        dimensions=8,
        fields=["text"],
        distance_type="cosine",
    )
    async with store_factory(index=index, embed=CharacterEmbeddings(dims=4)) as store:
        with pytest.raises(ValueError, match="Embedding dimension mismatch"):
            store.put(("dimensions",), "item", {"text": "wrong dimensions"})
