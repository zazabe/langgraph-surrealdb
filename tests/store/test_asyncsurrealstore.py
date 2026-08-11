from __future__ import annotations

import asyncio

from langgraph.store.base import GetOp, PutOp

from langgraph_surrealdb.store.aio import AsyncSurrealStore


async def test_crud_search_and_batch_order(store: AsyncSurrealStore) -> None:
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
    store: AsyncSurrealStore,
) -> None:
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
    assert await store.alist_namespaces(prefix=("tenant", "*"), suffix=("public",)) == [
        ("tenant", "docs", "public"),
        ("tenant", "images", "public"),
    ]
    assert await store.alist_namespaces(prefix=("tenant",), max_depth=2) == [
        ("tenant", "docs"),
        ("tenant", "images"),
    ]


async def test_ttl_expiry_and_refresh(store: AsyncSurrealStore) -> None:
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


async def test_vector_search(
    store_vector: AsyncSurrealStore,
) -> None:
    await store_vector.aput(("docs",), "apple", {"text": "red apple"})
    await store_vector.aput(("docs",), "car", {"text": "blue car"})
    results = await store_vector.asearch(("docs",), query="apple")
    assert [result.key for result in results] == ["apple", "car"]
    assert results[0].score is not None
    assert results[1].score is not None
    assert results[0].score > results[1].score

    await store_vector.aput(("docs",), "apple", {"text": "train engine"})
    updated = await store_vector.asearch(("docs",), query="apple")
    assert updated[0].key == "car"

    await store_vector.aput(
        ("docs",), "hidden", {"text": "apple apple apple"}, index=False
    )
    vector_results = await store_vector.asearch(("docs",), query="apple")
    assert all(result.key != "hidden" for result in vector_results)
    ordinary_results = await store_vector.asearch(("docs",))
    assert any(result.key == "hidden" for result in ordinary_results)
